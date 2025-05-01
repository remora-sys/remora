use crate::{
    config::LoadBalancingPolicy,
    executor::{
        api::{
            ExecutableTransaction, Executor, ExecutorIndex, PrimaryToProxyMessage,
            RemoraTransaction, RequiredStates,
        },
        versioned_dependency_controller::VersionedDependencyController,
    },
    proxy::core::ProxyId,
};
use dashmap::DashMap;
use std::{collections::BTreeMap, sync::Arc};
use sui_types::base_types::{ObjectID, SequenceNumber};
use tokio::sync::mpsc::{Receiver, Sender};

/// Processor for transactions that involve shared objects.
/// Used only for load balancing policy selection.
pub(crate) struct SharedTxnProcessor<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) policy: LoadBalancingPolicy,
    pub(crate) index: usize,
    pub(crate) states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
    pub(crate) dependency_controller: Arc<VersionedDependencyController>,
}

pub(crate) struct VersionAssignmentTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) executor: Arc<E>,
}

impl<E> VersionAssignmentTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) async fn process_version_assignments(
        &mut self,
        mut shared_txn_receiver: Receiver<Vec<RemoraTransaction<E>>>,
        sender: Sender<(RemoraTransaction<E>, Vec<(ObjectID, SequenceNumber)>)>,
    ) {
        while let Some(shared_txns) = shared_txn_receiver.recv().await {
            for transaction in shared_txns {
                let required_versions = self
                    .executor
                    .assign_shared_object_versions_and_return_required_versions(&transaction)
                    .await
                    .unwrap();

                sender.send((transaction, required_versions)).await.unwrap();
            }
        }
    }
}

impl<E> SharedTxnProcessor<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) async fn process_shared_txns(
        &mut self,
        mut shared_txn_receiver: Receiver<(RemoraTransaction<E>, Vec<(ObjectID, SequenceNumber)>)>,
    ) {
        while let Some((transaction, required_versions)) = shared_txn_receiver.recv().await {
            self.forward_shared_object_txn(transaction, required_versions)
                .await;
        }
    }

    /// Forwards transactions with shared objects to the appropriate proxy.
    pub(crate) async fn forward_shared_object_txn(
        &mut self,
        transaction: RemoraTransaction<E>,
        required_versions: Vec<(ObjectID, SequenceNumber)>,
    ) {
        let (prior_handles, current_handles) = self
            .dependency_controller
            .get_prior_dependency_and_update(0, required_versions.clone(), false, false);

        // Clone all needed fields to move into the spawned task
        let dependency_controller = self.dependency_controller.clone();
        let states_to_proxy = self.states_to_proxy.clone();
        let policy = self.policy.clone();
        let proxy_connections = self.proxy_connections.clone();
        let mut index = self.index;

        tokio::spawn(async move {
            // Wait for prior dependencies to complete
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }

            // Remove the dependency when done
            dependency_controller.remove_dependency(required_versions.clone());

            if let Some((proxy_index, stateless_proxy_id)) = Self::get_proxy_for_shared_objects(
                &policy,
                &proxy_connections,
                &states_to_proxy,
                &mut index,
                &required_versions,
            ) {
                // Stateless transaction doesn't need missing states
                let stateless_msg =
                    PrimaryToProxyMessage::StatelessTxn(Arc::new(transaction.clone()));
                Self::send_to_proxy(&proxy_connections, stateless_proxy_id, stateless_msg).await;

                // Stateful transaction needs missing states
                let stateful_missing_states = Self::get_missing_states_for_transaction(
                    &transaction,
                    Some(required_versions),
                    proxy_index,
                    states_to_proxy,
                )
                .await;

                let stateful_msg = PrimaryToProxyMessage::Txn(
                    Arc::new(transaction.clone()),
                    stateless_proxy_id,
                    stateful_missing_states,
                );

                Self::send_to_proxy(&proxy_connections, proxy_index, stateful_msg).await;
            } else {
                tracing::warn!("No proxies available for transaction with shared objects");
            }

            // Notify any dependencies waiting on this transaction
            for notify in current_handles {
                notify.notify_one();
            }
        });

        // Update the index in the original struct
        self.index = index;
    }

    /// Get assigned proxy for shared objects in a transaction.
    /// This is the main entry point for load balancing policy selection.
    fn get_proxy_for_shared_objects(
        policy: &LoadBalancingPolicy,
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        index: &mut usize,
        required_versions: &[(ObjectID, SequenceNumber)],
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        match policy {
            LoadBalancingPolicy::RoundRobin => {
                Self::get_proxy_for_shared_objects_round_robin(proxy_connections, index)
            }
            LoadBalancingPolicy::Zeus => Self::get_proxy_for_shared_objects_most_states(
                proxy_connections,
                states_to_proxy,
                required_versions,
            ),
            LoadBalancingPolicy::Dedicated => {
                // Dedicated: proxy 0 for stateless, proxy 1 for stateful
                Some((0, 1))
            }
            LoadBalancingPolicy::Combined => {
                unimplemented!()
            }
        }
    }

    /// Get assigned proxy for shared objects using round-robin.
    fn get_proxy_for_shared_objects_round_robin(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        index: &mut usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        let proxy_index = *index % proxy_count;
        *index = (*index + 1) % proxy_count;

        Some((proxy_index, proxy_index))
    }

    /// Get assigned proxy based on which proxy hosts the most states needed by this transaction.
    fn get_proxy_for_shared_objects_most_states(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: &[(ObjectID, SequenceNumber)],
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        if required_versions.is_empty() {
            // If no shared objects, use first proxy
            return Some((0, 0));
        }

        // Count how many objects each proxy already has
        let mut proxy_state_counts = vec![0; proxy_count];
        for (id, v) in required_versions {
            if let Some(proxy_index) = states_to_proxy.get(&(*id, *v)) {
                if *proxy_index < proxy_count {
                    proxy_state_counts[*proxy_index] += 1;
                }
            }
        }

        // Find the proxy with the most states
        let mut max_count = 0;
        let mut best_proxy = 0;

        for (index, count) in proxy_state_counts.iter().enumerate() {
            if *count > max_count {
                max_count = *count;
                best_proxy = index;
            }
        }

        Some((best_proxy, best_proxy))
    }

    /// Helper method to determine missing states for a transaction
    /// and update the states ownership map
    async fn get_missing_states_for_transaction(
        transaction: &RemoraTransaction<E>,
        required_versions: Option<Vec<(ObjectID, SequenceNumber)>>,
        proxy_index: ExecutorIndex,
        states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
    ) -> RequiredStates {
        let mut required_states = BTreeMap::new();

        tracing::debug!(
            "Transaction {:?} required versions: {:?}",
            transaction.digest(),
            required_versions
        );

        if let Some(required_versions) = required_versions {
            for (object_id, seq_num) in required_versions {
                let previous_owner = states_to_proxy.get(&(object_id, seq_num));

                // Insert into required_states map - with previous owner if object needs migration,
                // with None if it's already at the correct proxy or hasn't been assigned yet
                let previous_owner_value = if let Some(owner) = previous_owner {
                    if *owner != proxy_index {
                        Some(*owner)
                    } else {
                        None
                    }
                } else {
                    None
                };

                required_states.insert((object_id, seq_num), previous_owner_value);

                // Always update the mapping to point to this proxy
                states_to_proxy.insert((object_id, seq_num), proxy_index);
            }
        }

        required_states
    }

    /// Simplified method to send a message to a proxy
    async fn send_to_proxy(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        dest_proxy: ExecutorIndex,
        message: PrimaryToProxyMessage<<E as Executor>::Transaction>,
    ) {
        if let Some(proxy_connection) = proxy_connections.get(&dest_proxy) {
            let proxy_connection = proxy_connection.clone();
            tokio::spawn(async move {
                if proxy_connection.send(message).await.is_ok() {
                    tracing::debug!("Sent transaction to proxy {}", dest_proxy);
                } else {
                    tracing::warn!(
                        "Failed to send transaction to proxy {}, removing connection",
                        dest_proxy
                    );
                }
            });
        } else {
            tracing::warn!("Proxy connection {} not found", dest_proxy);
        }
    }
}
