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
use rustc_hash::FxHashMap;
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};
use sui_types::base_types::{ObjectID, SequenceNumber};
use tokio::sync::mpsc::{Receiver, Sender};

pub(crate) struct VersionAssignmentTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    // Mapping of object ID to its current version for shared objects
    pub(crate) shared_object_versions: FxHashMap<ObjectID, SequenceNumber>,
    // PhantomData to indicate we're using the generic parameter
    pub(crate) _phantom: PhantomData<E>,
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
                let required_versions = self.assign_shared_object_versions(&transaction);
                tracing::debug!(
                    "Version assignment task received transaction {:?}",
                    transaction.digest()
                );

                sender.send((transaction, required_versions)).await.unwrap();
            }
        }
    }

    /// Assign versions to shared objects in a transaction
    ///
    /// 1. Get the shared object IDs from the transaction
    /// 2. Find the maximum version among all objects
    /// 3. Assign the next version (max + 1) to all objects
    /// 4. Return the list of (ObjectID, SequenceNumber) pairs
    fn assign_shared_object_versions(
        &mut self,
        transaction: &RemoraTransaction<E>,
    ) -> Vec<(ObjectID, SequenceNumber)> {
        // Get all shared object IDs from the transaction
        let shared_object_ids = transaction.shared_object_ids();

        if shared_object_ids.is_empty() {
            return Vec::new();
        }

        // Find the maximum version for all objects in the transaction
        let mut max_version = SequenceNumber::from(2);
        let initial_version = SequenceNumber::from(2);
        let mut result = Vec::with_capacity(shared_object_ids.len());

        // First collect current versions for result and find max
        for obj_id in shared_object_ids {
            let current_version = self
                .shared_object_versions
                .get(obj_id)
                .copied()
                .unwrap_or(initial_version);

            // Add current version to result
            result.push((*obj_id, current_version));

            // Update max version if needed
            if current_version > max_version {
                max_version = current_version;
            }
        }

        // Calculate the new version (max + 1)
        let new_version = max_version.next();

        // Update all objects to the new version
        for obj_id in shared_object_ids {
            self.shared_object_versions.insert(*obj_id, new_version);
        }

        result
    }
}

/// Processor for transactions that involve shared objects.
/// Used only for load balancing policy selection.
pub(crate) struct SharedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) policy: LoadBalancingPolicy,
    pub(crate) txn_cnt: usize,
    pub(crate) states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
    pub(crate) dependency_controller: Arc<VersionedDependencyController>,
}

impl<E> SharedObjTxnForwarder<E>
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
        let (prior_handles, current_handles) = match required_versions.is_empty() {
            true => (Vec::new(), Vec::new()),
            false => self.dependency_controller.get_prior_dependency_and_update(
                0,
                required_versions.clone(),
                false,
                false,
            ),
        };

        // Clone all needed fields to move into the spawned task
        let dependency_controller = self.dependency_controller.clone();
        let states_to_proxy = self.states_to_proxy.clone();
        let policy = self.policy.clone();
        let proxy_connections = self.proxy_connections.clone();
        let txn_cnt = self.txn_cnt;
        self.txn_cnt = self.txn_cnt + 1;

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
                txn_cnt,
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
    }

    /// Get assigned proxy for shared objects in a transaction.
    /// This is the main entry point for load balancing policy selection.
    fn get_proxy_for_shared_objects(
        policy: &LoadBalancingPolicy,
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        txn_cnt: usize,
        required_versions: &[(ObjectID, SequenceNumber)],
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        match policy {
            LoadBalancingPolicy::RoundRobin => {
                Self::get_proxy_for_shared_objects_round_robin(proxy_connections, txn_cnt)
            }
            LoadBalancingPolicy::Zeus => Self::get_proxy_for_shared_objects_most_states(
                proxy_connections,
                states_to_proxy,
                required_versions,
            ),
            LoadBalancingPolicy::Dedicated => {
                // Dedicated: proxy 0 for stateless, proxy 1 for stateful
                Some((1, 0))
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
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        let proxy_index = txn_cnt % proxy_count;

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
