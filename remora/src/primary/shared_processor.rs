use crate::executor::api::{Executor, ExecutorIndex, PrimaryToProxyMessage, RemoraTransaction, RequiredStates, ExecutableTransaction};
use rustc_hash::FxHashMap;
use std::{
    collections::BTreeMap,
    sync::Arc,
};
use sui_types::base_types::{ObjectID, SequenceNumber};
use tokio::sync::mpsc::{Receiver, Sender};
use dashmap::DashMap;
use crate::config::LoadBalancingPolicy;
use crate::proxy::core::ProxyId;

/// Processor for transactions that involve shared objects.
/// Used only for load balancing policy selection.
pub(crate) struct SharedTxnProcessor<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) executor: Arc<E>,
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) policy: LoadBalancingPolicy,
    pub(crate) index: usize,
    pub(crate) states_to_proxy: FxHashMap<ObjectID, ExecutorIndex>,
}

impl<E> SharedTxnProcessor<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) async fn process_shared_txns(
        &mut self,
        mut shared_txn_receiver: Receiver<Vec<RemoraTransaction<E>>>,
    ) {
        while let Some(shared_txns) = shared_txn_receiver.recv().await {
            for transaction in shared_txns {
                let required_versions = self
                    .executor
                    .assign_shared_object_versions_and_return_required_versions(&transaction)
                    .await
                    .unwrap();
                self.forward_shared_object_txn(transaction, required_versions)
                    .await;
            }
        }
    }

    /// Forwards transactions with shared objects to the appropriate proxy.
    pub(crate) async fn forward_shared_object_txn(
        &mut self,
        transaction: RemoraTransaction<E>,
        required_versions: Vec<(ObjectID, SequenceNumber)>,
    ) {
        if let Some((proxy_index, stateless_proxy_id)) =
            self.get_proxy_for_shared_objects(&required_versions)
        {
            // Stateless transaction doesn't need missing states
            let stateless_msg = PrimaryToProxyMessage::StatelessTxn(Arc::new(transaction.clone()));
            self.send_to_proxy(stateless_proxy_id, stateless_msg).await;

            // Stateful transaction needs missing states
            let stateful_missing_states = self
                .get_missing_states_for_transaction(
                    &transaction,
                    Some(required_versions),
                    proxy_index,
                )
                .await;
            let stateful_msg = PrimaryToProxyMessage::Txn(
                Arc::new(transaction.clone()),
                stateless_proxy_id,
                stateful_missing_states,
            );
            self.send_to_proxy(proxy_index, stateful_msg).await;
        } else {
            tracing::warn!("No proxies available for transaction with shared objects");
        }
    }

    /// Get assigned proxy for shared objects in a transaction.
    /// This is the main entry point for load balancing policy selection.
    fn get_proxy_for_shared_objects(
        &mut self,
        required_versions: &[(ObjectID, SequenceNumber)],
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        match &self.policy {
            LoadBalancingPolicy::RoundRobin => self.get_proxy_for_shared_objects_round_robin(),
            LoadBalancingPolicy::Zeus => {
                self.get_proxy_for_shared_objects_most_states(required_versions)
            }
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
        &mut self,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = self.proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        let proxy_index = self.index % proxy_count;
        self.index = (self.index + 1) % proxy_count;

        Some((proxy_index, proxy_index))
    }

    /// Get assigned proxy based on which proxy hosts the most states needed by this transaction.
    fn get_proxy_for_shared_objects_most_states(
        &self,
        required_versions: &[(ObjectID, SequenceNumber)],
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = self.proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        if required_versions.is_empty() {
            // If no shared objects, use first proxy
            return Some((0, 0));
        }

        // Count how many objects each proxy already has
        let mut proxy_state_counts = vec![0; proxy_count];

        for (id, _) in required_versions {
            if let Some(proxy_index) = self.states_to_proxy.get(id) {
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
        &mut self,
        transaction: &RemoraTransaction<E>,
        required_versions: Option<Vec<(ObjectID, SequenceNumber)>>,
        proxy_index: ExecutorIndex,
    ) -> RequiredStates {
        let mut required_states = BTreeMap::new();

        tracing::debug!(
            "Transaction {:?} required versions: {:?}",
            transaction.digest(),
            required_versions
        );

        if let Some(required_versions) = required_versions {
            for (object_id, seq_num) in required_versions {
                let previous_owner = self.states_to_proxy.get(&object_id);

                // Insert into required_states map - with previous owner if object needs migration,
                // with None if it's already at the correct proxy or hasn't been assigned yet
                let previous_owner_value = previous_owner
                    .filter(|&owner| *owner != proxy_index)
                    .copied();

                required_states.insert((object_id, seq_num), previous_owner_value);

                // Always update the mapping to point to this proxy
                self.states_to_proxy.insert(object_id, proxy_index);
            }
        }

        required_states
    }
    
    /// Simplified method to send a message to a proxy
    async fn send_to_proxy(
        &self,
        dest_proxy: ExecutorIndex,
        message: PrimaryToProxyMessage<<E as Executor>::Transaction>,
    ) {
        if let Some(proxy_connection) = self.proxy_connections.get(&dest_proxy) {
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
