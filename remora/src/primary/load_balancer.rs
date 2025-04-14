// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use rustc_hash::FxHashMap;
use sui_types::{base_types::ObjectID, digests::TransactionDigest, transaction::InputObjectKind};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::api::{
        ExecutableTransaction, ExecutionResults, Executor, ExecutorIndex, MissingStates, NewStates,
        PrimaryToProxyMessage, RemoraTransaction, Store,
    },
    metrics::Metrics,
};

/// Defines different load balancing policies for distributing transactions.
#[derive(Debug, Clone)]
pub enum LoadBalancingPolicy {
    /// Simple round-robin distribution
    RoundRobin,
    /// Send to proxy that already has most of the required states
    Zeus,
}

/// A load balancer is responsible for distributing transactions to proxies.
pub struct LoadBalancer<E: Executor> {
    /// The executor is only used to assigned shared object versions.
    executor: E,
    /// The store is only used to assigned shared object versions.
    store: Store<E>,
    /// Receive handles to forward transactions to proxies. When a new client connects,
    /// this channel receives a sender from the network layer which is used to forward
    /// transactions to the proxies.
    rx_proxy_connections: Receiver<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
    /// Holds senders to forward transactions to proxies.
    proxy_connections: Vec<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: ExecutorIndex,
    /// The receiver of new effects from local executor and needs to forward to proxies.
    rx_states_sync: Receiver<ExecutionResults<E>>,
    /// The mapping of the states and the proxy index.
    states_to_proxy: FxHashMap<ObjectID, ExecutorIndex>,
    /// The routing information of stateless.
    stateless_routing: FxHashMap<TransactionDigest, ExecutorIndex>,
    /// The load balancing policy.
    policy: LoadBalancingPolicy,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

const FIB_CONSTANT: u64 = 11400714819323198485; // Golden ratio * 2^64

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        executor: E,
        store: Store<E>,
        rx_proxy_connections: Receiver<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
        rx_states_sync: Receiver<ExecutionResults<E>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            executor,
            store,
            rx_proxy_connections,
            proxy_connections: Vec::new(),
            rx_committed_txns,
            index: 0,
            rx_states_sync,
            states_to_proxy: FxHashMap::default(),
            stateless_routing: FxHashMap::default(),
            metrics,
            policy: LoadBalancingPolicy::RoundRobin,
        }
    }

    /// Helper to get all shared object IDs from a transaction.
    fn get_shared_object_ids(&self, transaction: &E::Transaction) -> Vec<ObjectID> {
        transaction
            .input_objects()
            .iter()
            .filter_map(|input_object| {
                if let InputObjectKind::SharedMoveObject { id, .. } = input_object {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Fibonacci Hashing for ObjectID → Proxy Index Mapping (Fast & Even Distribution)
    fn fast_fibonacci_hash(&self, object_id: &ObjectID) -> ExecutorIndex {
        let mut hash = 0u64;
        for chunk in object_id.chunks(8) {
            let mut chunk_array = [0u8; 8];
            chunk_array[..chunk.len()].copy_from_slice(chunk);
            let num = u64::from_ne_bytes(chunk_array);
            hash ^= num; // XOR to spread entropy
        }

        // Apply Fibonacci hashing for fast and even distribution
        let proxy_count = self.proxy_connections.len().max(1); // Avoid div by zero
        ((hash.wrapping_mul(FIB_CONSTANT)) >> (64 - proxy_count.ilog2())) as usize % proxy_count
    }

    /// Get assigned proxies for shared objects in a transaction.
    /*fn get_proxies_for_shared_objects(
        &self,
        shared_object_ids: &[ObjectID],
    ) -> HashSet<ExecutorIndex> {
        shared_object_ids
            .iter()
            .map(|id| self.fast_fibonacci_hash(id))
            .collect()
    }*/

    /// Get assigned proxy for shared objects in a transaction.
    /// This is the main entry point for load balancing policy selection.
    fn get_proxy_for_shared_objects(
        &mut self,
        shared_object_ids: &[ObjectID],
    ) -> Option<ExecutorIndex> {
        match &mut self.policy {
            LoadBalancingPolicy::RoundRobin => self.get_proxy_for_shared_objects_round_robin(),
            LoadBalancingPolicy::Zeus => {
                self.get_proxy_for_shared_objects_most_states(shared_object_ids)
            }
        }
    }

    /// Get assigned proxy for shared objects using round-robin.
    fn get_proxy_for_shared_objects_round_robin(&mut self) -> Option<ExecutorIndex> {
        let proxy_count = self.proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        let proxy_index = self.index % proxy_count;
        self.index = (self.index + 1) % proxy_count;

        Some(proxy_index)
    }

    /// Get assigned proxy based on which proxy hosts the most states needed by this transaction.
    fn get_proxy_for_shared_objects_most_states(
        &self,
        shared_object_ids: &[ObjectID],
    ) -> Option<ExecutorIndex> {
        let proxy_count = self.proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        if shared_object_ids.is_empty() {
            // If no shared objects, use first proxy
            return Some(0);
        }

        // Count how many objects each proxy already has
        let mut proxy_state_counts = vec![0; proxy_count];

        for id in shared_object_ids {
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

        Some(best_proxy)
    }

    #[deprecated]
    /// Prepare state updates based on sharding
    fn prepare_state_updates(
        &mut self,
        execution_result: ExecutionResults<E>,
    ) -> FxHashMap<ExecutorIndex, NewStates> {
        // HashMap to hold the updates for each executor
        let mut updates_by_executor: FxHashMap<ExecutorIndex, NewStates> = FxHashMap::default();

        for (object_id, object) in execution_result.new_state.unwrap() {
            let executor_id = self.fast_fibonacci_hash(&object_id);
            let entry = updates_by_executor.entry(executor_id).or_default();
            entry.insert(object_id, object);
        }

        updates_by_executor
    }

    /// Determines the correct forwarding target for a transaction.
    async fn forward_txn_to_proxy(&mut self, transaction: RemoraTransaction<E>) {
        // If no proxies exist, send to the local executor.
        if self.proxy_connections.is_empty() {
            tracing::error!("No proxies available");
            return;
        }

        let shared_object_ids = self.get_shared_object_ids(transaction.deref());

        if shared_object_ids.is_empty() {
            self.forward_owned_object_only_txn(transaction).await;
        } else {
            self.forward_shared_object_txn(transaction, shared_object_ids)
                .await;
        }
    }

    /// Helper method to determine missing states for a transaction
    /// and update the states ownership map
    fn get_missing_states_for_transaction(
        &mut self,
        transaction: &RemoraTransaction<E>,
        proxy_index: ExecutorIndex,
    ) -> MissingStates {
        let mut missing_states = BTreeMap::new();

        let object_ids = E::get_objects_for_dependency_tracking(
            self.executor.context().clone(),
            self.store.clone(),
            transaction.clone(),
        );

        tracing::debug!(
            "sent to proxy {}: obj_id vec {:?}",
            proxy_index,
            object_ids.iter().map(|(id, _)| id).collect::<Vec<_>>()
        );

        for (object_id, _) in object_ids {
            // Check if this object is already mapped to a proxy
            if let Some(previous_owner) = self.states_to_proxy.get(&object_id) {
                if *previous_owner != proxy_index {
                    missing_states.insert(object_id, *previous_owner);
                    self.states_to_proxy.insert(object_id, proxy_index);
                }
            } else {
                // If not mapped yet, assign it to this proxy
                self.states_to_proxy.insert(object_id, proxy_index);
            }
        }

        missing_states
    }

    /// Sends a transaction to a specific proxy and handles connection failures.
    async fn send_transaction_to_proxy(
        &mut self,
        proxy_index: usize,
        transaction: RemoraTransaction<E>,
        is_stateful: bool,
    ) -> bool {
        let message = if is_stateful {
            let stateless_proxy_id = self.stateless_routing.remove(transaction.digest()).unwrap();
            let missing_states = self.get_missing_states_for_transaction(&transaction, proxy_index);
            PrimaryToProxyMessage::Txn(transaction, stateless_proxy_id, missing_states)
        } else {
            self.stateless_routing
                .insert(*transaction.digest(), proxy_index);
            PrimaryToProxyMessage::StatelessTxn(transaction)
        };

        if self.proxy_connections[proxy_index]
            .send(message)
            .await
            .is_ok()
        {
            tracing::debug!("Sent transaction to proxy {}", proxy_index);
            true
        } else {
            tracing::warn!(
                "Failed to send transaction to proxy {}, removing connection",
                proxy_index
            );
            self.proxy_connections.swap_remove(proxy_index);
            false
        }
    }

    /// Forwards transactions with owned-object only using round-robin.
    async fn forward_owned_object_only_txn(&mut self, transaction: RemoraTransaction<E>) {
        let proxy_index = self.index % self.proxy_connections.len();
        self.index += 1;

        self.send_transaction_to_proxy(proxy_index, transaction.clone(), false)
            .await;
        self.send_transaction_to_proxy(proxy_index, transaction, true)
            .await;
    }

    /// Forwards transactions with shared objects to the appropriate proxy.
    async fn forward_shared_object_txn(
        &mut self,
        transaction: RemoraTransaction<E>,
        shared_object_ids: Vec<ObjectID>,
    ) {
        if let Some(proxy_index) = self.get_proxy_for_shared_objects(&shared_object_ids) {
            self.send_transaction_to_proxy(proxy_index, transaction.clone(), false)
                .await;
            self.send_transaction_to_proxy(proxy_index, transaction, true)
                .await;
        } else {
            tracing::warn!("No proxies available for transaction with shared objects");
        }
    }
    /// Run the load balancer.
    pub async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Load balancer started");
        let mut txn_cnt = 0;
        loop {
            tokio::select! {
                Some(connection) = self.rx_proxy_connections.recv() => {
                    self.proxy_connections.push(connection);
                    tracing::info!("Added a new proxy connection");
                }

                Some(transactions) = self.rx_committed_txns.recv() => {

                    // Assign shared objects version.
                    self.executor
                        .assign_shared_object_versions(
                            &transactions.iter().map(|tx| tx.deref().clone()).collect::<Vec<_>>()
                        )
                        .await;

                    txn_cnt += 1;
                    if txn_cnt == 1 {
                        self.metrics.register_start_time();
                    }

                    for transaction in transactions {
                        self.forward_txn_to_proxy(transaction).await;
                    }
                }

                /*Some(result) = self.rx_states_sync.recv() => {
                    // send states updates to the proxy
                    if self.proxy_connections.is_empty() {
                        tracing::debug!("Skip states updating given no available other executors");
                        continue;
                    }

                    let states_updates = self.prepare_state_updates(result);
                    for (proxy_index, update) in states_updates {
                        match self.proxy_connections[proxy_index].send(PrimaryToProxyMessage::States(update)).await {
                            Ok(()) => {
                                tracing::debug!("Sent updates to proxy {}", proxy_index);
                            }
                            Err(_) => {
                                tracing::warn!("Failed to send states to proxy {}", proxy_index);
                                self.proxy_connections.swap_remove(proxy_index);
                            }
                        }
                    }
                }*/

                else => Err(NodeError::ShuttingDown)?,
            }
        }
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send,
        <E as Executor>::Transaction: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BenchmarkParameters;
    use crate::executor::sui::SuiExecutor;
    use crate::proxy::core::ProxyCore;
    use dashmap::DashMap;
    use tokio::sync::mpsc::{channel, Sender};

    // Helper function to set up common test environment
    async fn setup_test_environment(
        config: &BenchmarkParameters,
    ) -> (
        SuiExecutor,
        Arc<Metrics>,
        Arc<<SuiExecutor as Executor>::Store>,
        Sender<Sender<PrimaryToProxyMessage<<SuiExecutor as Executor>::Transaction>>>,
        Sender<Vec<RemoraTransaction<SuiExecutor>>>,
        Receiver<Sender<PrimaryToProxyMessage<<SuiExecutor as Executor>::Transaction>>>,
        Receiver<Vec<RemoraTransaction<SuiExecutor>>>,
        Receiver<ExecutionResults<SuiExecutor>>,
    ) {
        let executor = SuiExecutor::new(&config).await;

        // Create channels for load balancer
        let (tx_proxy_connections, rx_proxy_connections) = channel(100);
        let (tx_committed_txns, rx_committed_txns) = channel(100);
        let (_tx_results, rx_results) = channel(100);

        // Create metrics and store
        let metrics = Arc::new(Metrics::new_for_tests());
        let store = Arc::new(executor.init_store());

        (
            executor,
            metrics,
            store,
            tx_proxy_connections,
            tx_committed_txns,
            rx_proxy_connections,
            rx_committed_txns,
            rx_results,
        )
    }

    // Helper function to generate test transactions
    async fn generate_test_transactions(
        config: &BenchmarkParameters,
        count: usize,
    ) -> Vec<RemoraTransaction<SuiExecutor>> {
        let transactions = SuiExecutor::generate_transactions(config, None).await;
        transactions
            .into_iter()
            .take(count)
            .map(|tx| RemoraTransaction::<SuiExecutor>::new_for_tests(tx))
            .collect()
    }

    #[tokio::test]
    async fn test_load_balancer_rr_forwarding() {
        let config = BenchmarkParameters::new_for_tests();
        let (
            executor,
            metrics,
            store,
            tx_proxy_connections,
            tx_committed_txns,
            rx_proxy_connections,
            rx_committed_txns,
            _rx_results,
        ) = setup_test_environment(&config).await;

        // Create load balancer
        let (_tx_states_sync, rx_states_sync) = channel(100);
        let lb = LoadBalancer::new(
            executor.clone(),
            store.clone(),
            rx_proxy_connections,
            rx_committed_txns,
            rx_states_sync,
            metrics,
        );

        // Setup proxy channels
        let (tx_to_proxy1, mut rx_from_lb1) = channel(100);
        let (tx_to_proxy2, mut rx_from_lb2) = channel(100);

        // Spawn load balancer
        let _lb_handle = lb.spawn();

        // Connect proxies to load balancer
        tx_proxy_connections.send(tx_to_proxy1).await.unwrap();
        tx_proxy_connections.send(tx_to_proxy2).await.unwrap();

        // Generate transactions
        let remora_txns = generate_test_transactions(&config, 5).await;

        // Send transactions to load balancer
        tx_committed_txns.send(remora_txns.clone()).await.unwrap();

        // Verify transactions were forwarded to proxies
        let mut received_stateless = 0;
        let mut received_stateful = 0;

        // Check messages received by proxies
        for _ in 0..10 {
            tokio::select! {
                Some(msg) = rx_from_lb1.recv() => {
                    match msg {
                        PrimaryToProxyMessage::StatelessTxn(_) => received_stateless += 1,
                        PrimaryToProxyMessage::Txn(_, _, _) => received_stateful += 1,
                    }
                }
                Some(msg) = rx_from_lb2.recv() => {
                    match msg {
                        PrimaryToProxyMessage::StatelessTxn(_) => received_stateless += 1,
                        PrimaryToProxyMessage::Txn(_, _, _) => received_stateful += 1,
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    break;
                }
            }
        }

        // We should have received both stateless and stateful versions of each transaction
        assert_eq!(
            received_stateless, 5,
            "Should have received 5 stateless transactions"
        );
        assert_eq!(
            received_stateful, 5,
            "Should have received 5 stateful transactions"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[tracing_test::traced_test]
    async fn test_load_balancer_with_one_proxy() {
        // Setup test environment
        let config = BenchmarkParameters::new_for_tests();
        let (
            executor,
            metrics,
            store,
            tx_proxy_connections,
            tx_committed_txns,
            rx_proxy_connections,
            rx_committed_txns,
            _rx_results,
        ) = setup_test_environment(&config).await;

        // Create load balancer
        let (_tx_states_sync, rx_states_sync) = channel(100);
        let lb = LoadBalancer::new(
            executor.clone(),
            store.clone(),
            rx_proxy_connections,
            rx_committed_txns,
            rx_states_sync,
            metrics.clone(),
        );

        // Setup proxy and its channels
        let (tx_to_proxy, rx_from_lb) = channel(100);
        let (tx_results, _) = channel(100);
        let (_tx_inter_proxy_requests, rx_inter_proxy_requests) = channel(100);
        let tx_inter_proxy_replies = Arc::new(DashMap::new());

        // Create and spawn proxy core
        // Generate transactions
        let remora_txns = generate_test_transactions(&config, 3).await;

        let executor = SuiExecutor::new(&config).await;
        let proxy_store = Arc::new(executor.init_store());
        let proxy = ProxyCore::new(
            0,
            executor.clone(),
            proxy_store,
            rx_from_lb,
            tx_results,
            rx_inter_proxy_requests,
            tx_inter_proxy_replies,
            metrics.clone(),
        );
        let _proxy_handle = proxy.spawn();

        // Spawn load balancer
        let _lb_handle = lb.spawn();

        // Connect proxy to load balancer
        tx_proxy_connections.send(tx_to_proxy).await.unwrap();

        // Send transactions to load balancer
        tx_committed_txns.send(remora_txns.clone()).await.unwrap();

        // Add sleep to ensure all operations complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[tracing_test::traced_test]
    async fn test_load_balancer_with_two_proxies() {
        // Setup test environment
        let config = BenchmarkParameters::new_for_contention_tests();
        let (
            executor,
            metrics,
            store,
            tx_proxy_connections,
            tx_committed_txns,
            rx_proxy_connections,
            rx_committed_txns,
            _rx_results,
        ) = setup_test_environment(&config).await;

        // Create load balancer
        let (_tx_states_sync, rx_states_sync) = channel(100);
        let lb = LoadBalancer::new(
            executor.clone(),
            store.clone(),
            rx_proxy_connections,
            rx_committed_txns,
            rx_states_sync,
            metrics.clone(),
        );

        // Setup first proxy and its channels
        let (tx_to_proxy1, rx_from_lb1) = channel(100);
        let (tx_results1, mut rx_results1) = channel(100);
        let (tx_inter_proxy_requests1, rx_inter_proxy_requests1) = channel(100);
        let tx_inter_proxy_replies1 = Arc::new(DashMap::new());

        // Setup second proxy and its channels
        let (tx_to_proxy2, rx_from_lb2) = channel(100);
        let (tx_results2, mut rx_results2) = channel(100);
        let (tx_inter_proxy_requests2, rx_inter_proxy_requests2) = channel(100);
        let tx_inter_proxy_replies2 = Arc::new(DashMap::new());

        tx_inter_proxy_replies1.insert(1, tx_inter_proxy_requests2);
        tx_inter_proxy_replies2.insert(0, tx_inter_proxy_requests1);

        // Create and spawn first proxy core
        let proxy_store1 = Arc::new(executor.init_store());
        let proxy1 = ProxyCore::new(
            0,
            executor.clone(),
            proxy_store1,
            rx_from_lb1,
            tx_results1.clone(),
            rx_inter_proxy_requests1,
            tx_inter_proxy_replies1,
            metrics.clone(),
        );
        let _proxy_handle1 = proxy1.spawn();

        // Create and spawn second proxy core
        let proxy_store2 = Arc::new(executor.init_store());
        let proxy2 = ProxyCore::new(
            1,
            executor.clone(),
            proxy_store2,
            rx_from_lb2,
            tx_results2.clone(),
            rx_inter_proxy_requests2,
            tx_inter_proxy_replies2,
            metrics.clone(),
        );
        let _proxy_handle2 = proxy2.spawn();

        // Connect proxies to load balancer
        tx_proxy_connections.send(tx_to_proxy1).await.unwrap();
        tx_proxy_connections.send(tx_to_proxy2).await.unwrap();

        // Spawn load balancer
        let _lb_handle = lb.spawn();

        // Generate transactions
        let remora_txns = generate_test_transactions(&config, 4).await;

        // Send transactions to load balancer
        tx_committed_txns.send(remora_txns.clone()).await.unwrap();

        // Add sleep to ensure all operations complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Poll the results channels to verify transaction completion
        let mut finished_count = 0;

        // Check results from first proxy
        while let Ok(_) = rx_results1.try_recv() {
            finished_count += 1;
        }

        // Check results from second proxy
        while let Ok(_) = rx_results2.try_recv() {
            finished_count += 1;
        }

        // Verify that all 4 transactions were processed
        assert_eq!(
            finished_count, 4,
            "Expected 4 completed transactions, got {}",
            finished_count
        );
    }
}
