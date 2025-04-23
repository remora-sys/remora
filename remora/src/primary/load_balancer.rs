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
    config::LoadBalancingPolicy,
    error::{NodeError, NodeResult},
    executor::api::{
        ExecutableTransaction, ExecutionResults, Executor, ExecutorIndex, PrimaryToProxyMessage,
        RemoraTransaction, RequiredStates, Store,
    },
    metrics::Metrics,
    proxy::core::ProxyId,
};

/// A load balancer is responsible for distributing transactions to proxies.
pub struct LoadBalancer<E: Executor> {
    /// The executor is only used to assigned shared object versions.
    executor: E,
    /// The store is only used to assigned shared object versions.
    store: Store<E>,
    /// The proxy connections.
    proxy_connections:
        FxHashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: ExecutorIndex,
    /// The mapping of the states and the proxy index.
    states_to_proxy: FxHashMap<ObjectID, ExecutorIndex>,
    /// The routing information of stateless.
    stateless_routing: FxHashMap<TransactionDigest, ExecutorIndex>,
    /// The load balancing policy.
    policy: LoadBalancingPolicy,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        executor: E,
        store: Store<E>,
        proxy_connections: FxHashMap<
            ProxyId,
            Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
        >,
        rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
        policy: LoadBalancingPolicy,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            executor,
            store,
            proxy_connections,
            rx_committed_txns,
            index: 0,
            states_to_proxy: FxHashMap::default(),
            stateless_routing: FxHashMap::default(),
            metrics,
            policy,
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
            LoadBalancingPolicy::Dedicated => {
                // Dedicated: proxy 0 for stateless, proxy 1 for stateful
                let proxy_index = if shared_object_ids.is_empty() { 0 } else { 1 };
                Some(proxy_index)
            }
            LoadBalancingPolicy::Combined => {
                unimplemented!()
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
    async fn get_missing_states_for_transaction(
        &mut self,
        transaction: &RemoraTransaction<E>,
        proxy_index: ExecutorIndex,
    ) -> RequiredStates {
        let mut required_states = BTreeMap::new();

        // Add tracing for required versions using the executor API
        let required_versions = self
            .executor
            .get_required_shared_object_versions(&transaction.digest())
            .await;

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

    /// Sends a transaction to a specific proxy and handles connection failures.
    async fn send_transaction_to_proxy(
        &mut self,
        proxy_index: usize,
        transaction: RemoraTransaction<E>,
        is_stateful: bool,
        is_combined: bool,
    ) -> bool {
        let message = if is_combined {
            let missing_states = self
                .get_missing_states_for_transaction(&transaction, proxy_index)
                .await;
            tracing::debug!(
                "Sending combined transaction to proxy {}: digest={:?}, missing_states_count={}",
                proxy_index,
                transaction.digest(),
                missing_states.len()
            );
            PrimaryToProxyMessage::CombinedTxn(transaction, proxy_index, missing_states)
        } else if is_stateful {
            let stateless_proxy_id = self.stateless_routing.remove(transaction.digest()).unwrap();
            let missing_states = self
                .get_missing_states_for_transaction(&transaction, proxy_index)
                .await;
            tracing::debug!(
                "Sending stateful transaction to proxy {}: digest={:?}, stateless_proxy_id={}, missing_states_count={}",
                proxy_index,
                transaction.digest(),
                stateless_proxy_id,
                missing_states.len()
            );
            PrimaryToProxyMessage::Txn(transaction, stateless_proxy_id, missing_states)
        } else {
            tracing::debug!(
                "Sending stateless transaction to proxy {}: digest={:?}",
                proxy_index,
                transaction.digest()
            );
            self.stateless_routing
                .insert(*transaction.digest(), proxy_index);
            PrimaryToProxyMessage::StatelessTxn(transaction)
        };

        if self
            .proxy_connections
            .get(&proxy_index)
            .unwrap()
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
            self.proxy_connections.remove(&proxy_index);
            false
        }
    }

    /// Forwards transactions with owned-object only using the selected policy.
    async fn forward_owned_object_only_txn(&mut self, transaction: RemoraTransaction<E>) {
        match &self.policy {
            LoadBalancingPolicy::RoundRobin | LoadBalancingPolicy::Zeus => {
                let proxy_index = self.index % self.proxy_connections.len();
                self.index += 1;

                self.send_transaction_to_proxy(proxy_index, transaction.clone(), false, false)
                    .await;
                self.send_transaction_to_proxy(proxy_index, transaction, true, false)
                    .await;
            }
            LoadBalancingPolicy::Dedicated => {
                let stateless_proxy = 0;
                let stateful_proxy = 1 % self.proxy_connections.len();

                self.send_transaction_to_proxy(stateless_proxy, transaction.clone(), false, false)
                    .await;
                self.send_transaction_to_proxy(stateful_proxy, transaction, true, false)
                    .await;
            }
            LoadBalancingPolicy::Combined => {
                let proxy_index = self.index % self.proxy_connections.len();
                self.index += 1;

                self.send_transaction_to_proxy(proxy_index, transaction, false, true)
                    .await;
            }
        }
    }

    /// Forwards transactions with shared objects to the appropriate proxy.
    async fn forward_shared_object_txn(
        &mut self,
        transaction: RemoraTransaction<E>,
        shared_object_ids: Vec<ObjectID>,
    ) {
        if let Some(proxy_index) = self.get_proxy_for_shared_objects(&shared_object_ids) {
            self.send_transaction_to_proxy(proxy_index, transaction.clone(), false, false)
                .await;
            self.send_transaction_to_proxy(proxy_index, transaction, true, false)
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
                Some(transactions) = self.rx_committed_txns.recv() => {

                    // TODO: shall we avoid assign for non-shared-object txns
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
    use crate::config::{BenchmarkParameters, LoadBalancingPolicy};
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
        Store<SuiExecutor>,
        Sender<Vec<RemoraTransaction<SuiExecutor>>>,
        Receiver<Vec<RemoraTransaction<SuiExecutor>>>,
        Receiver<ExecutionResults<SuiExecutor>>,
    ) {
        let executor = SuiExecutor::new(&config).await;

        // Create channels for load balancer
        let (tx_committed_txns, rx_committed_txns) = channel(100);
        let (_tx_results, rx_results) = channel(100);

        // Create metrics and store
        let metrics = Arc::new(Metrics::new_for_tests());
        let store = executor.init_store();

        (
            executor,
            metrics,
            Arc::new(store),
            tx_committed_txns,
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
        let (executor, metrics, store, tx_committed_txns, rx_committed_txns, _rx_results) =
            setup_test_environment(&config).await;

        // Setup proxy channels
        let (tx_to_proxy1, mut rx_from_lb1) = channel(100);
        let (tx_to_proxy2, mut rx_from_lb2) = channel(100);

        // Create proxy connections map
        let mut proxy_connections = FxHashMap::default();
        proxy_connections.insert(0, tx_to_proxy1);
        proxy_connections.insert(1, tx_to_proxy2);

        // Create load balancer
        let lb = LoadBalancer::new(
            executor.clone(),
            store.clone(),
            proxy_connections,
            rx_committed_txns,
            LoadBalancingPolicy::RoundRobin,
            metrics,
        );

        // Spawn load balancer
        let _lb_handle = lb.spawn();

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
                        PrimaryToProxyMessage::CombinedTxn(_, _, _) => unreachable!(),
                    }
                }
                Some(msg) = rx_from_lb2.recv() => {
                    match msg {
                        PrimaryToProxyMessage::StatelessTxn(_) => received_stateless += 1,
                        PrimaryToProxyMessage::Txn(_, _, _) => received_stateful += 1,
                        PrimaryToProxyMessage::CombinedTxn(_, _, _) => unreachable!(),
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
    async fn test_load_balancer_with_one_proxy() {
        let config = BenchmarkParameters::new_for_tests();
        let (executor, metrics, store, tx_committed_txns, rx_committed_txns, _rx_results) =
            setup_test_environment(&config).await;

        // Setup proxy and its channels
        let (tx_to_proxy, rx_from_lb) = channel(100);
        let (tx_results, _) = channel(100);
        let (_tx_inter_proxy_requests, rx_inter_proxy_requests) = channel(100);
        let tx_inter_proxy_replies = Arc::new(DashMap::new());

        // Create proxy connections map
        let mut proxy_connections = FxHashMap::default();
        proxy_connections.insert(0, tx_to_proxy);

        // Create load balancer
        let lb = LoadBalancer::new(
            executor.clone(),
            store.clone(),
            proxy_connections,
            rx_committed_txns,
            LoadBalancingPolicy::RoundRobin,
            metrics.clone(),
        );

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

        // Send transactions to load balancer
        tx_committed_txns.send(remora_txns.clone()).await.unwrap();

        // Add sleep to ensure all operations complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_load_balancer_with_two_proxies() {
        let config = BenchmarkParameters::new_for_contention_tests();
        let (executor, metrics, store, tx_committed_txns, rx_committed_txns, _rx_results) =
            setup_test_environment(&config).await;

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

        // Create proxy connections map
        let mut proxy_connections = FxHashMap::default();
        proxy_connections.insert(0, tx_to_proxy1);
        proxy_connections.insert(1, tx_to_proxy2);

        // Create load balancer
        let lb = LoadBalancer::new(
            executor.clone(),
            store.clone(),
            proxy_connections,
            rx_committed_txns,
            LoadBalancingPolicy::RoundRobin,
            metrics.clone(),
        );

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

    #[tokio::test]
    async fn test_load_balancer_dedicated_forwarding() {
        let config = BenchmarkParameters::new_for_tests();
        let (executor, metrics, store, tx_committed_txns, rx_committed_txns, _rx_results) =
            setup_test_environment(&config).await;

        // Setup proxy channels
        let (tx_to_proxy0, mut rx_from_lb0) = channel(100);
        let (tx_to_proxy1, mut rx_from_lb1) = channel(100);

        // Create proxy connections map
        let mut proxy_connections = FxHashMap::default();
        proxy_connections.insert(0, tx_to_proxy0);
        proxy_connections.insert(1, tx_to_proxy1);

        // Create load balancer with Dedicated policy
        let lb = LoadBalancer::new(
            executor.clone(),
            store.clone(),
            proxy_connections,
            rx_committed_txns,
            LoadBalancingPolicy::Dedicated,
            metrics,
        );

        // Spawn load balancer
        let _lb_handle = lb.spawn();

        // Generate transactions
        let remora_txns = generate_test_transactions(&config, 5).await;

        // Send transactions to load balancer
        tx_committed_txns.send(remora_txns.clone()).await.unwrap();

        // Counters for each proxy
        let mut stateless_on_0 = 0;
        let mut stateful_on_1 = 0;

        // Check messages received by proxies
        for _ in 0..5 {
            if let Some(msg) = rx_from_lb0.recv().await {
                match msg {
                    PrimaryToProxyMessage::StatelessTxn(_) => stateless_on_0 += 1,
                    _ => panic!("Proxy 0 should only receive stateless transactions"),
                }
            }
        }
        for _ in 0..5 {
            if let Some(msg) = rx_from_lb1.recv().await {
                match msg {
                    PrimaryToProxyMessage::Txn(_, _, _) => stateful_on_1 += 1,
                    _ => panic!("Proxy 1 should only receive stateful transactions"),
                }
            }
        }

        // We should have received both stateless and stateful versions of each transaction
        assert_eq!(
            stateless_on_0, 5,
            "Proxy 0 should have received 5 stateless transactions"
        );
        assert_eq!(
            stateful_on_1, 5,
            "Proxy 1 should have received 5 stateful transactions"
        );
    }
}
