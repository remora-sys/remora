// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dashmap::DashMap;
    use tokio::sync::mpsc::{channel, Receiver, Sender};

    use crate::{
        config::{BenchmarkParameters, LoadBalancingPolicy, ProxyMode},
        executor::{
            api::{ExecutionResults, Executor, PrimaryToProxyMessage, RemoraTransaction},
            sui::SuiExecutor,
        },
        metrics::Metrics,
        primary::owned_obj_txn_forwarder::OwnedObjTxnForwarder,
    };

    // Helper function to set up common test environment
    async fn setup_test_environment(
        config: &BenchmarkParameters,
    ) -> (
        SuiExecutor,
        Arc<Metrics>,
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

        (
            executor,
            metrics,
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

    // Add this at the beginning of the test module
    #[tokio::test(flavor = "multi_thread", worker_threads = 32)]
    #[cfg(feature = "benchmark")]
    async fn test_parallel_forwarding_benchmark() {
        use std::time::Instant;

        // Create proxy connections map with a high capacity channel
        let (tx_benchmark, mut rx_benchmark) = channel(20000);
        let proxy_connections = Arc::new(DashMap::new());
        proxy_connections.insert(0, tx_benchmark.clone());
        proxy_connections.insert(1, tx_benchmark);

        let mut owned_txn_processor = OwnedObjTxnForwarder::<SuiExecutor> {
            proxy_connections: proxy_connections.clone(),
            policy: LoadBalancingPolicy::RoundRobin,
            index: 0,
        };

        // Run a mini benchmark
        let transaction_count = 100000; // Small count for tests
        let transactions = owned_txn_processor
            .create_benchmark_transactions(transaction_count)
            .await;
        let handle = tokio::spawn(async move {
            owned_txn_processor
                .forward_owned_txns_in_parallel(transactions)
                .await;
        });

        let instant = Instant::now();
        let mut cnt = 0;
        while let Some(_) = rx_benchmark.recv().await {
            cnt += 1;
            if cnt == transaction_count * 2 {
                break;
            }
        }
        let elapsed = instant.elapsed();
        let throughput = transaction_count as f64 / elapsed.as_secs_f64();
        println!("Throughput = {:.2} tps", throughput);
        handle.await.unwrap();
    }

    #[cfg(feature = "benchmark")]
    pub async fn create_benchmark_shared_object_transactions<E: Executor>(
        count: usize,
    ) -> (BenchmarkParameters, Vec<RemoraTransaction<E>>) {
        use crate::config::WorkloadType;
        use std::time::Duration;
        let config = BenchmarkParameters {
            load: count as u64,
            duration: Duration::from_secs(1),
            workload: WorkloadType::Zipfian {
                alpha: 0.00,
                number_of_inputs: 2,
            },
            verification_duration: Duration::from_secs(0),
        };
        let transactions = E::generate_transactions(&config, None).await;
        let remora_txns: Vec<RemoraTransaction<E>> = transactions
            .into_iter()
            .take(count)
            .map(|tx| RemoraTransaction::<E>::new_for_tests(tx))
            .collect();

        (config, remora_txns)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 32)]
    #[cfg(feature = "benchmark")]
    async fn test_version_assignment_throughput() {
        use crate::primary::shared_obj_txn_forwarder::VersionAssignmentTask;
        use std::time::Instant;
        // Generate test transactions
        let transaction_count = 100000; // Use a smaller count for this test
        let (config, transactions) =
            create_benchmark_shared_object_transactions::<SuiExecutor>(transaction_count).await;

        // Create channels for the version assignment task
        let (tx_shared_txns, rx_shared_txns) = channel(20000);
        let (tx_assigned, mut rx_assigned) = channel(20000);

        // Create the version assignment task
        let mut version_assignment_task = VersionAssignmentTask::<SuiExecutor> {
            shared_object_versions: rustc_hash::FxHashMap::default(),
            _phantom: std::marker::PhantomData,
        };

        // Create and spawn the version assignment task
        let handle = tokio::spawn(async move {
            version_assignment_task
                .process_version_assignments(rx_shared_txns, tx_assigned)
                .await;
        });

        // Send transactions to the version assignment task
        tx_shared_txns.send(transactions).await.unwrap();

        // Measure throughput of receiving version-assigned transactions
        let instant = Instant::now();
        let mut cnt = 0;
        while let Some(_) = rx_assigned.recv().await {
            cnt += 1;
            if cnt == transaction_count - 1 {
                break;
            }
        }
        let elapsed = instant.elapsed();
        let throughput = transaction_count as f64 / elapsed.as_secs_f64();
        println!("Version Assignment Throughput = {:.2} txns/s", throughput);

        // Drop channels to terminate the task
        drop(tx_shared_txns);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_owned_processor_forwarding() {
        let config = BenchmarkParameters::new_for_tests();
        let (_executor, _metrics, _tx_committed_txns, _rx_committed_txns, _rx_results) =
            setup_test_environment(&config).await;

        // Setup proxy channels
        let (tx_to_proxy1, mut rx_from_processor1) = channel(100);
        let (tx_to_proxy2, mut rx_from_processor2) = channel(100);

        // Create proxy connections map
        let proxy_connections = Arc::new(DashMap::new());
        proxy_connections.insert(0, tx_to_proxy1);
        proxy_connections.insert(1, tx_to_proxy2);

        // Create owned processor
        let mut owned_processor = OwnedObjTxnForwarder::<SuiExecutor> {
            proxy_connections,
            index: 0,
            proxy_mode: ProxyMode::Separation,
        };

        // Generate transactions
        let remora_txns = generate_test_transactions(&config, 5).await;

        // Forward transactions
        owned_processor
            .forward_owned_txns_in_parallel(remora_txns)
            .await;

        // Verify transactions were forwarded to proxies
        let mut received_stateless = 0;
        let mut received_stateful = 0;

        // Check messages received by proxies
        for _ in 0..10 {
            tokio::select! {
                Some(msg) = rx_from_processor1.recv() => {
                    match msg {
                        PrimaryToProxyMessage::StatelessTxn(_) => received_stateless += 1,
                        PrimaryToProxyMessage::Txn(_, _, _) => received_stateful += 1,
                        PrimaryToProxyMessage::CombinedTxn(_, _, _) => unreachable!(),
                    }
                }
                Some(msg) = rx_from_processor2.recv() => {
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

    #[tokio::test]
    async fn test_shared_processor_forwarding() {
        use crate::config::ProxyMode;
        use crate::executor::versioned_dependency_controller::VersionedDependencyController;
        use crate::primary::shared_obj_txn_forwarder::SharedObjTxnForwarder;
        use sui_types::base_types::{ObjectID, SequenceNumber};

        let config = BenchmarkParameters::new_for_contention_tests();
        let (_executor, _metrics, _tx_committed_txns, _rx_committed_txns, _rx_results) =
            setup_test_environment(&config).await;

        // Setup proxy channels
        let (tx_to_proxy1, mut rx_from_processor1) = channel(100);
        let (tx_to_proxy2, mut rx_from_processor2) = channel(100);

        // Create proxy connections map
        let proxy_connections = Arc::new(DashMap::new());
        proxy_connections.insert(0, tx_to_proxy1);
        proxy_connections.insert(1, tx_to_proxy2);

        // Create states_to_proxy map and dependency controller
        let states_to_proxy = Arc::new(DashMap::new());
        let dependency_controller = Arc::new(VersionedDependencyController::default());

        // Create shared processor
        let mut shared_processor = SharedObjTxnForwarder::<SuiExecutor>::new(
            dependency_controller.clone(),
            states_to_proxy.clone(),
            LoadBalancingPolicy::RoundRobin,
            proxy_connections.clone(),
            ProxyMode::Separation,
            Arc::new(Metrics::new_for_tests()),
            Arc::new(DashMap::new()),
            (0..proxy_connections.len())
                .map(|_| Arc::new(DashMap::new()))
                .collect(),
        );

        // Generate transactions
        let remora_txns = generate_test_transactions(&config, 5).await;
        let required_versions = vec![(ObjectID::random(), SequenceNumber::new())];

        // Create a channel to send transactions to the processor
        let (tx_shared_txns, rx_shared_txns) = channel(100);

        // Send transactions through the channel
        for txn in remora_txns {
            tx_shared_txns
                .send((txn, required_versions.clone()))
                .await
                .unwrap();
        }

        // Drop the sender to close the channel
        drop(tx_shared_txns);

        // Process all transactions from the channel
        shared_processor.process_shared_txns(rx_shared_txns).await;

        // Wait a bit for async processing
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // For RoundRobin policy, each proxy should receive equal number of transactions
        let mut proxy1_stateless = 0;
        let mut proxy1_stateful = 0;
        let mut proxy2_stateless = 0;
        let mut proxy2_stateful = 0;

        // Check messages received by proxy 1
        while let Ok(msg) = rx_from_processor1.try_recv() {
            match msg {
                PrimaryToProxyMessage::StatelessTxn(_) => proxy1_stateless += 1,
                PrimaryToProxyMessage::Txn(_, _, _) => proxy1_stateful += 1,
                _ => unreachable!(),
            }
        }

        // Check messages received by proxy 2
        while let Ok(msg) = rx_from_processor2.try_recv() {
            match msg {
                PrimaryToProxyMessage::StatelessTxn(_) => proxy2_stateless += 1,
                PrimaryToProxyMessage::Txn(_, _, _) => proxy2_stateful += 1,
                _ => unreachable!(),
            }
        }

        // With RoundRobin, each proxy should receive approximately equal number of transactions
        assert_eq!(
            proxy1_stateless + proxy2_stateless,
            5,
            "Should have received 5 stateless transactions in total"
        );
        assert_eq!(
            proxy1_stateful + proxy2_stateful,
            5,
            "Should have received 5 stateful transactions in total"
        );

        // Each proxy should have received either 2 or 3 transactions of each type
        assert!(
            (proxy1_stateless == 2 || proxy1_stateless == 3)
                && (proxy2_stateless == 2 || proxy2_stateless == 3),
            "Each proxy should receive either 2 or 3 stateless transactions"
        );
        assert!(
            (proxy1_stateful == 2 || proxy1_stateful == 3)
                && (proxy2_stateful == 2 || proxy2_stateful == 3),
            "Each proxy should receive either 2 or 3 stateful transactions"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 32)]
    #[cfg(feature = "benchmark")]
    async fn test_combined_version_assignment_and_processing_throughput() {
        use crate::config::DEFAULT_CHANNEL_SIZE;
        use crate::executor::versioned_dependency_controller::VersionedDependencyController;
        use crate::primary::shared_obj_txn_forwarder::{
            SharedObjTxnForwarder, VersionAssignmentTask,
        };
        use std::time::Instant;

        // Configure benchmark params
        let transaction_count = 100000; // Use a smaller count for this test
        let (_, transactions) =
            create_benchmark_shared_object_transactions::<SuiExecutor>(transaction_count).await;
        println!("finished creating transactions");

        // Create proxy connections with a high capacity channel
        let (tx_benchmark, mut rx_benchmark) = channel(DEFAULT_CHANNEL_SIZE);
        let proxy_connections = Arc::new(DashMap::new());
        proxy_connections.insert(0, tx_benchmark.clone());
        proxy_connections.insert(1, tx_benchmark);

        // Create states_to_proxy map and dependency controller
        let states_to_proxy = Arc::new(DashMap::new());
        let dependency_controller = Arc::new(VersionedDependencyController::default());

        // Create channels between version assignment and shared processor
        let (tx_shared_txns, rx_shared_txns) = channel(DEFAULT_CHANNEL_SIZE);
        let (tx_assigned, rx_assigned) = channel(DEFAULT_CHANNEL_SIZE);

        // Create the version assignment task
        let mut version_assignment_task = VersionAssignmentTask::<SuiExecutor> {
            shared_object_versions: rustc_hash::FxHashMap::default(),
            _phantom: std::marker::PhantomData,
        };

        // Create shared processor
        let mut shared_processor = SharedObjTxnForwarder::<SuiExecutor> {
            proxy_connections: proxy_connections.clone(),
            policy: LoadBalancingPolicy::Zeus,
            txn_cnt: 0,
            states_to_proxy: states_to_proxy.clone(),
            dependency_controller: dependency_controller.clone(),
            metrics: Arc::new(Metrics::new_for_tests()),
            proxy_mode: ProxyMode::Separation,
            proxy_loads: Arc::new(DashMap::new()),
            proxy_access_histories: (0..proxy_connections.len())
                .map(|_| Arc::new(DashMap::new()))
                .collect(),
        };

        // Spawn tasks for version assignment and shared processing
        let version_task = tokio::spawn(async move {
            version_assignment_task
                .process_version_assignments(rx_shared_txns, tx_assigned)
                .await;
        });

        let processor_task = tokio::spawn(async move {
            shared_processor.process_shared_txns(rx_assigned).await;
        });

        // Start measuring throughput
        let instant = Instant::now();

        // Send transactions to the version assignment task
        tx_shared_txns.send(transactions).await.unwrap();

        // Count received messages at the end of the pipeline
        let mut cnt = 0;
        // Each transaction generates 2 messages: stateless and stateful
        while let Some(_) = rx_benchmark.recv().await {
            cnt += 1;
            if cnt == transaction_count * 2 - 2 {
                break;
            }
        }

        let elapsed = instant.elapsed();
        let throughput = transaction_count as f64 / elapsed.as_secs_f64();
        println!("Combined Throughput = {:.2} txns/s", throughput);

        // Drop the channel to terminate the tasks
        drop(tx_shared_txns);
        version_task.await.unwrap();
        processor_task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 32)]
    #[cfg(feature = "benchmark")]
    async fn test_proxy_throughput() {
        use crate::executor::api::ExecutableTransaction;
        use crate::executor::api::RequiredStates;
        use crate::proxy::core::ProxyCore;
        use prometheus::Registry;
        use std::time::Instant;
        use sui_types::base_types::SequenceNumber;

        // Generate test transactions with shared objects
        let transaction_count = 10000; // Use a smaller count for this test
        let (config, transactions) =
            create_benchmark_shared_object_transactions::<SuiExecutor>(transaction_count).await;
        println!("finished creating transactions");

        // Create channels for proxy communication
        let (tx_primary_to_proxy, rx_primary_to_proxy) = channel(20000);
        let (tx_proxy_to_primary, mut rx_proxy_to_primary) = channel(20000);
        let (tx_inter_proxy_requests, rx_inter_proxy_requests) = channel(100);
        let tx_inter_proxy_replies = Arc::new(DashMap::new());

        let executor = SuiExecutor::new(&config).await;
        let store = executor.init_store();
        let registry = Registry::new();
        let metrics = Arc::new(Metrics::new(&registry));

        let proxy_core = ProxyCore::<SuiExecutor>::new(
            0,
            executor,
            store.into(),
            rx_primary_to_proxy,
            tx_proxy_to_primary,
            rx_inter_proxy_requests,
            tx_inter_proxy_replies.clone(),
            metrics,
        );

        // Spawn the proxy core task
        let proxy_handle = proxy_core.spawn();

        // Start measuring throughput
        let instant = Instant::now();

        // This simulates when the zipfian is 0
        //assert_eq!(config.workload.alpha, 0.00);

        // Prepare messages to send to the proxy
        let messages = transactions
            .into_iter()
            .map(|transaction| {
                // Create dummy required states for stateful transactions
                let mut required_states = RequiredStates::new();
                for obj_id in transaction.input_objects() {
                    required_states.insert((obj_id.object_id(), SequenceNumber::from(2)), None);
                }
                let tx = Arc::new(transaction);
                (
                    PrimaryToProxyMessage::StatelessTxn(tx.clone()),
                    PrimaryToProxyMessage::Txn(tx, 0, required_states),
                )
            })
            .collect::<Vec<_>>();

        // Launch a tokio task to send the messages
        let tx_primary_to_proxy_clone = tx_primary_to_proxy.clone();
        tokio::spawn(async move {
            for (msg_0, msg_1) in messages {
                if tx_primary_to_proxy_clone.send(msg_0).await.is_err() {
                    break;
                }
                if tx_primary_to_proxy_clone.send(msg_1).await.is_err() {
                    break;
                }
            }
        });

        // Count received messages from the proxy
        let mut cnt = 0;
        while let Some(_) = rx_proxy_to_primary.recv().await {
            cnt += 1;
            if cnt == transaction_count - 1 {
                break;
            }
        }

        let elapsed = instant.elapsed();
        let throughput = transaction_count as f64 / elapsed.as_secs_f64();
        println!("Proxy Stateful Throughput = {:.2} txns/s", throughput);

        // Clean up
        drop(tx_primary_to_proxy);
        // let _ = proxy_handle.await;
    }
}
*/
