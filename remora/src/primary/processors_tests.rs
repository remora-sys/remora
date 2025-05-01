// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dashmap::DashMap;
    use tokio::sync::mpsc::{channel, Receiver, Sender};

    use crate::{
        config::{BenchmarkParameters, LoadBalancingPolicy},
        executor::{
            api::{ExecutionResults, PrimaryToProxyMessage, RemoraTransaction, Executor},
            sui::SuiExecutor,
        },
        metrics::Metrics,
        primary::owned_processors::OwnedTxnProcessor,
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
        let config = BenchmarkParameters::new_for_tests();

        // Create proxy connections map with a high capacity channel
        let (tx_benchmark, mut rx_benchmark) = channel(20000);
        let proxy_connections = Arc::new(DashMap::new());
        proxy_connections.insert(0, tx_benchmark.clone());
        proxy_connections.insert(1, tx_benchmark);

        let mut owned_txn_processor = OwnedTxnProcessor::<SuiExecutor> {
            proxy_connections: proxy_connections.clone(),
            policy: LoadBalancingPolicy::RoundRobin,
            index: 0,
        };

        // Run a mini benchmark
        let transaction_count = 1000000; // Small count for tests
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
        let mut owned_processor = OwnedTxnProcessor::<SuiExecutor> {
            proxy_connections,
            policy: LoadBalancingPolicy::RoundRobin,
            index: 0,
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
    async fn test_dedicated_policy_forwarding() {
        let config = BenchmarkParameters::new_for_tests();
        let (_executor, _metrics, _tx_committed_txns, _rx_committed_txns, _rx_results) =
            setup_test_environment(&config).await;

        // Setup proxy channels
        let (tx_to_proxy0, mut rx_from_processor0) = channel(100);
        let (tx_to_proxy1, mut rx_from_processor1) = channel(100);

        // Create proxy connections map
        let proxy_connections = Arc::new(DashMap::new());
        proxy_connections.insert(0, tx_to_proxy0);
        proxy_connections.insert(1, tx_to_proxy1);

        // Create owned processor with Dedicated policy
        let mut owned_processor = OwnedTxnProcessor::<SuiExecutor> {
            proxy_connections,
            policy: LoadBalancingPolicy::Dedicated,
            index: 0,
        };

        // Generate transactions
        let remora_txns = generate_test_transactions(&config, 5).await;

        // Forward transactions
        owned_processor
            .forward_owned_txns_in_parallel(remora_txns)
            .await;

        // Counters for each proxy
        let mut stateless_on_0 = 0;
        let mut stateful_on_1 = 0;

        // Check messages received by proxies
        for _ in 0..5 {
            if let Some(msg) = rx_from_processor0.recv().await {
                match msg {
                    PrimaryToProxyMessage::StatelessTxn(_) => stateless_on_0 += 1,
                    _ => panic!("Proxy 0 should only receive stateless transactions"),
                }
            }
        }
        for _ in 0..5 {
            if let Some(msg) = rx_from_processor1.recv().await {
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
