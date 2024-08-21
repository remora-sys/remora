// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc};

use tokio::{
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};

use super::{core::PrimaryCore, load_balancer::LoadBalancer, mock_consensus::MockConsensus};
use crate::{
    config::ValidatorConfig,
    executor::sui::{SuiExecutionEffects, SuiExecutor, SuiTransactionWithTimestamp},
    metrics::Metrics,
    networking::server::NetworkServer,
    proxy::core::ProxyCore,
};

/// Default channel size for communication between components.
const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// The single machine validator is a simple validator that runs all components.
pub struct PrimaryNode {
    /// The handles for all components.
    pub handles: Vec<JoinHandle<()>>,
    /// The receiver for the final execution results.
    pub rx_output: Receiver<(SuiTransactionWithTimestamp, SuiExecutionEffects)>,
    /// The metrics for the validator.
    pub metrics: Arc<Metrics>,
}

impl PrimaryNode {
    /// Start the single machine validator.
    pub async fn start(
        executor: SuiExecutor,
        config: &ValidatorConfig,
        primary_address: SocketAddr,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (tx_client_transactions, rx_client_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_load_balancer_load, rx_load_balancer_load) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_connections, rx_proxy_connections) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_commits, rx_commits) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_output, rx_output) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        // Boot the local proxies. Additional proxies can still remotely connect.
        let mut handles = Vec::new();
        for i in 0..config.collocated_pre_executors.primary {
            let proxy_id = format!("primary-{i}");
            let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let store = executor.create_in_memory_store();
            let proxy_handle = ProxyCore::new(
                proxy_id,
                executor.clone(),
                store,
                rx,
                tx_proxy_results.clone(),
                metrics.clone(),
            )
            .spawn();
            handles.push(proxy_handle);
            tx_proxy_connections.send(tx).await.expect("Channel open");
        }

        // Boot the consensus.
        let consensus_handle = MockConsensus::new(
            config.consensus_delay_model.clone(),
            config.consensus_parameters.clone(),
            rx_load_balancer_load,
            tx_commits,
        )
        .spawn();
        handles.push(consensus_handle);

        // Boot the primary executor.
        let store = executor.create_in_memory_store();
        let primary_handle =
            PrimaryCore::new(executor, store, rx_commits, rx_proxy_results, tx_output).spawn();
        handles.push(primary_handle);

        // Boot the load balancer.
        let load_balancer_handle = LoadBalancer::new(
            rx_client_transactions,
            tx_load_balancer_load,
            rx_proxy_connections,
            metrics.clone(),
        )
        .spawn();
        handles.push(load_balancer_handle);

        // Boot the server.
        // TODO: Introduce error type and add the server handle to the handles list.
        let _server_handle = NetworkServer::new(
            primary_address,
            tx_proxy_connections,
            tx_client_transactions,
        )
        .spawn();

        Self {
            handles,
            rx_output,
            metrics,
        }
    }

    /// Collect the results from the validator.
    pub async fn collect_results(mut self) {
        while let Some((tx, result)) = self.rx_output.recv().await {
            tracing::debug!("Received output: {:?}", result);
            assert!(result.success());
            let submit_timestamp = tx.timestamp();
            // TODO: Record transactions success and failure.
            self.metrics.update_metrics(submit_timestamp);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        config::{get_test_address, BenchmarkConfig, CollocatedPreExecutors, ValidatorConfig},
        executor::sui::SuiExecutor,
        load_generator::LoadGenerator,
        metrics::Metrics,
        primary::node::PrimaryNode,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn execute_transactions() {
        let config = ValidatorConfig::new_for_tests();
        let primary_address = get_test_address();
        let benchmark_config = BenchmarkConfig::new_for_tests();

        // Create a Sui executor.
        let executor = SuiExecutor::new(&benchmark_config).await;

        // Start the validator.
        let validator_metrics = Arc::new(Metrics::new_for_tests());
        let mut primary =
            PrimaryNode::start(executor, &config, primary_address, validator_metrics).await;
        tokio::task::yield_now().await;

        // Generate transactions.
        let load_generator_metrics = Metrics::new_for_tests();
        let mut load_generator =
            LoadGenerator::new(benchmark_config, primary_address, load_generator_metrics);

        let transactions = load_generator.initialize().await;
        let total_transactions = transactions.len();
        load_generator.run(transactions).await;

        // Wait for all transactions to be processed.
        for _ in 0..total_transactions {
            let (_tx, result) = primary.rx_output.recv().await.unwrap();
            assert!(result.success());
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn no_proxies() {
        let primary_address = get_test_address();
        let config = ValidatorConfig {
            collocated_pre_executors: CollocatedPreExecutors {
                primary: 0,
                proxy: 0,
            },
            ..ValidatorConfig::new_for_tests()
        };
        let benchmark_config = BenchmarkConfig::new_for_tests();

        // Create a Sui executor.
        let executor = SuiExecutor::new(&benchmark_config).await;

        // Start the validator.
        let validator_metrics = Arc::new(Metrics::new_for_tests());
        let mut validator = PrimaryNode::start(
            executor.clone(),
            &config,
            primary_address,
            validator_metrics,
        )
        .await;
        tokio::task::yield_now().await;

        // Generate transactions.
        let load_generator_metrics = Metrics::new_for_tests();
        let mut load_generator =
            LoadGenerator::new(benchmark_config, primary_address, load_generator_metrics);

        let transactions = load_generator.initialize().await;
        let total_transactions = transactions.len();
        load_generator.run(transactions).await;

        // Wait for all transactions to be processed.
        for _ in 0..total_transactions {
            let (_tx, result) = validator.rx_output.recv().await.unwrap();
            assert!(result.success());
        }
    }
}
