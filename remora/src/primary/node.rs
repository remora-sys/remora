// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, sync::Arc};

use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use super::{core::PrimaryCore, load_balancer::LoadBalancer, mock_consensus::MockConsensus};
use crate::{
    config::ValidatorConfig,
    error::NodeResult,
    executor::sui::{SuiExecutionResults, SuiExecutor, SuiTransaction},
    metrics::Metrics,
    networking::server::NetworkServer,
    proxy::core::ProxyCore,
};

/// Default channel size for communication between components.
const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// The single machine validator is a simple validator that runs all components.
pub struct PrimaryNode {
    /// The handles for the core components.
    pub primary_handles: Vec<JoinHandle<NodeResult<()>>>,
    /// The handle for the (mock) consensus.
    pub consensus_handle: JoinHandle<()>,
    /// The handle for the network server.
    pub network_handle: JoinHandle<io::Result<()>>,
    /// The receiver for the final execution results.
    pub rx_output: Receiver<(SuiTransaction, SuiExecutionResults)>,
    /// The receiver for client connections. These channels can be used to reply to the clients.
    pub rx_client_connections: Receiver<Sender<()>>,
    /// The metrics for the validator.
    pub metrics: Arc<Metrics>,
}

impl PrimaryNode {
    /// Start the single machine validator.
    pub async fn start(
        executor: SuiExecutor,
        config: &ValidatorConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (tx_client_connections, rx_client_connections) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_client_transactions, rx_client_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_forwarded_load, rx_forwarded_load) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_connections, rx_proxy_connections) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_commits, rx_commits) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_output, rx_output) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let mut primary_handles = Vec::new();

        // Boot the client transactions server. This component receives client transactions from the
        // the network and forwards them to the load balancer.
        // TODO: Introduce error type and add the server handle to the handles list.
        let _server_handle = NetworkServer::new(
            config.client_server_address,
            tx_client_connections,
            tx_client_transactions,
        )
        .spawn();

        // Boot the load balancer. This component forwards transactions to the consensus and proxies.
        let load_balancer_handle = LoadBalancer::new(
            rx_client_transactions,
            tx_forwarded_load,
            rx_proxy_connections,
            metrics.clone(),
        )
        .spawn();
        primary_handles.push(load_balancer_handle);

        // Boot the (mock) consensus. This component delays transactions simulating consensus and
        // then forwards them to the primary executor.
        let consensus_handle = MockConsensus::new(
            config.validator_parameters.consensus_delay_model.clone(),
            config.validator_parameters.consensus_parameters.clone(),
            rx_forwarded_load,
            tx_commits,
        )
        .spawn();

        // Boot the local proxies. Additional proxies can still remotely connect. Proxies
        // receive transactions in parallel with the consensus for pre-execution.
        for i in 0..config.validator_parameters.collocated_pre_executors.primary {
            let proxy_id = format!("primary-{i}");
            let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let store = Arc::new(executor.create_in_memory_store());
            let proxy_handle = ProxyCore::new(
                proxy_id,
                executor.clone(),
                store,
                rx,
                tx_proxy_results.clone(),
                metrics.clone(),
            )
            .spawn();
            primary_handles.push(proxy_handle);
            tx_proxy_connections.send(tx).await.expect("Channel open");
        }

        // Boot another server handling connections from (additional) remote proxies. These remote
        // proxies perform the same functions the the local proxies.
        let network_handle = NetworkServer::new(
            config.proxy_server_address,
            tx_proxy_connections,
            tx_proxy_results,
        )
        .spawn();

        // Boot the primary executor. This component receives ordered transactions from consensus.
        // It then combines the pre-execution results from the proxies and re-executes the transactions
        // only if necessary.
        let store = Arc::new(executor.create_in_memory_store());
        let primary_handle =
            PrimaryCore::new(executor, store, rx_commits, rx_proxy_results, tx_output).spawn();
        primary_handles.push(primary_handle);

        Self {
            primary_handles,
            consensus_handle,
            network_handle,
            rx_output,
            rx_client_connections,
            metrics,
        }
    }

    /// Collect the results from the validator.
    pub async fn collect_results(mut self) {
        // Collect client connections.
        // TODO: In a real system, these connections would be used to reply to the clients, acknowledging
        // the receipt of the transaction and its final execution status.
        let mut client_connections = Vec::new();

        loop {
            tokio::select! {
                Some((tx, result)) = self.rx_output.recv() => {
                    tracing::debug!("Received output: {:?}", result);
                    assert!(result.success());
                    let submit_timestamp = tx.timestamp();
                    // TODO: Record transactions success and failure.
                    self.metrics.update_metrics(submit_timestamp);
                }
                Some(connection) = self.rx_client_connections.recv() => {
                    tracing::info!("Received a new client connection");
                    client_connections.push(connection);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        config::{
            BenchmarkParameters,
            CollocatedPreExecutors,
            ValidatorConfig,
            ValidatorParameters,
        },
        executor::sui::SuiExecutor,
        load_generator::LoadGenerator,
        metrics::Metrics,
        primary::node::PrimaryNode,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn execute_transactions() {
        let config = ValidatorConfig::new_for_tests();
        let benchmark_config = BenchmarkParameters::new_for_tests();

        // Create a Sui executor.
        let executor = SuiExecutor::new(&benchmark_config).await;

        // Start the validator.
        let validator_metrics = Arc::new(Metrics::new_for_tests());
        let mut primary = PrimaryNode::start(executor, &config, validator_metrics).await;
        tokio::task::yield_now().await;

        // Generate transactions.
        let load_generator_metrics = Metrics::new_for_tests();
        let mut load_generator = LoadGenerator::new(
            benchmark_config,
            config.client_server_address,
            load_generator_metrics,
        );

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
        let validator_parameters = ValidatorParameters {
            collocated_pre_executors: CollocatedPreExecutors {
                primary: 0,
                proxy: 0,
            },
            ..ValidatorParameters::new_for_tests()
        };
        let config = ValidatorConfig {
            validator_parameters,
            ..ValidatorConfig::new_for_tests()
        };
        let benchmark_config = BenchmarkParameters::new_for_tests();
        let primary_address = config.client_server_address;

        // Create a Sui executor.
        let executor = SuiExecutor::new(&benchmark_config).await;

        // Start the validator.
        let validator_metrics = Arc::new(Metrics::new_for_tests());
        let mut validator = PrimaryNode::start(executor.clone(), &config, validator_metrics).await;
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
