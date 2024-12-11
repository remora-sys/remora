// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, sync::Arc};

use dashmap::DashMap;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use super::{
    core::{PendingTransactions, PrimaryCore},
    load_balancer::LoadBalancer,
    mock_consensus::MockConsensus,
};
use crate::{
    config::ValidatorConfig,
    error::NodeResult,
    executor::{
        api::Timestamp,
        sui::{SuiExecutionResults, SuiExecutor},
    },
    metrics::Metrics,
    networking::server::NetworkServer,
    proxy::core::{ProxyCore, ProxyMode},
};

/// Default channel size for communication between components.
const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// The single machine validator is a simple validator that runs all components.
pub struct PrimaryNode {
    /// The handles for the core components.
    pub primary_handles: Vec<JoinHandle<NodeResult<()>>>,
    /// The handle for the (mock) consensus.
    pub consensus_handle: JoinHandle<()>,
    /// The handles for the network servers.
    pub network_handles: Vec<JoinHandle<io::Result<()>>>,
    /// The receiver for the final execution results.
    pub rx_output: Receiver<(Timestamp, SuiExecutionResults)>,
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
        let (tx_committed_txns, rx_committed_txns) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_output, rx_output) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_executor_local, rx_executor_local) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_states_sync, rx_states_sync) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let mut primary_handles = Vec::new();
        let mut network_handles = Vec::new();

        let pending_txns: PendingTransactions<SuiExecutor> = Arc::new(DashMap::new());

        // Boot the load balancer. This component forwards transactions to the consensus and proxies.
        let load_balancer_handle = LoadBalancer::<SuiExecutor>::new(
            rx_client_transactions,
            tx_forwarded_load,
            rx_proxy_connections,
            rx_committed_txns,
            pending_txns.clone(),
            tx_executor_local.clone(),
            rx_states_sync,
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

        let mode = match config.parallel_proxy {
            false => ProxyMode::SingleThreaded,
            true => ProxyMode::MultiThreaded,
        };

        // Boot the local proxies. Additional proxies can still remotely connect. Proxies
        // receive transactions in parallel with the consensus for pre-execution.
        for i in 0..config.validator_parameters.collocated_pre_executors.primary {
            let proxy_id = format!("primary-{i}");
            let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let store = Arc::new(executor.create_in_memory_store());
            executor.load_state_for_shared_objects().await; // TODO: check if duplicated
            ProxyCore::new(
                proxy_id,
                executor.clone(),
                mode,
                store,
                rx,
                tx_proxy_results.clone(),
                metrics.clone(),
            )
            .spawn_with_threads();
            tx_proxy_connections.send(tx).await.expect("Channel open");
        }

        // Boot another server handling connections from (additional) remote proxies. These remote
        // proxies perform the same functions as the local proxies.
        let proxy_network_handle = NetworkServer::new(
            config.proxy_server_address,
            tx_proxy_connections,
            tx_proxy_results,
        )
        .spawn();
        network_handles.push(proxy_network_handle);

        // Boot the primary executor. This component receives ordered transactions from consensus.
        // It then combines the pre-execution results from the proxies and re-executes the transactions
        // only if necessary.
        let store = Arc::new(executor.create_in_memory_store());
        executor.load_state_for_shared_objects().await;
        let primary_handle = PrimaryCore::new(
            executor,
            store,
            rx_commits,
            rx_proxy_results,
            tx_output,
            pending_txns,
            tx_committed_txns,
            tx_executor_local,
            rx_executor_local,
            tx_states_sync,
        )
        .spawn();
        primary_handles.push(primary_handle);

        // Boot the client transactions server. This component receives client transactions from the
        // the network and forwards them to the load balancer.
        let transactions_network_handle = NetworkServer::new(
            config.client_server_address,
            tx_client_connections,
            tx_client_transactions,
        )
        .spawn();
        network_handles.push(transactions_network_handle);

        Self {
            primary_handles,
            consensus_handle,
            network_handles,
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
                Some((timestamp, result)) = self.rx_output.recv() => {
                    tracing::debug!("Received output: {:?}", result);
                    assert!(result.success());
                    // TODO: Record transactions success and failure.
                    self.metrics.update_metrics(timestamp);
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
        client::load_generator::LoadGenerator,
        config::{
            BenchmarkParameters, CollocatedPreExecutors, ValidatorConfig, ValidatorParameters,
        },
        executor::sui::SuiExecutor,
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
        let mut load_generator = LoadGenerator::new(benchmark_config, config.client_server_address);

        let transactions = load_generator.initialize().await;
        let total_transactions = transactions.len();
        load_generator.run(transactions).await;

        // Wait for all transactions to be processed.
        for _ in 0..total_transactions {
            let (_ts, result) = primary.rx_output.recv().await.unwrap();
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
        let mut load_generator = LoadGenerator::new(benchmark_config, primary_address);

        let transactions = load_generator.initialize().await;
        let total_transactions = transactions.len();
        load_generator.run(transactions).await;

        // Wait for all transactions to be processed.
        for _ in 0..total_transactions {
            let (_ts, result) = validator.rx_output.recv().await.unwrap();
            assert!(result.success());
        }
    }
}
