// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rustc_hash::FxHashMap;
use std::{io, marker::PhantomData, sync::Arc};

use serde::de::DeserializeOwned;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use super::{load_balancer::LoadBalancer, mock_consensus::MockConsensus};
use crate::{
    config::{ValidatorConfig, DEFAULT_CHANNEL_SIZE},
    error::NodeResult,
    executor::api::{Executor, PrimaryToProxyMessage},
    metrics::Metrics,
    networking::{client::NetworkClient, server::NetworkServer},
};

/// The single machine validator is a simple validator that runs all components.
pub struct PrimaryNode<E: Executor> {
    pub phantom_data: PhantomData<E>,
    /// The handles for the core components.
    pub primary_handles: Vec<JoinHandle<NodeResult<()>>>,
    /// The handle for the (mock) consensus.
    pub consensus_handle: JoinHandle<()>,
    /// The handles for the network servers.
    pub network_handles: Vec<JoinHandle<io::Result<()>>>,
    /// The receiver for client connections. These channels can be used to reply to the clients.
    pub rx_client_connections: Receiver<Sender<()>>,
    /// The metrics for the validator.
    pub metrics: Arc<Metrics>,
}

impl<E: Executor + Sync + Send + 'static> PrimaryNode<E> {
    /// Start the single machine validator.
    pub async fn start(executor: E, config: &ValidatorConfig, metrics: Arc<Metrics>) -> Self
    where
        <E as Executor>::Store: Sync + Send,
        <E as Executor>::Transaction: Send + Sync + 'static,
        <E as Executor>::ExecutionContext: Send + Sync,
        <E as Executor>::ExecutionResults: Send + Sync + DeserializeOwned,
    {
        let (tx_client_connections, rx_client_connections) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_client_transactions, rx_client_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_committed_txns, rx_committed_txns) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        // For storing proxy connections
        let mut proxy_connections = FxHashMap::default();

        let mut primary_handles = Vec::new();
        let mut network_handles = Vec::new();

        let store = Arc::new(executor.init_store());

        // Connect to each proxy server
        for (idx, proxy_info) in config.proxies.iter().enumerate() {
            let (tx_proxy, rx_proxy) =
                mpsc::channel::<PrimaryToProxyMessage<E::Transaction>>(DEFAULT_CHANNEL_SIZE);

            let network_client_handle = NetworkClient::new(
                proxy_info.listen_primary_address,
                tx_proxy.clone(),
                rx_proxy,
            )
            .spawn();

            network_handles.push(network_client_handle);
            proxy_connections.insert(proxy_info.proxy_id.clone(), tx_proxy);
        }

        // Boot the load balancer. This component forwards transactions to the consensus and proxies.
        let load_balancer_handle = LoadBalancer::<E>::new(
            executor.clone(),
            store.clone(),
            proxy_connections, // Pass the hashmap of proxy connections instead of a channel
            rx_committed_txns,
            metrics.clone(),
        )
        .spawn();
        primary_handles.push(load_balancer_handle);

        // Boot the (mock) consensus. This component delays transactions simulating consensus and
        // then forwards them to the primary executor.
        let consensus_handle = MockConsensus::new(
            config.validator_parameters.consensus_delay_model.clone(),
            config.validator_parameters.consensus_parameters.clone(),
            rx_client_transactions,
            tx_committed_txns,
        )
        .spawn();

        // Boot the client transactions server. This component receives client transactions from the
        // the network and forwards them to the load balancer.
        let client_port = config.client_server_address.port();
        let localhost = std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED);
        let client_server_address = std::net::SocketAddr::new(localhost, client_port);
        let transactions_network_handle = NetworkServer::new(
            client_server_address,
            tx_client_connections,
            tx_client_transactions,
        )
        .spawn();
        network_handles.push(transactions_network_handle);

        Self {
            phantom_data: PhantomData,
            primary_handles,
            consensus_handle,
            network_handles,
            rx_client_connections,
            metrics,
        }
    }

    /// Collect the results from the validator.
    pub async fn collect_results(mut self)
    where
        <E as Executor>::Transaction: std::fmt::Debug,
    {
        // Collect client connections.
        // TODO: In a real system, these connections would be used to reply to the clients, acknowledging
        // the receipt of the transaction and its final execution status.
        let mut client_connections = Vec::new();

        loop {
            tokio::select! {
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
        config::{BenchmarkParameters, ValidatorConfig, ValidatorParameters},
        executor::sui::SuiExecutor,
        metrics::Metrics,
        primary::node::PrimaryNode,
    };

    #[tokio::test]
    async fn execute_transactions() {
        let config = ValidatorConfig::new_for_tests();
        let benchmark_config = BenchmarkParameters::new_for_tests();

        // Create a Sui executor.
        let executor = SuiExecutor::new(&benchmark_config).await;

        // Start the validator.
        let validator_metrics = Arc::new(Metrics::new_for_tests());
        let _primary = PrimaryNode::start(executor, &config, validator_metrics).await;
        tokio::task::yield_now().await;

        // Generate transactions.
        let mut load_generator =
            LoadGenerator::<SuiExecutor>::new(benchmark_config, config.client_server_address);

        let transactions = load_generator.initialize().await;
        load_generator.run(transactions).await;
    }

    #[tokio::test]
    async fn no_proxies() {
        let validator_parameters = ValidatorParameters::new_for_tests();
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
        let _validator = PrimaryNode::start(executor.clone(), &config, validator_metrics).await;
        tokio::task::yield_now().await;

        // Generate transactions.
        let mut load_generator =
            LoadGenerator::<SuiExecutor>::new(benchmark_config, primary_address);

        let transactions = load_generator.initialize().await;
        load_generator.run(transactions).await;
    }
}
