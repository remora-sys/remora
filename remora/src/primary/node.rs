// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use serde::de::DeserializeOwned;
use std::{
    io,
    marker::PhantomData,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use super::{load_balancer::LoadBalancer, mock_consensus::MockConsensus};
use crate::checkpoint::primary::EpochManager;
use crate::checkpoint::state_collector::StateCollector;
use crate::executor::api::ProxyToPrimaryMessage;
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
    /// Epoch manager (Phase 1: in-memory only)
    pub epoch_manager: Arc<EpochManager>,
}

impl<E: Executor + Sync + Send + 'static> PrimaryNode<E> {
    /// Start the single machine validator.
    pub async fn start(_executor: E, config: &ValidatorConfig, metrics: Arc<Metrics>) -> Self
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
        let proxy_connections = Arc::new(DashMap::new());

        let mut primary_handles = Vec::new();
        let mut network_handles = Vec::new();

        // Initialize epoch manager
        let epoch_manager = Arc::new(EpochManager::new());

        // Connect to each proxy server
        for proxy_info in config.proxies.iter() {
            let (tx_proxy, rx_proxy) =
                mpsc::channel::<PrimaryToProxyMessage<E::Transaction>>(DEFAULT_CHANNEL_SIZE);

            let network_client_handle = NetworkClient::new(
                proxy_info.listen_primary_address,
                tx_proxy.clone(),
                rx_proxy,
            )
            .spawn();

            network_handles.push(network_client_handle);
            proxy_connections.insert(proxy_info.proxy_id, tx_proxy);
        }

        // Create channels for checkpoint coordination and proxy->primary snapshots
        let (tx_epoch_notify, mut rx_epoch_notify) =
            mpsc::channel::<crate::checkpoint::EpochId>(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_snapshots, rx_proxy_snapshots) =
            mpsc::channel::<Vec<u8>>(DEFAULT_CHANNEL_SIZE);

        // Spawn the state collector task
        let expected_proxies = config.proxies.len();


        // persistent storage
        /*let snapshot_path = std::path::PathBuf::from("./data/primary/snapshots");
        let snapshot_store = RocksSnapshotStore::open(snapshot_path)
            .map(Some)
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "Failed to open RocksSnapshotStore: {:?}. Continuing without persistence.",
                    e
                );
                None
            });

        let mut collector = StateCollector::new(expected_proxies);
        if let Some(store) = snapshot_store {
            collector = collector.with_store(store);
        }*/
        let collector = StateCollector::new(expected_proxies);

        // Start a server on the primary to accept proxy->primary snapshots
        let (tx_snapshot_conn, mut rx_snapshot_conn) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let primary_listen_addr = config.proxy_server_address;
        let snapshot_server_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            primary_listen_addr.port(),
        );
        let snapshot_server_handle = crate::networking::server::NetworkServer::<Vec<u8>, ()>::new(
            snapshot_server_addr,
            tx_snapshot_conn,
            tx_proxy_snapshots.clone(),
        )
        .spawn();
        network_handles.push(snapshot_server_handle);

        // Drain snapshot connection notifications to keep the server alive
        tokio::spawn(async move {
            while (rx_snapshot_conn.recv().await).is_some() {
                // Keep receiver alive
            }
        });

        // Collector task: receive epoch notifications and proxy snapshots
        let collector_handle = tokio::spawn(async move {
            let (tx_epoch_complete, mut rx_epoch_complete) =
                mpsc::channel::<crate::checkpoint::EpochId>(DEFAULT_CHANNEL_SIZE);
            let mut collector_inner = collector;
            let mut rx_snapshots = rx_proxy_snapshots;

            loop {
                tokio::select! {
                    Some(epoch) = rx_epoch_notify.recv() => {
                        tracing::info!("Collector: starting epoch {:?}", epoch);
                        collector_inner.start_epoch(epoch, tx_epoch_complete.clone());
                    }
                    Some(completed) = rx_epoch_complete.recv() => {
                        tracing::info!("Collector: epoch {:?} complete", completed);
                    }
                    Some(bytes) = rx_snapshots.recv() => {
                        match bincode::deserialize::<ProxyToPrimaryMessage>(&bytes) {
                            Ok(ProxyToPrimaryMessage::StateSnapshot(proxy_id, epoch, snapshot)) => {
                                collector_inner.process_snapshot(proxy_id, epoch, snapshot);
                            }
                            Err(e) => {
                                tracing::error!("Failed to deserialize snapshot: {:?}", e);
                            }
                        }
                    }
                    else => { break; }
                }
            }
            crate::error::NodeResult::Ok(())
        });
        primary_handles.push(collector_handle);

        // Boot the load balancer. This component forwards transactions to the consensus and proxies.
        let load_balancer_handle = LoadBalancer::<E>::new(
            proxy_connections,
            rx_committed_txns,
            config.validator_parameters.load_balancing_policy.clone(),
            config.validator_parameters.proxy_mode.clone(),
            metrics.clone(),
            tx_epoch_notify,
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
            epoch_manager,
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

        let transactions = load_generator.initialize(None).await;
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

        let transactions = load_generator.initialize(None).await;
        load_generator.run(transactions).await;
    }
}
