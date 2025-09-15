// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use serde::{de::DeserializeOwned, Serialize};
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

use crate::{
    config::{ValidatorConfig, DEFAULT_CHANNEL_SIZE},
    error::NodeResult,
    executor::api::{
        ExecutionResults, Executor, PrimaryToProxyMessage, ProxyToPrimaryMessage,
        ProxyToProxyMessage,
    },
    metrics::Metrics,
    networking::{client::NetworkClient, server::NetworkServer},
    proxy::core::{ProxyCore, ProxyId},
};

pub struct ProxyNode<E: Executor> {
    pub phantom_data: PhantomData<E>,
    /// The handles for the core components.
    core_handles: Vec<Vec<JoinHandle<NodeResult<()>>>>,
    /// The receiver for the proxy results.
    rx_proxy_results: Receiver<ExecutionResults<E>>,
    /// The handle for the network client.
    _network_handles: Vec<JoinHandle<io::Result<()>>>,
    /// The handles for the connection listeners.
    _connection_listener_handles: Vec<JoinHandle<()>>,
    /// The metrics for the proxy
    metrics: Arc<Metrics>,
}

impl<E: Executor + Send + Sync + 'static> ProxyNode<E> {
    pub async fn start(
        proxy_id: ProxyId,
        executor: E,
        config: &ValidatorConfig,
        metrics: Arc<Metrics>,
    ) -> Self
    where
        <E as Executor>::Store: Sync + Send,
        <E as Executor>::Transaction: Send + Sync + 'static,
        <E as Executor>::ExecutionContext: Send + Sync,
        <E as Executor>::ExecutionResults: Send + Sync + Serialize + DeserializeOwned,
    {
        let mut core_handles = Vec::new();
        let mut network_handles = Vec::new();

        let id = proxy_id;
        let (tx_transactions, rx_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        // Create channels for inter-proxy communication
        let (tx_inter_proxy_requests, rx_inter_proxy_requests) =
            mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let tx_inter_proxy_replies = Arc::new(DashMap::new());

        // Create a network client to send replies to the primary (snapshots)
        let (tx_primary_replies, rx_primary_replies) =
            mpsc::channel::<ProxyToPrimaryMessage>(DEFAULT_CHANNEL_SIZE);

        // Find our proxy info from the config
        let our_proxy_info = config
            .proxies
            .iter()
            .find(|p| p.proxy_id == id)
            .expect("Could not find our proxy in the config");

        // Create a proper channel for new connections from other proxies
        let (tx_connections, mut rx_connections) =
            mpsc::channel::<Sender<ProxyToProxyMessage>>(DEFAULT_CHANNEL_SIZE);

        // Use configured addresses directly
        let listen_proxy_address = our_proxy_info.listen_proxy_address;
        let listen_primary_address = our_proxy_info.listen_primary_address;

        // Bind servers to 0.0.0.0 to accept connections from external IPs
        let bind_proxy_address = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            listen_proxy_address.port(),
        );
        let bind_primary_address = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            listen_primary_address.port(),
        );

        // Create a server that listens for connections from other proxies
        let inter_proxy_server_handle = NetworkServer::new(
            bind_proxy_address,
            tx_connections.clone(),
            tx_inter_proxy_requests.clone(),
        )
        .spawn();
        network_handles.push(inter_proxy_server_handle);

        // Spawn a task to handle incoming proxy connections
        let connection_handle = tokio::spawn(async move {
            let mut keepalive = Vec::new();
            while let Some(new_connection) = rx_connections.recv().await {
                tracing::info!("Received new proxy connection");
                // Store the connection sender to keep the connection alive.
                keepalive.push(new_connection);
            }
        });
        let mut connection_listener_handles = vec![connection_handle];

        let (tx_primary_connection, mut rx_primary_connection) = mpsc::channel::<
            Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
        >(DEFAULT_CHANNEL_SIZE);

        // Create a server that listens for connections from the primary
        let primary_connection_handle =
            NetworkServer::new(bind_primary_address, tx_primary_connection, tx_transactions)
                .spawn();
        network_handles.push(primary_connection_handle);

        // Drain primary connection notifications to keep the server alive
        let primary_conn_drain_handle = tokio::spawn(async move {
            let mut keepalive = Vec::new();
            while let Some(new_connection) = rx_primary_connection.recv().await {
                keepalive.push(new_connection);
            }
        });
        connection_listener_handles.push(primary_conn_drain_handle);

        // Outbound client from proxy to primary to send snapshots
        let (tx_unused, _rx_unused) = mpsc::channel::<Vec<u8>>(DEFAULT_CHANNEL_SIZE);
        let snapshot_client_handle =
            NetworkClient::<Vec<u8>, Vec<u8>>::new(config.proxy_server_address, tx_unused, {
                // Serialize messages to bytes for the network worker
                let (tx_bytes, rx_bytes) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
                let mut rx_msgs = rx_primary_replies;
                tokio::spawn(async move {
                    while let Some(msg) = rx_msgs.recv().await {
                        if let Ok(bytes) = bincode::serialize(&msg) {
                            let _ = tx_bytes.send(bytes).await;
                        }
                    }
                });
                rx_bytes
            })
            .spawn();
        network_handles.push(snapshot_client_handle);

        // Create connections to other proxies
        for proxy_info in &config.proxies {
            // Skip creating a connection to self
            if proxy_info.proxy_id != id {
                let (tx_replies, rx_replies) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
                tx_inter_proxy_replies.insert(proxy_info.proxy_id, tx_replies.clone());

                // Keep both ends of the channel to avoid dropping
                let (tx_placeholder, _) =
                    mpsc::channel::<ProxyToProxyMessage>(DEFAULT_CHANNEL_SIZE);

                // Create network client to connect to other proxy
                let client_handle =
                    NetworkClient::new(proxy_info.listen_proxy_address, tx_placeholder, rx_replies)
                        .spawn();

                network_handles.push(client_handle);
            }
        }

        let store = executor.init_store();
        let core_handle = ProxyCore::new(
            id,
            executor.clone(),
            store,
            rx_transactions,
            tx_proxy_results,
            rx_inter_proxy_requests,
            tx_inter_proxy_replies,
            tx_primary_replies,
            config.validator_parameters.proxy_mode,
            metrics.clone(),
        )
        .spawn();
        core_handles.push(core_handle);

        Self {
            phantom_data: PhantomData,
            core_handles,
            rx_proxy_results,
            _connection_listener_handles: connection_listener_handles,
            _network_handles: network_handles,
            metrics,
        }
    }

    /// Collect the results from the validator and wait for core handles to complete.
    pub async fn await_completion(&mut self) {
        // Take ownership of the core_handles to avoid ownership issues
        let core_handles = std::mem::take(&mut self.core_handles);

        // Spawn a task to wait for all core handles to complete
        let mut core_completion = tokio::spawn(async move {
            for core_handle in core_handles {
                futures::future::join_all(core_handle).await;
            }
            tracing::info!("All core tasks have completed");
        });

        // Process results until core tasks complete
        let mut sampled_results_count: u64 = 0;
        while let Some(result) = tokio::select! {
            result = self.rx_proxy_results.recv() => result,
            _ = &mut core_completion => None,
        } {
            // tracing::debug!("Received output: {:?}", result);
            assert!(result.success());
            let submit_timestamp = result.transaction_timestamp();
            // TODO: Record transactions success and failure.
            self.metrics.update_metrics(submit_timestamp, "default");

            // Emit latency time-series every 10 results to reduce log volume
            sampled_results_count += 1;
            if sampled_results_count % 10 == 0 {
                let now_secs = Metrics::now().as_secs_f64();
                let latency_ms = (now_secs - submit_timestamp) * 1000.0;
                tracing::info!("ts={:.6} latency_ms={:.2}", now_secs, latency_ms);
            }
        }

        // Ensure core_completion task is done
        if let Err(e) = core_completion.await {
            tracing::error!("Error in core completion task: {:?}", e);
        }
    }
}
