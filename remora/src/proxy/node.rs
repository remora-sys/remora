// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{de::DeserializeOwned, Serialize};
use std::{io, marker::PhantomData, sync::Arc};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    config::{ValidatorConfig, DEFAULT_CHANNEL_SIZE},
    error::NodeResult,
    executor::api::{ExecutionResults, Executor, PrimaryToProxyMessage, ProxyToProxyMessage},
    metrics::Metrics,
    networking::{client::NetworkClient, server::NetworkServer},
    proxy::core::{ProxyCore, ProxyId},
};
use dashmap::DashMap;

pub struct ProxyNode<E: Executor> {
    pub phantom_data: PhantomData<E>,
    /// The handles for the core components.
    core_handles: Vec<JoinHandle<NodeResult<()>>>,
    /// The receiver for the proxy results.
    rx_proxy_results: Receiver<ExecutionResults<E>>,
    /// The handle for the network client.
    _network_handles: Vec<JoinHandle<io::Result<()>>>,
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

        // Find our proxy info from the config
        let our_proxy_info = config
            .proxies
            .iter()
            .find(|p| p.proxy_id == id)
            .expect("Could not find our proxy in the config");

        // Create connections to other proxies
        for proxy_info in &config.proxies {
            // Skip creating a connection to self
            if proxy_info.proxy_id != id {
                let (tx_replies, rx_replies) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
                tx_inter_proxy_replies.insert(proxy_info.proxy_id.clone(), tx_replies.clone());

                // Keep both ends of the channel to avoid dropping
                let (tx_placeholder, _) =
                    mpsc::channel::<ProxyToProxyMessage>(DEFAULT_CHANNEL_SIZE);

                // Create network client to connect to other proxy
                let client_handle =
                    NetworkClient::new(proxy_info.listen_address, tx_placeholder, rx_replies)
                        .spawn();

                network_handles.push(client_handle);
            }
        }

        let store = Arc::new(executor.init_store());
        let core_handle = ProxyCore::new(
            id,
            executor.clone(),
            store,
            rx_transactions,
            tx_proxy_results,
            rx_inter_proxy_requests,
            tx_inter_proxy_replies,
            metrics.clone(),
        )
        .spawn();
        core_handles.push(core_handle);

        // Create a proper channel for new connections from other proxies
        let (tx_connections, _) =
            mpsc::channel::<Sender<ProxyToProxyMessage>>(DEFAULT_CHANNEL_SIZE);
        // Create a server that listens for connections from other proxies
        let inter_proxy_server_handle = NetworkServer::new(
            our_proxy_info.listen_address,
            tx_connections,
            tx_inter_proxy_requests.clone(),
        )
        .spawn();
        network_handles.push(inter_proxy_server_handle);

        let (_, rx_placeholder) = mpsc::channel::<
            PrimaryToProxyMessage<<E as Executor>::Transaction>,
        >(DEFAULT_CHANNEL_SIZE);
        let network_handle =
            NetworkClient::new(config.proxy_server_address, tx_transactions, rx_placeholder)
                .spawn();
        network_handles.push(network_handle);

        Self {
            phantom_data: PhantomData,
            core_handles,
            rx_proxy_results,
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
            futures::future::join_all(core_handles).await;
            tracing::info!("All core tasks have completed");
        });

        // Process results until core tasks complete
        while let Some(result) = tokio::select! {
            result = self.rx_proxy_results.recv() => result,
            _ = &mut core_completion => None,
        } {
            // tracing::debug!("Received output: {:?}", result);
            assert!(result.success());
            let submit_timestamp = result.transaction_timestamp();
            // TODO: Record transactions success and failure.
            self.metrics.update_metrics(submit_timestamp);
        }

        // Ensure core_completion task is done
        if let Err(e) = core_completion.await {
            tracing::error!("Error in core completion task: {:?}", e);
        }
    }
}
