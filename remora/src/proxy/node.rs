// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, io, marker::PhantomData, sync::Arc};
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};

use super::core::{ProxyCore, ProxyId};
use crate::{
    config::{ValidatorConfig, DEFAULT_CHANNEL_SIZE},
    error::NodeResult,
    executor::api::{Executor, ProxyToProxyMessage},
    metrics::Metrics,
    networking::{client::NetworkClient, server::NetworkServer},
};
use dashmap::DashMap;

pub struct ProxyNode<E: Executor> {
    pub phantom_data: PhantomData<E>,
    /// The handles for the core components.
    core_handles: Vec<JoinHandle<NodeResult<()>>>,
    /// The handle for the network client.
    _network_handles: Vec<JoinHandle<io::Result<()>>>,
    /// The  metrics for the proxy
    _metrics: Arc<Metrics>,
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
                tx_inter_proxy_replies.insert(proxy_info.proxy_id.clone(), tx_replies);

                // ignore the outgoing direction
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

        // communication for distributed transactions from the load balancer in the primary
        let network_handle = NetworkClient::new(
            config.proxy_server_address,
            tx_transactions,
            rx_proxy_results,
        )
        .spawn();
        network_handles.push(network_handle);

        // ignore the outgoing direction
        let (tx_placeholder, _) =
            mpsc::channel::<Sender<ProxyToProxyMessage>>(DEFAULT_CHANNEL_SIZE);
        // Create a server that listens for connections from other proxies
        let inter_proxy_server_handle = NetworkServer::new(
            our_proxy_info.listen_address,
            tx_placeholder,
            tx_inter_proxy_requests.clone(),
        )
        .spawn();
        network_handles.push(inter_proxy_server_handle);

        Self {
            phantom_data: PhantomData,
            core_handles,
            _network_handles: network_handles,
            _metrics: metrics,
        }
    }

    /// Collect the results from the validator.
    pub fn await_completion(self) {
        for handle in self.core_handles {
            // tokio::task::JoinHandle requires awaiting in an async context
            match tokio::runtime::Handle::current().block_on(handle) {
                Ok(Ok(_)) => println!("Thread completed successfully!"),
                Ok(Err(e)) => println!("Thread failed with error: {:?}", e),
                Err(e) => println!("Thread panicked: {:?}", e),
            }
        }
    }
}
