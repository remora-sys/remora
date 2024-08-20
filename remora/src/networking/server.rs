// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io;

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    net::TcpListener,
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};

use crate::{config::ValidatorConfig, networking::worker::ConnectionWorker};

/// The size of the server worker channel.
pub const WORKER_CHANNEL_SIZE: usize = 1000;

/// A server run by the primary machine that listens for connections from proxies.
/// When a new proxy connects, a new worker is spawned. The server leverages the
/// bidirectional channel opened by the proxy to send transactions to the proxy.
/// The server also listens for proxy results and forwards them to the primary.
pub struct NetworkServer<T, R> {
    /// The configuration for the validator.
    config: ValidatorConfig,
    /// The sender for proxy connections. When a new proxy connects, this channel
    /// is used to propagate a sender to the load balancer, which will use it
    /// to send transactions through the network.
    tx_proxy_connections: Sender<Sender<T>>,
    /// The sender for proxy results received from the network.
    tx_proxy_results: Sender<R>,
}

impl<T, R> NetworkServer<T, R>
where
    T: Send + Serialize + 'static,
    R: Send + DeserializeOwned + 'static,
{
    /// Create a new server.
    pub fn new(
        config: ValidatorConfig,
        tx_proxy_connections: Sender<Sender<T>>,
        tx_proxy_results: Sender<R>,
    ) -> Self {
        Self {
            config,
            tx_proxy_connections,
            tx_proxy_results,
        }
    }

    /// Run the server.
    pub async fn run(&self) -> io::Result<()> {
        let server = TcpListener::bind(self.config.validator_address).await?;
        tracing::debug!("Listening on {}", self.config.validator_address);

        loop {
            let (stream, peer) = server.accept().await?;
            stream.set_nodelay(true)?;
            tracing::info!("Accepted connection from proxy {peer}");

            // Spawn a worker to handle the connection.
            let (tx_transactions, rx_transactions) = mpsc::channel(WORKER_CHANNEL_SIZE);
            let tx_proxy_results = self.tx_proxy_results.clone();
            let worker = ConnectionWorker::new(stream, tx_proxy_results, rx_transactions);
            let _handle = worker.spawn();

            // Notify the load balancer that a new proxy has connected.
            let result = self.tx_proxy_connections.send(tx_transactions).await;
            if result.is_err() {
                tracing::warn!("Failed to send proxy connection to load balancer, stopping server");
                break Ok(());
            }
        }
    }

    /// Spawn the server in a new task.
    pub fn spawn(self) -> JoinHandle<io::Result<()>> {
        tokio::spawn(async move { self.run().await })
    }
}
