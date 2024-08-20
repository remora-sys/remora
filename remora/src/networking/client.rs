// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    net::TcpSocket,
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};

use crate::{config::ValidatorConfig, networking::worker::ConnectionWorker};

/// A client ran by the proxy to connects to the primary machine and receive transactions
/// to process. The client also sends the results of the transactions back to the primary.
pub struct NetworkClient<T, R> {
    /// The configuration for the validator.
    config: ValidatorConfig,
    /// The sender for transactions received from the network.
    tx_transactions: Sender<T>,
    /// The receiver for proxy results to send through the network.
    rx_proxy_results: Receiver<R>,
}

impl<T, R> NetworkClient<T, R>
where
    T: Send + DeserializeOwned,
    R: Send + Serialize,
{
    /// Create a new client.
    pub fn new(
        config: ValidatorConfig,
        tx_transactions: Sender<T>,
        rx_proxy_results: Receiver<R>,
    ) -> Self {
        Self {
            config,
            tx_transactions,
            rx_proxy_results,
        }
    }

    /// Run the client.
    pub async fn run(self) -> io::Result<()> {
        tracing::info!(
            "Trying to connect to primary {}",
            self.config.validator_address
        );

        let stream = loop {
            let socket = if self.config.validator_address.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };

            match socket.connect(self.config.validator_address).await {
                Ok(stream) => break stream,
                Err(e) => {
                    tracing::info!("Failed to connect to primary (retrying): {e}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        };
        tracing::info!("Connected to {}", self.config.validator_address);

        let worker = ConnectionWorker::new(stream, self.tx_transactions, self.rx_proxy_results);
        worker.run().await;
        Ok(())
    }
}

impl<T, R> NetworkClient<T, R>
where
    T: Send + DeserializeOwned + 'static,
    R: Send + Serialize + 'static,
{
    /// Spawn the client in a new task.
    pub fn spawn(self) -> JoinHandle<io::Result<()>> {
        tokio::spawn(async move { self.run().await })
    }
}
