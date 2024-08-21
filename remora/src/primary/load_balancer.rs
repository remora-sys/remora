// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::metrics::Metrics;

/// A load balancer is responsible for distributing transactions to the consensus and proxies.
pub struct LoadBalancer<T> {
    /// The receiver for transactions.
    rx_transactions: Receiver<T>,
    /// The sender to forward transactions to the consensus.
    tx_consensus: Sender<T>,
    /// Receive handles to forward transactions to proxies. When a new client connects,
    /// this channel receives a sender from the network layer which is used to forward
    /// transactions to the proxies.
    rx_proxy_connections: Receiver<Sender<T>>,
    /// Holds senders to forward transactions to proxies.
    proxy_connections: Vec<Sender<T>>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: usize,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

impl<T: Clone> LoadBalancer<T> {
    /// Create a new load balancer.
    pub fn new(
        rx_transactions: Receiver<T>,
        tx_consensus: Sender<T>,
        rx_proxy_connections: Receiver<Sender<T>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            rx_transactions,
            tx_consensus,
            rx_proxy_connections,
            proxy_connections: Vec::new(),
            index: 0,
            metrics,
        }
    }

    /// Forward a transaction to the consensus and proxies.
    async fn forward_transaction(&mut self, transaction: T) -> Option<()> {
        if self.index == 0 {
            self.metrics.register_start_time();
        }

        // Send the transaction to the consensus.
        if self.tx_consensus.send(transaction.clone()).await.is_err() {
            tracing::warn!("Failed to send transaction to consensus, stopping load balancer");
            return None;
        }

        // Send the transaction to the proxies. If the connection to a proxy fails, remove it
        // from the list of connections and try with another proxy.
        while !self.proxy_connections.is_empty() {
            let i = self.index % self.proxy_connections.len();
            let proxy = &self.proxy_connections[i];
            match proxy.send(transaction.clone()).await {
                Ok(()) => {
                    tracing::debug!("Sent transaction to proxy");
                    self.index += 1;
                    break;
                }
                Err(_) => {
                    tracing::warn!("Failed to send transaction to proxy, trying other proxies");
                    self.proxy_connections.swap_remove(i);
                }
            }
        }
        Some(())
    }

    /// Run the load balancer.
    pub async fn run(&mut self) {
        tracing::info!("Load balancer started");
        loop {
            tokio::select! {
                Some(transaction) = self.rx_transactions.recv() => {
                    if self.forward_transaction(transaction).await.is_none() {
                        break;
                    }
                },
                Some(connection) = self.rx_proxy_connections.recv() => {
                    self.proxy_connections.push(connection);
                    tracing::info!("Added a new proxy connection");
                }
                else => {
                    tracing::warn!("No more transactions to process, stopping load balancer");
                    break;
                }
            }
        }
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self) -> JoinHandle<()>
    where
        T: Send + 'static,
    {
        tokio::spawn(async move {
            self.run().await;
        })
    }
}
