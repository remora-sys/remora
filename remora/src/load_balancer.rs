// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    executor::{Executor, TransactionWithTimestamp},
    metrics::Metrics,
    proxy::ProxyId,
};

/// A load balancer is responsible for distributing transactions to the consensus and proxies.
pub struct LoadBalancer<E: Executor> {
    /// The receiver for transactions.
    rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
    /// The sender to forward transactions to the consensus.
    tx_consensus: Sender<TransactionWithTimestamp<E::Transaction>>,
    /// The senders to forward transactions to proxies.
    tx_proxies: Vec<Sender<TransactionWithTimestamp<E::Transaction>>>,
}

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
        tx_consensus: Sender<TransactionWithTimestamp<E::Transaction>>,
        tx_proxies: Vec<Sender<TransactionWithTimestamp<E::Transaction>>>,
    ) -> Self {
        Self {
            rx_transactions,
            tx_consensus,
            tx_proxies,
        }
    }

    /// Try other proxies if the target proxy fails to send the transaction.
    async fn try_other_proxies(
        &self,
        failed: ProxyId,
        transaction: TransactionWithTimestamp<E::Transaction>,
    ) {
        let mut j = (failed + 1) % self.tx_proxies.len();
        loop {
            if j == failed {
                tracing::warn!("All proxies failed to send transaction");
                break;
            }

            let proxy = &self.tx_proxies[j];
            if proxy.send(transaction.clone()).await.is_ok() {
                tracing::info!("Sent transaction to proxy {j}");
                break;
            }

            j = (j + 1) % self.tx_proxies.len();
        }
    }

    /// Run the load balancer.
    pub async fn run(&mut self, metrics: Arc<Metrics>) {
        tracing::info!("Load balancer started");

        let mut i = 0;
        while let Some(transaction) = self.rx_transactions.recv().await {
            if i == 0 {
                metrics.register_start_time();
            }

            if self.tx_consensus.send(transaction.clone()).await.is_err() {
                tracing::warn!("Failed to send transaction to primary, stopping load balancer");
                break;
            }

            let proxy_id = i % self.tx_proxies.len();
            let proxy = &self.tx_proxies[proxy_id];
            match proxy.send(transaction.clone()).await {
                Ok(()) => {
                    tracing::debug!("Sent transaction to proxy {proxy_id}");
                }
                Err(_) => {
                    tracing::warn!(
                        "Failed to send transaction to proxy {proxy_id}, trying other proxies"
                    );
                    self.try_other_proxies(proxy_id, transaction).await;
                }
            }

            i += 1;
        }
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self, metrics: Arc<Metrics>) -> JoinHandle<()>
    where
        E: 'static,
        <E as Executor>::Transaction: Send,
    {
        tokio::spawn(async move {
            self.run(metrics).await;
        })
    }
}
