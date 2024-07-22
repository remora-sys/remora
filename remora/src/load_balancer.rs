// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{executor::SuiTransactionWithTimestamp, proxy::ProxyId};

pub struct LoadBalancer {
    rx_transactions: Receiver<SuiTransactionWithTimestamp>,
    tx_consensus: Sender<SuiTransactionWithTimestamp>,
    tx_proxies: Vec<Sender<SuiTransactionWithTimestamp>>,
}

impl LoadBalancer {
    pub fn new(
        rx_transactions: Receiver<SuiTransactionWithTimestamp>,
        tx_consensus: Sender<SuiTransactionWithTimestamp>,
        tx_proxies: Vec<Sender<SuiTransactionWithTimestamp>>,
    ) -> Self {
        Self {
            rx_transactions,
            tx_consensus,
            tx_proxies,
        }
    }

    async fn try_other_proxies(&self, failed: ProxyId, transaction: SuiTransactionWithTimestamp) {
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

    pub async fn run(&mut self) {
        tracing::info!("Load balancer started");

        let mut i = 0;
        while let Some(transaction) = self.rx_transactions.recv().await {
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

    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }
}
