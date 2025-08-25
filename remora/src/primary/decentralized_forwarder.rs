// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::ProxyMode,
    error::NodeResult,
    executor::api::{Executor, PrimaryToProxyMessage, RemoraTransaction, TransactionWithTimestamp},
    metrics::Metrics,
    proxy::core::ProxyId,
};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

/// Decentralized forwarder that broadcasts transaction batches to all proxies
/// instead of making centralized routing decisions
pub struct DecentralizedForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub proxy_mode: ProxyMode,
    pub metrics: Arc<Metrics>,
    pub batch_sequence: u64, // For deterministic ordering
}

impl<E> DecentralizedForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    /// Create a new decentralized forwarder
    pub fn new(
        proxy_connections: Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        proxy_mode: ProxyMode,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            proxy_connections,
            proxy_mode,
            metrics,
            batch_sequence: 0,
        }
    }

    /// Process committed transactions by broadcasting them to all proxies
    pub async fn process_committed_transactions(
        &mut self,
        mut rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    ) {
        let mut txn_cnt = 0;

        while let Some(transactions) = rx_committed_txns.recv().await {
            txn_cnt += 1;
            if txn_cnt == 1 {
                self.metrics.register_start_time();
            }

            // Convert RemoraTransaction<E> to TransactionWithTimestamp<E::Transaction>
            let transaction_batch: Vec<_> = transactions
                .into_iter()
                .map(|tx| tx) // RemoraTransaction<E> is already TransactionWithTimestamp<E::Transaction>
                .collect();

            // Increment batch sequence for determinism
            self.batch_sequence += 1;

            // Broadcast the transaction batch to all proxies
            self.broadcast_transaction_batch(transaction_batch, self.batch_sequence)
                .await;
        }
    }

    /// Broadcast a transaction batch to all connected proxies
    async fn broadcast_transaction_batch(
        &self,
        transactions: Vec<TransactionWithTimestamp<E::Transaction>>,
        batch_sequence: u64,
    ) {
        let batch_message = PrimaryToProxyMessage::TransactionBatch(transactions, batch_sequence);

        // Send to all proxies concurrently
        let mut send_tasks = Vec::new();

        for proxy_entry in self.proxy_connections.iter() {
            let proxy_id = *proxy_entry.key();
            let proxy_sender = proxy_entry.value().clone();
            let message = batch_message.clone();

            let task = tokio::spawn(async move {
                if let Err(e) = proxy_sender.send(message).await {
                    tracing::warn!(
                        "Failed to send transaction batch to proxy {}: {:?}",
                        proxy_id,
                        e
                    );
                } else {
                    tracing::debug!(
                        "Sent transaction batch {} to proxy {}",
                        batch_sequence,
                        proxy_id
                    );
                }
            });

            send_tasks.push(task);
        }

        tracing::debug!(
            "Broadcasted transaction batch {} to {} proxies",
            batch_sequence,
            self.proxy_connections.len()
        );
    }

    /// Spawn the decentralized forwarder in a new task
    pub fn spawn(
        mut self,
        rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    ) -> tokio::task::JoinHandle<NodeResult<()>> {
        tokio::spawn(async move {
            self.process_committed_transactions(rx_committed_txns).await;
            Ok(())
        })
    }
}
