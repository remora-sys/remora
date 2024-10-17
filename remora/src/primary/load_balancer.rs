// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::api::{Executor, Transaction},
    metrics::Metrics,
};

/// A load balancer is responsible for distributing transactions to the consensus and proxies.
pub struct LoadBalancer<E: Executor> {
    /// The receiver for transactions.
    rx_transactions: Receiver<Transaction<E>>,
    /// The sender to forward transactions to the consensus.
    tx_consensus: Sender<Transaction<E>>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: usize,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        rx_transactions: Receiver<Transaction<E>>,
        tx_consensus: Sender<Transaction<E>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            rx_transactions,
            tx_consensus,
            index: 0,
            metrics,
        }
    }

    /// Forward a transaction to the consensus and proxies.
    async fn forward_transaction(&mut self, transaction: Transaction<E>) -> NodeResult<()> {
        if self.index == 0 {
            self.metrics.register_start_time();
        }

        // Send the transaction to the consensus.
        self.tx_consensus
            .send(transaction.clone())
            .await
            .map_err(|_| NodeError::ShuttingDown)?;

        Ok(())
    }

    /// Run the load balancer.
    pub async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Load balancer started");
        loop {
            tokio::select! {
                Some(transaction) = self.rx_transactions.recv() => self
                    .forward_transaction(transaction)
                    .await
                    .map_err(|_| NodeError::ShuttingDown)?,
                                else => Err(NodeError::ShuttingDown)?
            }
        }
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Transaction<E>: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}
