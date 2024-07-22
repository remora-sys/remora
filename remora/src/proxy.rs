// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    executor::{Executor, SuiExecutor, SuiTransactionWithTimestamp},
    types::TransactionWithResults,
};

pub type ProxyId = usize;

/// A proxy is responsible for pre-executing transactions.
pub struct Proxy {
    /// The ID of the proxy.
    id: ProxyId,
    /// The executor for the transactions.
    executor: SuiExecutor,
    /// The receiver for transactions.
    rx_transactions: Receiver<SuiTransactionWithTimestamp>,
    /// The sender for transactions with results.
    tx_results: Sender<TransactionWithResults>,
}

impl Proxy {
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: SuiExecutor,
        rx_transactions: Receiver<SuiTransactionWithTimestamp>,
        tx_results: Sender<TransactionWithResults>,
    ) -> Self {
        Self {
            id,
            executor,
            rx_transactions,
            tx_results,
        }
    }

    /// Pre-execute a transaction.
    // TODO: Naive single-threaded execution.
    async fn pre_execute(
        &mut self,
        transaction: SuiTransactionWithTimestamp,
    ) -> TransactionWithResults {
        self.executor.execute(transaction).await
    }

    /// Run the proxy.
    pub async fn run(&mut self) {
        tracing::info!("Proxy {} started", self.id);

        while let Some(transaction) = self.rx_transactions.recv().await {
            let execution_result = self.pre_execute(transaction).await;
            if self.tx_results.send(execution_result).await.is_err() {
                tracing::warn!(
                    "Failed to send execution result, stopping proxy {}",
                    self.id
                );
                break;
            }
        }
    }

    /// Spawn the proxy in a new task.
    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }
}

#[cfg(test)]
mod tests {

    use tokio::sync::mpsc;

    use super::*;
    use crate::config::BenchmarkConfig;

    #[tokio::test]
    async fn pre_execute() {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let config = BenchmarkConfig::new_for_tests();
        let mut executor = SuiExecutor::new(&config).await;
        let transactions = executor.generate_transactions().await;
        let proxy = Proxy::new(0, executor, rx_proxy, tx_results);

        // Send transactions to the proxy.
        for tx in transactions {
            let transaction = SuiTransactionWithTimestamp::new_for_tests(tx);
            tx_proxy.send(transaction).await.unwrap();
        }

        // Spawn the proxy.
        proxy.spawn();

        // Receive the results.
        let results = rx_results.recv().await.unwrap();
        assert!(results.success());
    }
}
