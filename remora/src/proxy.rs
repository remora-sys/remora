// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::executor::{Executor, TransactionWithTimestamp};

pub type ProxyId = usize;

/// A proxy is responsible for pre-executing transactions.
pub struct Proxy<E: Executor> {
    /// The ID of the proxy.
    id: ProxyId,
    /// The executor for the transactions.
    executor: E,
    /// The object store.
    store: E::Store,
    /// The receiver for transactions.
    rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
    /// The sender for transactions with results.
    tx_results: Sender<E::TransactionResults>,
}

impl<E: Executor> Proxy<E> {
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: E,
        store: E::Store,
        rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
        tx_results: Sender<E::TransactionResults>,
    ) -> Self {
        Self {
            id,
            executor,
            store,
            rx_transactions,
            tx_results,
        }
    }

    /// Pre-execute a transaction.
    // TODO: Naive single-threaded execution.
    async fn pre_execute(
        &mut self,
        transaction: &TransactionWithTimestamp<E::Transaction>,
    ) -> E::TransactionResults {
        self.executor.execute(&self.store, transaction).await
    }

    /// Run the proxy.
    pub async fn run(&mut self) {
        tracing::info!("Proxy {} started", self.id);

        while let Some(transaction) = self.rx_transactions.recv().await {
            let execution_result = self.pre_execute(&transaction).await;
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
    pub fn spawn(mut self) -> JoinHandle<()>
    where
        E: Executor + Send + Sync + 'static,
        <E as Executor>::TransactionResults: Send,
        <E as Executor>::Store: Send,
        TransactionWithTimestamp<<E as Executor>::Transaction>: Send + Sync,
    {
        tokio::spawn(async move {
            self.run().await;
        })
    }
}

#[cfg(test)]
mod tests {

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkConfig,
        executor::{Executor, SuiExecutor, SuiTransactionWithTimestamp},
        proxy::Proxy,
    };

    #[tokio::test]
    async fn pre_execute() {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let config = BenchmarkConfig::new_for_tests();
        let mut executor = SuiExecutor::new(&config).await;
        let store = executor.create_in_memory_store();
        let transactions = executor.generate_transactions().await;
        let proxy = Proxy::new(0, executor, store, rx_proxy, tx_results);

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
