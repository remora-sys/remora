// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::{
        api::{ExecutableTransaction, ExecutionResults, Executor, Store, Transaction},
        dependency_controller::DependencyController,
    },
    metrics::Metrics,
};

pub type ProxyId = String;

#[derive(Clone, Copy)]
pub enum ProxyMode {
    SingleThreaded,
    MultiThreaded,
}

/// A proxy is responsible for pre-executing transactions.
pub struct ProxyCore<E: Executor> {
    /// The ID of the proxy.
    id: ProxyId,
    /// The executor for the transactions.
    executor: E,
    /// The mode of proxy (parallel or sequential).
    mode: ProxyMode,
    /// The object store.
    store: Store<E>,
    /// The receiver for transactions.
    rx_transactions: Receiver<Transaction<E>>,
    /// The sender for transactions with results.
    tx_results: Sender<ExecutionResults<E>>,
    /// The dependency controller for multi-core tx execution.
    dependency_controller: Option<DependencyController>,
    /// The  metrics for the proxy
    metrics: Arc<Metrics>,
}

impl<E: Executor> ProxyCore<E> {
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: E,
        mode: ProxyMode,
        store: Store<E>,
        rx_transactions: Receiver<Transaction<E>>,
        tx_results: Sender<ExecutionResults<E>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let dependency_controller = match mode {
            ProxyMode::MultiThreaded => Some(DependencyController::new()),
            ProxyMode::SingleThreaded => None,
        };

        Self {
            id,
            executor,
            mode,
            store,
            rx_transactions,
            tx_results,
            dependency_controller,
            metrics,
        }
    }

    /// Run the proxy.
    pub async fn run(&mut self) -> NodeResult<()>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        Transaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
    {
        tracing::info!("Proxy {} started", self.id);
        match self.mode {
            ProxyMode::SingleThreaded => {
                while let Some(transaction) = self.rx_transactions.recv().await {
                    self.metrics.increase_proxy_load(&self.id);
                    let execution_result =
                        E::execute(self.executor.context(), self.store.clone(), &transaction).await;
                    self.metrics.decrease_proxy_load(&self.id);
                    if self.tx_results.send(execution_result).await.is_err() {
                        tracing::warn!(
                            "Failed to send execution result, stopping proxy {}",
                            self.id
                        );
                        break;
                    }
                }
            }
            ProxyMode::MultiThreaded => {
                let mut task_id = 0;
                let ctx = self.executor.context();
                while let Some(transaction) = self.rx_transactions.recv().await {
                    self.metrics.increase_proxy_load(&self.id);
                    task_id += 1;
                    let (prior_handles, current_handles) = self
                        .dependency_controller
                        .as_mut()
                        .expect("DependencyController should be initialized")
                        .get_dependencies(task_id, transaction.input_object_ids());

                    let store = self.store.clone();
                    let id = self.id.clone();
                    let tx_results = self.tx_results.clone();
                    let ctx = ctx.clone();
                    let metrics = self.metrics.clone();
                    tokio::spawn(async move {
                        for prior_notify in prior_handles {
                            prior_notify.notified().await;
                        }

                        let execution_result = E::execute(ctx, store, &transaction).await;

                        for notify in current_handles {
                            notify.notify_one();
                        }

                        tx_results
                            .send(execution_result)
                            .await
                            .map_err(|_| NodeError::ShuttingDown)?;
                        metrics.decrease_proxy_load(&id);
                        Ok::<_, NodeError>(())
                    });
                }
            }
        }
        Ok(())
    }

    /// Spawn the proxy in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        Transaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::sui::{generate_transactions, SuiExecutor, SuiTransaction},
        metrics::Metrics,
        proxy::core::{ProxyCore, ProxyMode},
    };

    async fn pre_execute(mode: ProxyMode) {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        let store = Arc::new(executor.create_in_memory_store());
        let metrics = Arc::new(Metrics::new_for_tests());
        let proxy_id = "0".to_string();
        let proxy = ProxyCore::new(
            proxy_id, executor, mode, store, rx_proxy, tx_results, metrics,
        );

        // Send transactions to the proxy.
        let transactions = generate_transactions(&config).await;
        for tx in transactions {
            let transaction = SuiTransaction::new_for_tests(tx);
            tx_proxy.send(transaction).await.unwrap();
        }

        // Spawn the proxy.
        proxy.spawn();

        // Receive the results.
        let results = rx_results.recv().await.unwrap();
        assert!(results.success());
    }

    #[tokio::test]
    async fn test_single_threaded_proxy() {
        pre_execute(ProxyMode::SingleThreaded).await;
    }

    #[tokio::test]
    async fn test_multi_threaded_proxy() {
        pre_execute(ProxyMode::MultiThreaded).await;
    }
}
