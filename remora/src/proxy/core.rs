// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use sui_types::transaction::InputObjectKind;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Notify,
    },
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::{
        api::{
            ExecutableTransaction,
            ExecutionResults,
            Executor,
            PrimaryToProxyMessage,
            RemoraTransaction,
            StateStore,
            Store,
        },
        dependency_controller::DependencyController, sui::get_object_ids_for_dependency_tracking,
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
    rx_transactions: Receiver<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
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
        rx_transactions: Receiver<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
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
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        tracing::info!("Proxy {} started", self.id);
        match self.mode {
            ProxyMode::SingleThreaded => {
                while let Some(message) = self.rx_transactions.recv().await {
                    match message {
                        PrimaryToProxyMessage::Txn(transaction) => {
                            self.metrics.increase_proxy_load(&self.id);
                            let execution_result = E::execute(
                                self.executor.context(),
                                self.store.clone(),
                                transaction,
                            )
                            .await;
                            self.metrics.decrease_proxy_load(&self.id);
                            if self.tx_results.send(execution_result).await.is_err() {
                                tracing::warn!(
                                    "Failed to send execution result, stopping proxy {}",
                                    self.id
                                );
                                break;
                            }
                        }

                        PrimaryToProxyMessage::States(states) => {
                            self.store.commit_new_objects(states);
                        }
                    }
                }
            }

            ProxyMode::MultiThreaded => {
                let mut task_id = 0;
                loop {
                    tokio::select! {
                        Some(message) = self.rx_transactions.recv() => {
                            match message {
                                PrimaryToProxyMessage::Txn(transaction) => {
                                    if task_id == 0 {
                                        self.metrics.register_start_time();
                                    }
                                    task_id += 1;
                                    self.metrics.increase_proxy_load(&self.id);
                                    let (prior_handles, current_handles) = self.get_dependencies(transaction.clone(), task_id);
                                    self.schedule_txn_parallel(transaction, prior_handles, current_handles).await.expect("Failed to schedule transaction");
                                }

                                PrimaryToProxyMessage::States(states) => {
                                    self.store.commit_new_objects(states);
                                }
                            }
                        }
                        else => Err(NodeError::ShuttingDown)?
                    }
                }
            }
        }
        Ok(())
    }

    pub fn get_dependencies(
        &mut self,
        transaction: RemoraTransaction<E>,
        task_id: u64,
    ) -> (Vec<Arc<Notify>>, Vec<Arc<Notify>>) {
        let obj_ids = get_object_ids_for_dependency_tracking::<E>(transaction);

        self.dependency_controller
            .as_mut()
            .expect("DependencyController should be initialized")
            .get_dependencies(task_id, obj_ids)
    }

    pub async fn schedule_txn_parallel(
        &mut self,
        transaction: RemoraTransaction<E>,
        prior_handles: Vec<Arc<Notify>>,
        current_handles: Vec<Arc<Notify>>,
    ) -> NodeResult<()>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        let store = self.store.clone();
        let id = self.id.clone();
        let tx_results = self.tx_results.clone();
        let ctx = self.executor.context().clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }

            // check the version ID for shared objects
            // skip if versions don't match
            let ready_to_execute =
                !transaction.input_objects().iter().any(|input_object| {
                    matches!(
                        input_object,
                        InputObjectKind::SharedMoveObject {
                            id: _,
                            initial_shared_version: _,
                            mutable: _,
                        }
                    )
                }) || E::pre_execute_check(ctx.clone(), store.clone(), &transaction);

            if ready_to_execute {
                let execution_result = E::execute(ctx, store, transaction.clone()).await;
                tx_results
                    .send(execution_result)
                    .await
                    .map_err(|_| NodeError::ShuttingDown)?;
            } else {
                tracing::warn!("Proxy skipped execution");
            }

            for notify in current_handles {
                notify.notify_one();
            }

            metrics.decrease_proxy_load(&id);
            metrics.update_metrics(transaction.timestamp());
            Ok::<_, NodeError>(())
        });
        Ok(())
    }

    /// Sptransaction_awn the proxy in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }

    pub fn spawn_with_threads(mut self) -> std::thread::JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        let num_threads = num_cpus::get();

        // spawn the custom runtime in a dedicated thread to ensure active
        std::thread::spawn(move || {
            // Build a custom Tokio runtime with the specified number of worker threads
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_threads)
                .enable_all()
                .build()
                .unwrap();

            // Block on the runtime to keep it alive and process tasks
            rt.block_on(async move {
                let _ = self.run().await;
            });
            Ok::<_, NodeError>(())
        })
    }
}

#[cfg(test)]
mod tests {

    use std::{
        sync::Arc,
        time::Duration,
    };

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::{
            api::{Executor, PrimaryToProxyMessage, RemoraTransaction},
            fake::{generate_fake_transactions, FakeExecutionContext, FakeExecutor},
            sui::{generate_sui_transactions, SuiExecutor, SuiTransaction},
        },
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
        let transactions = generate_sui_transactions(&config, None).await;
        for tx in transactions {
            let transaction = SuiTransaction::new_for_tests(tx);
            let message = PrimaryToProxyMessage::Txn(transaction);
            tx_proxy.send(message).await.unwrap();
        }

        // Spawn the proxy.
        proxy.spawn();

        // Receive the results.
        let results = rx_results.recv().await.unwrap();
        assert!(results.success());
    }

    // need to unify this in the executor/fake.rs
    pub type RFakeTransaction = RemoraTransaction<FakeExecutor>;

    async fn pre_execute_fake(mode: ProxyMode) {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let config = BenchmarkParameters::new_for_fake_tests();
        let execution_duration = Duration::from_micros(500);
        let checks_duration = Duration::from_micros(500);
        let execution_context = FakeExecutionContext::new(execution_duration, checks_duration);

        let executor = FakeExecutor::new(execution_context);
        let store = Arc::new(executor.init_store());
        let metrics = Arc::new(Metrics::new_for_tests());
        let proxy_id = "0".to_string();
        let proxy = ProxyCore::new(
            proxy_id, executor, mode, store, rx_proxy, tx_results, metrics,
        );

        // Send transactions to the proxy.
        let transactions = generate_fake_transactions(&config).await;
        for tx in transactions {
            let transaction = RFakeTransaction::new_for_tests(tx);
            let message = PrimaryToProxyMessage::Txn(transaction);
            tx_proxy.send(message).await.unwrap();
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

    #[tokio::test]
    async fn test_single_threaded_proxy_fake_transactions() {
        pre_execute_fake(ProxyMode::SingleThreaded).await;
    }
}
