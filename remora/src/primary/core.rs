// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use sui_types::base_types::{ObjectID, ObjectRef};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::{
        api::{
            ExecutionResults, Executor, RemoraTransaction, StateStore, Store, Timestamp, TransactionWithTimestamp
        },
        dependency_controller::DependencyController,
        sui::get_object_ids_for_dependency_tracking,
    }
};

/// The primary executor is responsible for executing transactions and merging the results
/// from the proxies.
pub struct PrimaryCore<E: Executor> {
    /// The executor for the transactions.
    executor: E,
    /// The object store.
    store: Store<E>,
    /// The receiver for proxy results.
    rx_proxies: Receiver<ExecutionResults<E>>,
    /// Output channel for the final results.
    tx_output: Sender<(Timestamp, ExecutionResults<E>)>,
    /// The sender to a local executor.
    tx_executor_local: Sender<RemoraTransaction<E>>,
    /// The receiver to a local executor.
    rx_executor_local: Receiver<RemoraTransaction<E>>,
    /// The sender to sync updates to proxy via load-balancer.
    tx_states_sync: Sender<ExecutionResults<E>>,
    /// The dependency controller for multi-core tx execution.
    dependency_controller: DependencyController,
}

impl<E: Executor + Sync> PrimaryCore<E> {
    /// Create a new primary executor.
    pub fn new(
        executor: E,
        store: Store<E>,
        rx_proxies: Receiver<ExecutionResults<E>>,
        tx_output: Sender<(Timestamp, ExecutionResults<E>)>,
        tx_executor_local: Sender<RemoraTransaction<E>>,
        rx_executor_local: Receiver<RemoraTransaction<E>>,
        tx_states_sync: Sender<ExecutionResults<E>>,
    ) -> Self {
        Self {
            executor,
            store,
            rx_proxies,
            tx_output,
            tx_executor_local,
            rx_executor_local,
            tx_states_sync,
            dependency_controller: DependencyController::new(),
        }
    }

    /// Get the input objects for a transaction.
    // TODO: This function should return an error when the input object is not found
    // or the input objects are malformed instead of panicking.
    fn get_input_object_ids_and_versions(
        store: Store<E>,
        obj_ids: Vec<ObjectID>,
    ) -> HashMap<ObjectID, ObjectRef> {
        obj_ids
            .iter()
            .map(|id| {
                store
                    .read_object(id)
                    .expect("Failed to read objects from store")
                    .map(|object| (object.id(), object.compute_object_reference()))
                    .expect("Input object not found") // TODO: Return error instead of panic
            })
            .collect()
    }

    pub async fn check_and_apply_proxy_results(
        &mut self,
        store: Store<E>,
        tx_output: Sender<(Timestamp, ExecutionResults<E>)>,
        tx_executor_local: Sender<RemoraTransaction<E>>,
        proxy_result: ExecutionResults<E>,
        task_id: u64,
    ) where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        let mut skip = true;

        let obj_ids = get_object_ids_for_dependency_tracking::<E>(proxy_result.transaction.clone()); 

        // FIXME: ad-hoc passing test to ensure the object is created on the primary
        // Should impl the part for load-gen and primary to import from a same workload
        // trace to init the same store context
        if !E::pre_execute_check_objects(store.clone(), &proxy_result.transaction) {
            E::optimistically_pre_generate_objects(store.clone(), &proxy_result.transaction);
        }

        let (prior_handles, current_handles) = self.dependency_controller
            .get_dependencies(task_id, obj_ids.clone());

        tokio::spawn(async move {
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }

            let initial_state = Self::get_input_object_ids_and_versions(store.clone(), obj_ids);
            for (id, vid) in &proxy_result.modified_at_versions() {
                let (_, v, _) = initial_state
                    .get(id)
                    .expect("Transaction's inputs already checked");
                if v != vid {
                    tracing::warn!("Failed to apply result due to obj: {}, vid: {} while current v is {}", id, vid, v);
                    skip = false;
                }
            }

            if skip {
                let effects = proxy_result.clone();
                store.commit_objects(effects.updates, effects.new_state);
                if tx_output
                    .send((proxy_result.transaction.timestamp(), proxy_result))
                    .await
                    .is_err()
                {
                    tracing::warn!("Failed to output execution result, stopping primary executor");
                }
            } else {
                tracing::warn!("Failed to apply proxy results, sends to local executor");
                if tx_executor_local
                    .send(proxy_result.transaction.clone())
                    .await
                    .is_err()
                {
                    tracing::warn!("Failed to send transaction to the local executor");
                }
            }

            for notify in current_handles {
                notify.notify_one();
            }
        });
    }

    async fn local_execute(&mut self, transaction: TransactionWithTimestamp<E::Transaction>, task_id: u64)
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        let ctx = self.executor.context();
        let store = self.store.clone();
        let tx_output = self.tx_output.clone();
        let tx_states_sync = self.tx_states_sync.clone();

        let obj_ids = get_object_ids_for_dependency_tracking::<E>(transaction.clone()); 

        let (prior_handles, current_handles) = self.dependency_controller
            .get_dependencies(task_id, obj_ids);

        tokio::spawn(async move {
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }

            let txn_result = E::execute(ctx, store, transaction.clone()).await;

            if tx_output
                .send((transaction.timestamp(), txn_result.clone()))
                .await
                .is_err()
            {
                tracing::warn!("Failed to output execution result, stopping primary executor");
            }

            // Sends the sync updates after each local execution
            if tx_states_sync.send(txn_result.clone()).await.is_err() {
                tracing::warn!("Failed to send execution results of local executor to load balancer");
            }

            for notify in current_handles {
                notify.notify_one();
            }
        });
    }

    /// Run the primary executor.
    pub async fn run(&mut self) -> NodeResult<()>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        let mut task_id = 0;

        loop {
            tokio::select! {
                // Receive an execution result from a proxy.
                Some(proxy_result) = self.rx_proxies.recv() => {
                    tracing::debug!("Received proxy result");
                    task_id += 1;
                    let store = self.store.clone();
                    let tx_output = self.tx_output.clone();
                    let tx_executor_local = self.tx_executor_local.clone();
                    self.check_and_apply_proxy_results(store, tx_output, tx_executor_local, proxy_result, task_id).await;
                }

                // Receive a transaction for local execution.
                Some(transaction) = self.rx_executor_local.recv() => {
                    task_id += 1;
                    tracing::debug!("Received transaction for local execution");
                    self.local_execute(transaction, task_id).await;
                }

                // The channel is closed.
                else => Err(NodeError::ShuttingDown)?
            }
        }
    }

    /// Spawn the primary executor in a new task.
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
}

/*
#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::{
            api::Executor,
            sui::{generate_transactions, SuiExecutor, SuiTransaction},
        },
        primary::core::PrimaryCore,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn merge_results() {
        let (tx_commit, rx_commit) = mpsc::channel(100);
        let (tx_results, rx_results) = mpsc::channel(100);
        let (tx_output, mut rx_output) = mpsc::channel(100);

        // Generate transactions.
        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        let ctx = executor.context();
        let transactions: Vec<_> = generate_transactions(&config)
            .await
            .into_iter()
            .map(|tx| SuiTransaction::new_for_tests(tx))
            .collect();
        let total_transactions = transactions.len();

        // Pre-execute the transactions.
        let mut proxy_results = Vec::new();
        let proxy_store = Arc::new(executor.create_in_memory_store());
        for tx in transactions.clone() {
            let results = SuiExecutor::execute(ctx.clone(), proxy_store.clone(), &tx).await;
            proxy_results.push(results);
        }

        // Boot the primary executor.
        let store = Arc::new(executor.create_in_memory_store());
        PrimaryCore::new(executor, store, rx_commit, rx_results, tx_output).spawn();

        // Merge the proxy results into the primary.
        for r in proxy_results {
            tx_results.send(r).await.unwrap();
        }
        tokio::task::yield_now().await;

        // Send the transactions to the primary executor.
        tx_commit.send(transactions).await.unwrap();

        // Check the results.
        for _ in 0..total_transactions {
            let (_, result) = rx_output.recv().await.unwrap();
            assert!(result.success());
        }
    }
}
*/
