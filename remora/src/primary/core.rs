// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use sui_types::{
    base_types::{ObjectID, ObjectRef},
    storage::ObjectStore,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::api::{
        ExecutableTransaction, ExecutionResults, Executor, RemoraTransaction, StateStore, Store,
        Timestamp, TransactionWithTimestamp,
    },
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
}

impl<E: Executor> PrimaryCore<E> {
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
        }
    }

    /// Get the input objects for a transaction.
    // TODO: This function should return an error when the input object is not found
    // or the input objects are malformed instead of panicking.
    fn get_input_objects(store: Store<E>, transaction: &E::Transaction) -> HashMap<ObjectID, ObjectRef> {
        transaction
            .input_objects()
            .iter()
            .map(|kind| {
                store
                    .get_object(&kind.object_id())
                    .expect("Failed to read objects from store")
                    .map(|object| (object.id(), object.compute_object_reference()))
                    .expect("Input object not found") // TODO: Return error instead of panic
            })
            .collect()
    }

    pub async fn check_and_apply_proxy_results(&mut self, proxy_result: ExecutionResults<E>) 
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
    {
        let store = self.store.clone();
        let tx_output = self.tx_output.clone();
        let tx_executor_local = self.tx_executor_local.clone();

        tokio::spawn(async move {
            let mut skip = true;
            let initial_state = Self::get_input_objects(store.clone(), &proxy_result.transaction);
            for (id, vid) in &proxy_result.modified_at_versions() {
                let (_, v, _) = initial_state
                    .get(id)
                    .expect("Transaction's inputs already checked");
                if v != vid {
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
        });
    }

    async fn local_execute(&mut self, transaction: TransactionWithTimestamp<E::Transaction>)
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
    {
        let ctx = self.executor.context();
        let store = self.store.clone();
        let tx_output = self.tx_output.clone();
        let tx_states_sync = self.tx_states_sync.clone();

        // FIXME: this probably doesn't work for shared objects
        // Need to track dependency and merge commit_objects with local execution
        tokio::spawn(async move {
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
                tracing::warn!(
                    "Failed to send execution results of local executor to load balancer"
                );
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
    {
        loop {
            tokio::select! {
                // Receive an execution result from a proxy.
                Some(proxy_result) = self.rx_proxies.recv() => {
                    tracing::debug!("Received proxy result");
                    self.check_and_apply_proxy_results(proxy_result).await;
                }

                // Receieve a transaction for local execution.
                Some(transaction) = self.rx_executor_local.recv() => {
                    tracing::debug!("Received transaction for local execution");
                    self.local_execute(transaction).await;
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
