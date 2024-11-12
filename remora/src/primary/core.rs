// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use sui_types::{
    base_types::{ObjectID, ObjectRef},
    digests::TransactionDigest,
    storage::ObjectStore,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use super::mock_consensus::ConsensusCommit;
use crate::{
    error::{NodeError, NodeResult},
    executor::api::{
        ExecutableTransaction, ExecutionResults, Executor, ExecutorIndex, StateStore, Store,
        Transaction, TransactionWithTimestamp,
    },
};

pub type PendingTransactions<E> = Arc<
    DashMap<
        TransactionDigest,
        (
            ExecutorIndex,
            TransactionWithTimestamp<<E as Executor>::Transaction>,
        ),
    >,
>;

/// The primary executor is responsible for executing transactions and merging the results
/// from the proxies.
pub struct PrimaryCore<E: Executor> {
    /// The executor for the transactions.
    executor: E,
    /// The object store.
    store: Store<E>,
    /// The receiver for consensus commits.
    rx_commits: Receiver<ConsensusCommit<Transaction<E>>>,
    /// The receiver for proxy results.
    rx_proxies: Receiver<ExecutionResults<E>>,
    /// Output channel for the final results.
    tx_output: Sender<(Transaction<E>, ExecutionResults<E>)>,
    /// The transactions sent out to proxies
    pending_txns: PendingTransactions<E>,
    /// The sender to load balancer.
    tx_committed_txns: Sender<Transaction<E>>,
    /// The receiver to a local executor if no pre-executor is available.
    rx_executor_backup: Receiver<Transaction<E>>,
    /// The sender to sync updates to proxy via load-balancer.
    tx_states_sync: Sender<ExecutionResults<E>>,
}

impl<E: Executor> PrimaryCore<E> {
    /// Create a new primary executor.
    pub fn new(
        executor: E,
        store: Store<E>,
        rx_commits: Receiver<ConsensusCommit<Transaction<E>>>,
        rx_proxies: Receiver<ExecutionResults<E>>,
        tx_output: Sender<(Transaction<E>, ExecutionResults<E>)>,
        pending_txns: PendingTransactions<E>,
        tx_committed_txns: Sender<Transaction<E>>,
        rx_executor_backup: Receiver<Transaction<E>>,
        tx_states_sync: Sender<ExecutionResults<E>>,
    ) -> Self {
        Self {
            executor,
            store,
            rx_commits,
            rx_proxies,
            tx_output,
            pending_txns,
            tx_committed_txns,
            rx_executor_backup,
            tx_states_sync,
        }
    }

    /// Get the input objects for a transaction.
    // TODO: This function should return an error when the input object is not found
    // or the input objects are malformed instead of panicking.
    fn get_input_objects(&self, transaction: &E::Transaction) -> HashMap<ObjectID, ObjectRef> {
        transaction
            .input_objects()
            .iter()
            .map(|kind| {
                self.store
                    .get_object(&kind.object_id())
                    .expect("Failed to read objects from store")
                    .map(|object| (object.id(), object.compute_object_reference()))
                    .expect("Input object not found") // TODO: Return error instead of panic
            })
            .collect()
    }

    pub async fn check_and_apply_proxy_results(
        &mut self,
        pending_txns: PendingTransactions<E>,
        proxy_result: ExecutionResults<E>,
    ) {
        let mut skip = true;
        if let Some((_, (_, transaction))) = pending_txns.remove(proxy_result.transaction_digest())
        {
            let initial_state = self.get_input_objects(&transaction);
            for (id, vid) in &proxy_result.modified_at_versions() {
                let (_, v, _) = initial_state
                    .get(id)
                    .expect("Transaction's inputs already checked");
                if v != vid {
                    skip = false;
                }
            }

            let results: ExecutionResults<E> = if skip {
                let effects = proxy_result.clone();
                self.store
                    .commit_objects(effects.updates, effects.new_state);
                proxy_result
            } else {
                tracing::trace!("Re-executing transaction");
                let ctx = self.executor.context();
                E::execute(ctx, self.store.clone(), &transaction).await
            };

            if self
                .tx_output
                .send((transaction.clone(), results))
                .await
                .is_err()
            {
                tracing::warn!("Failed to output execution result, stopping primary executor");
            }
        } else {
            tracing::warn!("The received result is not in pending txns");
        }
    }

    async fn local_execute(
        &mut self,
        transaction: TransactionWithTimestamp<E::Transaction>,
    ) -> ExecutionResults<E> {
        // use the local executor
        let ctx = self.executor.context();
        let txn_result = E::execute(ctx, self.store.clone(), &transaction).await;
        if self
            .tx_output
            .send((transaction.clone(), txn_result.clone()))
            .await
            .is_err()
        {
            tracing::warn!("Failed to output execution result, stopping primary executor");
        }

        txn_result
    }

    /// Run the primary executor.
    pub async fn run(&mut self) -> NodeResult<()> {
        loop {
            tokio::select! {
                // Receive a commit from the consensus.
                Some(commit) = self.rx_commits.recv() => {
                    tracing::debug!("Received commit");
                    for transaction in commit {
                        // send to the load balancer
                        if self.tx_committed_txns.send(transaction.clone()).await.is_err() {
                            self.local_execute(transaction).await;
                        }
                    }
                }

                // Receive an execution result from a proxy.
                Some(proxy_result) = self.rx_proxies.recv() => {
                    tracing::debug!("Received proxy result");
                    self.check_and_apply_proxy_results(self.pending_txns.clone(), proxy_result).await;
                }

                // Receieve a transaction for local execution.
                Some(transaction) = self.rx_executor_backup.recv() => {
                    tracing::debug!("Received transaction for local execution");
                    let effects = self.local_execute(transaction).await;
                    if self.tx_states_sync.send(effects).await.is_err() {
                        tracing::warn!("Failed to send execution results of local executor to load balancer");
                    }
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
        Transaction<E>: Send + Sync,
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
