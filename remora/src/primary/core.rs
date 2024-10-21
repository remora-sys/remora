// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, ops::Deref};

use dashmap::DashMap;
use rustc_hash::FxHashMap;
use sui_types::{
    base_types::{ObjectID, ObjectRef},
    digests::TransactionDigest,
    storage::ObjectStore,
    transaction::InputObjectKind,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use super::mock_consensus::ConsensusCommit;
use crate::{
    error::{NodeError, NodeResult},
    executor::api::{
        ExecutableTransaction, ExecutionResults, Executor, StateStore, Store, Transaction,
        TransactionWithTimestamp,
    },
};

/// The primary executor is responsible for executing transactions and merging the results
/// from the proxies.
pub struct PrimaryCore<E: Executor> {
    /// The executor for the transactions.
    executor: E,
    /// The object store.
    store: Store<E>,
    /// The receiver for consensus commits.
    rx_commits: Receiver<ConsensusCommit<Transaction<E>>>,
    /// Receive handles to forward transactions to proxies. When a new client connects,
    /// this channel receives a sender from the network layer which is used to forward
    /// transactions to the proxies.
    rx_proxy_connections: Receiver<Sender<Transaction<E>>>,
    /// Holds senders to forward transactions to proxies.
    proxy_connections: Vec<Sender<Transaction<E>>>,
    /// The receiver for proxy results.
    rx_proxies: Receiver<ExecutionResults<E>>,
    /// Output channel for the final results.
    tx_output: Sender<(Transaction<E>, ExecutionResults<E>)>,
    /// Keeps track of shared-objects and its shards (proxy)
    shared_object_shards: FxHashMap<ObjectID, usize>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: usize,
}

impl<E: Executor> PrimaryCore<E> {
    /// Create a new primary executor.
    pub fn new(
        executor: E,
        store: Store<E>,
        rx_commits: Receiver<ConsensusCommit<Transaction<E>>>,
        rx_proxy_connections: Receiver<Sender<Transaction<E>>>,
        rx_proxies: Receiver<ExecutionResults<E>>,
        tx_output: Sender<(Transaction<E>, ExecutionResults<E>)>,
    ) -> Self {
        Self {
            executor,
            store,
            rx_commits,
            rx_proxy_connections,
            proxy_connections: Vec::new(),
            rx_proxies,
            tx_output,
            shared_object_shards: FxHashMap::default(),
            index: 0,
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
        pending_txns: &DashMap<TransactionDigest, TransactionWithTimestamp<E::Transaction>>,
        proxy_result: ExecutionResults<E>,
    ) {
        let mut skip = true;
        if let Some((_, transaction)) = pending_txns.remove(proxy_result.transaction_digest()) {
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

    /// Merge the results from the proxies and re-execute the transaction if necessary.
    // TODO: Naive merging strategy for now.
    #[allow(dead_code)]
    pub async fn merge_results(
        &mut self,
        proxy_results: &DashMap<TransactionDigest, ExecutionResults<E>>,
        transaction: &Transaction<E>,
    ) -> ExecutionResults<E> {
        let mut skip = true;

        if let Some((_, proxy_result)) = proxy_results.remove(transaction.deref().digest()) {
            let initial_state = self.get_input_objects(transaction);
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
                self.store
                    .commit_objects(effects.updates, effects.new_state);
                return proxy_result;
            }
        }

        tracing::info!("Re-executing transaction");
        let ctx = self.executor.context();
        E::execute(ctx, self.store.clone(), transaction).await
    }

    /// Run the primary executor.
    pub async fn run(&mut self) -> NodeResult<()> {
        let pending_txns: DashMap<
            TransactionDigest,
            TransactionWithTimestamp<<E as Executor>::Transaction>,
        > = DashMap::new();

        loop {
            tokio::select! {
            Some(connection) = self.rx_proxy_connections.recv() => {
                self.proxy_connections.push(connection);
                tracing::info!("Added a new proxy connection");
            }

            // Receive a commit from the consensus.
            Some(commit) = self.rx_commits.recv() => {
                tracing::debug!("Received commit");
                for transaction in commit {
                    // forward to proxies if exist else to the local executor
                    if !self.proxy_connections.is_empty() {
                        // Determine proxy_index based on shared object or round-robin.
                        let proxy_index = if self.check_shared_objects(transaction.deref()) {
                            // If a shared object is found, check the hashmap for proxy assignment
                            if let Some(&proxy_index) = self.get_proxy_for_shared_object(&transaction) {
                                proxy_index
                            } else {
                                // If no proxy is assigned to this shared object, assign one in a round-robin fashion
                                let proxy_index = self.index % self.proxy_connections.len();
                                self.assign_shared_object_to_proxy(&transaction, proxy_index);
                                proxy_index
                            }
                        } else {
                            // No shared object, use round-robin to select proxy
                            self.index % self.proxy_connections.len()
                        };

                        // Send the transaction to the selected proxy
                        match self.proxy_connections[proxy_index]
                            .send(transaction.clone())
                            .await
                        {
                            Ok(()) => {
                                tracing::debug!("Sent transaction to proxy {}", proxy_index);
                                self.index += 1;
                            }
                            Err(_) => {
                                tracing::warn!(
                                    "Failed to send transaction to proxy {}, trying other proxies",
                                    proxy_index
                                );
                                self.proxy_connections.swap_remove(proxy_index);
                            }
                        }
                        pending_txns.insert(*transaction.digest(), transaction.clone());
                    } else {
                        // use the local executor
                        let ctx = self.executor.context();
                        let txn_result = E::execute(ctx, self.store.clone(), &transaction).await;
                        if self
                            .tx_output
                            .send((transaction.clone(), txn_result))
                            .await
                            .is_err()
                            {
                                tracing::warn!("Failed to output execution result, stopping primary executor");
                            }
                    }
                }
            }

            // Receive a execution result from a proxy.
            Some(proxy_result) = self.rx_proxies.recv() => {
                tracing::debug!("Received proxy result");
                self.check_and_apply_proxy_results(&pending_txns, proxy_result).await;
            }

            // The channel is closed.
            else => Err(NodeError::ShuttingDown)?
            }
        }
    }

    // TODO: a bit verbose and duplicated
    /// Helper to check if tx contains shared-objects
    fn check_shared_objects(&self, transaction: &E::Transaction) -> bool {
        transaction.input_objects().iter().any(|input_object| {
            matches!(
                input_object,
                InputObjectKind::SharedMoveObject {
                    id: _,
                    initial_shared_version: _,
                    mutable: _
                }
            )
        })
    }

    /// Helper function to get proxy for a shared object (if it exists).
    fn get_proxy_for_shared_object(&self, transaction: &E::Transaction) -> Option<&usize> {
        transaction.input_objects().iter().find_map(|input_object| {
            if let InputObjectKind::SharedMoveObject { id, .. } = input_object {
                self.shared_object_shards.get(id)
            } else {
                None
            }
        })
    }

    /// Helper function to assign a shared object to a proxy and update the map.
    fn assign_shared_object_to_proxy(&mut self, transaction: &E::Transaction, proxy_index: usize) {
        if let Some(shared_object_id) =
            transaction.input_objects().iter().find_map(|input_object| {
                if let InputObjectKind::SharedMoveObject { id, .. } = input_object {
                    Some(*id)
                } else {
                    None
                }
            })
        {
            self.shared_object_shards
                .insert(shared_object_id, proxy_index);
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
