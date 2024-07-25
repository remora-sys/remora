// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use dashmap::DashMap;
use sui_single_node_benchmark::mock_storage::InMemoryObjectStore;
use sui_types::{
    base_types::{ObjectID, ObjectRef},
    digests::TransactionDigest,
    effects::TransactionEffectsAPI,
    storage::ObjectStore,
    transaction::{CertifiedTransaction, TransactionDataAPI},
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    executor::{Executor, SuiExecutor, SuiTransactionResults, SuiTransactionWithTimestamp},
    mock_consensus::ConsensusCommit,
};

/// The primary executor is responsible for executing transactions and merging the results
/// from the proxies.
pub struct PrimaryExecutor {
    /// The executor for the transactions.
    executor: SuiExecutor,
    /// The object store.
    store: InMemoryObjectStore,
    /// The receiver for consensus commits.
    rx_commits: Receiver<ConsensusCommit<SuiTransactionWithTimestamp>>,
    /// The receiver for proxy results.
    rx_proxies: Receiver<SuiTransactionResults>,
    /// Output channel for the final results.
    tx_output: Sender<(SuiTransactionWithTimestamp, SuiTransactionResults)>,
}

impl PrimaryExecutor {
    /// Create a new primary executor.
    pub fn new(
        executor: SuiExecutor,
        store: InMemoryObjectStore,
        rx_commits: Receiver<ConsensusCommit<SuiTransactionWithTimestamp>>,
        rx_proxies: Receiver<SuiTransactionResults>,
        tx_output: Sender<(SuiTransactionWithTimestamp, SuiTransactionResults)>,
    ) -> Self {
        Self {
            executor,
            store,
            rx_commits,
            rx_proxies,
            tx_output,
        }
    }

    /// Get the input objects for a transaction.
    // TODO: This function should return an error when the input object is not found
    // or the input objects are malformed instead of panicking.
    fn get_input_objects(
        store: &InMemoryObjectStore,
        transaction: &CertifiedTransaction,
    ) -> HashMap<ObjectID, ObjectRef> {
        transaction
            .transaction_data()
            .input_objects()
            .expect("Cannot get input object kinds") // TODO: Return error instead of panic
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

    /// Merge the results from the proxies and re-execute the transaction if necessary.
    // TODO: Naive merging strategy for now.
    pub async fn merge_results(
        &mut self,
        proxy_results: &DashMap<TransactionDigest, SuiTransactionResults>,
        transaction: &SuiTransactionWithTimestamp,
    ) -> SuiTransactionResults {
        let mut skip = true;

        if let Some((_, proxy_result)) = proxy_results.remove(transaction.digest()) {
            let effects = &proxy_result.tx_effects;
            let initial_state = Self::get_input_objects(&self.store, &*transaction);
            for (id, vid) in &effects.modified_at_versions() {
                let (_, v, _) = initial_state
                    .get(id)
                    .expect("Transaction's inputs already checked");
                if v != vid {
                    skip = false;
                }
            }
            if skip {
                self.store
                    .commit_effects(effects.clone(), proxy_result.written.clone());
                return proxy_result;
            }
        }

        tracing::trace!("Re-executing transaction");
        self.executor.execute(&self.store, &transaction).await
    }

    /// Run the primary executor.
    pub async fn run(&mut self) {
        let proxy_results = DashMap::new();

        loop {
            tokio::select! {
                // Receive a commit from the consensus.
                Some(commit) = self.rx_commits.recv() => {
                    tracing::debug!("Received commit");
                    for tx in commit {
                        let results = self.merge_results(&proxy_results, &tx).await;
                        if self.tx_output.send((tx,results)).await.is_err() {
                            tracing::warn!("Failed to output execution result, stopping primary executor");
                            break;
                        }
                    }
                }

                // Receive a execution result from a proxy.
                Some(proxy_result) = self.rx_proxies.recv() => {
                    proxy_results.insert(
                        *proxy_result.tx_effects.transaction_digest(),
                        proxy_result
                    );
                    tracing::debug!("Received proxy result");
                }
            }
        }
    }

    /// Spawn the primary executor in a new task.
    pub fn spawn(mut self) -> JoinHandle<()> {
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
        primary::PrimaryExecutor,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn merge_results() {
        let (tx_commit, rx_commit) = mpsc::channel(100);
        let (tx_results, rx_results) = mpsc::channel(100);
        let (tx_output, mut rx_output) = mpsc::channel(100);

        // Generate transactions.
        let config = BenchmarkConfig::new_for_tests();
        let mut executor = SuiExecutor::new(&config).await;
        let transactions: Vec<_> = executor
            .generate_transactions()
            .await
            .into_iter()
            .map(|tx| SuiTransactionWithTimestamp::new_for_tests(tx))
            .collect();
        let total_transactions = transactions.len();

        // Pre-execute the transactions.
        let mut proxy_results = Vec::new();
        let proxy_store = executor.create_in_memory_store();
        for tx in transactions.clone() {
            proxy_results.push(executor.execute(&proxy_store, &tx).await);
        }

        // Boot the primary executor.
        let store = executor.create_in_memory_store();
        PrimaryExecutor::new(executor, store, rx_commit, rx_results, tx_output).spawn();

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
