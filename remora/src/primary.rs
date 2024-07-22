// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use sui_single_node_benchmark::mock_storage::InMemoryObjectStore;
use sui_types::{
    base_types::{ObjectID, ObjectRef},
    effects::TransactionEffectsAPI,
    storage::ObjectStore,
    transaction::{CertifiedTransaction, InputObjectKind, TransactionDataAPI},
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    executor::{Executor, SuiExecutor, SuiTransactionWithTimestamp},
    metrics::Metrics,
    mock_consensus::ConsensusCommit,
    types::{PreResType, TransactionWithResults},
};

/// The primary executor is responsible for executing transactions and merging the results
/// from the proxies.
pub struct PrimaryExecutor {
    /// The executor for the transactions.
    executor: SuiExecutor,
    /// The receiver for consensus commits.
    rx_commits: Receiver<ConsensusCommit<SuiTransactionWithTimestamp>>,
    /// The receiver for proxy results.
    rx_proxies: Receiver<TransactionWithResults>,
    /// Output channel for the final results.
    tx_output: Sender<TransactionWithResults>,
    /// The metrics.
    metrics: Arc<Metrics>,
}

impl PrimaryExecutor {
    /// Create a new primary executor.
    pub fn new(
        executor: SuiExecutor,
        rx_commits: Receiver<ConsensusCommit<SuiTransactionWithTimestamp>>,
        rx_proxies: Receiver<TransactionWithResults>,
        tx_output: Sender<TransactionWithResults>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            executor,
            rx_commits,
            rx_proxies,
            tx_output,
            metrics,
        }
    }

    /// Get the input objects for a transaction.
    fn get_input_objects(
        store: &InMemoryObjectStore,
        tx: &CertifiedTransaction,
    ) -> HashMap<ObjectID, ObjectRef> {
        let tx_data = tx.transaction_data();
        let input_object_kinds = tx_data
            .input_objects()
            .expect("Cannot get input object kinds");

        let mut input_object_data = Vec::new();
        for kind in &input_object_kinds {
            let obj = match kind {
                InputObjectKind::MovePackage(id)
                | InputObjectKind::SharedMoveObject { id, .. }
                | InputObjectKind::ImmOrOwnedMoveObject((id, _, _)) => {
                    store.get_object(id).unwrap().unwrap()
                }
            };
            input_object_data.push(obj);
        }

        let mut res = HashMap::new();
        for obj in input_object_data {
            res.insert(obj.id(), obj.compute_object_reference());
        }
        res
    }

    /// Merge the results from the proxies and re-execute the transaction if necessary.
    // TODO: Naive merging strategy for now.
    pub async fn merge_results(
        &mut self,
        proxy_results: &PreResType,
        tx: SuiTransactionWithTimestamp,
    ) -> TransactionWithResults {
        let store = self.executor.store();
        let mut skip = true;

        if let Some(proxy_result) = proxy_results.get(tx.digest()) {
            let effects = &proxy_result.tx_effects;
            let init_state = Self::get_input_objects(store, &*tx);
            for (id, vid) in effects.modified_at_versions() {
                let (_, v, _) = *init_state.get(&id).unwrap();
                if v != vid {
                    skip = false;
                }
            }
            if skip {
                store.commit_effects(effects.clone(), proxy_result.written.clone());
                return proxy_result.clone();
            }
        }

        tracing::trace!("Re-executing transaction");
        self.executor.execute(tx).await
    }

    /// Run the primary executor.
    pub async fn run(&mut self) {
        let proxy_results: PreResType = DashMap::new();

        loop {
            tokio::select! {
                // Receive a commit from the consensus.
                Some(commit) = self.rx_commits.recv() => {
                    tracing::debug!("Received commit");
                    for tx in commit {
                        let submit_timestamp = tx.timestamp();
                        let results = self.merge_results(&proxy_results, tx).await;
                        self.metrics.update_metrics(submit_timestamp);
                        if self.tx_output.send(results).await.is_err() {
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
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkConfig,
        executor::{Executor, SuiExecutor, SuiTransactionWithTimestamp},
        metrics::Metrics,
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
        let num_transactions = transactions.len();

        // Pre-execute the transactions.
        let mut pre_execution_results = Vec::new();
        for tx in transactions.clone() {
            pre_execution_results.push(executor.execute(tx).await);
        }

        // Boot the primary executor.
        let metrics = Arc::new(Metrics::new_for_tests());
        PrimaryExecutor::new(executor, rx_commit, rx_results, tx_output, metrics).spawn();

        // Merge the results.
        for r in pre_execution_results {
            tx_results.send(r).await.unwrap();
        }
        tokio::task::yield_now().await;

        // Send the transactions to the primary executor.
        tx_commit.send(transactions).await.unwrap();

        // Check the results.
        for _ in 0..num_transactions {
            let result = rx_output.recv().await.unwrap();
            assert!(result.success());
        }
    }
}
