// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::Debug, future::Future, ops::Deref, sync::Arc};

use serde::{Deserialize, Serialize};
use sui_single_node_benchmark::benchmark_context::BenchmarkContext;
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
    effects::TransactionEffectsAPI,
    object::Object,
    storage::BackingStore,
    transaction::InputObjectKind,
};

/// A transaction that can be executed.
pub trait ExecutableTransaction {
    /// The digest of the transaction.
    fn digest(&self) -> &TransactionDigest;

    /// The input objects kind of the transaction.
    fn input_objects(&self) -> Vec<InputObjectKind>;

    /// The object IDs for the input objects.
    fn input_object_ids(&self) -> Vec<ObjectID> {
        self.input_objects()
            .iter()
            .map(|kind| kind.object_id())
            .collect()
    }
}

/// A transaction with a timestamp. This is used to compute performance.
#[derive(Clone, Serialize, Deserialize)]
pub struct TransactionWithTimestamp<T: ExecutableTransaction + Clone> {
    /// The transaction.
    transaction: T,
    /// The timestamp when the transaction was created.
    timestamp: f64,
}

impl<T: ExecutableTransaction + Clone> TransactionWithTimestamp<T> {
    /// Create a new transaction with a timestamp.
    pub fn new(transaction: T, timestamp: f64) -> Self {
        Self {
            transaction,
            timestamp,
        }
    }

    /// Get the timestamp of the transaction.
    pub fn timestamp(&self) -> f64 {
        self.timestamp
    }

    /// Create a new transaction with a fake timestamp for tests.
    pub fn new_for_tests(transaction: T) -> Self {
        Self {
            transaction,
            timestamp: 0.0,
        }
    }
}

impl<T: ExecutableTransaction + Clone> Deref for TransactionWithTimestamp<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionResultsAndEffects<U: Clone + Debug> {
    pub updates: U,
    pub new_state: BTreeMap<ObjectID, Object>,
}

impl<U: TransactionEffectsAPI + Clone + Debug> ExecutionResultsAndEffects<U> {
    pub fn new(updates: U, new_state: BTreeMap<ObjectID, Object>) -> Self {
        Self { updates, new_state }
    }

    pub fn success(&self) -> bool {
        self.updates.status().is_ok()
    }

    pub fn transaction_digest(&self) -> &TransactionDigest {
        self.updates.transaction_digest()
    }

    pub fn modified_at_versions(&self) -> Vec<(ObjectID, SequenceNumber)> {
        self.updates.modified_at_versions()
    }
}

pub trait StateStore<U>: BackingStore {
    /// Commit the objects to the store.
    fn commit_objects(&self, updates: U, new_state: BTreeMap<ObjectID, Object>);
}

/// The executor is responsible for executing transactions and generating new transactions.
pub trait Executor {
    /// The type of transaction to execute.
    type Transaction: Clone + ExecutableTransaction;
    /// The type of results from executing a transaction.
    type ExecutionResults: Clone + TransactionEffectsAPI + Debug;
    /// The type of store to store objects.
    type Store: StateStore<Self::ExecutionResults>;

    /// Get the context for the benchmark.
    fn get_context(&self) -> Arc<BenchmarkContext>;

    /// Execute a transaction and return the results.
    fn execute(
        ctx: Arc<BenchmarkContext>,
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionResultsAndEffects<Self::ExecutionResults>> + Send;
}

/// Short for a transaction with a timestamp.
pub type Transaction<E> = TransactionWithTimestamp<<E as Executor>::Transaction>;

/// Short for the results of executing a transaction.
pub type ExecutionResults<E> = ExecutionResultsAndEffects<<E as Executor>::ExecutionResults>;
