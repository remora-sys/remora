// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::Debug, future::Future, ops::Deref, sync::Arc};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
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

pub type Timestamp = f64;

/// A transaction with a timestamp. This is used to compute performance.
#[derive(Clone, Serialize, Deserialize)]
pub struct TransactionWithTimestamp<T: ExecutableTransaction + Clone> {
    /// The transaction.
    transaction: T,
    /// The timestamp when the transaction was created.
    timestamp: Timestamp,
}

impl<T: ExecutableTransaction + Clone> TransactionWithTimestamp<T> {
    /// Create a new transaction with a timestamp.
    pub fn new(transaction: T, timestamp: Timestamp) -> Self {
        Self {
            transaction,
            timestamp,
        }
    }

    /// Get the timestamp of the transaction.
    pub fn timestamp(&self) -> Timestamp {
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
    fn commit_new_objects(&self, new_state: BTreeMap<ObjectID, Object>);
}

/// The executor is responsible for executing transactions and generating new transactions.
pub trait Executor: Clone {
    /// The type of transaction to execute.
    type Transaction: Clone + ExecutableTransaction + Serialize + DeserializeOwned;
    /// The type of results from executing a transaction.
    type ExecutionResults: Clone + TransactionEffectsAPI + Debug;
    /// The type of store to store objects.
    type Store: StateStore<Self::ExecutionResults>;

    /// Get the context for the benchmark.
    fn context(&self) -> Arc<BenchmarkContext>;

    /// Execute a transaction and return the results.
    fn execute(
        ctx: Arc<BenchmarkContext>,
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionResultsAndEffects<Self::ExecutionResults>> + Send;

    /// Check version ID check prior to execution
    fn pre_execute_check(
        ctx: Arc<BenchmarkContext>,
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool;
}

/// Short for a transaction with a timestamp.
pub type RemoraTransaction<E> = TransactionWithTimestamp<<E as Executor>::Transaction>;

/// Short for the results of executing a transaction.
pub type ExecutionResults<E> = ExecutionResultsAndEffects<<E as Executor>::ExecutionResults>;

/// Short for the store used by the executor.
pub type Store<E> = Arc<<E as Executor>::Store>;

pub type NewStates = BTreeMap<ObjectID, Object>;

#[derive(Clone, Serialize, Deserialize)]
pub enum PrimaryToProxyMessage<T>
where
    T: ExecutableTransaction + Clone,
{
    Txn(TransactionWithTimestamp<T>),
    States(NewStates),
}

pub type ExecutorIndex = usize;
