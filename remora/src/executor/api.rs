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

#[derive(Clone, Serialize, Deserialize)]
pub struct TransactionWithTimestamp<T: Clone> {
    transaction: T,
    timestamp: f64,
}

impl<T: Clone> TransactionWithTimestamp<T> {
    pub fn new(transaction: T, timestamp: f64) -> Self {
        Self {
            transaction,
            timestamp,
        }
    }

    pub fn timestamp(&self) -> f64 {
        self.timestamp
    }

    pub fn new_for_tests(transaction: T) -> Self {
        Self {
            transaction,
            timestamp: 0.0,
        }
    }
}

impl<T: Clone> Deref for TransactionWithTimestamp<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionEffects<C: Clone + Debug> {
    pub changes: C,
    pub new_state: BTreeMap<ObjectID, Object>,
}

impl<C: TransactionEffectsAPI + Clone + Debug> ExecutionEffects<C> {
    pub fn new(changes: C, new_state: BTreeMap<ObjectID, Object>) -> Self {
        Self { changes, new_state }
    }

    pub fn success(&self) -> bool {
        self.changes.status().is_ok()
    }

    pub fn transaction_digest(&self) -> &TransactionDigest {
        self.changes.transaction_digest()
    }

    pub fn modified_at_versions(&self) -> Vec<(ObjectID, SequenceNumber)> {
        self.changes.modified_at_versions()
    }
}

pub trait ExecutableTransaction {
    fn digest(&self) -> &TransactionDigest;

    fn input_objects(&self) -> Vec<InputObjectKind>;

    fn input_object_ids(&self) -> Vec<ObjectID>;
}

pub trait StateStore<C>: BackingStore {
    /// Commit the objects to the store.
    fn commit_objects(&self, changes: C, new_state: BTreeMap<ObjectID, Object>);
}

/// The executor is responsible for executing transactions and generating new transactions.
pub trait Executor {
    /// The type of transaction to execute.
    type Transaction: Clone + ExecutableTransaction;
    /// The type of results from executing a transaction.
    type StateChanges: Clone + TransactionEffectsAPI + Debug;
    /// The type of store to store objects.
    type Store: StateStore<Self::StateChanges>;

    /// Execute a transaction and return the results.
    fn execute(
        &mut self,
        store: &Self::Store,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionEffects<Self::StateChanges>> + Send;

    ///
    fn get_context(&self) -> Arc<BenchmarkContext>;

    fn exec_on_ctx(
        ctx: Arc<BenchmarkContext>,
        store: Arc<Self::Store>,
        transaction: TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionEffects<Self::StateChanges>> + Send;
}
