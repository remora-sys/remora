// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, RwLock},
    time::Duration,
};

use sui_single_node_benchmark::benchmark_context::BenchmarkContext;
use sui_types::{
    base_types::{ObjectID, ObjectRef, SequenceNumber},
    committee::EpochId,
    digests::{TransactionDigest, TransactionEventsDigest},
    effects::{InputSharedObject, ObjectChange, TransactionEffectsAPI},
    execution_status::ExecutionStatus,
    gas::GasCostSummary,
    object::{Object, Owner},
    transaction::InputObjectKind,
};

use super::api::{
    ExecutableTransaction,
    ExecutionResultsAndEffects,
    Executor,
    StateStore,
    TransactionWithTimestamp,
};

#[derive(Clone)]
pub struct FakeTransaction {
    pub digest: TransactionDigest,
    inputs: Vec<InputObjectKind>,
}

impl ExecutableTransaction for FakeTransaction {
    fn digest(&self) -> &TransactionDigest {
        &self.digest
    }

    fn input_objects(&self) -> Vec<InputObjectKind> {
        self.inputs.clone()
    }
}

#[derive(Debug, Clone)]
pub struct FakeTransactionEffects {
    transaction_digest: TransactionDigest,
    modified_at_versions: Vec<(ObjectID, SequenceNumber)>,
}

impl TransactionEffectsAPI for FakeTransactionEffects {
    fn status(&self) -> &ExecutionStatus {
        &ExecutionStatus::Success
    }

    fn into_status(self) -> ExecutionStatus {
        unreachable!()
    }

    fn executed_epoch(&self) -> EpochId {
        unreachable!()
    }

    fn modified_at_versions(&self) -> Vec<(ObjectID, SequenceNumber)> {
        self.modified_at_versions.clone()
    }

    fn lamport_version(&self) -> SequenceNumber {
        unreachable!()
    }

    fn old_object_metadata(&self) -> Vec<(ObjectRef, Owner)> {
        unreachable!()
    }

    fn input_shared_objects(&self) -> Vec<InputSharedObject> {
        unreachable!()
    }

    fn created(&self) -> Vec<(ObjectRef, Owner)> {
        unreachable!()
    }

    fn mutated(&self) -> Vec<(ObjectRef, Owner)> {
        unreachable!()
    }

    fn unwrapped(&self) -> Vec<(ObjectRef, Owner)> {
        unreachable!()
    }

    fn deleted(&self) -> Vec<ObjectRef> {
        unreachable!()
    }

    fn unwrapped_then_deleted(&self) -> Vec<ObjectRef> {
        unreachable!()
    }

    fn wrapped(&self) -> Vec<ObjectRef> {
        unreachable!()
    }

    fn object_changes(&self) -> Vec<ObjectChange> {
        unreachable!()
    }

    fn gas_object(&self) -> (ObjectRef, Owner) {
        unreachable!()
    }

    fn events_digest(&self) -> Option<&TransactionEventsDigest> {
        unreachable!()
    }

    fn dependencies(&self) -> &[TransactionDigest] {
        unreachable!()
    }

    fn transaction_digest(&self) -> &TransactionDigest {
        &self.transaction_digest
    }

    fn gas_cost_summary(&self) -> &GasCostSummary {
        unreachable!()
    }

    fn status_mut_for_testing(&mut self) -> &mut ExecutionStatus {
        unreachable!()
    }

    fn gas_cost_summary_mut_for_testing(&mut self) -> &mut GasCostSummary {
        unreachable!()
    }

    fn transaction_digest_mut_for_testing(&mut self) -> &mut TransactionDigest {
        unreachable!()
    }

    fn dependencies_mut_for_testing(&mut self) -> &mut Vec<TransactionDigest> {
        unreachable!()
    }

    fn unsafe_add_input_shared_object_for_testing(&mut self, _kind: InputSharedObject) {
        unreachable!()
    }

    fn unsafe_add_deleted_live_object_for_testing(&mut self, _obj_ref: ObjectRef) {
        unreachable!()
    }

    fn unsafe_add_object_tombstone_for_testing(&mut self, _obj_ref: ObjectRef) {
        unreachable!()
    }
}

pub struct FakeObjectStore {
    objects: Arc<RwLock<BTreeMap<ObjectID, Object>>>,
}

impl<FakeTransactionEffects> StateStore<FakeTransactionEffects> for FakeObjectStore {
    fn commit_objects(
        &self,
        _updates: FakeTransactionEffects,
        new_state: BTreeMap<ObjectID, Object>,
    ) {
        let mut objects = self.objects.write().unwrap();
        for (object_id, object) in new_state {
            objects.insert(object_id, object);
        }
    }

    fn commit_new_objects(&self, new_state: BTreeMap<ObjectID, Object>) {
        todo!()
    }

    fn read_object(
        &self,
        id: &ObjectID,
    ) -> Result<Option<Object>, sui_types::storage::error::Error> {
        let objects = self.objects.read().unwrap();
        Ok(objects.get(id).cloned())
    }
}

pub struct FakeExecutor {
    // TODO: Unused, this should be removed by either making the benchmark context generic or
    // changing the Executor trait structure.
    benchmark_context: Arc<BenchmarkContext>,
    /// The duration of the transaction execution (in number of spins).
    execution_duration: u64,
    /// The duration of the checks that are done before executing the transaction (in number of spins).
    checks_duration: u64,
}

impl FakeExecutor {
    pub fn new(
        benchmark_context: BenchmarkContext,
        execution_duration: Duration,
        checks_duration: Duration,
    ) -> Self {
        Self {
            benchmark_context: Arc::new(benchmark_context),
            execution_duration: Self::calibrate(execution_duration),
            checks_duration: Self::calibrate(checks_duration),
        }
    }

    fn calibrate(duration: Duration) -> u64 {
        // https://gist.github.com/Scofield626/48bc1926dbd22a197573d9d7b0142ff6
        todo!()
    }
}

impl Executor for FakeExecutor {
    type Transaction = FakeTransaction;

    type ExecutionResults = FakeTransactionEffects;

    type Store = FakeObjectStore;

    fn context(&self) -> Arc<BenchmarkContext> {
        self.benchmark_context.clone()
    }

    fn execute(
        _ctx: Arc<BenchmarkContext>,
        _store: Arc<Self::Store>,
        _transaction: TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionResultsAndEffects<Self::Transaction, Self::ExecutionResults>> + Send {
        // for _ in 0..self.execution_duration {
        //     std::hint::spin_loop();
        // }

        async move { todo!() }
    }

    fn pre_execute_check(
        ctx: Arc<BenchmarkContext>,
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        todo!()
    }

    fn assign_shared_object_versions(
        &self,
        _transactions: &[Self::Transaction],
    ) -> impl Future<Output = ()> + std::marker::Send {
        todo!()
    }
}
