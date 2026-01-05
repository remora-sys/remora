// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TPC-C transaction executor implementing the Executor trait.
//!
//! Provides a standalone executor for TPC-C NEW_ORDER and PAYMENT transactions
//! with real business logic execution.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;

use serde::{Deserialize, Serialize};
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::digests::TransactionDigest;
use sui_types::effects::TransactionEffectsAPI;
use sui_types::execution_status::ExecutionStatus;
use sui_types::gas::GasCostSummary;
use sui_types::object::{MoveObject, Object, Owner};
use sui_types::transaction::InputObjectKind;

use crate::config::{BenchmarkParameters, WorkloadType};
use crate::executor::api::{
    ExecutableTransaction, ExecutionResultsAndEffects, Executor, StateStore,
    TransactionWithTimestamp,
};
use crate::executor::calibration::Calibration;

use super::constants::*;
use super::data::TpccState;
use super::transactions::{OrderItem, TpccTransaction};

// =============================================================================
// TPC-C Transaction (implements ExecutableTransaction)
// =============================================================================

/// A TPC-C transaction that can be executed
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TpccExecutableTransaction {
    /// Transaction digest
    pub digest: TransactionDigest,
    /// The TPC-C transaction parameters
    pub txn: TpccTransaction,
    /// Input objects for this transaction (stored directly to avoid reconstruction)
    pub inputs: Vec<InputObjectKind>,
}

impl TpccExecutableTransaction {
    pub fn new(txn: TpccTransaction) -> Self {
        let inputs = txn
            .access_set()
            .into_iter()
            .map(|id| InputObjectKind::SharedMoveObject {
                id,
                initial_shared_version: SequenceNumber::from(2),
                mutable: true,
            })
            .collect();
        Self {
            digest: TransactionDigest::random(),
            txn,
            inputs,
        }
    }
}

impl ExecutableTransaction for TpccExecutableTransaction {
    fn digest(&self) -> &TransactionDigest {
        &self.digest
    }

    fn input_objects(&self) -> Vec<InputObjectKind> {
        self.inputs.clone()
    }

    fn shared_object_ids(&self) -> Vec<ObjectID> {
        self.inputs
            .iter()
            .filter_map(|kind| match kind {
                InputObjectKind::SharedMoveObject { id, .. } => Some(*id),
                _ => None,
            })
            .collect()
    }
}

// =============================================================================
// TPC-C Transaction Effects
// =============================================================================

/// Effects from executing a TPC-C transaction
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TpccTransactionEffects {
    pub transaction_digest: TransactionDigest,
    pub modified_at_versions: Vec<(ObjectID, SequenceNumber)>,
}

impl TransactionEffectsAPI for TpccTransactionEffects {
    fn status(&self) -> &ExecutionStatus {
        &ExecutionStatus::Success
    }

    fn into_status(self) -> ExecutionStatus {
        unreachable!()
    }

    fn executed_epoch(&self) -> sui_types::committee::EpochId {
        unreachable!()
    }

    fn modified_at_versions(&self) -> Vec<(ObjectID, SequenceNumber)> {
        self.modified_at_versions.clone()
    }

    fn lamport_version(&self) -> SequenceNumber {
        unreachable!()
    }

    fn old_object_metadata(&self) -> Vec<(sui_types::base_types::ObjectRef, Owner)> {
        unreachable!()
    }

    fn input_shared_objects(&self) -> Vec<sui_types::effects::InputSharedObject> {
        unreachable!()
    }

    fn created(&self) -> Vec<(sui_types::base_types::ObjectRef, Owner)> {
        unreachable!()
    }

    fn mutated(&self) -> Vec<(sui_types::base_types::ObjectRef, Owner)> {
        unreachable!()
    }

    fn unwrapped(&self) -> Vec<(sui_types::base_types::ObjectRef, Owner)> {
        unreachable!()
    }

    fn deleted(&self) -> Vec<sui_types::base_types::ObjectRef> {
        unreachable!()
    }

    fn unwrapped_then_deleted(&self) -> Vec<sui_types::base_types::ObjectRef> {
        unreachable!()
    }

    fn wrapped(&self) -> Vec<sui_types::base_types::ObjectRef> {
        unreachable!()
    }

    fn object_changes(&self) -> Vec<sui_types::effects::ObjectChange> {
        unreachable!()
    }

    fn gas_object(&self) -> (sui_types::base_types::ObjectRef, Owner) {
        unreachable!()
    }

    fn events_digest(&self) -> Option<&sui_types::digests::TransactionEventsDigest> {
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

    fn unsafe_add_input_shared_object_for_testing(
        &mut self,
        _kind: sui_types::effects::InputSharedObject,
    ) {
        unreachable!()
    }

    fn unsafe_add_deleted_live_object_for_testing(
        &mut self,
        _obj_ref: sui_types::base_types::ObjectRef,
    ) {
        unreachable!()
    }

    fn unsafe_add_object_tombstone_for_testing(
        &mut self,
        _obj_ref: sui_types::base_types::ObjectRef,
    ) {
        unreachable!()
    }
}

// =============================================================================
// TPC-C Object Store
// =============================================================================

/// Object store for TPC-C transactions
#[derive(Clone)]
pub struct TpccObjectStore {
    objects: Arc<DashMap<ObjectID, Object>>,
}

impl TpccObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(DashMap::new()),
        }
    }

    pub fn write_object(&self, object: Object) {
        self.objects.insert(object.id(), object);
    }
}

impl Default for TpccObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore<TpccTransactionEffects> for TpccObjectStore {
    fn read_object(
        &self,
        id: &ObjectID,
    ) -> Result<Option<Object>, sui_types::storage::error::Error> {
        Ok(self.objects.get(id).map(|r| r.value().clone()))
    }

    fn commit_objects(
        &self,
        _updates: TpccTransactionEffects,
        new_state: BTreeMap<ObjectID, Object>,
    ) {
        for (id, object) in new_state {
            self.objects.insert(id, object);
        }
    }

    fn commit_new_objects(&self, new_state: BTreeMap<ObjectID, Object>) {
        for (id, object) in new_state {
            self.objects.insert(id, object);
        }
    }
}

// =============================================================================
// TPC-C Execution Context
// =============================================================================

/// Execution context for TPC-C transactions
pub struct TpccExecutionContext {
    /// TPC-C database state for real execution
    pub tpcc_state: TpccState,
    /// Verification spins for signature verification simulation
    pub verification_spins: u64,
}

impl TpccExecutionContext {
    pub fn new(num_warehouses: usize, verification_duration: std::time::Duration) -> Self {
        Self {
            tpcc_state: TpccState::new(num_warehouses),
            verification_spins: Calibration::calibrate(verification_duration),
        }
    }
}

// =============================================================================
// TPC-C Executor
// =============================================================================

/// Executor for TPC-C transactions with real business logic
#[derive(Clone)]
pub struct TpccExecutor {
    execution_context: Arc<TpccExecutionContext>,
    store: Arc<TpccObjectStore>,
}

impl TpccExecutor {
    pub async fn new(config: &BenchmarkParameters) -> Self {
        let num_warehouses = match &config.workload {
            WorkloadType::Tpcc { num_warehouses, .. } => *num_warehouses,
            _ => panic!("TpccExecutor requires Tpcc workload type"),
        };

        let ctx = TpccExecutionContext::new(num_warehouses, config.verification_duration);
        let store = Arc::new(TpccObjectStore::new());

        // Initialize store with TPC-C objects
        Self::init_objects(&store, num_warehouses);

        Self {
            execution_context: Arc::new(ctx),
            store,
        }
    }

    fn init_objects(store: &TpccObjectStore, num_warehouses: usize) {
        // Warehouses
        for w_id in 1..=num_warehouses as u32 {
            let id = TpccState::object_id_for_warehouse(w_id);
            store.write_object(Self::create_shared_object(id));
        }

        // Districts
        for w_id in 1..=num_warehouses as u32 {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE as u32 {
                let id = TpccState::object_id_for_district(w_id, d_id);
                store.write_object(Self::create_shared_object(id));
            }
        }

        // Customers
        for w_id in 1..=num_warehouses as u32 {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE as u32 {
                for c_id in 1..=CUSTOMERS_PER_DISTRICT as u32 {
                    let id = TpccState::object_id_for_customer(w_id, d_id, c_id);
                    store.write_object(Self::create_shared_object(id));
                }
            }
        }

        // Stock
        for w_id in 1..=num_warehouses as u32 {
            for i_id in 1..=STOCK_PER_WAREHOUSE as u32 {
                let id = TpccState::object_id_for_stock(w_id, i_id);
                store.write_object(Self::create_shared_object(id));
            }
        }

        // Items
        for i_id in 1..=NUM_ITEMS as u32 {
            let id = TpccState::object_id_for_item(i_id);
            store.write_object(Self::create_shared_object(id));
        }
    }

    fn create_shared_object(id: ObjectID) -> Object {
        let version = SequenceNumber::from_u64(2);
        let obj = MoveObject::new_gas_coin(version, id, 10);
        let owner = Owner::Shared {
            initial_shared_version: version,
        };
        Object::new_move(obj, owner, TransactionDigest::genesis_marker())
    }

    fn update_object_with_version(input: Object, version: SequenceNumber) -> Object {
        let id = input.id();
        let obj = MoveObject::new_gas_coin(version, id, 10);
        let owner = if input.is_shared() {
            Owner::Shared {
                initial_shared_version: version,
            }
        } else {
            input
                .as_inner()
                .get_owner_and_id()
                .expect("Should be single owner")
                .0
        };
        Object::new_move(obj, owner, TransactionDigest::genesis_marker())
    }

    /// Execute TPC-C business logic and commit state changes
    fn execute_tpcc_logic(state: &TpccState, txn: &TpccTransaction) {
        match txn {
            TpccTransaction::NewOrder {
                w_id,
                d_id,
                c_id,
                items,
            } => {
                Self::execute_new_order(state, *w_id, *d_id, *c_id, items);
            }
            TpccTransaction::Payment {
                w_id,
                d_id,
                c_w_id,
                c_d_id,
                c_id,
                h_amount,
            } => {
                Self::execute_payment(state, *w_id, *d_id, *c_w_id, *c_d_id, *c_id, *h_amount);
            }
        }
    }

    fn execute_new_order(state: &TpccState, w_id: u32, d_id: u32, c_id: u32, items: &[OrderItem]) {
        // 1. Read warehouse tax rate
        let w_tax = state
            .warehouses
            .get(&w_id)
            .expect("Warehouse not found")
            .w_tax;

        // 2. Read district tax (FastIds: order ID now comes from atomic counter)
        let d_tax = state
            .districts
            .get(&(w_id, d_id))
            .expect("District not found")
            .d_tax;
        let _o_id = state.next_order_id(w_id, d_id); // FastIds optimization

        // 3. Read customer discount
        let c_discount = state
            .customers
            .get(&(w_id, d_id, c_id))
            .expect("Customer not found")
            .c_discount;

        // 4. Process each item and update stock
        let mut total = 0.0;
        for order_item in items {
            let i_price = state
                .items
                .get(&order_item.i_id)
                .expect("Item not found")
                .i_price;

            // Update stock quantity (mutable access via DashMap)
            let mut stock = state
                .stock
                .get_mut(&(order_item.supply_w_id, order_item.i_id))
                .expect("Stock not found");

            // TPC-C stock update logic
            if stock.s_quantity >= order_item.quantity as i32 + 10 {
                stock.s_quantity -= order_item.quantity as i32;
            } else {
                stock.s_quantity += 91 - order_item.quantity as i32;
            }
            stock.s_ytd += order_item.quantity as u32;
            stock.s_order_cnt += 1;
            if order_item.supply_w_id != w_id {
                stock.s_remote_cnt += 1;
            }

            let ol_amount = i_price * order_item.quantity as f64;
            total += ol_amount;
        }

        // 5. Apply tax and discount (result computed for correctness)
        let _final_total = total * (1.0 - c_discount) * (1.0 + w_tax + d_tax);
    }

    fn execute_payment(
        state: &TpccState,
        w_id: u32,
        d_id: u32,
        c_w_id: u32,
        c_d_id: u32,
        c_id: u32,
        h_amount: f64,
    ) {
        // 1. Update warehouse YTD
        {
            let mut warehouse = state
                .warehouses
                .get_mut(&w_id)
                .expect("Warehouse not found");
            warehouse.w_ytd += h_amount;
        }

        // 2. Update district YTD
        {
            let mut district = state
                .districts
                .get_mut(&(w_id, d_id))
                .expect("District not found");
            district.d_ytd += h_amount;
        }

        // 3. Update customer balance and payment info
        {
            let mut customer = state
                .customers
                .get_mut(&(c_w_id, c_d_id, c_id))
                .expect("Customer not found");
            customer.c_balance -= h_amount;
            customer.c_ytd_payment += h_amount;
            customer.c_payment_cnt += 1;
        }
    }
}

impl Executor for TpccExecutor {
    type Transaction = TpccExecutableTransaction;
    type ExecutionResults = TpccTransactionEffects;
    type Store = TpccObjectStore;
    type ExecutionContext = TpccExecutionContext;

    fn context(&self) -> Arc<TpccExecutionContext> {
        self.execution_context.clone()
    }

    fn execute(
        ctx: Arc<TpccExecutionContext>,
        store: Arc<TpccObjectStore>,
        transaction: TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionResultsAndEffects<Self::Transaction, Self::ExecutionResults>> + Send
    {
        // Execute TPC-C business logic (CPU work simulation)
        Self::execute_tpcc_logic(&ctx.tpcc_state, &transaction.txn);

        let mut modified_at_versions = Vec::new();
        let mut new_state = BTreeMap::new();

        // Find max version across input objects
        let mut max_version = SequenceNumber::from(2);
        for (id, version) in &transaction.shared_objects {
            if let Some(v) = version {
                if *v > max_version {
                    max_version = *v;
                }
                modified_at_versions.push((*id, *v));
            }
        }

        let next_version = max_version.next();

        // Update all objects with consistent version (design choice: both reads and writes bump versions)
        for reference in &transaction.inputs {
            let id = reference.object_id();
            let input_object = store
                .read_object(&id)
                .expect("Failed to access store")
                .unwrap_or_else(|| panic!("Unknown object {id}"));

            let output_object = Self::update_object_with_version(input_object, next_version);
            new_state.insert(id, output_object);
        }

        let updates = TpccTransactionEffects {
            transaction_digest: *transaction.digest(),
            modified_at_versions,
        };
        store.commit_objects(updates.clone(), new_state.clone());

        async move { ExecutionResultsAndEffects::new(transaction, Some(updates), Some(new_state)) }
    }

    fn pre_execute_check(
        _ctx: Arc<TpccExecutionContext>,
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        for reference in &transaction.inputs {
            let id = reference.object_id();
            if let Some(object) = store.read_object(&id).ok().flatten() {
                // Check shared object version matches expected version
                if let InputObjectKind::SharedMoveObject { .. } = reference {
                    if let Some((_, version)) = transaction
                        .shared_objects
                        .iter()
                        .find(|(obj_id, _)| *obj_id == &id)
                    {
                        if let Some(expected_version) = version {
                            if object.version() != *expected_version {
                                tracing::debug!(
                                    "Version mismatch for object {:?}: expected {:?}, actual {:?}",
                                    id,
                                    expected_version,
                                    object.version()
                                );
                                return false;
                            }
                        }
                    }
                }
            } else {
                tracing::debug!("Object {:?} not found in store", id);
                return false;
            }
        }
        true
    }

    fn assign_shared_object_versions_with_required_versions(
        &self,
        _transactions: &[Self::Transaction],
        _required_versions: &[(ObjectID, SequenceNumber)],
    ) -> impl Future<Output = ()> + std::marker::Send {
        async {}
    }

    fn generate_transactions(
        config: &BenchmarkParameters,
        _working_directory: Option<PathBuf>,
    ) -> impl Future<Output = Vec<Self::Transaction>> + Send {
        let (num_warehouses, payment_ratio) = match &config.workload {
            WorkloadType::Tpcc {
                num_warehouses,
                payment_ratio,
            } => (*num_warehouses, *payment_ratio),
            _ => (1, 0.5),
        };

        let total_txns = config.load * config.duration.as_secs();

        async move {
            // Parallelize transaction generation across multiple tasks
            const NUM_GENERATOR_TASKS: u64 = 16;
            let chunk_size = (total_txns + NUM_GENERATOR_TASKS - 1) / NUM_GENERATOR_TASKS;

            let handles: Vec<_> = (0..NUM_GENERATOR_TASKS)
                .map(|task_id| {
                    let start_idx = task_id * chunk_size;
                    let end_idx = ((task_id + 1) * chunk_size).min(total_txns);
                    let count = end_idx.saturating_sub(start_idx);

                    tokio::task::spawn_blocking(move || {
                        // Each task gets a unique seed derived from task_id for reproducibility
                        let mut generator = super::generator::TpccGenerator::new(
                            num_warehouses,
                            payment_ratio,
                            task_id,
                        );

                        (0..count)
                            .map(|_| {
                                let txn = generator.generate_transaction();
                                TpccExecutableTransaction::new(txn)
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();

            // Join all tasks and flatten results
            let mut transactions = Vec::with_capacity(total_txns as usize);
            for handle in handles {
                match handle.await {
                    Ok(chunk) => transactions.extend(chunk),
                    Err(e) => tracing::error!("Transaction generation task failed: {:?}", e),
                }
            }

            transactions
        }
    }

    fn init_store(&self) -> Arc<Self::Store> {
        self.store.clone()
    }

    fn verify_transaction(
        ctx: Arc<TpccExecutionContext>,
        _transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = bool> + Send {
        Calibration::calibrated_work(ctx.verification_spins);
        async { true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tpcc_executable_transaction() {
        let txn = TpccTransaction::NewOrder {
            w_id: 1,
            d_id: 1,
            c_id: 1,
            items: vec![OrderItem {
                i_id: 1,
                supply_w_id: 1,
                quantity: 5,
            }],
        };

        let exec_txn = TpccExecutableTransaction::new(txn);
        assert!(!exec_txn.inputs.is_empty());
        assert!(!exec_txn.shared_object_ids().is_empty());
    }

    #[test]
    fn test_tpcc_object_store() {
        let store = TpccObjectStore::new();
        let id = ObjectID::random();
        let obj = TpccExecutor::create_shared_object(id);

        store.write_object(obj.clone());
        let read = store.read_object(&id).unwrap();
        assert!(read.is_some());
        assert_eq!(read.unwrap().id(), id);
    }
}
