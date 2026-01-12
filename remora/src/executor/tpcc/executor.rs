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

use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::digests::TransactionDigest;
use sui_types::effects::TransactionEffectsAPI;
use sui_types::execution_status::ExecutionStatus;
use sui_types::gas::GasCostSummary;
use sui_types::object::{Object, Owner};
use sui_types::transaction::InputObjectKind;

use crate::config::{BenchmarkParameters, WorkloadType};
use crate::executor::api::{
    ExecutableTransaction, ExecutionResultsAndEffects, Executor, StateStore,
    TransactionWithTimestamp,
};
use crate::executor::calibration::Calibration;

use super::constants::*;
use super::data::{
    decode_tpcc_data, encode_tpcc_object, Customer, District, Item, Stock, TpccObjectKind,
    TpccState, Warehouse,
};
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
    /// Verification spins for signature verification simulation
    pub verification_spins: u64,
}

impl TpccExecutionContext {
    pub fn new(verification_duration: std::time::Duration) -> Self {
        Self {
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

        let ctx = TpccExecutionContext::new(config.verification_duration);
        let store = Arc::new(TpccObjectStore::new());

        // Initialize store with TPC-C objects
        Self::init_objects(&store, num_warehouses);

        Self {
            execution_context: Arc::new(ctx),
            store,
        }
    }

    fn init_objects(store: &TpccObjectStore, num_warehouses: usize) {
        let version = SequenceNumber::from_u64(2);
        let mut rng = StdRng::seed_from_u64(42);

        // Items (shared across all warehouses)
        for i_id in 1..=NUM_ITEMS as u32 {
            let item = Item {
                i_id,
                i_price: rng.gen_range(MIN_PRICE_CENTS..=MAX_PRICE_CENTS),
                i_name: format!("Item_{}", i_id),
                i_data: format!("ItemData_{}", i_id),
            };
            let id = TpccState::object_id_for_item(i_id);
            store.write_object(encode_tpcc_object(id, version, TpccObjectKind::Item, item));
        }

        for w_id in 1..=num_warehouses as u32 {
            // Warehouse
            let warehouse = Warehouse {
                w_id,
                w_ytd: INITIAL_W_YTD_CENTS,
                w_tax: rng.gen_range(MIN_TAX_BPS..=MAX_TAX_BPS),
                w_name: format!("Warehouse_{}", w_id),
            };
            let id = TpccState::object_id_for_warehouse(w_id);
            store.write_object(encode_tpcc_object(
                id,
                version,
                TpccObjectKind::Warehouse,
                warehouse,
            ));

            // Districts + Customers
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE as u32 {
                let district = District {
                    d_id,
                    d_w_id: w_id,
                    d_ytd: INITIAL_D_YTD_CENTS,
                    d_tax: rng.gen_range(MIN_TAX_BPS..=MAX_TAX_BPS),
                    d_next_o_id: INITIAL_NEXT_O_ID,
                    d_name: format!("District_{}_{}", w_id, d_id),
                };
                let id = TpccState::object_id_for_district(w_id, d_id);
                store.write_object(encode_tpcc_object(
                    id,
                    version,
                    TpccObjectKind::District,
                    district,
                ));

                for c_id in 1..=CUSTOMERS_PER_DISTRICT as u32 {
                    let customer = Customer {
                        c_id,
                        c_d_id: d_id,
                        c_w_id: w_id,
                        c_balance: INITIAL_BALANCE_CENTS,
                        c_ytd_payment: INITIAL_YTD_PAYMENT_CENTS,
                        c_payment_cnt: INITIAL_PAYMENT_CNT,
                        c_discount: rng.gen_range(MIN_DISCOUNT_BPS..=MAX_DISCOUNT_BPS),
                        c_first: format!("First_{}", c_id),
                        c_last: format!("Last_{}", c_id),
                    };
                    let id = TpccState::object_id_for_customer(w_id, d_id, c_id);
                    store.write_object(encode_tpcc_object(
                        id,
                        version,
                        TpccObjectKind::Customer,
                        customer,
                    ));
                }
            }

            // Stock
            for i_id in 1..=STOCK_PER_WAREHOUSE as u32 {
                let stock = Stock {
                    s_i_id: i_id,
                    s_w_id: w_id,
                    s_quantity: rng.gen_range(MIN_QUANTITY..=MAX_QUANTITY),
                    s_ytd: 0,
                    s_order_cnt: 0,
                    s_remote_cnt: 0,
                };
                let id = TpccState::object_id_for_stock(w_id, i_id);
                store.write_object(encode_tpcc_object(
                    id,
                    version,
                    TpccObjectKind::Stock,
                    stock,
                ));
            }
        }
    }

    fn update_object_with_version(input: &Object, version: SequenceNumber) -> Object {
        let move_obj = input
            .as_inner()
            .data
            .try_as_move()
            .expect("TPCC object must be a Move object")
            .clone();
        let mut updated = move_obj.clone();
        updated.increment_version_to(version);

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
        Object::new_move(updated, owner, TransactionDigest::genesis_marker())
    }

    fn get_object(
        store: &TpccObjectStore,
        cache: &mut BTreeMap<ObjectID, Object>,
        id: ObjectID,
    ) -> Object {
        if let Some(object) = cache.get(&id) {
            return object.clone();
        }

        let object = store
            .read_object(&id)
            .expect("Failed to access store")
            .unwrap_or_else(|| panic!("Unknown object {id}"));
        cache.insert(id, object.clone());
        object
    }

    fn read_tpcc_data<T: DeserializeOwned>(
        store: &TpccObjectStore,
        cache: &mut BTreeMap<ObjectID, Object>,
        id: ObjectID,
        kind: TpccObjectKind,
    ) -> T {
        let object = Self::get_object(store, cache, id);
        decode_tpcc_data(&object, kind)
    }

    /// Execute TPC-C business logic and return updated objects.
    fn execute_tpcc_logic(
        store: &TpccObjectStore,
        txn: &TpccTransaction,
        next_version: SequenceNumber,
        cache: &mut BTreeMap<ObjectID, Object>,
    ) -> BTreeMap<ObjectID, Object> {
        match txn {
            TpccTransaction::NewOrder {
                w_id,
                d_id,
                c_id,
                items,
            } => Self::execute_new_order(store, cache, next_version, *w_id, *d_id, *c_id, items),
            TpccTransaction::Payment {
                w_id,
                d_id,
                c_w_id,
                c_d_id,
                c_id,
                h_amount,
            } => Self::execute_payment(
                store,
                cache,
                next_version,
                *w_id,
                *d_id,
                *c_w_id,
                *c_d_id,
                *c_id,
                *h_amount,
            ),
        }
    }

    fn execute_new_order(
        store: &TpccObjectStore,
        cache: &mut BTreeMap<ObjectID, Object>,
        next_version: SequenceNumber,
        w_id: u32,
        d_id: u32,
        c_id: u32,
        items: &[OrderItem],
    ) -> BTreeMap<ObjectID, Object> {
        let warehouse_id = TpccState::object_id_for_warehouse(w_id);
        let warehouse: Warehouse =
            Self::read_tpcc_data(store, cache, warehouse_id, TpccObjectKind::Warehouse);
        let w_tax = warehouse.w_tax;

        let district_id = TpccState::object_id_for_district(w_id, d_id);
        let mut district: District =
            Self::read_tpcc_data(store, cache, district_id, TpccObjectKind::District);
        let d_tax = district.d_tax;
        district.d_next_o_id += 1;

        let customer_id = TpccState::object_id_for_customer(w_id, d_id, c_id);
        let customer: Customer =
            Self::read_tpcc_data(store, cache, customer_id, TpccObjectKind::Customer);
        let c_discount = customer.c_discount;

        let mut total_cents: i64 = 0;
        let mut stock_updates: BTreeMap<ObjectID, Stock> = BTreeMap::new();

        for order_item in items {
            let item_id = TpccState::object_id_for_item(order_item.i_id);
            let item: Item = Self::read_tpcc_data(store, cache, item_id, TpccObjectKind::Item);
            let i_price_cents = item.i_price as i64;

            let stock_id = TpccState::object_id_for_stock(order_item.supply_w_id, order_item.i_id);
            let stock_entry = stock_updates.entry(stock_id).or_insert_with(|| {
                Self::read_tpcc_data(store, cache, stock_id, TpccObjectKind::Stock)
            });

            if stock_entry.s_quantity >= order_item.quantity as i32 + 10 {
                stock_entry.s_quantity -= order_item.quantity as i32;
            } else {
                stock_entry.s_quantity += 91 - order_item.quantity as i32;
            }
            stock_entry.s_ytd += order_item.quantity as u32;
            stock_entry.s_order_cnt += 1;
            if order_item.supply_w_id != w_id {
                stock_entry.s_remote_cnt += 1;
            }

            let ol_amount_cents = i_price_cents * order_item.quantity as i64;
            total_cents += ol_amount_cents;
        }

        let discount_factor = RATE_SCALE as i128 - c_discount as i128;
        let tax_factor = RATE_SCALE as i128 + w_tax as i128 + d_tax as i128;
        let _final_total_cents = total_cents as i128 * discount_factor * tax_factor
            / (RATE_SCALE as i128 * RATE_SCALE as i128);

        let mut updated_objects = BTreeMap::new();
        updated_objects.insert(
            district_id,
            encode_tpcc_object(
                district_id,
                next_version,
                TpccObjectKind::District,
                district,
            ),
        );
        for (stock_id, stock) in stock_updates {
            updated_objects.insert(
                stock_id,
                encode_tpcc_object(stock_id, next_version, TpccObjectKind::Stock, stock),
            );
        }

        updated_objects
    }

    fn execute_payment(
        store: &TpccObjectStore,
        cache: &mut BTreeMap<ObjectID, Object>,
        next_version: SequenceNumber,
        w_id: u32,
        d_id: u32,
        c_w_id: u32,
        c_d_id: u32,
        c_id: u32,
        h_amount: i64,
    ) -> BTreeMap<ObjectID, Object> {
        let warehouse_id = TpccState::object_id_for_warehouse(w_id);
        let mut warehouse: Warehouse =
            Self::read_tpcc_data(store, cache, warehouse_id, TpccObjectKind::Warehouse);
        warehouse.w_ytd += h_amount;

        let district_id = TpccState::object_id_for_district(w_id, d_id);
        let mut district: District =
            Self::read_tpcc_data(store, cache, district_id, TpccObjectKind::District);
        district.d_ytd += h_amount;

        let customer_id = TpccState::object_id_for_customer(c_w_id, c_d_id, c_id);
        let mut customer: Customer =
            Self::read_tpcc_data(store, cache, customer_id, TpccObjectKind::Customer);
        customer.c_balance -= h_amount;
        customer.c_ytd_payment += h_amount;
        customer.c_payment_cnt += 1;

        let mut updated_objects = BTreeMap::new();
        updated_objects.insert(
            warehouse_id,
            encode_tpcc_object(
                warehouse_id,
                next_version,
                TpccObjectKind::Warehouse,
                warehouse,
            ),
        );
        updated_objects.insert(
            district_id,
            encode_tpcc_object(
                district_id,
                next_version,
                TpccObjectKind::District,
                district,
            ),
        );
        updated_objects.insert(
            customer_id,
            encode_tpcc_object(
                customer_id,
                next_version,
                TpccObjectKind::Customer,
                customer,
            ),
        );

        updated_objects
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
        _ctx: Arc<TpccExecutionContext>,
        store: Arc<TpccObjectStore>,
        transaction: TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionResultsAndEffects<Self::Transaction, Self::ExecutionResults>> + Send
    {
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

        let mut object_cache = BTreeMap::new();
        let updated_objects =
            Self::execute_tpcc_logic(&store, &transaction.txn, next_version, &mut object_cache);

        // Update all objects with consistent version (design choice: both reads and writes bump versions)
        for reference in &transaction.inputs {
            let id = reference.object_id();
            let output_object = if let Some(updated) = updated_objects.get(&id) {
                updated.clone()
            } else {
                let input_object = Self::get_object(&store, &mut object_cache, id);
                Self::update_object_with_version(&input_object, next_version)
            };
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
        let (num_warehouses, payment_ratio, num_nodes, hotspot_percentage, rotation_interval_secs) =
            match &config.workload {
                WorkloadType::Tpcc {
                    num_warehouses,
                    payment_ratio,
                    num_nodes,
                    hotspot_percentage,
                    rotation_interval_secs,
                } => (
                    *num_warehouses,
                    *payment_ratio,
                    *num_nodes,
                    *hotspot_percentage,
                    *rotation_interval_secs,
                ),
                _ => (1, 0.5, 1, 0.0, 20),
            };

        let total_txns = config.load * config.duration.as_secs();
        let expected_tps = config.load;

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
                        // Calculate per-task TPS: each task generates count transactions over duration seconds
                        // This ensures rotation timing is correct when workload is split across tasks
                        let per_task_tps =
                            (expected_tps + NUM_GENERATOR_TASKS - 1) / NUM_GENERATOR_TASKS;

                        // Each task gets a unique seed derived from task_id for reproducibility
                        let mut generator = super::generator::TpccGenerator::new(
                            num_warehouses,
                            payment_ratio,
                            num_nodes,
                            hotspot_percentage,
                            rotation_interval_secs,
                            per_task_tps,
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
    use sui_types::base_types::SequenceNumber;

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
        let warehouse = Warehouse {
            w_id: 1,
            w_ytd: INITIAL_W_YTD_CENTS,
            w_tax: MIN_TAX_BPS,
            w_name: "Warehouse_1".to_string(),
        };
        let obj = encode_tpcc_object(
            id,
            SequenceNumber::from_u64(2),
            TpccObjectKind::Warehouse,
            warehouse,
        );

        store.write_object(obj.clone());
        let read = store.read_object(&id).unwrap();
        assert!(read.is_some());
        assert_eq!(read.unwrap().id(), id);
    }
}
