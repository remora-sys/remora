// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    marker::PhantomData,
    sync::{Arc, RwLock},
    time::Duration,
};

use futures::{stream::FuturesUnordered, StreamExt};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use sui_types::{
    base_types::{ObjectID, ObjectRef, SequenceNumber, SuiAddress},
    committee::EpochId,
    digests::{TransactionDigest, TransactionEventsDigest},
    effects::{InputSharedObject, ObjectChange, TransactionEffectsAPI},
    execution_status::ExecutionStatus,
    gas::GasCostSummary,
    object::{MoveObject, Object, Owner},
    transaction::InputObjectKind,
};

use super::{
    super::config::{BenchmarkParameters, WorkloadType},
    api::{
        ExecutableTransaction, ExecutionResultsAndEffects, Executor, StateStore,
        TransactionWithTimestamp,
    },
    calibration::Calibration,
    sui::get_object_ids_for_dependency_tracking,
};

/// A fake owned object for testing.
pub fn fake_owned_object(version: u64) -> Object {
    let id = ObjectID::random();
    fake_owned_object_with_id(version, id)
}

/// A fake owned object for testing.
pub fn fake_owned_object_with_id(version: u64, id: ObjectID) -> Object {
    let object_version = SequenceNumber::from_u64(version);
    let owner = SuiAddress::random_for_testing_only();
    Object::with_id_owner_version_for_testing(id, object_version, owner)
}

/// A fake shared object for testing.
pub fn fake_shared_object(initial_version: u64) -> Object {
    let id = ObjectID::random();
    fake_shared_object_with_id(initial_version, id)
}

/// A fake shared object with a fixed Id for testing.
pub fn fake_shared_object_with_id(initial_version: u64, id: ObjectID) -> Object {
    let object_version = SequenceNumber::from_u64(initial_version);
    let obj = MoveObject::new_gas_coin(object_version, id, 10);
    let owner = Owner::Shared {
        initial_shared_version: obj.version(),
    };
    Object::new_move(obj, owner, TransactionDigest::genesis_marker())
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FakeTransaction {
    pub digest: TransactionDigest,
    inputs: Vec<InputObjectKind>,
}

impl FakeTransaction {
    pub fn new(inputs: Vec<InputObjectKind>) -> Self {
        Self {
            digest: TransactionDigest::random(),
            inputs,
        }
    }

    pub fn from_store(
        store: &FakeObjectStore<FakeTransactionEffects>,
        inputs: Vec<ObjectID>,
    ) -> Self {
        let inputs = inputs
            .iter()
            .map(|id| {
                let object = store
                    .read_object(id)
                    .expect("Failed to access store")
                    .unwrap_or_else(|| panic!("Unknown object {id}"));
                if object.is_shared() {
                    InputObjectKind::SharedMoveObject {
                        id: object.id(),
                        initial_shared_version: object.version(),
                        mutable: true,
                    }
                } else {
                    InputObjectKind::ImmOrOwnedMoveObject(object.compute_object_reference())
                }
            })
            .collect();
        Self::new(inputs)
    }
}

impl ExecutableTransaction for FakeTransaction {
    fn digest(&self) -> &TransactionDigest {
        &self.digest
    }

    fn input_objects(&self) -> Vec<InputObjectKind> {
        self.inputs.clone()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FakeTransactionEffects {
    transaction_digest: TransactionDigest,
    modified_at_versions: Vec<(ObjectID, SequenceNumber)>,
}

/// TODO: We may get away with using the TransactionEffectAPI trait.
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

pub struct FakeObjectStore<FakeTransactionEffects> {
    _phantom: PhantomData<FakeTransactionEffects>,
    objects: Arc<RwLock<BTreeMap<ObjectID, Object>>>,
}

impl FakeObjectStore<FakeTransactionEffects> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
            objects: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn write_object(&self, object: Object) {
        let mut objects = self.objects.write().unwrap();
        objects.insert(object.id(), object);
    }
}

impl Default for FakeObjectStore<FakeTransactionEffects> {
    fn default() -> Self {
        Self::new()
    }
}

impl<FakeTransactionEffects> StateStore<FakeTransactionEffects>
    for FakeObjectStore<FakeTransactionEffects>
{
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

    fn commit_new_objects(&self, _new_state: BTreeMap<ObjectID, Object>) {
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

pub struct FakeExecutionContext {
    /// The duration of the transaction execution (in number of spins).
    pub execution_spins: u64,
    /// The duraiton of the verification (in number of spins).
    pub verification_spins: u64,
}

impl FakeExecutionContext {
    pub fn new(execution_duration: Duration, verification_duration: Duration) -> Self {
        Self {
            execution_spins: Calibration::calibrate(execution_duration),
            verification_spins: Calibration::calibrate(verification_duration),
        }
    }
}

#[derive(Clone)]
pub struct FakeExecutor {
    execution_context: Arc<FakeExecutionContext>,
}

impl FakeExecutor {
    pub async fn new(config: &BenchmarkParameters) -> Self {
        let execution_duration = match config.workload {
            WorkloadType::FakedNoContention {
                execution_duration,
                number_of_inputs: _,
            } => execution_duration,
            WorkloadType::FakedContention {
                execution_duration,
                number_of_inputs: _,
                contention: _,
            } => execution_duration,
            WorkloadType::FakeSolanaTransactions { execution_duration } => execution_duration,
            WorkloadType::FakeEthereumTransfers { execution_duration } => execution_duration,
            WorkloadType::FakeEthereumNftMint { execution_duration } => execution_duration,
            WorkloadType::FakeUniswapNormal { execution_duration } => execution_duration,
            WorkloadType::FakeUniswapPeak { execution_duration } => execution_duration,
            _ => {
                panic!("Error: Unsupported workload type for fake executor")
            }
        };
        let ctx = FakeExecutionContext::new(execution_duration, config.verification_duration);
        Self {
            execution_context: Arc::new(ctx),
        }
    }

    pub fn update_object(input: Object) -> Object {
        let id = ObjectID::random();
        let version = SequenceNumber::from_u64(input.version().value() + 1);
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
}

impl Executor for FakeExecutor {
    type Transaction = FakeTransaction;
    type ExecutionResults = FakeTransactionEffects;
    type Store = FakeObjectStore<FakeTransactionEffects>;
    type ExecutionContext = FakeExecutionContext;

    fn context(&self) -> Arc<FakeExecutionContext> {
        self.execution_context.clone()
    }

    fn execute(
        ctx: Arc<FakeExecutionContext>,
        store: Arc<FakeObjectStore<FakeTransactionEffects>>,
        transaction: TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = ExecutionResultsAndEffects<Self::Transaction, Self::ExecutionResults>> + Send
    {
        // Simulate execution
        Calibration::calibrated_work(ctx.execution_spins);

        let mut modified_at_versions = Vec::new();
        let mut new_state = BTreeMap::new();
        for reference in &transaction.inputs {
            // Read input objects.
            let id = reference.object_id();
            let input_object = store
                .read_object(&id)
                .expect("Failed to access store")
                .unwrap_or_else(|| panic!("Unknown object {id}"));
            modified_at_versions.push((id, input_object.version()));

            // Create output objects.
            let output_object = Self::update_object(input_object);
            new_state.insert(id, output_object);
        }

        // Update the store.
        let updates = FakeTransactionEffects {
            transaction_digest: *transaction.digest(),
            modified_at_versions,
        };
        store.commit_objects(updates.clone(), new_state.clone());

        async move { ExecutionResultsAndEffects::new(transaction, Some(updates), Some(new_state)) }
    }

    fn pre_execute_check(
        _ctx: Arc<FakeExecutionContext>,
        _store: Arc<Self::Store>,
        _transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        true
    }

    fn pre_execute_check_objects(
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        for reference in &transaction.inputs {
            let id = reference.object_id();
            if store
                .read_object(&id)
                .expect("failed to access store")
                .is_none()
            {
                return false;
            }
        }
        true
    }

    /// Assign a shared object version.
    async fn assign_shared_object_versions(&self, _transactions: &[Self::Transaction]) {
        //todo!()
    }

    fn generate_transactions(
        config: &BenchmarkParameters,
        _working_directory: Option<std::path::PathBuf>,
    ) -> impl Future<Output = Vec<Self::Transaction>> + Send {
        generate_fake_transactions(config)
    }

    fn init_store(&self) -> Self::Store {
        FakeObjectStore::new()
    }

    fn optimistically_pre_generate_objects(
        store: Arc<Self::Store>,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) {
        let obj_ids = get_object_ids_for_dependency_tracking::<FakeExecutor>(transaction.clone());
        for obj_id in obj_ids {
            store.write_object(fake_owned_object_with_id(0, obj_id));
        }
    }

    async fn verify_transaction(
        ctx: Arc<Self::ExecutionContext>,
        _transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        // Simulate verification
        Calibration::calibrated_work(ctx.verification_spins);
        true
    }

    fn get_objects_for_dependency_tracking(
        _ctx: Arc<Self::ExecutionContext>,
        _store: Arc<Self::Store>,
        _transaction: TransactionWithTimestamp<Self::Transaction>,
    ) -> Vec<(ObjectID, SequenceNumber)> {
        todo!()
    }
}

pub fn generate_fake_owned_object_transaction(number_of_inputs: usize) -> FakeTransaction {
    let inputs: Vec<_> = (0..number_of_inputs)
        .map(|_| {
            let object = fake_owned_object(0);
            let reference = object.compute_object_reference();
            InputObjectKind::ImmOrOwnedMoveObject(reference)
        })
        .collect();
    FakeTransaction::new(inputs)
}

/// Generate a fake transaction with a given number of inputs and contention level.
/// Setting contention to 0 will generate a transaction accessing shared objects
/// that are not overlapped with any other transactions, while setting contention
/// to 100 will generate a transaction accessing a same shared object with all
/// other transactions.
pub fn generate_fake_shared_object_transaction(
    number_of_inputs: usize,
    contention: u64,
) -> FakeTransaction {
    let contention = contention.min(100);
    let coin = rand::thread_rng().gen_range(0..100);
    let inputs: Vec<_> = (0..number_of_inputs)
        .map(|i| {
            // Depending on the contention level, the first object may have a fixed id.
            let object = if contention > coin && i == 0 {
                fake_shared_object_with_id(0, ObjectID::ZERO)
            } else {
                fake_shared_object(0)
            };
            let reference = object.compute_object_reference();
            InputObjectKind::ImmOrOwnedMoveObject(reference)
        })
        .collect();
    FakeTransaction::new(inputs)
}

pub async fn parallel_generate_transaction<F>(
    cnt: u64,
    number_of_inputs: usize,
    func: F,
) -> Vec<FakeTransaction>
where
    F: Fn(usize) -> FakeTransaction + Send + 'static + Copy,
{
    let tasks: FuturesUnordered<_> = Default::default();
    for _ in 0..cnt {
        tasks.push(tokio::spawn(async move { func(number_of_inputs) }));
    }
    let results: Vec<_> = tasks.collect().await;
    results
        .into_iter()
        .filter_map(|res| match res {
            Ok(tx) => Some(tx),
            Err(err) => {
                eprintln!("Faked Transaction generation faild {:?}", err);
                None
            }
        })
        .collect()
}

pub async fn generate_fake_transactions(config: &BenchmarkParameters) -> Vec<FakeTransaction> {
    let pre_generation = config.load * config.duration.as_secs();

    match config.workload {
        WorkloadType::FakedNoContention {
            execution_duration: _,
            number_of_inputs,
        } => {
            parallel_generate_transaction(
                pre_generation,
                number_of_inputs,
                generate_fake_owned_object_transaction,
            )
            .await
        }
        WorkloadType::FakedContention {
            execution_duration: _,
            number_of_inputs,
            contention,
        } => {
            parallel_generate_transaction(pre_generation, number_of_inputs, move |x| {
                generate_fake_shared_object_transaction(x, contention)
            })
            .await
        }
        WorkloadType::FakeSolanaTransactions { .. } => {
            let mut rng = StdRng::seed_from_u64(0);
            let (_, transactions) = generate_fake_load_objects_and_transactions(
                &mut rng,
                pre_generation as usize,
                solana_load,
            );
            transactions
        }
        WorkloadType::FakeEthereumTransfers { .. } => {
            let mut rng = StdRng::seed_from_u64(0);
            let (_, transactions) = generate_fake_load_objects_and_transactions(
                &mut rng,
                pre_generation as usize,
                eth_transfers,
            );
            transactions
        }
        WorkloadType::FakeEthereumNftMint { .. } => {
            let mut rng = StdRng::seed_from_u64(0);
            let (_, transactions) = generate_fake_load_objects_and_transactions(
                &mut rng,
                pre_generation as usize,
                eth_mint,
            );
            transactions
        }
        WorkloadType::FakeUniswapNormal { .. } => {
            let mut rng = StdRng::seed_from_u64(0);
            let (_, transactions) = generate_fake_load_objects_and_transactions(
                &mut rng,
                pre_generation as usize,
                uniswap_normal,
            );
            transactions
        }
        WorkloadType::FakeUniswapPeak { .. } => {
            let mut rng = StdRng::seed_from_u64(0);
            let (_, transactions) = generate_fake_load_objects_and_transactions(
                &mut rng,
                pre_generation as usize,
                uniswap_peak,
            );
            transactions
        }

        _ => {
            panic!("Error: Unsupported workloadtype in the fake executor");
        }
    }
}

pub fn generate_fake_load_objects_and_transactions<R, F>(
    rng: &mut R,
    tx_count: usize,
    load: F,
) -> (HashSet<Object>, Vec<FakeTransaction>)
where
    R: Rng,
    F: Fn(&mut R) -> Vec<usize>,
{
    let mut objects = HashSet::new();
    let mut transactions = Vec::new();
    for _ in 0..tx_count {
        let objects = load(rng)
            .into_iter()
            .map(|id| {
                let mut bytes = [0u8; ObjectID::LENGTH];
                let n_bytes = id.to_le_bytes();
                let copy_len = n_bytes.len().min(ObjectID::LENGTH);
                bytes[..copy_len].copy_from_slice(&n_bytes[..copy_len]);

                let object_id = ObjectID::from_bytes(bytes).expect("Cannot convert bytes");
                let object = fake_shared_object_with_id(0, object_id);
                objects.insert(object.clone());
                let reference = object.compute_object_reference();
                InputObjectKind::ImmOrOwnedMoveObject(reference)
            })
            .collect();
        transactions.push(FakeTransaction::new(objects));
    }
    (objects, transactions)
}

pub fn solana_load(rng: &mut StdRng) -> Vec<usize> {
    let (inputs, _) = sui_single_node_benchmark::load_statistics::solana_concurrency(rng);
    inputs
}

pub fn eth_transfers(rng: &mut StdRng) -> Vec<usize> {
    let (sender, recipient) = sui_single_node_benchmark::load_statistics::ethereum_transfers(rng);
    vec![sender, recipient]
}

pub fn eth_mint(rng: &mut StdRng) -> Vec<usize> {
    let (nft, minter) = sui_single_node_benchmark::load_statistics::ethereum_nft_mint(rng);
    vec![nft, minter]
}

pub fn uniswap_normal(rng: &mut StdRng) -> Vec<usize> {
    let coin_pair = sui_single_node_benchmark::load_statistics::ethereum_uniswap_normal(rng);
    vec![coin_pair]
}

pub fn uniswap_peak(rng: &mut StdRng) -> Vec<usize> {
    let coin_pair = sui_single_node_benchmark::load_statistics::ethereum_uniswap_peak(rng);
    vec![coin_pair]
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use rand::{rngs::StdRng, SeedableRng};
    use sui_types::base_types::ObjectID;
    use tokio::time::Instant;

    use crate::{
        config::{default_fake_execution_duration, BenchmarkParameters, WorkloadType},
        executor::{
            api::{Executor, TransactionWithTimestamp},
            fake::{
                fake_owned_object, fake_shared_object, fake_shared_object_with_id,
                generate_fake_load_objects_and_transactions,
                generate_fake_shared_object_transaction, FakeExecutor, FakeObjectStore,
                FakeTransaction,
            },
        },
    };

    #[tokio::test]
    async fn execute_fake_owned_object_transaction() {
        let store = Arc::new(FakeObjectStore::new());
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;
        let ctx = executor.context();

        let inputs: Vec<_> = (0..2)
            .map(|_| {
                let object = fake_owned_object(0);
                let id = object.id();
                store.write_object(object);
                id
            })
            .collect();
        let transaction = FakeTransaction::from_store(&store, inputs);
        let transaction_with_timestamp = TransactionWithTimestamp::new(transaction, 0.0);

        let start = Instant::now();
        let result = FakeExecutor::execute(ctx, store, transaction_with_timestamp).await;
        let duration = start.elapsed();

        assert!(result.success());
        assert!(duration >= default_fake_execution_duration());
    }

    #[tokio::test]
    async fn execute_fake_shared_object_transaction() {
        let store = Arc::new(FakeObjectStore::new());
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;
        let ctx = executor.context();

        let inputs: Vec<_> = (0..2)
            .map(|_| {
                let object = fake_shared_object(0);
                let id = object.id();
                store.write_object(object);
                id
            })
            .collect();
        let transaction = FakeTransaction::from_store(&store, inputs);
        let transaction_with_timestamp = TransactionWithTimestamp::new(transaction, 0.0);

        let start = Instant::now();
        let result = FakeExecutor::execute(ctx, store, transaction_with_timestamp).await;
        let duration = start.elapsed();

        assert!(result.success());
        assert!(duration >= default_fake_execution_duration());
    }

    #[tokio::test]
    async fn execute_fake_shared_object_transaction_with_contention() {
        let store = Arc::new(FakeObjectStore::new());
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;
        let ctx = executor.context();

        // Write the object to the store
        let object = fake_shared_object_with_id(0, ObjectID::ZERO);
        store.write_object(object);

        for _ in 0..10 {
            let contention = 100;
            let transaction = generate_fake_shared_object_transaction(1, contention);
            let transaction_with_timestamp = TransactionWithTimestamp::new(transaction, 0.0);

            let start = Instant::now();
            let result =
                FakeExecutor::execute(ctx.clone(), store.clone(), transaction_with_timestamp).await;
            let duration = start.elapsed();

            assert!(result.success());
            assert!(duration >= default_fake_execution_duration());
        }
    }

    #[tokio::test]
    async fn execute_fake_solana_transactions() {
        let store = Arc::new(FakeObjectStore::new());
        let config = BenchmarkParameters {
            workload: WorkloadType::FakeSolanaTransactions {
                execution_duration: default_fake_execution_duration(),
            },
            ..BenchmarkParameters::new_for_fake_tests()
        };
        let executor = FakeExecutor::new(&config).await;
        let ctx = executor.context();

        // Generate objects and transactions.
        let mut rng = StdRng::seed_from_u64(0);
        let (objects, transactions) = generate_fake_load_objects_and_transactions(
            &mut rng,
            10,
            crate::executor::fake::solana_load,
        );

        // Write the object to the store.
        for object in objects {
            store.write_object(object);
        }

        for transaction in transactions {
            let transaction_with_timestamp = TransactionWithTimestamp::new(transaction, 0.0);

            let start = Instant::now();
            let result =
                FakeExecutor::execute(ctx.clone(), store.clone(), transaction_with_timestamp).await;
            let duration = start.elapsed();

            assert!(result.success());
            assert!(duration >= default_fake_execution_duration());
        }
    }

    #[tokio::test]
    async fn execute_fake_ethereum_transactions() {
        let store = Arc::new(FakeObjectStore::new());
        let config = BenchmarkParameters {
            workload: WorkloadType::FakeEthereumTransfers {
                execution_duration: default_fake_execution_duration(),
            },
            ..BenchmarkParameters::new_for_fake_tests()
        };
        let executor = FakeExecutor::new(&config).await;
        let ctx = executor.context();

        // Generate objects and transactions.
        let mut rng = StdRng::seed_from_u64(0);
        let loads = [
            crate::executor::fake::eth_transfers,
            crate::executor::fake::eth_mint,
            crate::executor::fake::uniswap_normal,
            crate::executor::fake::uniswap_peak,
        ];

        let mut objects = HashSet::new();
        let mut transactions = Vec::new();
        for load in loads {
            let (os, txs) = generate_fake_load_objects_and_transactions(&mut rng, 3, load);
            objects.extend(os);
            transactions.extend(txs);
        }

        // Write the object to the store.
        for object in objects {
            store.write_object(object);
        }

        for transaction in transactions {
            let transaction_with_timestamp = TransactionWithTimestamp::new(transaction, 0.0);

            let start = Instant::now();
            let result =
                FakeExecutor::execute(ctx.clone(), store.clone(), transaction_with_timestamp).await;
            let duration = start.elapsed();

            assert!(result.success());
            assert!(duration >= default_fake_execution_duration());
        }
    }
}
