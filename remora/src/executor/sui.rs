// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{BufReader, Read},
    path::PathBuf,
    sync::Arc,
};

use sui_single_node_benchmark::{
    benchmark_context::BenchmarkContext,
    command::{Component, WorkloadKind},
    mock_account::Account,
    mock_storage::InMemoryObjectStore,
    workload::Workload,
};
use sui_types::{
    base_types::{ObjectID, SequenceNumber, SuiAddress},
    digests::TransactionDigest,
    effects::{TransactionEffects, TransactionEffectsAPI},
    object::Object,
    storage::ObjectStore,
    transaction::{CheckedInputObjects, InputObjectKind, Transaction, TransactionDataAPI},
};
use tokio::{sync::Mutex, time::Instant};

use super::{
    api::{ExecutableTransaction, ExecutionResults, Executor, RemoraTransaction, StateStore},
    calibration::Calibration,
};
use crate::config::{BenchmarkParameters, ConfigErrorType, WorkloadType};

/// Represents a Sui transaction.
pub type SuiTransaction = RemoraTransaction<SuiExecutor>;

/// Represents the results of the execution of a Sui transaction.
pub type SuiExecutionResults = ExecutionResults<SuiExecutor>;

impl ExecutableTransaction for Transaction {
    fn digest(&self) -> &TransactionDigest {
        self.digest()
    }

    fn input_objects(&self) -> Vec<InputObjectKind> {
        // TODO: Verify transaction syntax in the proxy.
        self.transaction_data()
            .input_objects()
            .expect("Transaction syntax already checked")
    }

    fn shared_object_ids(&self) -> Vec<ObjectID> {
        self.transaction_data()
            .input_objects()
            .expect("Transaction syntax already checked")
            .iter()
            .filter_map(|kind| match kind {
                InputObjectKind::SharedMoveObject { id, .. } => Some(*id),
                _ => None,
            })
            .collect()
    }
}

impl StateStore<TransactionEffects> for InMemoryObjectStore {
    fn commit_objects(&self, updates: TransactionEffects, new_state: BTreeMap<ObjectID, Object>) {
        self.commit_effects(updates, new_state);
    }

    fn commit_new_objects(&self, new_state: BTreeMap<ObjectID, Object>) {
        self.commit_new_objects(new_state);
    }

    fn read_object(
        &self,
        id: &ObjectID,
    ) -> Result<Option<Object>, sui_types::storage::error::Error> {
        self.get_object(id)
    }
}

/// Wrapper context that contains both the benchmark context and verification context
#[derive(Clone)]
pub struct SuiExecutionContext {
    benchmark_ctx: BenchmarkContext,
    verification_spins: u64,
}

impl SuiExecutionContext {
    /// Get a reference to the benchmark context
    pub fn benchmark_ctx(&self) -> &BenchmarkContext {
        &self.benchmark_ctx
    }

    /// Get the verification spins value
    pub fn verification_spins(&self) -> u64 {
        self.verification_spins
    }
}

#[derive(Clone)]
pub struct SuiExecutor {
    ctx: Arc<SuiExecutionContext>,
    /// Lock ensuring at most one proxy (or the primary) assigns the shared object transaction locks.
    /// This is likely not the best design, but the Sui epoch store is not very forgiving.
    shared_object_versions_assignment_lock: Arc<Mutex<()>>,
}

pub fn init_workload(config: &BenchmarkParameters) -> Workload {
    let pre_generation = config.load * config.duration.as_secs();

    // Determine the workload.
    let workload_type = match config.workload {
        WorkloadType::Transfers => Ok(WorkloadKind::PTB {
            num_transfers: 0,
            num_dynamic_fields: 0,
            use_batch_mint: false,
            computation: 0,
            use_native_transfer: false,
            num_mints: 0,
            num_shared_objects: 0,
            nft_size: 32,
        }),
        WorkloadType::SharedObjects { txs_per_counter } => Ok(WorkloadKind::Counter {
            txs_per_counter: txs_per_counter as u64,
        }),
        WorkloadType::SolanaTransactions => Ok(WorkloadKind::SolanaTransactions),
        WorkloadType::EthereumTransfers => Ok(WorkloadKind::EthereumTransfers),
        WorkloadType::EthereumNftMint => Ok(WorkloadKind::EthereumNftMint),
        WorkloadType::UniswapNormal => Ok(WorkloadKind::UniswapNormal),
        WorkloadType::UniswapPeak => Ok(WorkloadKind::UniswapPeak),
        WorkloadType::Zipfian {
            alpha,
            number_of_inputs,
        } => Ok(WorkloadKind::ZipfianWorkload {
            theta: alpha,
            number_of_inputs,
        }),
        _ => Err(ConfigErrorType::InvalidWorkload),
    };

    // Create genesis.
    tracing::debug!("Creating genesis for {pre_generation} transactions...");
    Workload::new(pre_generation, workload_type.unwrap())
}

pub async fn generate_sui_transactions(
    config: &BenchmarkParameters,
    working_directory: Option<PathBuf>,
) -> Vec<Transaction> {
    tracing::debug!("Generating all transactions...");
    let workload = init_workload(config);
    let mut ctx = if let Some(path) = working_directory {
        BenchmarkContext::new_with_exportable_state(
            workload.clone(),
            Component::PipeTxsToChannel,
            false,
            path,
        )
        .await
    } else {
        BenchmarkContext::new(workload.clone(), Component::PipeTxsToChannel, false).await
    };
    let start_time = Instant::now();
    let tx_generator = workload.create_tx_generator(&mut ctx).await;
    let transactions = ctx.generate_transactions(tx_generator).await;
    //let transactions = ctx.certify_transactions(transactions, false).await;
    let elapsed = start_time.elapsed();
    tracing::debug!(
        "Generated {} txs in {} ms",
        transactions.len(),
        elapsed.as_millis(),
    );

    transactions
}

pub fn export_to_files(
    accounts: &BTreeMap<SuiAddress, Account>,
    txs: &Vec<Transaction>,
    working_directory: PathBuf,
) {
    let start_time: std::time::Instant = std::time::Instant::now();

    let accounts_path = working_directory.join("accounts.dat");
    let txs_path = working_directory.join("txs.dat");

    let accounts_s = bincode::serialize(accounts).unwrap();
    let txs_s = bincode::serialize(txs).unwrap();

    fs::write(accounts_path, accounts_s).expect("Failed to write accounts");
    fs::write(txs_path, txs_s).expect("Failed to write txs");
    let elapsed = start_time.elapsed().as_millis() as f64;
    tracing::info!("Export took {} ms", elapsed,);
}

pub fn import_from_files(
    working_directory: PathBuf,
) -> (BTreeMap<SuiAddress, Account>, Vec<Transaction>) {
    let start_time: std::time::Instant = std::time::Instant::now();
    // Read the accounts file into a buffer
    let mut accounts_file = BufReader::new(
        fs::File::open(working_directory.join("accounts.dat")).expect("Failed to open accounts"),
    );
    let mut accounts_buf = Vec::new();
    accounts_file
        .read_to_end(&mut accounts_buf)
        .expect("Failed to read accounts file");

    // Read the transactions file into a buffer
    let mut txs_file = BufReader::new(
        fs::File::open(working_directory.join("txs.dat")).expect("Failed to open txs"),
    );
    let mut txs_buf = Vec::new();
    txs_file
        .read_to_end(&mut txs_buf)
        .expect("Failed to read txs file");

    // Deserialize from buffers
    let accounts: BTreeMap<SuiAddress, Account> = bincode::deserialize(&accounts_buf).unwrap();
    let txs: Vec<Transaction> = bincode::deserialize(&txs_buf).unwrap();

    let elapsed = start_time.elapsed().as_millis() as f64;
    tracing::info!("Import took {} ms", elapsed,);
    (accounts, txs)
}

pub async fn pre_generate_txn_log(config: &BenchmarkParameters, log_path: &str) {
    fs::create_dir_all(log_path)
        .unwrap_or_else(|_| panic!("Failed to create directory '{}'", log_path));

    // generate txs and export to files
    let workload = init_workload(config);
    let mut ctx = BenchmarkContext::new(workload.clone(), Component::PipeTxsToChannel, false).await;
    let tx_generator = workload.create_tx_generator(&mut ctx).await;
    let txs = ctx.generate_transactions(tx_generator).await;

    export_to_files(ctx.get_accounts(), &txs, log_path.into());
    tracing::info!("Finish generating and exporting");
}

pub fn get_object_ids_for_dependency_tracking<E: Executor>(
    transaction: RemoraTransaction<E>,
) -> Vec<ObjectID> {
    // filter pkg id from the obj_id
    transaction
        .input_objects()
        .into_iter()
        .filter_map(|kind| {
            match kind {
                InputObjectKind::ImmOrOwnedMoveObject((obj_id, _, _)) => Some(obj_id),
                InputObjectKind::SharedMoveObject {
                    id: obj_id,
                    initial_shared_version: _,
                    mutable: _,
                } => Some(obj_id),
                _ => None, // filter out move package
            }
        })
        .collect::<Vec<_>>()
}

pub const LOG_DIR: &str = "/tmp/";

impl SuiExecutor {
    pub async fn new(config: &BenchmarkParameters) -> Self {
        let workload = init_workload(config);
        let component = Component::PipeTxsToChannel;
        let start_time = Instant::now();
        let mut ctx = BenchmarkContext::new(workload.clone(), component, false).await;
        let verification_spins = Calibration::calibrate(config.verification_duration);
        let _ = workload.create_tx_generator(&mut ctx).await;
        let elapsed = start_time.elapsed();
        tracing::debug!(
            "Genesis created {} accounts/s in {} ms",
            workload.num_accounts() as f64 / elapsed.as_secs_f64(),
            elapsed.as_millis(),
        );

        let context = SuiExecutionContext {
            benchmark_ctx: ctx,
            verification_spins,
        };

        Self {
            ctx: Arc::new(context),
            shared_object_versions_assignment_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn create_in_memory_store(&self) -> Arc<InMemoryObjectStore> {
        Arc::new(self.ctx
            .benchmark_ctx()
            .validator()
            .create_in_memory_store())
    }
}

impl Executor for SuiExecutor {
    type Transaction = Transaction;
    type ExecutionResults = TransactionEffects;
    type Store = InMemoryObjectStore;
    type ExecutionContext = SuiExecutionContext;

    fn context(&self) -> Arc<SuiExecutionContext> {
        self.ctx.clone()
    }

    async fn execute(
        ctx: Arc<SuiExecutionContext>,
        store: Arc<InMemoryObjectStore>,
        transaction: SuiTransaction,
    ) -> SuiExecutionResults {
        let start_time = Instant::now();

        let tx_id = transaction.digest();
        let input_objects = transaction.transaction_data().input_objects().unwrap();
        let validator = ctx.benchmark_ctx().validator();
        let epoch_store = validator.get_epoch_store();
        let protocol_config = epoch_store.protocol_config();
        let reference_gas_price = epoch_store.reference_gas_price();

        tracing::debug!(
            "[{tx_id}] Reading objects for execution...{:?}",
            input_objects
                .iter()
                .map(|input_object| (input_object.object_id(), input_object.version()))
                .collect::<Vec<_>>()
        );
        let objects = store
            .read_objects_for_execution(&**epoch_store, &transaction.key(), &input_objects)
            .unwrap();

        let (kind, signer, gas) = transaction.transaction_data().execution_parts();
        let gas_status = sui_transaction_checks::get_gas_status(
            &objects,
            &gas,
            protocol_config,
            reference_gas_price,
            transaction.transaction_data(),
        )
        .unwrap();

        let objects = CheckedInputObjects::new_with_checked_transaction_inputs(objects);

        let (inner_temp_store, _, effects, _) = validator
            .get_epoch_store()
            .executor()
            .execute_transaction_to_effects(
                &store,
                protocol_config,
                validator.get_validator().metrics.limits_metrics.clone(),
                false,
                &HashSet::new(),
                &epoch_store.epoch(),
                0,
                objects,
                gas,
                gas_status,
                kind,
                signer,
                *transaction.digest(),
            );
        if effects.status().is_err() {
            tracing::error!(
                "[{tx_id}] Transaction failed ({:?}): {effects:?}",
                effects.status()
            );
            panic!("Transaction failed: {:?}", effects.status());
        }

        let written = inner_temp_store.written.clone();

        // Commit the objects to the store.
        tracing::debug!(
            "[{tx_id}] Committing objects to the store.: {:?}",
            inner_temp_store
                .written
                .iter()
                .map(|(id, o)| (id, o.version()))
                .collect::<Vec<_>>()
        );
        store.commit_objects(inner_temp_store);

        let elapsed = start_time.elapsed();
        tracing::debug!(
            "[{tx_id}] Transaction execution took {} us",
            elapsed.as_micros()
        );

        // TODO: should avoid duplicated txn in returns
        SuiExecutionResults::new(transaction, Some(effects), Some(written))
    }

    fn pre_execute_check(
        ctx: Arc<SuiExecutionContext>,
        store: Arc<Self::Store>,
        transaction: &super::api::TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        let input_objects = transaction.transaction_data().input_objects().unwrap();
        let validator = ctx.benchmark_ctx().validator();
        let epoch_store = validator.get_epoch_store();

        store
            .read_objects_for_execution(&**epoch_store, &transaction.key(), &input_objects)
            .is_ok()
    }

    fn pre_execute_check_objects(
        _store: Arc<Self::Store>,
        _transaction: &super::api::TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        // FIXME
        true
    }

    async fn assign_shared_object_versions(&self, transactions: &[Self::Transaction]) {
        let _guard = self.shared_object_versions_assignment_lock.lock().await;
        self.context()
            .benchmark_ctx()
            .validator()
            .assigned_shared_object_versions_on_transaction_not_idempotent(transactions)
            .await;
    }

    async fn assign_shared_object_versions_and_return_required_versions(
        &self,
        transaction: &Self::Transaction,
    ) -> Option<Vec<(ObjectID, SequenceNumber)>> {
        let _guard = self.shared_object_versions_assignment_lock.lock().await;
        self.context()
            .benchmark_ctx()
            .validator()
            .assign_shared_object_versions_and_return_required_versions(transaction)
            .await
    }

    async fn assign_shared_object_versions_with_required_versions(
        &self,
        transactions: &[Self::Transaction],
        required_versions: &[(ObjectID, SequenceNumber)],
    ) {
        // Collect all shared object IDs from the transactions
        let shared_object_ids: std::collections::HashSet<_> = transactions
            .iter()
            .flat_map(|tx| tx.shared_object_ids())
            .collect();

        // Filter required_versions to only include shared object IDs
        let filtered_required_versions: Vec<_> = required_versions
            .iter()
            .cloned()
            .filter(|(id, _)| shared_object_ids.contains(id))
            .collect();

        self.context()
            .benchmark_ctx()
            .validator()
            .assigned_shared_object_versions_on_transaction_not_idempotent_with_required_versions(
                transactions,
                &filtered_required_versions,
            )
            .await;
    }

    async fn get_required_shared_object_versions(
        &self,
        transaction: &TransactionDigest,
    ) -> Option<Vec<(ObjectID, SequenceNumber)>> {
        self.context()
            .benchmark_ctx()
            .validator()
            .get_required_shared_object_versions(transaction)
            .await
    }

    fn generate_transactions(
        config: &BenchmarkParameters,
        working_directory: Option<PathBuf>,
    ) -> impl std::future::Future<Output = Vec<Self::Transaction>> + Send {
        generate_sui_transactions(config, working_directory)
    }

    fn init_store(&self) -> Arc<Self::Store> {
        self.create_in_memory_store()
    }

    fn optimistically_pre_generate_objects(
        _store: Arc<Self::Store>,
        _transaction: &super::api::TransactionWithTimestamp<Self::Transaction>,
    ) {
        todo!()
    }

    async fn verify_transaction(
        ctx: Arc<Self::ExecutionContext>,
        _transaction: &super::api::TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        let start_time = Instant::now();
        let tx_id = _transaction.digest();
        let spins = ctx.verification_spins();

        Calibration::calibrated_work(spins);

        let elapsed = start_time.elapsed();
        tracing::debug!(
            "[{tx_id}] Transaction verification took {} us",
            elapsed.as_micros()
        );
        true
    }

    fn get_objects_for_dependency_tracking(
        ctx: Arc<Self::ExecutionContext>,
        store: Arc<InMemoryObjectStore>,
        transaction: SuiTransaction,
    ) -> Vec<(ObjectID, SequenceNumber)> {
        // filter pkg id from the obj_id
        let input_objects = transaction.transaction_data().input_objects().unwrap();
        let validator = ctx.benchmark_ctx().validator();
        let epoch_store = validator.get_epoch_store();

        store.get_object_id_and_versions(&**epoch_store, &transaction.key(), &input_objects)
    }
}

#[cfg(test)]
mod tests {

    use std::{path::PathBuf, time::Duration};

    use tokio::time::Instant;

    use crate::{
        config::BenchmarkParameters,
        executor::{
            api::Executor,
            sui::{generate_sui_transactions, SuiExecutor, SuiTransaction},
        },
    };

    #[tokio::test]
    async fn test_sui_executor() {
        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        let store = executor.create_in_memory_store();
        let ctx = executor.context();

        let transactions = generate_sui_transactions(&config, None).await;
        assert!(transactions.len() == 10);

        for tx in transactions {
            let transaction = SuiTransaction::new_for_tests(tx);
            let results = SuiExecutor::execute(ctx.clone(), store.clone(), transaction).await;
            assert!(results.success());
        }
    }

    #[tokio::test]
    async fn shared_object_test_with_imported_file() {
        use std::fs;

        use crate::config::WorkloadType;

        let config = BenchmarkParameters {
            workload: WorkloadType::SharedObjects { txs_per_counter: 2 },
            ..BenchmarkParameters::new_for_tests()
        };

        let working_directory = "./test_export";
        super::pre_generate_txn_log(&config, working_directory).await;

        // execute on another executor
        let executor = SuiExecutor::new(&config).await;
        let store = executor.create_in_memory_store();

        // import txs to assign shared-object versions
        let (_, read_txs) = super::import_from_files(working_directory.into());
        executor
            .context()
            .benchmark_ctx()
            .validator()
            .assigned_shared_object_versions_on_transaction(&read_txs) // Important!!
            .await;

        let ctx = executor.context();
        for tx in read_txs {
            let transaction = SuiTransaction::new_for_tests(tx);
            let results = SuiExecutor::execute(ctx.clone(), store.clone(), transaction).await;
            assert!(results.success());
        }

        // Clean up directory after the test finishes
        fs::remove_dir_all(&working_directory).expect("Failed to delete working directory");
    }

    #[tokio::test]
    #[ignore = "slow"]
    async fn test_generate_transactions_with_exportable_state() {
        let config = BenchmarkParameters {
            load: 100,
            duration: Duration::from_secs(1000),
            ..BenchmarkParameters::new_for_tests()
        };
        let working_directory = PathBuf::from("./test_export");
        let start_time = Instant::now();
        let transactions = generate_sui_transactions(&config, Some(working_directory)).await;
        let elapsed = start_time.elapsed();
        tracing::info!(
            "Generated {} txs in {} ms",
            transactions.len(),
            elapsed.as_millis(),
        );

        assert!(false)
    }
}
