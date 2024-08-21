// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet},
    ops::Deref,
    sync::Arc,
};

use sui_single_node_benchmark::{
    benchmark_context::BenchmarkContext,
    command::{Component, WorkloadKind},
    mock_storage::InMemoryObjectStore,
    workload::Workload,
};
use sui_types::{
    base_types::ObjectID,
    digests::TransactionDigest,
    effects::{TransactionEffects, TransactionEffectsAPI},
    executable_transaction::VerifiedExecutableTransaction,
    object::Object,
    transaction::{CertifiedTransaction, InputObjectKind, TransactionDataAPI, VerifiedCertificate},
};
use tokio::time::Instant;

use super::api::{
    ExecutableTransaction,
    ExecutionEffects,
    Executor,
    StateStore,
    TransactionWithTimestamp,
};
use crate::config::{BenchmarkConfig, WorkloadType};

pub type SuiTransactionWithTimestamp = TransactionWithTimestamp<CertifiedTransaction>;
pub type SuiExecutionEffects = ExecutionEffects<TransactionEffects>;

impl ExecutableTransaction for CertifiedTransaction {
    fn digest(&self) -> &TransactionDigest {
        self.digest()
    }

    fn input_objects(&self) -> Vec<InputObjectKind> {
        // TODO: Return error instead of panic
        self.transaction_data()
            .input_objects()
            .expect("Cannot get input object kinds")
    }

    fn input_object_ids(&self) -> Vec<ObjectID> {
        self.transaction_data()
            .input_objects()
            .expect("Cannot get input object kinds")
            .iter()
            .map(|kind| kind.object_id())
            .collect()
    }
}

impl StateStore<TransactionEffects> for InMemoryObjectStore {
    fn commit_objects(&self, changes: TransactionEffects, new_state: BTreeMap<ObjectID, Object>) {
        self.commit_effects(changes, new_state);
    }
}

#[derive(Clone)]
pub struct SuiExecutor {
    ctx: Arc<BenchmarkContext>,
}

pub fn init_workload(config: &BenchmarkConfig) -> Workload {
    let pre_generation = config.load * config.duration.as_secs();

    // Determine the workload.
    let workload_type = match config.workload {
        WorkloadType::Transfers => WorkloadKind::PTB {
            num_transfers: 0,
            num_dynamic_fields: 0,
            use_batch_mint: false,
            computation: 0,
            use_native_transfer: false,
            num_mints: 0,
            num_shared_objects: 0,
            nft_size: 32,
        },
        WorkloadType::SharedObjects => WorkloadKind::PTB {
            num_transfers: 0,
            num_dynamic_fields: 0,
            use_batch_mint: false,
            computation: 0,
            use_native_transfer: false,
            num_mints: 0,
            num_shared_objects: 1,
            nft_size: 32,
        },
    };

    // Create genesis.
    tracing::debug!("Creating genesis for {pre_generation} transactions...");
    Workload::new(pre_generation, workload_type)
}

pub async fn generate_transactions(config: &BenchmarkConfig) -> Vec<CertifiedTransaction> {
    tracing::debug!("Generating all transactions...");
    let workload = init_workload(config);
    let mut ctx = BenchmarkContext::new(workload.clone(), Component::PipeTxsToChannel, true).await;
    let start_time = Instant::now();
    let tx_generator = workload.create_tx_generator(&mut ctx).await;
    let transactions = ctx.generate_transactions(tx_generator).await;
    let transactions = ctx.certify_transactions(transactions, false).await;
    let elapsed = start_time.elapsed();
    tracing::debug!(
        "Generated {} txs in {} ms",
        transactions.len(),
        elapsed.as_millis(),
    );

    transactions
}

// use std::{fs, io::BufReader, path::PathBuf};

// use sui_single_node_benchmark::mock_account::Account;
// use sui_types::base_types::SuiAddress;
// pub fn export_to_files(
//     accounts: &BTreeMap<SuiAddress, Account>,
//     txs: &Vec<CertifiedTransaction>,
//     working_directory: PathBuf,
// ) {
//     let start_time: std::time::Instant = std::time::Instant::now();

//     let accounts_path = working_directory.join("accounts.dat");
//     let txs_path = working_directory.join("txs.dat");

//     let accounts_s = bincode::serialize(accounts).unwrap();
//     let txs_s = bincode::serialize(txs).unwrap();

//     fs::write(accounts_path, accounts_s).expect("Failed to write accounts");
//     fs::write(txs_path, txs_s).expect("Failed to write txs");
//     let elapsed = start_time.elapsed().as_millis() as f64;
//     println!("Export took {} ms", elapsed,);
// }

// pub fn import_from_files(
//     working_directory: PathBuf,
// ) -> (BTreeMap<SuiAddress, Account>, Vec<CertifiedTransaction>) {
//     let start_time: std::time::Instant = std::time::Instant::now();

//     let accounts_file = BufReader::new(
//         fs::File::open(working_directory.join("accounts.dat")).expect("Failed to open accounts"),
//     );
//     let txs_file = BufReader::new(
//         fs::File::open(working_directory.join("txs.dat")).expect("Failed to open txs"),
//     );

//     let accounts = bincode::deserialize_from(accounts_file).unwrap();
//     let txs = bincode::deserialize_from(txs_file).unwrap();
//     let elapsed = start_time.elapsed().as_millis() as f64;
//     println!("Import took {} ms", elapsed,);
//     (accounts, txs)
// }

impl SuiExecutor {
    pub async fn new(config: &BenchmarkConfig) -> Self {
        let workload = init_workload(config);
        let component = Component::PipeTxsToChannel;
        let start_time = Instant::now();
        let mut ctx = BenchmarkContext::new(workload.clone(), component, true).await;
        let _ = workload.create_tx_generator(&mut ctx).await;
        let elapsed = start_time.elapsed();
        tracing::debug!(
            "Genesis created {} accounts/s in {} ms",
            workload.num_accounts() as f64 / elapsed.as_secs_f64(),
            elapsed.as_millis(),
        );

        Self { ctx: Arc::new(ctx) }
    }

    pub fn create_in_memory_store(&self) -> InMemoryObjectStore {
        self.ctx.validator().create_in_memory_store()
    }
}

impl Executor for SuiExecutor {
    type Transaction = CertifiedTransaction;
    type StateChanges = TransactionEffects;
    type Store = InMemoryObjectStore;

    fn get_context(&self) -> Arc<BenchmarkContext> {
        self.ctx.clone()
    }

    async fn exec_on_ctx(
        ctx: Arc<BenchmarkContext>,
        store: Arc<Self::Store>,
        transaction: TransactionWithTimestamp<Self::Transaction>,
    ) -> SuiExecutionEffects {
        let input_objects = transaction.transaction_data().input_objects().unwrap();

        // FIXME: ugly deref
        let objects = store
            .read_objects_for_execution(
                &**(ctx.validator().get_epoch_store()),
                &transaction.key(),
                &input_objects,
            )
            .unwrap();

        let validator = ctx.validator();
        let protocol_config = validator.get_epoch_store().protocol_config();
        let reference_gas_price = validator.get_epoch_store().reference_gas_price();

        let executable = VerifiedExecutableTransaction::new_from_certificate(
            VerifiedCertificate::new_unchecked(transaction.deref().clone()),
        );

        let (gas_status, input_objects) = sui_transaction_checks::check_certificate_input(
            &executable,
            objects,
            protocol_config,
            reference_gas_price,
        )
        .unwrap();
        let (kind, signer, gas) = executable.transaction_data().execution_parts();
        let (inner_temp_store, _, effects, _) = ctx
            .validator()
            .get_epoch_store()
            .executor()
            .execute_transaction_to_effects(
                &store,
                protocol_config,
                ctx.validator()
                    .get_validator()
                    .metrics
                    .limits_metrics
                    .clone(),
                false,
                &HashSet::new(),
                &ctx.validator().get_epoch_store().epoch(),
                0,
                input_objects,
                gas,
                gas_status,
                kind,
                signer,
                *executable.digest(),
            );
        debug_assert!(effects.status().is_ok());

        let written = inner_temp_store.written.clone();

        // Commit the objects to the store.
        store.commit_objects(inner_temp_store);

        SuiExecutionEffects::new(effects, written)
    }

    async fn execute(
        &mut self,
        store: &InMemoryObjectStore,
        transaction: &SuiTransactionWithTimestamp,
    ) -> SuiExecutionEffects {
        let input_objects = transaction.transaction_data().input_objects().unwrap();

        // FIXME: ugly deref
        let objects = store
            .read_objects_for_execution(
                &**(self.ctx.validator().get_epoch_store()),
                &transaction.key(),
                &input_objects,
            )
            .unwrap();

        let validator = self.ctx.validator();
        let protocol_config = validator.get_epoch_store().protocol_config();
        let reference_gas_price = validator.get_epoch_store().reference_gas_price();

        let executable = VerifiedExecutableTransaction::new_from_certificate(
            VerifiedCertificate::new_unchecked(transaction.deref().clone()),
        );

        let _validator = self.ctx.validator();
        let (gas_status, input_objects) = sui_transaction_checks::check_certificate_input(
            &executable,
            objects,
            protocol_config,
            reference_gas_price,
        )
        .unwrap();
        let (kind, signer, gas) = executable.transaction_data().execution_parts();
        let (inner_temp_store, _, effects, _) = self
            .ctx
            .validator()
            .get_epoch_store()
            .executor()
            .execute_transaction_to_effects(
                store,
                protocol_config,
                self.ctx
                    .validator()
                    .get_validator()
                    .metrics
                    .limits_metrics
                    .clone(),
                false,
                &HashSet::new(),
                &self.ctx.validator().get_epoch_store().epoch(),
                0,
                input_objects,
                gas,
                gas_status,
                kind,
                signer,
                *executable.digest(),
            );
        debug_assert!(effects.status().is_ok());

        let written = inner_temp_store.written.clone();

        // Commit the objects to the store.
        store.commit_objects(inner_temp_store);

        SuiExecutionEffects::new(effects, written)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::config::WorkloadType;

    #[tokio::test]
    async fn test_sui_executor() {
        let config = BenchmarkConfig::new_for_tests();
        let mut executor = SuiExecutor::new(&config).await;
        let store = executor.create_in_memory_store();

        let transactions = generate_transactions(&config).await;
        assert!(transactions.len() > 10);

        for tx in transactions {
            let transaction = TransactionWithTimestamp::new_for_tests(tx);
            let results = executor.execute(&store, &transaction).await;
            assert!(results.success());
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_sui_executor_with_shared_objects() {
        let config = BenchmarkConfig {
            workload: WorkloadType::SharedObjects,
            ..BenchmarkConfig::new_for_tests()
        };
        let mut executor = SuiExecutor::new(&config).await;
        let store = executor.create_in_memory_store();
        let workload = init_workload(&config);
        let mut ctx =
            BenchmarkContext::new(workload.clone(), Component::PipeTxsToChannel, true).await;
        let start_time = Instant::now();
        let tx_generator = workload.create_tx_generator(&mut ctx).await;
        let transactions = ctx.generate_transactions(tx_generator).await;
        let transactions = ctx.certify_transactions(transactions, false).await;

        let elapsed = start_time.elapsed();
        tracing::debug!(
            "Generated {} txs in {} ms",
            transactions.len(),
            elapsed.as_millis(),
        );

        assert!(transactions.len() > 10);

        for tx in transactions {
            let transaction = TransactionWithTimestamp::new_for_tests(tx);
            let results = executor.execute(&store, &transaction).await;
            assert!(results.success());
        }
    }

    // #[tokio::test]
    // async fn shared_object_test_with_imported_file() {
    //     let config = BenchmarkConfig {
    //         workload: WorkloadType::SharedObjects,
    //         ..BenchmarkConfig::new_for_tests()
    //     };

    //     let working_directory = "~/test_export";
    //     fs::create_dir_all(&working_directory).expect(&format!(
    //         "Failed to create directory '{}'",
    //         working_directory
    //     ));

    //     // generate txs and export to files
    //     let workload = init_workload(&config);
    //     let mut ctx =
    //         BenchmarkContext::new(workload.clone(), Component::PipeTxsToChannel, true).await;
    //     let tx_generator = workload.create_tx_generator(&mut ctx).await;
    //     let txs = ctx.generate_transactions(tx_generator).await;
    //     let txs = ctx.certify_transactions(txs, false).await;

    //     super::export_to_files(ctx.get_accounts(), &txs, working_directory.into());

    //     // execute on another executor
    //     let mut executor = SuiExecutor::new(&config).await;
    //     let store = executor.create_in_memory_store();

    //     // import txs to assign shared-object versions
    //     let (read_accounts, read_txs) = super::import_from_files(working_directory.into());
    //     assert_eq!(read_accounts.len(), ctx.get_accounts().len());
    //     executor
    //         .get_context()
    //         .validator()
    //         .assigned_shared_object_versions(&read_txs) // Important!!
    //         .await;

    //     for tx in read_txs {
    //         let transaction = TransactionWithTimestamp::new_for_tests(tx);
    //         let results = executor.execute(&store, &transaction).await;
    //         assert!(results.success());
    //     }
    // }
}
