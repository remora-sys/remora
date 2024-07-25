// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    ops::Deref,
};

use serde::{Deserialize, Serialize};
use sui_single_node_benchmark::{
    benchmark_context::BenchmarkContext,
    command::{Component, WorkloadKind},
    mock_storage::InMemoryObjectStore,
    workload::Workload,
};
use sui_types::{
    base_types::ObjectID,
    effects::{TransactionEffects, TransactionEffectsAPI},
    executable_transaction::VerifiedExecutableTransaction,
    object::Object,
    storage::BackingStore,
    transaction::{CertifiedTransaction, TransactionDataAPI, VerifiedCertificate},
};
use tokio::time::Instant;

use crate::config::{BenchmarkConfig, WorkloadType};

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

pub trait Executor {
    type Transaction: Clone;
    type TransactionResults;
    type Store: BackingStore;

    fn execute(
        &mut self,
        store: &Self::Store,
        transaction: &TransactionWithTimestamp<Self::Transaction>,
    ) -> impl Future<Output = Self::TransactionResults> + Send;

    fn generate_transactions(&mut self) -> impl Future<Output = Vec<Self::Transaction>> + Send;
}

pub type SuiTransactionWithTimestamp = TransactionWithTimestamp<CertifiedTransaction>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TransactionWithResults {
    // pub full_tx: TransactionWithEffects,
    pub tx_effects: TransactionEffects, // determined after execution
    // pub deleted: BTreeMap<ObjectID, (SequenceNumber, DeleteKind)>,
    pub written: BTreeMap<ObjectID, Object>,
    // pub missing_objs: HashSet<ObjectID>,
}

impl TransactionWithResults {
    pub fn success(&self) -> bool {
        self.tx_effects.status().is_ok()
    }
}

#[derive(Clone)]
pub struct SuiExecutor {
    ctx: BenchmarkContext,
    workload: Workload,
}

impl SuiExecutor {
    pub async fn new(config: &BenchmarkConfig) -> Self {
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
        };

        // Create genesis.
        tracing::debug!("Creating genesis for {pre_generation} transactions...");
        let start_time = Instant::now();
        let workload = Workload::new(pre_generation, workload_type);
        let component = Component::PipeTxsToChannel;
        let ctx = BenchmarkContext::new(workload.clone(), component, true).await;
        let elapsed = start_time.elapsed();
        tracing::debug!(
            "Genesis created {} accounts/s in {} ms",
            workload.num_accounts() as f64 / elapsed.as_secs_f64(),
            elapsed.as_millis(),
        );

        Self { ctx, workload }
    }

    pub fn create_in_memory_store(&self) -> InMemoryObjectStore {
        self.ctx.validator().create_in_memory_store()
    }
}

impl Executor for SuiExecutor {
    type Transaction = CertifiedTransaction;
    type TransactionResults = TransactionWithResults;
    type Store = InMemoryObjectStore;

    async fn execute(
        &mut self,
        store: &InMemoryObjectStore,
        transaction: &SuiTransactionWithTimestamp,
    ) -> TransactionWithResults {
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

        TransactionWithResults {
            tx_effects: effects,
            written,
        }
    }

    async fn generate_transactions(&mut self) -> Vec<CertifiedTransaction> {
        tracing::debug!("Generating all transactions...");
        let start_time = Instant::now();
        let tx_generator = self.workload.create_tx_generator(&mut self.ctx).await;
        let transactions = self.ctx.generate_transactions(tx_generator).await;
        let transactions = self.ctx.certify_transactions(transactions, false).await;
        let elapsed = start_time.elapsed();
        tracing::debug!(
            "Generated {} txs in {} ms",
            transactions.len(),
            elapsed.as_millis(),
        );

        transactions
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::WorkloadType;

    #[tokio::test]
    async fn test_sui_executor() {
        let config = BenchmarkConfig {
            load: 10,
            duration: Duration::from_secs(1),
            workload: WorkloadType::Transfers,
        };

        let mut executor = SuiExecutor::new(&config).await;
        let store = executor.create_in_memory_store();

        let transactions = executor.generate_transactions().await;
        assert!(transactions.len() > 10);

        for tx in transactions {
            let transaction = TransactionWithTimestamp::new_for_tests(tx);
            let results = executor.execute(&store, &transaction).await;
            assert!(results.tx_effects.status().is_ok());
        }
    }
}
