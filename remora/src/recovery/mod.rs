use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
};

use crate::checkpoint::EpochId;
use crate::executor::api::{ExecutableTransaction, TransactionWithTimestamp};

#[cfg(test)]
mod test_recovery;

#[derive(Clone, Debug)]
pub struct LogRecord<T: ExecutableTransaction + Clone> {
    pub txn_digest: TransactionDigest,
    pub transaction: Arc<TransactionWithTimestamp<T>>,
    pub required_states: BTreeMap<(ObjectID, SequenceNumber), Option<usize>>,
    pub epoch: EpochId,
}

impl<T: ExecutableTransaction + Clone> LogRecord<T> {
    /// Calculate the produced version for this transaction.
    /// All objects in a transaction advance to max(all_required_versions) + 1.
    pub fn produced_version(&self) -> SequenceNumber {
        let max_version = self
            .required_states
            .keys()
            .map(|(_, v)| v)
            .max()
            .copied()
            .unwrap_or(SequenceNumber::from(2));
        SequenceNumber::from_u64(max_version.value() + 1)
    }
}

/// In-memory per-epoch transaction logger.
/// Epoch segments can be pruned (removed) atomically when acknowledged.
#[derive(Default)]
pub struct EpochLogger<T: ExecutableTransaction + Clone> {
    /// epoch -> ordered log records
    segments: DashMap<EpochId, Vec<LogRecord<T>>>,
}

impl<T: ExecutableTransaction + Clone> EpochLogger<T> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            segments: DashMap::new(),
        })
    }

    pub fn append(&self, epoch: EpochId, record: LogRecord<T>) {
        self.segments
            .entry(epoch)
            .and_modify(|v| v.push(record.clone()))
            .or_insert_with(|| vec![record.clone()]);

        tracing::debug!(
            epoch = epoch.0,
            txn_digest = ?record.txn_digest,
            "EpochLogger: appended transaction"
        );
    }

    pub fn get_epoch(&self, epoch: EpochId) -> Option<Vec<LogRecord<T>>> {
        self.segments.get(&epoch).map(|r| r.clone())
    }

    /// Remove an epoch's segment after it is acknowledged/persisted.
    pub fn prune_epoch(&self, epoch: EpochId) {
        self.segments.remove(&epoch);
    }

    /// Get all epoch segments for pruning logic.
    pub fn get_segments(&self) -> &DashMap<EpochId, Vec<LogRecord<T>>> {
        &self.segments
    }
}

/// Minimal recovery coordinator using the in-memory EpochLogger
#[derive(Default)]
pub struct RecoveryCoordinator<T: ExecutableTransaction + Clone> {
    logger: Arc<EpochLogger<T>>,
}

impl<T: ExecutableTransaction + Clone> RecoveryCoordinator<T> {
    pub fn new(logger: Arc<EpochLogger<T>>) -> Arc<Self> {
        Arc::new(Self { logger })
    }

    /// Get the current primary persist epoch (replay cut) from the state collector.
    pub fn get_persist_epoch(
        &self,
        state_collector: &crate::checkpoint::state_collector::StateCollector,
    ) -> EpochId {
        EpochId(state_collector.get_persist_index())
    }

    /// Collect ALL uncommitted transactions from ALL proxies.
    ///
    /// This is the core of the simplified recovery approach:
    /// - Replays ALL transactions (not just dirty ones for failed proxy)
    /// - Dependencies across proxies satisfied automatically by replaying all
    /// - No bridging transaction computation needed
    ///
    /// Uses watermark-based filtering: transactions with consensus_index > completed_up_to
    /// are uncommitted and need to be replayed.
    ///
    /// Collect ALL uncommitted transactions from ALL proxies.
    ///
    /// This is the core of the simplified recovery approach:
    /// - Replays ALL transactions (not just dirty ones for failed proxy)
    /// - Dependencies across proxies satisfied automatically by replaying all
    /// - No bridging transaction computation needed
    ///
    /// Uses epoch-based filtering: transactions with epoch > completed_epoch_id
    /// are uncommitted and need to be replayed.
    ///
    /// # Arguments
    /// * `completed_epoch_id` - Epoch watermark (highest fully completed epoch across all proxies)
    ///
    /// # Returns
    /// All uncommitted transactions in epoch order
    pub fn collect_uncommitted_transactions(
        &self,
        completed_epoch_id: EpochId,
    ) -> Vec<LogRecord<T>> {
        let mut uncommitted_txns = Vec::new();

        // Scan ALL epoch logger entries
        for epoch_entry in self.logger.segments.iter() {
            let epoch = *epoch_entry.key();
            let records = epoch_entry.value();

            // Include ALL transactions with epoch > completed_epoch_id
            // Note: We do NOT filter by destination_proxy - include transactions to ALL proxies
            if epoch > completed_epoch_id {
                uncommitted_txns.extend(records.iter().cloned());

                tracing::debug!(
                    epoch = epoch.0,
                    epoch_total = records.len(),
                    completed_epoch_id = completed_epoch_id.0,
                    "Collected epoch for uncommitted transactions"
                );

                for record in records {
                    tracing::info!(
                        epoch = epoch.0,
                        txn_digest = ?record.txn_digest,
                        "add uncommitted transaction"
                    );
                }
            }
        }

        // Sort by epoch to maintain causality
        uncommitted_txns.sort_by_key(|r| r.epoch.0);

        tracing::info!(
            total_uncommitted = uncommitted_txns.len(),
            completed_epoch_id = completed_epoch_id.0,
            epochs_scanned = self.logger.segments.len(),
            "Collected ALL uncommitted transactions from epoch {} onward (includes all proxies)",
            completed_epoch_id.0 + 1
        );

        uncommitted_txns
    }

    /// Simplified recovery entry point.
    ///
    /// This replaces the complex `begin_recovery_with_bridging()` approach with a simpler flow:
    /// 1. Collect ALL uncommitted transactions (from all proxies)
    /// 2. Return them in epoch order for replay
    ///
    /// No bridging transaction computation needed - dependencies satisfied by replaying all.
    ///
    /// # Arguments
    /// * `completed_epoch_id` - Epoch watermark (from failed proxy's persist_index)
    ///
    /// # Returns
    /// All uncommitted transactions ready for replay, in epoch order
    pub fn begin_recovery_simple(&self, completed_epoch_id: EpochId) -> Vec<LogRecord<T>> {
        tracing::info!(
            completed_epoch_id = completed_epoch_id.0,
            "Beginning simplified recovery (no bridging computation)"
        );

        // Collect ALL uncommitted transactions
        let uncommitted_txns = self.collect_uncommitted_transactions(completed_epoch_id);

        tracing::info!(
            total_replay_txns = uncommitted_txns.len(),
            completed_epoch_id = completed_epoch_id.0,
            "Simplified recovery plan ready: {} transactions to replay",
            uncommitted_txns.len()
        );

        uncommitted_txns
    }
}
