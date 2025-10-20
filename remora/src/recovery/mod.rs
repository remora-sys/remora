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
    pub consensus_index: Option<u64>,
    pub txn_digest: TransactionDigest,
    pub transaction: Arc<TransactionWithTimestamp<T>>,
    pub destination_proxy: usize,
    pub required_states: BTreeMap<(ObjectID, SequenceNumber), Option<usize>>,
    pub epoch: EpochId,
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
            destination_proxy = record.destination_proxy,
            consensus_index = record.consensus_index,
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

    /// Deprecated: batch_size is no longer used since recovery sends all dirty
    /// transactions at once. Use new() instead.
    pub fn new_with_batch_size(logger: Arc<EpochLogger<T>>, _batch_size: usize) -> Arc<Self> {
        Self::new(logger)
    }

    /// Begin recovery for a failed proxy. Returns the replacement proxy id to use.
    pub fn begin_recovery(&self, _failed_proxy: usize, standby_proxy: usize) -> usize {
        // Record failure; promotion is external policy. Return the standby for activation.
        standby_proxy
    }

    /// Get the next batch of replay items for a failed proxy.
    /// Returns None when all items have been replayed.
    pub fn get_next_replay_batch(
        &self,
        failed_proxy: usize,
        persist_index: u64,
    ) -> Option<Vec<LogRecord<T>>> {
        let dirty_entries = self.drain_dirty_queue(failed_proxy, persist_index);

        // Always log comprehensive diagnostics for debugging
        tracing::info!(
            failed_proxy,
            persist_index,
            dirty_entries_count = dirty_entries.len(),
            "get_next_replay_batch called"
        );

        if dirty_entries.is_empty() {
            // Detailed diagnostics: show ALL epochs and their transaction counts
            let mut all_epochs_info: Vec<(u64, usize, usize)> = Vec::new(); // (epoch, total_txns, matching_txns)
            for seg in self.logger.segments.iter() {
                let epoch = *seg.key();
                let total_count = seg.value().len();
                let proxy_count = seg
                    .value()
                    .iter()
                    .filter(|r| r.destination_proxy == failed_proxy)
                    .count();
                let matching_count = seg
                    .value()
                    .iter()
                    .filter(|r| r.destination_proxy == failed_proxy)
                    .filter(|r| r.epoch.0 >= persist_index)
                    .count();
                all_epochs_info.push((epoch.0, total_count, proxy_count));

                if proxy_count > 0 {
                    tracing::info!(
                        epoch = epoch.0,
                        total_txns = total_count,
                        failed_proxy_txns = proxy_count,
                        matching_txns_after_persist = matching_count,
                        persist_index,
                        comparison = format!(
                            "epoch {} > persist_index {}? {}",
                            epoch.0,
                            persist_index,
                            epoch.0 > persist_index
                        ),
                        "Epoch details for failed proxy"
                    );
                }
            }
            tracing::warn!(
                failed_proxy,
                persist_index,
                total_epochs = all_epochs_info.len(),
                ?all_epochs_info,
                "No dirty transactions found - detailed epoch breakdown"
            );
            return None;
        }

        // Return all dirty entries at once (batch_size no longer used)
        let batch: Vec<LogRecord<T>> = dirty_entries.into_iter().collect();

        if batch.is_empty() {
            None
        } else {
            tracing::info!(
                failed_proxy,
                batch_size = batch.len(),
                "Returning replay batch"
            );
            Some(batch)
        }
    }

    /// Get the current primary persist index (replay cut) from the state collector.
    pub fn get_persist_index(
        &self,
        state_collector: &crate::checkpoint::state_collector::StateCollector,
    ) -> u64 {
        state_collector.get_persist_index()
    }

    /// Collect replay items for a proxy from a given cut (inclusive) within an epoch.
    pub fn collect_replay_set(
        &self,
        epoch: EpochId,
        from_index: u64,
        failed_proxy: usize,
    ) -> Vec<LogRecord<T>> {
        match self.logger.get_epoch(epoch) {
            Some(records) => records
                .into_iter()
                .filter(|r| r.destination_proxy == failed_proxy)
                .filter(|r| r.consensus_index.map(|i| i >= from_index).unwrap_or(false))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Collect dirty queue entries for a failed proxy from all pending epochs.
    /// This implements the union described in the failure recovery plan.
    pub fn drain_dirty_queue(&self, failed_proxy: usize, persist_index: u64) -> Vec<LogRecord<T>> {
        let mut dirty_entries = Vec::new();

        // Collect from all epochs in the logger
        for epoch_entry in self.logger.segments.iter() {
            let _epoch = *epoch_entry.key();
            let records = epoch_entry.value();

            // Filter for failed proxy and epoch > persist_index
            // Note: persist_index represents the last fully acknowledged epoch,
            // so we need to replay transactions from epochs AFTER that point.
            // We use > (not >=) because transactions in the persist_index epoch
            // have already been completed and acknowledged by the proxy.
            let epoch_entries: Vec<LogRecord<T>> = records
                .iter()
                .filter(|r| r.destination_proxy == failed_proxy)
                .filter(|r| r.epoch.0 > persist_index)
                .cloned()
                .collect();

            dirty_entries.extend(epoch_entries);
        }

        dirty_entries
    }
}
