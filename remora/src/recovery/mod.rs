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
            .or_insert_with(|| vec![record]);
    }

    pub fn get_epoch(&self, epoch: EpochId) -> Option<Vec<LogRecord<T>>> {
        self.segments.get(&epoch).map(|r| r.clone())
    }

    /// Remove an epoch's segment after it is acknowledged/persisted.
    pub fn prune_epoch(&self, epoch: EpochId) {
        self.segments.remove(&epoch);
    }
}

/// Minimal recovery coordinator using the in-memory EpochLogger
#[derive(Default)]
pub struct RecoveryCoordinator<T: ExecutableTransaction + Clone> {
    logger: Arc<EpochLogger<T>>,
    /// Default batch size for replay batches
    batch_size: usize,
}

impl<T: ExecutableTransaction + Clone> RecoveryCoordinator<T> {
    pub fn new(logger: Arc<EpochLogger<T>>) -> Arc<Self> {
        Arc::new(Self {
            logger,
            batch_size: 10, // Default batch size
        })
    }

    pub fn new_with_batch_size(logger: Arc<EpochLogger<T>>, batch_size: usize) -> Arc<Self> {
        Arc::new(Self {
            logger,
            batch_size,
        })
    }

    /// Begin recovery for a failed proxy. Returns the replacement proxy id to use.
    pub fn begin_recovery(&self, _failed_proxy: usize, standby_proxy: usize) -> usize {
        // Record failure; promotion is external policy. Return the standby for activation.
        standby_proxy
    }

    /// Get the next batch of replay items for a failed proxy.
    /// Returns None when all items have been replayed.
    pub fn get_next_replay_batch(&self, failed_proxy: usize, persist_index: u64) -> Option<Vec<LogRecord<T>>> {
        let dirty_entries = self.drain_dirty_queue(failed_proxy, persist_index);
        if dirty_entries.is_empty() {
            // Emit a brief diagnostic to help understand why replay may stall
            let mut epoch_counts: Vec<(EpochId, usize)> = Vec::new();
            for seg in self.logger.segments.iter() {
                let epoch = *seg.key();
                let count = seg
                    .value()
                    .iter()
                    .filter(|r| r.destination_proxy == failed_proxy)
                    .filter(|r| r.consensus_index.map(|i| i >= persist_index).unwrap_or(false))
                    .count();
                if count > 0 {
                    epoch_counts.push((epoch, count));
                }
            }
            tracing::debug!(
                failed_proxy,
                persist_index,
                epochs_with_entries = epoch_counts.len(),
                ?epoch_counts,
                "Dirty queue empty for failed proxy; no replay candidates"
            );
        }

        if dirty_entries.is_empty() {
            return None;
        }

        // Take up to batch_size items
        let batch: Vec<LogRecord<T>> = dirty_entries.into_iter().take(self.batch_size).collect();

        if batch.is_empty() {
            None
        } else {
            Some(batch)
        }
    }

    /// Get the current primary persist index (replay cut) from the state collector.
    pub fn get_persist_index(&self, state_collector: &crate::checkpoint::state_collector::StateCollector) -> u64 {
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

            // Filter for failed proxy and consensus_index >= persist_index
            let epoch_entries: Vec<LogRecord<T>> = records
                .iter()
                .filter(|r| r.destination_proxy == failed_proxy)
                .filter(|r| {
                    r.consensus_index
                        .map(|i| i >= persist_index)
                        .unwrap_or(false)
                })
                .cloned()
                .collect();

            dirty_entries.extend(epoch_entries);
        }

        dirty_entries
    }
}
