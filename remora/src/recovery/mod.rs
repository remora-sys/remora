use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use dashmap::DashMap;
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
};

use crate::checkpoint::EpochId;

#[cfg(test)]
mod test_recovery;

#[derive(Clone, Debug)]
pub struct LogRecord {
    pub consensus_index: Option<u64>,
    pub txn_digest: TransactionDigest,
    pub destination_proxy: usize,
    pub required_states: BTreeMap<(ObjectID, SequenceNumber), Option<usize>>,
}

/// In-memory per-epoch transaction logger.
/// Epoch segments can be pruned (removed) atomically when acknowledged.
#[derive(Default)]
pub struct EpochLogger {
    /// epoch -> ordered log records
    segments: DashMap<EpochId, Vec<LogRecord>>,
}

impl EpochLogger {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            segments: DashMap::new(),
        })
    }

    pub fn append(&self, epoch: EpochId, record: LogRecord) {
        self.segments
            .entry(epoch)
            .and_modify(|v| v.push(record.clone()))
            .or_insert_with(|| vec![record]);
    }

    pub fn get_epoch(&self, epoch: EpochId) -> Option<Vec<LogRecord>> {
        self.segments.get(&epoch).map(|r| r.clone())
    }

    /// Remove an epoch's segment after it is acknowledged/persisted.
    pub fn prune_epoch(&self, epoch: EpochId) {
        self.segments.remove(&epoch);
    }
}

/// Minimal recovery coordinator using the in-memory EpochLogger
#[derive(Default)]
pub struct RecoveryCoordinator {
    logger: Arc<EpochLogger>,
    /// Primary-level persist index: last fully acknowledged epoch's consensus index
    primary_persist_index: AtomicU64,
}

impl RecoveryCoordinator {
    pub fn new(logger: Arc<EpochLogger>) -> Arc<Self> {
        Arc::new(Self {
            logger,
            primary_persist_index: AtomicU64::new(0),
        })
    }

    /// Begin recovery for a failed proxy. Returns the replacement proxy id to use.
    pub fn begin_recovery(&self, _failed_proxy: usize, standby_proxy: usize) -> usize {
        // Record failure; promotion is external policy. Return the standby for activation.
        standby_proxy
    }

    /// Get the current primary persist index (replay cut).
    pub fn get_persist_index(&self) -> u64 {
        self.primary_persist_index.load(Ordering::SeqCst)
    }

    /// Update the primary persist index when an epoch is acknowledged.
    pub fn update_persist_index(&self, consensus_index: u64) {
        self.primary_persist_index
            .store(consensus_index, Ordering::SeqCst);
    }

    /// Collect replay items for a proxy from a given cut (inclusive) within an epoch.
    pub fn collect_replay_set(
        &self,
        epoch: EpochId,
        from_index: u64,
        failed_proxy: usize,
    ) -> Vec<LogRecord> {
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
    pub fn drain_dirty_queue(&self, failed_proxy: usize) -> Vec<LogRecord> {
        let persist_index = self.get_persist_index();
        let mut dirty_entries = Vec::new();

        // Collect from all epochs in the logger
        for epoch_entry in self.logger.segments.iter() {
            let _epoch = *epoch_entry.key();
            let records = epoch_entry.value();

            // Filter for failed proxy and consensus_index >= persist_index
            let epoch_entries: Vec<LogRecord> = records
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
