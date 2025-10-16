use std::collections::BTreeMap;
use std::sync::Arc;

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
    /// Per-proxy last applied consensus index (best effort)
    last_applied: DashMap<usize, u64>,
}

impl RecoveryCoordinator {
    pub fn new(logger: Arc<EpochLogger>) -> Arc<Self> {
        Arc::new(Self {
            logger,
            last_applied: DashMap::new(),
        })
    }

    /// Begin recovery for a failed proxy. Returns the replacement proxy id to use.
    pub fn begin_recovery(&self, failed_proxy: usize, standby_proxy: usize) -> usize {
        // Record failure; promotion is external policy. Return the standby for activation.
        let _ = self.last_applied.remove(&failed_proxy);
        standby_proxy
    }

    /// Locate the replay cut for an epoch based on last applied index (best effort).
    pub fn locate_cut(&self, proxy: usize, default_cut: u64) -> u64 {
        self.last_applied
            .get(&proxy)
            .map(|v| *v.value())
            .unwrap_or(default_cut)
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

    /// Update last applied index for a proxy (call on successful dispatch/ack if needed).
    pub fn update_last_applied(&self, proxy: usize, index: u64) {
        self.last_applied.insert(proxy, index);
    }
}
