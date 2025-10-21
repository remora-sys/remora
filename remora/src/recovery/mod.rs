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

#[cfg(test)]
mod test_bridging;

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

    /// Build a version production graph for all transactions after persist_index.
    /// Returns (dirty_productions, healthy_productions) where:
    /// - dirty_productions[obj_id][version] = true if dirty txn produces it
    /// - healthy_productions[obj_id] = set of transactions that touch this object
    fn build_version_graph(
        &self,
        dirty_txns: &[LogRecord<T>],
        persist_index: u64,
        failed_proxy: usize,
    ) -> (
        std::collections::HashMap<ObjectID, std::collections::HashSet<SequenceNumber>>,
        std::collections::HashMap<ObjectID, Vec<LogRecord<T>>>,
    ) {
        use std::collections::{HashMap, HashSet};

        let mut dirty_productions: HashMap<ObjectID, HashSet<SequenceNumber>> = HashMap::new();
        let mut healthy_txns_by_object: HashMap<ObjectID, Vec<LogRecord<T>>> = HashMap::new();

        // Build dirty productions: what versions dirty txns will produce
        for record in dirty_txns {
            for ((obj_id, req_version), _) in &record.required_states {
                // Transaction requiring version V produces version V+1
                let produced_version = SequenceNumber::from_u64(req_version.value() + 1);
                dirty_productions
                    .entry(*obj_id)
                    .or_default()
                    .insert(produced_version);
            }
        }

        // Collect healthy proxy transactions from epoch logger
        for epoch_entry in self.logger.segments.iter() {
            if epoch_entry.key().0 <= persist_index {
                continue; // Skip committed epochs
            }

            for record in epoch_entry.value() {
                // Only consider healthy proxy transactions
                if record.destination_proxy == failed_proxy {
                    continue;
                }

                // Group by object for efficient lookup
                for ((obj_id, _req_version), _) in &record.required_states {
                    healthy_txns_by_object
                        .entry(*obj_id)
                        .or_default()
                        .push(record.clone());
                }
            }
        }

        (dirty_productions, healthy_txns_by_object)
    }

    /// Identify missing state versions needed by dirty transactions.
    ///
    /// With two-phase commit StateCollector, the logic is simplified:
    ///
    /// 1. **required == persisted**: Version available ✅
    /// 2. **required < persisted**: SHOULD NOT HAPPEN (two-phase commit prevents this!)
    /// 3. **required > persisted**: Check if produced by dirty txns OR healthy proxies
    /// 4. **Object not in collector**: Check if created by healthy proxies
    ///
    /// # Arguments
    /// * `dirty_txns` - Transactions from the failed proxy that need replay
    /// * `collector` - StateCollector with current persisted versions
    /// * `persist_index` - Snapshot epoch boundary
    ///
    /// # Returns
    /// Set of (ObjectID, SequenceNumber) pairs representing missing intermediate versions
    pub fn identify_missing_versions(
        &self,
        dirty_txns: &[LogRecord<T>],
        collector: &crate::checkpoint::state_collector::StateCollector,
        persist_index: u64,
    ) -> std::collections::HashSet<(ObjectID, SequenceNumber)> {
        use std::collections::HashSet;

        let mut missing = HashSet::new();

        // Get the failed proxy ID from the first dirty transaction
        let failed_proxy = if let Some(first) = dirty_txns.first() {
            first.destination_proxy
        } else {
            return missing; // No dirty transactions
        };

        // Build version graph once for efficient lookups
        let (dirty_productions, healthy_txns_by_object) =
            self.build_version_graph(dirty_txns, persist_index, failed_proxy);

        for txn in dirty_txns {
            for ((obj_id, required_version), _) in &txn.required_states {
                match collector.get_persisted_version(obj_id) {
                    Some(current_version) if current_version == *required_version => {
                        // Case 1: Exact match - version available in persisted state
                        tracing::trace!(
                            obj_id = ?obj_id,
                            version = required_version.value(),
                            "Version available in persisted state"
                        );
                    }
                    Some(current_version) if current_version > *required_version => {
                        // Case 2: Should NOT happen with two-phase commit!
                        // Persisted state should never be ahead of required versions
                        tracing::error!(
                            obj_id = ?obj_id,
                            required_version = required_version.value(),
                            current_version = current_version.value(),
                            "BUG: Persisted version > required version (two-phase commit violation!)"
                        );
                        // Still add to bridging set for recovery
                        missing.insert((*obj_id, *required_version));
                    }
                    Some(current_version) => {
                        // Case 3: required > persisted
                        // The required version is ahead of persisted state. Check where it comes from:

                        // First, check if dirty txns will produce it during replay
                        if dirty_productions
                            .get(obj_id)
                            .map(|versions| versions.contains(required_version))
                            .unwrap_or(false)
                        {
                            tracing::trace!(
                                obj_id = ?obj_id,
                                required_version = required_version.value(),
                                "Version will be produced by dirty transaction replay"
                            );
                        } else if let Some(healthy_txns) = healthy_txns_by_object.get(obj_id) {
                            // Check if healthy proxy produces it
                            let produced_by_healthy = healthy_txns.iter().any(|record| {
                                record.required_states.iter().any(|((obj, req_ver), _)| {
                                    *obj == *obj_id
                                        && SequenceNumber::from_u64(req_ver.value() + 1)
                                            == *required_version
                                })
                            });

                            if produced_by_healthy {
                                missing.insert((*obj_id, *required_version));
                                tracing::debug!(
                                    obj_id = ?obj_id,
                                    required_version = required_version.value(),
                                    "Version produced by healthy proxy, needs bridging"
                                );
                            } else {
                                // Healthy txns touch this object but don't produce the required version
                                tracing::trace!(
                                    obj_id = ?obj_id,
                                    required_version = required_version.value(),
                                    current_version = current_version.value(),
                                    "Version likely in failed proxy's temp state (already executed but not committed)"
                                );
                            }
                        } else {
                            // No healthy txns for this object at all
                            // Version is likely in failed proxy's temp state (pre-failure execution)
                            tracing::trace!(
                                obj_id = ?obj_id,
                                required_version = required_version.value(),
                                current_version = current_version.value(),
                                "Version in failed proxy's temp state - will be fetched via get_object_for_proxy()"
                            );
                        }
                    }
                    None => {
                        // Case 4: Object doesn't exist in persisted state
                        // Check if created by healthy proxy
                        if let Some(healthy_txns) = healthy_txns_by_object.get(obj_id) {
                            let produced_by_healthy = healthy_txns.iter().any(|record| {
                                record.required_states.iter().any(|((obj, req_ver), _)| {
                                    *obj == *obj_id
                                        && SequenceNumber::from_u64(req_ver.value() + 1)
                                            == *required_version
                                })
                            });

                            if produced_by_healthy {
                                missing.insert((*obj_id, *required_version));
                                tracing::debug!(
                                    obj_id = ?obj_id,
                                    required_version = required_version.value(),
                                    "Object created by healthy proxy, needs bridging"
                                );
                            } else {
                                tracing::trace!(
                                    obj_id = ?obj_id,
                                    required_version = required_version.value(),
                                    "Object not in persisted state, will be created during replay"
                                );
                            }
                        }
                    }
                }
            }
        }

        missing
    }

    /// Collect bridging transactions needed to regenerate missing state versions.
    ///
    /// Uses the prebuilt healthy_txns_by_object graph for efficient lookup.
    /// Returns transactions ordered by consensus to maintain causality.
    ///
    /// # Arguments
    /// * `missing_versions` - Set of (ObjectID, SequenceNumber) that need regeneration
    /// * `persist_index` - Starting epoch (snapshot point)
    /// * `failed_proxy` - The failed proxy ID
    ///
    /// # Returns
    /// Ordered list of transactions that must be replayed to regenerate missing versions
    pub fn collect_bridging_transactions(
        &self,
        missing_versions: &std::collections::HashSet<(ObjectID, SequenceNumber)>,
        persist_index: u64,
        failed_proxy: usize,
    ) -> Vec<LogRecord<T>> {
        use std::collections::HashSet;

        let mut bridging_txns = Vec::new();
        let mut seen_digests = HashSet::new();

        // Scan all epochs from persist_index onward
        for epoch_entry in self.logger.segments.iter() {
            let epoch = *epoch_entry.key();

            // Only look at epochs after the persisted snapshot
            if epoch.0 <= persist_index {
                continue;
            }

            for record in epoch_entry.value() {
                // ONLY consider transactions from HEALTHY proxies
                if record.destination_proxy == failed_proxy {
                    continue;
                }

                // Skip if we've already included this transaction
                if seen_digests.contains(&record.txn_digest) {
                    continue;
                }

                // Check if this transaction produces any of the missing versions
                let produces_needed_version =
                    record
                        .required_states
                        .iter()
                        .any(|((obj_id, req_version), _)| {
                            let produced_version =
                                SequenceNumber::from_u64(req_version.value() + 1);
                            missing_versions.contains(&(*obj_id, produced_version))
                        });

                if produces_needed_version {
                    bridging_txns.push(record.clone());
                    seen_digests.insert(record.txn_digest);

                    tracing::debug!(
                        txn_digest = ?record.txn_digest,
                        from_proxy = record.destination_proxy,
                        epoch = epoch.0,
                        "Included bridging transaction from healthy proxy"
                    );
                }
            }
        }

        // CRITICAL: Sort by consensus order to maintain causality
        bridging_txns.sort_by_key(|r| (r.epoch.0, r.consensus_index.unwrap_or(0)));

        tracing::info!(
            bridging_count = bridging_txns.len(),
            missing_versions_count = missing_versions.len(),
            failed_proxy,
            persist_index,
            "Collected bridging transactions from healthy proxies"
        );

        bridging_txns
    }

    /// Complete recovery workflow: collect both bridging and dirty transactions.
    ///
    /// This is the main entry point for recovery that orchestrates:
    /// 1. Collecting dirty transactions from failed proxy
    /// 2. Identifying missing versions needed by dirty transactions
    /// 3. Collecting bridging transactions to regenerate those versions
    ///
    /// # Returns
    /// Tuple of (bridging_txns, dirty_txns) in the order they should be replayed
    pub fn begin_recovery_with_bridging(
        &self,
        failed_proxy: usize,
        persist_index: u64,
        collector: &crate::checkpoint::state_collector::StateCollector,
    ) -> (Vec<LogRecord<T>>, Vec<LogRecord<T>>) {
        tracing::info!(
            failed_proxy,
            persist_index,
            "Beginning recovery with bridging transaction analysis"
        );

        // Step 1: Get dirty transactions for failed proxy
        let dirty_txns = self.drain_dirty_queue(failed_proxy, persist_index);

        tracing::info!(
            dirty_txn_count = dirty_txns.len(),
            "Collected dirty transactions from failed proxy"
        );

        if dirty_txns.is_empty() {
            tracing::info!("No dirty transactions to replay, recovery complete");
            return (Vec::new(), Vec::new());
        }

        // Step 2: Identify missing versions
        let missing_versions =
            self.identify_missing_versions(&dirty_txns, collector, persist_index);

        tracing::info!(
            missing_version_count = missing_versions.len(),
            missing_versions = ?missing_versions.iter().take(10).collect::<Vec<_>>(),
            "Identified missing state versions"
        );

        if missing_versions.is_empty() {
            tracing::info!("No missing versions, can replay dirty transactions directly");
            return (Vec::new(), dirty_txns);
        }

        // Step 3: Collect bridging transactions
        let bridging_txns =
            self.collect_bridging_transactions(&missing_versions, persist_index, failed_proxy);

        tracing::info!(
            bridging_count = bridging_txns.len(),
            dirty_count = dirty_txns.len(),
            "Recovery plan complete: bridging + dirty transactions"
        );

        (bridging_txns, dirty_txns)
    }
}
