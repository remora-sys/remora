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
    /// # Arguments
    /// * `completed_up_to` - Batch watermark (highest fully completed batch across all proxies)
    ///
    /// # Returns
    /// All uncommitted transactions in consensus order, including transactions to ALL proxies
    pub fn collect_uncommitted_transactions(
        &self,
        completed_up_to: u64,
    ) -> Vec<LogRecord<T>> {
        let mut uncommitted_txns = Vec::new();

        // Scan ALL epoch logger entries
        for epoch_entry in self.logger.segments.iter() {
            let epoch = *epoch_entry.key();
            let records = epoch_entry.value();

            // Include ALL transactions with consensus_index > completed_up_to
            // Note: We do NOT filter by destination_proxy - include transactions to ALL proxies
            let uncommitted_entries: Vec<LogRecord<T>> = records
                .iter()
                .filter(|r| r.consensus_index.unwrap_or(0) > completed_up_to)
                .cloned()
                .collect();

            tracing::debug!(
                epoch = epoch.0,
                epoch_total = records.len(),
                uncommitted_count = uncommitted_entries.len(),
                completed_up_to = completed_up_to,
                "Scanned epoch for uncommitted transactions"
            );

            uncommitted_txns.extend(uncommitted_entries);
        }

        // Sort by consensus_index to maintain causality
        uncommitted_txns.sort_by_key(|r| r.consensus_index.unwrap_or(0));

        tracing::info!(
            total_uncommitted = uncommitted_txns.len(),
            completed_up_to = completed_up_to,
            epochs_scanned = self.logger.segments.len(),
            "Collected ALL uncommitted transactions from batch {} onward (includes all proxies)",
            completed_up_to + 1
        );

        uncommitted_txns
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
        // IMPORTANT: All objects in a transaction advance to max(all_req_versions) + 1
        for record in dirty_txns {
            let produced_version = record.produced_version();

            // All objects in this transaction advance to the same version
            for ((obj_id, _req_version), _) in &record.required_states {
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
    /// CRITICAL FIX: Check version_ownership to see if healthy proxies still own required versions.
    /// version_ownership is STABLE (only updates on epoch commits), unlike states_to_proxy which
    /// is actively updated during forwarding. If a healthy proxy still owns the required version
    /// as its LATEST version, NO BRIDGING needed - normal state transfer will handle it.
    ///
    /// # Arguments
    /// * `dirty_txns` - Transactions from the failed proxy that need replay
    /// * `collector` - StateCollector with current persisted versions and version_ownership
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

        // CRITICAL: Build a map of latest versions owned by each healthy proxy
        // This uses version_ownership which is STABLE (only updates on epoch commits)
        // Key: (ProxyId, ObjectID) -> Latest SequenceNumber owned by that proxy
        let mut healthy_proxy_latest_versions: std::collections::HashMap<
            (usize, ObjectID),
            SequenceNumber,
        > = std::collections::HashMap::new();

        // Collect latest versions for all healthy proxies from version_ownership
        for entry in collector.version_ownership.iter() {
            let ((obj_id, version), proxy_id) = entry.pair();
            if *proxy_id != failed_proxy {
                // This is a healthy proxy
                healthy_proxy_latest_versions
                    .entry((*proxy_id, *obj_id))
                    .and_modify(|v| {
                        if *version > *v {
                            *v = *version;
                        }
                    })
                    .or_insert(*version);
            }
        }

        tracing::debug!(
            "Built healthy proxy latest versions map with {} entries",
            healthy_proxy_latest_versions.len()
        );

        for txn in dirty_txns {
            for ((obj_id, required_version), _) in &txn.required_states {
                if *required_version == SequenceNumber::from(2) {
                    continue;
                }
                match collector.get_persisted_version(obj_id) {
                    Some(current_version) if current_version == *required_version => {
                        // Case 1: Exact match - version available in persisted state
                        tracing::debug!(
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
                            tracing::debug!(
                                obj_id = ?obj_id,
                                required_version = required_version.value(),
                                "Version will be produced by dirty transaction replay"
                            );
                        } else {
                            // CRITICAL FIX: Check if a healthy proxy CURRENTLY owns this exact version
                            // A healthy proxy "currently owns" a version if it's their LATEST version
                            let owned_by_healthy = healthy_proxy_latest_versions.iter().any(
                                |((proxy_id, obj), latest_ver)| {
                                    obj == obj_id
                                        && latest_ver == required_version
                                        && *proxy_id != failed_proxy
                                },
                            );

                            if owned_by_healthy {
                                // Healthy proxy still owns this version as their latest - NO BRIDGING!
                                // Normal state transfer via required_states will handle it
                                tracing::debug!(
                                    obj_id = ?obj_id,
                                    required_version = required_version.value(),
                                    "Version is latest version owned by healthy proxy, no bridging needed"
                                );
                                continue; // Skip this version
                            }

                            // Version not currently owned by any healthy proxy
                            // Check if healthy proxy produced it in the past but has since advanced
                            if let Some(healthy_txns) = healthy_txns_by_object.get(obj_id) {
                                let produced_by_healthy = healthy_txns.iter().any(|record| {
                                    // Check if this transaction touches our object and produces required version
                                    record.required_states.keys().any(|(obj, _)| obj == obj_id)
                                        && record.produced_version() == *required_version
                                });

                                if produced_by_healthy {
                                    missing.insert((*obj_id, *required_version));
                                    tracing::debug!(
                                        obj_id = ?obj_id,
                                        required_version = required_version.value(),
                                        "Version produced by healthy proxy but no longer owned, needs bridging"
                                    );
                                } else {
                                    // Healthy txns touch this object but don't produce the required version
                                    tracing::debug!(
                                        obj_id = ?obj_id,
                                        required_version = required_version.value(),
                                        current_version = current_version.value(),
                                        "Version likely in failed proxy's temp state (already executed but not committed)"
                                    );
                                }
                            } else {
                                // No healthy txns for this object at all
                                // Version is likely in failed proxy's temp state (pre-failure execution)
                                tracing::debug!(
                                    obj_id = ?obj_id,
                                    required_version = required_version.value(),
                                    current_version = current_version.value(),
                                    "Version in failed proxy's temp state - will be fetched via get_object_for_proxy()"
                                );
                            }
                        }
                    }
                    None => {
                        // Case 4: Object doesn't exist in persisted state

                        // CRITICAL FIX: Check if a healthy proxy currently owns this exact version
                        let owned_by_healthy = healthy_proxy_latest_versions.iter().any(
                            |((proxy_id, obj), latest_ver)| {
                                obj == obj_id
                                    && latest_ver == required_version
                                    && *proxy_id != failed_proxy
                            },
                        );

                        if owned_by_healthy {
                            // Healthy proxy owns this version as their latest - NO BRIDGING!
                            tracing::debug!(
                                obj_id = ?obj_id,
                                required_version = required_version.value(),
                                "Object is latest version owned by healthy proxy, no bridging needed"
                            );
                            continue; // Skip this version
                        }

                        // Check if created by healthy proxy but no longer owned (they've advanced)
                        if let Some(healthy_txns) = healthy_txns_by_object.get(obj_id) {
                            let produced_by_healthy = healthy_txns.iter().any(|record| {
                                // Check if this transaction touches our object and produces required version
                                record.required_states.keys().any(|(obj, _)| obj == obj_id)
                                    && record.produced_version() == *required_version
                            });

                            if produced_by_healthy {
                                missing.insert((*obj_id, *required_version));
                                tracing::debug!(
                                    obj_id = ?obj_id,
                                    required_version = required_version.value(),
                                    "Object created by healthy proxy but no longer owned, needs bridging"
                                );
                            } else {
                                tracing::debug!(
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

        //tracing::info!("Missing versions: {:?}", missing.clone());
        missing
    }

    /// Collect bridging transactions needed to regenerate missing state versions.
    ///
    /// Uses iterative expansion to include chains of bridging transactions.
    /// Returns transactions ordered by consensus to maintain causality.
    ///
    /// IMPORTANT: This uses a fixed-point algorithm to handle transitive dependencies:
    /// - Start with versions needed by dirty transactions
    /// - Find healthy transactions that produce those versions
    /// - Check if those transactions need versions not in persisted state or dirty productions
    /// - If so, add those missing versions to the needed set and repeat
    /// - Continue until no new versions are needed
    ///
    /// This ensures we include complete chains like: H1 → H2 → dirty, where H2 depends on H1.
    ///
    /// # Arguments
    /// * `missing_versions` - Set of (ObjectID, SequenceNumber) that need regeneration
    /// * `persist_index` - Starting epoch (snapshot point)
    /// * `failed_proxy` - The failed proxy ID
    /// * `collector` - StateCollector to check persisted versions
    /// * `dirty_txns` - Dirty transactions that will be replayed
    ///
    /// # Returns
    /// Ordered list of transactions that must be replayed to regenerate missing versions
    pub fn collect_bridging_transactions(
        &self,
        missing_versions: &std::collections::HashSet<(ObjectID, SequenceNumber)>,
        persist_index: u64,
        failed_proxy: usize,
        collector: &crate::checkpoint::state_collector::StateCollector,
        dirty_txns: &[LogRecord<T>],
    ) -> Vec<LogRecord<T>> {
        use std::collections::HashSet;

        let mut bridging_txns = Vec::new();
        let mut seen_digests = HashSet::new();

        // Build set of versions that will be produced by dirty transaction replay
        // IMPORTANT: All objects in a transaction advance to max(all_req_versions) + 1
        let mut dirty_productions: HashSet<(ObjectID, SequenceNumber)> = HashSet::new();
        for record in dirty_txns {
            let produced_version = record.produced_version();
            for ((obj_id, _req_version), _) in &record.required_states {
                dirty_productions.insert((*obj_id, produced_version));
            }
        }

        // Start with the initially missing versions
        let mut needed_versions = missing_versions.clone();
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 100; // Prevent infinite loops

        // Fixed-point iteration: keep finding transactions until no new versions are needed
        loop {
            iterations += 1;
            if iterations > MAX_ITERATIONS {
                tracing::warn!("Bridging transaction collection exceeded max iterations, stopping");
                break;
            }

            let mut new_versions_needed = HashSet::new();
            let prev_bridging_count = bridging_txns.len();

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

                    // Check if this transaction produces any of the needed versions
                    let produced_version = record.produced_version();
                    let produces_needed_version = record
                        .required_states
                        .keys()
                        .any(|(obj_id, _)| needed_versions.contains(&(*obj_id, produced_version)));

                    if !produces_needed_version {
                        continue;
                    }

                    // Include this transaction in the bridging set
                    bridging_txns.push(record.clone());
                    seen_digests.insert(record.txn_digest);

                    tracing::debug!(
                        txn_digest = ?record.txn_digest,
                        from_proxy = record.destination_proxy,
                        epoch = epoch.0,
                        "Included bridging transaction from healthy proxy"
                    );

                    // Check what versions THIS transaction needs
                    // If any are missing, we'll need to include more transactions
                    for ((obj_id, req_version), _) in &record.required_states {
                        // Check if this required version is available
                        let available_in_persisted = collector
                            .get_persisted_version(obj_id)
                            .map(|v| v == *req_version)
                            .unwrap_or(false);

                        let produced_by_dirty =
                            dirty_productions.contains(&(*obj_id, *req_version));

                        if !available_in_persisted && !produced_by_dirty {
                            // This version is missing - need to find the transaction that produces it
                            new_versions_needed.insert((*obj_id, *req_version));
                        }
                    }
                }
            }

            // If no new transactions were added this iteration, we're done
            if bridging_txns.len() == prev_bridging_count {
                tracing::debug!(iterations, "Bridging transaction collection converged");
                break;
            }

            // Add the newly identified missing versions to the needed set for next iteration
            needed_versions.extend(new_versions_needed);
        }

        // CRITICAL: Sort by consensus order to maintain causality
        bridging_txns.sort_by_key(|r| (r.epoch.0, r.consensus_index.unwrap_or(0)));

        tracing::info!(
            bridging_count = bridging_txns.len(),
            missing_versions_count = missing_versions.len(),
            iterations,
            failed_proxy,
            persist_index,
            "Collected bridging transactions from healthy proxies (iterative)"
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
    /// Uses version_ownership from StateCollector which is STABLE (only updates on epoch commits)
    /// to determine which versions healthy proxies still own vs. have advanced past.
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

        // Step 2: Identify missing versions using stable version_ownership
        let missing_versions =
            self.identify_missing_versions(&dirty_txns, collector, persist_index);

        tracing::debug!(
            missing_version_count = missing_versions.len(),
            missing_versions = ?missing_versions.iter().take(10).collect::<Vec<_>>(),
            "Identified missing state versions"
        );

        if missing_versions.is_empty() {
            tracing::info!("No missing versions, can replay dirty transactions directly");
            return (Vec::new(), dirty_txns);
        }

        // Step 3: Collect bridging transactions
        let bridging_txns = self.collect_bridging_transactions(
            &missing_versions,
            persist_index,
            failed_proxy,
            collector,
            &dirty_txns,
        );

        tracing::info!(
            bridging_count = bridging_txns.len(),
            dirty_count = dirty_txns.len(),
            "Recovery plan complete: bridging + dirty transactions"
        );

        (bridging_txns, dirty_txns)
    }
}
