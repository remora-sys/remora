// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::{marker::PhantomData, sync::Arc, thread};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    checkpoint::state_collector::StateCollector,
    checkpoint::EpochId,
    config::{LoadBalancingPolicy, ProxyMode, DEFAULT_CHANNEL_SIZE},
    error::{NodeError, NodeResult},
    executor::{
        api::{ExecutionResults, Executor, PrimaryToProxyMessage, RemoraTransaction, Store},
        versioned_dependency_controller::VersionedDependencyController,
    },
    metrics::Metrics,
    networking::chunking::{chunk_replay_batch, ChunkingConfig},
    primary::{
        owned_obj_txn_forwarder::OwnedObjTxnForwarder,
        shared_obj_txn_forwarder::{SharedObjTxnForwarder, VersionAssignmentTask},
    },
    proxy::core::ProxyId,
    recovery::{EpochLogger, RecoveryCoordinator},
};
use sui_types::base_types::{ObjectID, SequenceNumber};

/// A load balancer is responsible for distributing transactions to proxies.
pub struct LoadBalancer<E: Executor> {
    /// The executor trait
    _phantom: PhantomData<E>,
    /// The proxy connections.
    proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    /// The load balancing policy.
    policy: LoadBalancingPolicy,
    /// The proxy mode.
    proxy_mode: ProxyMode,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
    /// Next epoch id to broadcast at consensus boundary (Phase 2 default: every batch)
    next_epoch_id: u64,
    /// Atomic current epoch id shared with forwarders for logging
    current_epoch_atomic: Arc<AtomicU64>,
    /// Sender to notify the checkpoint collector of new epochs
    epoch_tx: tokio::sync::mpsc::Sender<EpochId>,
    /// Cumulative transactions since last checkpoint
    txns_since_last_epoch: usize,
    /// In-memory per-epoch transaction logger
    epoch_logger: Arc<EpochLogger<E::Transaction>>,
    /// Recovery coordinator for failure handling
    recovery_coordinator: Arc<RecoveryCoordinator<E::Transaction>>,
    /// Standby exclusion toggle: when true, exclude the last proxy index from dispatch
    standby_excluded: Arc<AtomicBool>,
    /// Shared mapping of (object, version) -> set of proxy indices that own this version
    /// Multiple proxies can own the same version (e.g., after bridging transaction replay)
    states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), std::collections::HashSet<usize>>>,
    /// Reference to the collector for persisted version checks
    collector: Arc<StateCollector>,
    /// Configuration for message chunking to handle large recovery messages
    chunking_config: ChunkingConfig,
}

impl<E: Executor + Send + Sync + 'static> LoadBalancer<E>
where
    <E as Executor>::Transaction: Send + Sync + 'static,
{
    /// Create a new load balancer.
    pub fn new(
        proxy_connections: Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
        policy: LoadBalancingPolicy,
        proxy_mode: ProxyMode,
        metrics: Arc<Metrics>,
        epoch_tx: tokio::sync::mpsc::Sender<EpochId>,
        epoch_logger: Arc<EpochLogger<E::Transaction>>,
        collector: Arc<StateCollector>,
        max_message_size: usize,
    ) -> Self {
        tracing::info!("LB: proxy_mode: {:?}", proxy_mode);
        let states_to_proxy = Arc::new(DashMap::with_capacity(10000000));
        let recovery_coordinator = RecoveryCoordinator::new(epoch_logger.clone());
        let chunking_config = ChunkingConfig::new(max_message_size);
        tracing::info!(
            "LoadBalancer: max_message_size={} bytes, effective_max_size={} bytes",
            chunking_config.max_message_size,
            chunking_config.effective_max_size()
        );
        Self {
            _phantom: PhantomData,
            proxy_connections,
            rx_committed_txns,
            policy,
            proxy_mode,
            metrics,
            next_epoch_id: 1,
            current_epoch_atomic: Arc::new(AtomicU64::new(0)),
            epoch_tx,
            txns_since_last_epoch: 0,
            epoch_logger,
            recovery_coordinator,
            standby_excluded: Arc::new(AtomicBool::new(true)),
            states_to_proxy,
            collector,
            chunking_config,
        }
    }

    /// Promote the reserved standby proxy to active dispatch.
    pub fn promote_standby(&self) {
        self.standby_excluded.store(false, Ordering::SeqCst);
        tracing::info!("Standby proxy promoted to active; exclusion disabled");
    }

    /// Begin recovery for a failed proxy and promote standby.
    pub async fn begin_recovery(&mut self, failed_proxy: ProxyId) -> Option<ProxyId> {
        // Find the standby proxy (last proxy in connections)
        let standby_proxy = self
            .proxy_connections
            .iter()
            .map(|entry| *entry.key())
            .max()
            .unwrap_or(failed_proxy);

        if standby_proxy != failed_proxy {
            // Use the failed proxy's own persist_index
            let persist_index = self.collector.get_proxy_persist_index(failed_proxy);

            // CRITICAL FIX: Wait for in-flight forwarding tasks to complete their epoch logger appends.
            //
            // Race condition: Forwarding tasks run in parallel worker threads and append to the epoch
            // logger asynchronously. When we detect proxy failure and call begin_recovery(), some
            // forwarding tasks may still be in-flight - they've sent (or tried to send) to the failed
            // proxy but haven't yet appended to the epoch logger. If we collect dirty transactions
            // immediately, we'll miss these late-arriving transactions.
            //
            // Solution: Add a small delay after detecting failure to allow in-flight tasks to complete.
            // This gives worker threads time to finish their logger.append() calls (line 322 in
            // shared_obj_txn_forwarder.rs) before we snapshot the dirty transaction set.
            //
            // TODO: Replace with proper synchronization (e.g., worker pool flush or epoch barrier).
            tracing::info!(
                failed_proxy,
                "Waiting for in-flight forwarding tasks to complete epoch logger appends..."
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            tracing::info!(failed_proxy, "Delay complete, capturing atomic snapshot");

            // CRITICAL: Capture failed_proxy_states FIRST to establish snapshot boundary.
            // This must happen BEFORE computing recovery plan to ensure atomicity.
            let failed_proxy_states = self.collector.get_versions_by_proxy(failed_proxy);

            // CRITICAL: Compute recovery plan IMMEDIATELY after capturing states,
            // while temp_state and version_ownership are still consistent.
            // If epochs commit between these two calls, we get version mismatches
            // (e.g., init transfer has v3, but replay message references v4).
            let (bridging_txns, dirty_txns) =
                self.recovery_coordinator.begin_recovery_with_bridging(
                    failed_proxy as usize,
                    persist_index,
                    &self.collector,
                );

            tracing::info!(
                failed_proxy,
                standby_proxy,
                state_count = failed_proxy_states.len(),
                bridging_count = bridging_txns.len(),
                dirty_count = dirty_txns.len(),
                persist_index,
                "Captured atomic snapshot: {} states, {} bridging txns, {} dirty txns",
                failed_proxy_states.len(),
                bridging_txns.len(),
                dirty_txns.len()
            );

            // NOTE: Do NOT promote standby here! Promotion happens AFTER replay completes
            // in start_replay_process() to prevent duplicate transactions.
            // The standby_excluded flag remains true during recovery.

            // Remove failed proxy from connections
            tracing::info!(failed_proxy, "Attempting to remove failed proxy connection");
            let removed = self.proxy_connections.remove(&failed_proxy).is_some();
            tracing::info!(
                "Failed proxy removal result failed_proxy={} removed={}",
                failed_proxy,
                removed
            );

            // CRITICAL FIX: After removing a proxy, the resolve_proxy_id mapping changes.
            // Positional indices in states_to_proxy need to be adjusted to account for
            // the removed proxy. All indices > failed_proxy must be decremented by 1.
            //
            // Example: Before removal: connections = {0, 1, 2}, index 1 maps to ProxyId 1
            //          After removing 0: connections = {1, 2}, index 0 maps to ProxyId 1 (shifted!)
            //
            // States owned by the failed proxy need special handling:
            // - They will be replayed to the replacement proxy
            // - We clear them from the map so they'll be re-established during replay
            // - When replay transactions execute, they'll update states_to_proxy naturally
            let mut remapped_count = 0;
            let mut cleared_count = 0;

            // failed_proxy_states already captured at line 162 - use that snapshot
            // Remove states that were owned by the failed proxy
            for key in &failed_proxy_states {
                self.states_to_proxy.remove(key);
                cleared_count += 1;
            }

            // Decrement indices for proxies that came after the failed proxy
            for mut entry in self.states_to_proxy.iter_mut() {
                let owners = entry.value_mut();
                let mut to_remap = Vec::new();

                // Collect indices that need remapping
                for &idx in owners.iter() {
                    if idx > failed_proxy {
                        to_remap.push(idx);
                    }
                }

                // Remove old indices and insert remapped ones
                for idx in to_remap {
                    owners.remove(&idx);
                    owners.insert(idx - 1);
                    remapped_count += 1;
                }
            }
            tracing::info!(
                failed_proxy,
                remapped_count,
                cleared_count,
                "Adjusted state ownership indices after proxy removal: remapped {}, cleared {}",
                remapped_count,
                cleared_count
            );

            tracing::info!(
                "Recovery begun: failed proxy {} replaced by standby {}",
                failed_proxy,
                standby_proxy
            );

            // Start replay process for the replacement proxy
            let replacement_present = self.proxy_connections.contains_key(&standby_proxy);
            let conn_count = self.proxy_connections.len();
            tracing::info!(
                standby_proxy,
                replacement_present,
                conn_count,
                "Replay initiation precheck: replacement presence and connection count"
            );
            self.start_replay_process(
                failed_proxy,
                standby_proxy,
                failed_proxy_states,
                bridging_txns,
                dirty_txns,
            );
            tracing::info!(failed_proxy, standby_proxy, "Replay initiation requested");

            Some(standby_proxy)
        } else {
            tracing::error!(
                "No standby proxy available for recovery of proxy {}",
                failed_proxy
            );
            self.metrics
                .register_error(crate::metrics::ErrorType::TransactionRateTooHigh);
            None
        }
    }

    /// Get the next batch of replay items for a failed proxy.
    /// Returns None when all items have been replayed.
    pub fn get_next_replay_batch(
        &self,
        failed_proxy: ProxyId,
    ) -> Option<Vec<crate::recovery::LogRecord<E::Transaction>>> {
        // Use the failed proxy's own persist_index to find dirty transactions
        // in epochs after its last reported epoch
        let persist_index = self.collector.get_proxy_persist_index(failed_proxy);
        self.recovery_coordinator
            .get_next_replay_batch(failed_proxy as usize, persist_index)
    }

    /// Start the replay process for a replacement proxy.
    /// This method spawns a task to send bridging transactions (from healthy proxies) first,
    /// then dirty transactions (from failed proxy) to the replacement proxy.
    /// Also transfers all state versions owned by the failed proxy.
    /// After all replay messages are sent, it promotes the standby to active.
    ///
    /// CRITICAL: Takes pre-computed recovery plan to ensure atomicity - computing it again
    /// inside the spawned task would allow epochs to commit between captures, causing version mismatches.
    fn start_replay_process(
        &self,
        failed_proxy: ProxyId,
        replacement_proxy: ProxyId,
        failed_proxy_states: Vec<(ObjectID, SequenceNumber)>,
        bridging_txns: Vec<crate::recovery::LogRecord<E::Transaction>>,
        dirty_txns: Vec<crate::recovery::LogRecord<E::Transaction>>,
    ) {
        let proxy_connections = self.proxy_connections.clone();
        let collector = self.collector.clone();
        let standby_excluded = self.standby_excluded.clone();
        let chunking_config = self.chunking_config.clone();
        let states_to_proxy = self.states_to_proxy.clone();

        tokio::spawn(async move {
            tracing::info!(
                failed_proxy,
                replacement_proxy,
                bridging_count = bridging_txns.len(),
                dirty_count = dirty_txns.len(),
                "Replay task spawned with pre-computed recovery plan"
            );

            let failed_proxy_id = failed_proxy; // Capture for use in closure

            // Recovery plan already computed in begin_recovery() to ensure atomicity
            // with failed_proxy_states capture. Don't recompute here!

            if bridging_txns.is_empty() && dirty_txns.is_empty() {
                tracing::info!(failed_proxy, "No transactions to replay");

                // No replay needed, but still promote standby to active
                standby_excluded.store(false, std::sync::atomic::Ordering::SeqCst);
                tracing::info!(
                    replacement_proxy,
                    "Standby proxy promoted to active (no replay needed)"
                );
                return;
            }

            tracing::info!(
                failed_proxy,
                replacement_proxy,
                bridging_count = bridging_txns.len(),
                dirty_count = dirty_txns.len(),
                state_transfer_count = failed_proxy_states.len(),
                "Sending state transfer + bridging + dirty transactions to replacement proxy"
            );

            // Convert LogRecord to ReplayMsg and send to replacement proxy
            if let Some(proxy_tx) = proxy_connections.get(&replacement_proxy) {
                // CRITICAL: First, transfer ALL states owned by failed proxy as initial snapshot.
                //
                // Problem: Dirty + bridging transactions only cover states actually used by those
                // transactions. But the failed proxy may own many other state versions that aren't
                // referenced. When healthy proxies redirect state requests to the standby, it won't
                // have those states!
                //
                // Solution: Send an initial batch containing all state blobs owned by the failed proxy.
                // This ensures standby can respond to any state request that might be redirected to it.

                tracing::info!(
                    replacement_proxy,
                    state_count = failed_proxy_states.len(),
                    "Fetching {} state blobs owned by failed proxy for initial transfer",
                    failed_proxy_states.len()
                );

                // CRITICAL FIX: Identify "touched" objects that are handled by dirty/bridging txns
                // Recovery plan: persist_state + dirty_txns + bridging_txns = complete state
                // Initial transfer should ONLY include objects NOT touched by dirty/bridging
                //
                // Touched objects = any object appearing in:
                // 1. Dirty transaction required_states (will get state blob or be produced)
                // 2. Bridging transaction required_states (will get state blob or be produced)
                // 3. Bridging transaction outputs (produced by execution)
                //
                // Untouched objects = objects owned by failed proxy but not involved in replay

                let mut touched_objects = std::collections::HashSet::new();
                let mut touched_by_dirty_count = 0;
                let mut touched_by_bridging_count = 0;

                // Mark objects touched by dirty transactions
                tracing::debug!(
                    "Marking objects touched by {} dirty transactions",
                    dirty_txns.len()
                );
                for record in &dirty_txns {
                    for ((obj_id, version), _) in &record.required_states {
                        if touched_objects.insert(*obj_id) {
                            touched_by_dirty_count += 1;
                            tracing::debug!(
                                "Marked {:?} as touched by DIRTY txn (requires v{}, digest={:?})",
                                obj_id,
                                version.value(),
                                record.txn_digest
                            );
                        }
                    }
                }

                // Mark objects touched by bridging transactions (inputs and outputs)
                tracing::debug!(
                    "Marking objects touched by {} bridging transactions",
                    bridging_txns.len()
                );
                for record in &bridging_txns {
                    for ((obj_id, req_version), _) in &record.required_states {
                        if touched_objects.insert(*obj_id) {
                            touched_by_bridging_count += 1;
                            tracing::debug!(
                                "Marked {:?} as touched by BRIDGING txn (requires v{}, digest={:?})",
                                obj_id, req_version.value(), record.txn_digest
                            );
                        }
                        // Also mark the produced version's object (same object)
                        // produced_version = req_version + 1, but it's the same object
                    }
                }

                tracing::info!(
                    replacement_proxy,
                    touched_count = touched_objects.len(),
                    touched_by_dirty = touched_by_dirty_count,
                    touched_by_bridging = touched_by_bridging_count,
                    "Identified {} objects touched by dirty/bridging transactions ({} by dirty, {} by bridging) - will exclude from initial transfer",
                    touched_objects.len(),
                    touched_by_dirty_count,
                    touched_by_bridging_count
                );

                // Compute bridging_productions for later use in Step 3.2
                // This prevents fetching state blobs for versions produced by earlier bridging txns
                // IMPORTANT: All objects in a transaction advance to max(all_req_versions) + 1
                let mut bridging_productions = std::collections::HashSet::new();
                tracing::debug!(
                    "Computing bridging_productions for {} bridging transactions",
                    bridging_txns.len()
                );
                for record in &bridging_txns {
                    let produced_version = record.produced_version();
                    for ((obj_id, _req_version), _) in &record.required_states {
                        bridging_productions.insert((*obj_id, produced_version));
                        tracing::debug!(
                            "Bridging txn {:?} will produce {:?} @ v{}",
                            record.txn_digest,
                            obj_id,
                            produced_version.value()
                        );
                    }
                }
                tracing::debug!(
                    "Computed {} bridging production entries",
                    bridging_productions.len()
                );

                // Fetch state blobs for all states owned by the failed proxy
                // EXCEPT for "touched" objects that are handled by dirty/bridging transactions
                // Note: get_object_for_proxy now strictly returns exact versions owned by this proxy
                let mut initial_state_blobs = std::collections::BTreeMap::new();
                let mut missing_states = Vec::new();
                let mut excluded_for_replay = 0;
                for (object_id, version) in &failed_proxy_states {
                    // CRITICAL: Skip "touched" objects that appear in dirty/bridging transactions
                    // These objects will be handled by:
                    // - State blobs attached to dirty/bridging transactions
                    // - Execution of bridging transactions (producing new versions)
                    // Initial transfer should only include "untouched" passive objects
                    if touched_objects.contains(object_id) {
                        tracing::debug!(
                            "✗ Excluding {:?} v{} from initial transfer - touched by dirty/bridging txns",
                            object_id,
                            version.value()
                        );
                        excluded_for_replay += 1;
                        continue;
                    }
                    if let Some(object) =
                        collector.get_object_for_proxy(object_id, *version, failed_proxy as usize)
                    {
                        initial_state_blobs.insert(*object_id, object);
                        tracing::info!(
                            "✓ Including {:?} v{} in initial transfer - untouched object",
                            object_id,
                            version.value()
                        );
                    } else {
                        tracing::warn!(
                            "Failed to fetch state {:?} @ {:?} from collector for failed proxy {}",
                            object_id,
                            version,
                            failed_proxy
                        );
                        missing_states.push((*object_id, *version));
                    }
                }

                if !missing_states.is_empty() {
                    tracing::warn!(
                        "Failed to fetch {} state blobs for failed proxy {}: {:?}",
                        missing_states.len(),
                        failed_proxy,
                        missing_states
                    );
                }

                tracing::info!(
                    replacement_proxy,
                    fetched_count = initial_state_blobs.len(),
                    requested_count = failed_proxy_states.len(),
                    excluded_for_replay,
                    "Fetched {} / {} state blobs for initial transfer ({} excluded - touched by replay txns)",
                    initial_state_blobs.len(),
                    failed_proxy_states.len(),
                    excluded_for_replay
                );

                // Send initial state transfer as a special replay batch with epoch 0
                if !initial_state_blobs.is_empty() {
                    // Create a pure state transfer message (transaction = None)
                    // The proxy will only commit the state blobs without executing any transaction
                    let state_transfer_msg = crate::executor::api::ReplayMsg {
                        consensus_index: 0,
                        transaction: None, // No transaction - pure state transfer
                        required_versions: vec![],
                        state_blobs: initial_state_blobs.clone(),
                    };

                    let state_transfer_batch = crate::executor::api::ReplayBatch {
                        epoch: crate::checkpoint::EpochId(0), // Special epoch for state transfer
                        items: vec![state_transfer_msg],
                    };

                    // Chunk the state transfer batch if needed
                    let chunking_result =
                        chunk_replay_batch(state_transfer_batch, &chunking_config);

                    tracing::info!(
                        replacement_proxy,
                        total_state_count = initial_state_blobs.len(),
                        num_chunks = chunking_result.num_chunks,
                        max_chunk_size = chunking_result.max_chunk_size,
                        "Sending initial state transfer ({} chunks) to replacement proxy",
                        chunking_result.num_chunks
                    );

                    // Send all chunks
                    for (chunk_idx, chunk) in chunking_result.chunks.into_iter().enumerate() {
                        let msg = crate::executor::api::PrimaryToProxyMessage::Replay(chunk);
                        if let Err(e) = proxy_tx.value().send(msg).await {
                            tracing::error!(
                                "Failed to send initial state transfer chunk {}/{} to replacement proxy {}: {:?}",
                                chunk_idx + 1,
                                chunking_result.num_chunks,
                                replacement_proxy,
                                e
                            );
                            return;
                        }
                        tracing::debug!(
                            "Sent initial state transfer chunk {}/{} to replacement proxy {}",
                            chunk_idx + 1,
                            chunking_result.num_chunks,
                            replacement_proxy
                        );
                    }

                    tracing::info!(
                        replacement_proxy,
                        state_count = initial_state_blobs.len(),
                        num_chunks = chunking_result.num_chunks,
                        "Completed initial state transfer to replacement proxy"
                    );
                }

                // Combine bridging and dirty transactions in order
                // Bridging transactions must be replayed FIRST to regenerate missing versions
                // Clone the transactions so we can use them later to update states_to_proxy
                let all_txns = bridging_txns.iter().chain(dirty_txns.iter()).cloned();

                // Group transactions by epoch and send one batch per epoch
                let mut txns_by_epoch: std::collections::BTreeMap<
                    crate::checkpoint::EpochId,
                    Vec<_>,
                > = std::collections::BTreeMap::new();

                for record in all_txns {
                    txns_by_epoch.entry(record.epoch).or_default().push(record);
                }

                let total_epochs = txns_by_epoch.len();
                let mut sent_count = 0;
                // CRITICAL FIX: Track (object_id, version) pairs, not just object_id!
                // Multiple transactions may need different versions of the same object.
                let mut sent_blobs = std::collections::HashSet::new();

                // Build set of all states already sent in initial transfer
                // These are ALL versions owned by the failed proxy
                let initial_transfer_states: std::collections::HashSet<(ObjectID, SequenceNumber)> =
                    failed_proxy_states.iter().cloned().collect();

                // Note: bridging_productions was already computed above for excluding objects
                // from initial transfer, and is reused here to skip fetching state blobs for
                // versions that will be produced by earlier bridging transactions.

                // CAUSALITY VALIDATION: Track highest STATE BLOB version per object
                // We only track actual state blobs sent, NOT produced versions from transaction execution.
                // This ensures state blobs are sent in increasing order per object.
                let mut highest_blob_version: std::collections::HashMap<ObjectID, SequenceNumber> =
                    std::collections::HashMap::new();

                // Initialize with versions from initial transfer (untouched objects only)
                for (obj_id, object) in &initial_state_blobs {
                    highest_blob_version.insert(*obj_id, object.version());
                }

                for (epoch, epoch_records) in txns_by_epoch {
                    let mut replay_items = Vec::new();
                    for record in epoch_records {
                        // Hydrate transaction data from LogRecord
                        let transaction = (*record.transaction).clone();

                        // Fetch state blobs from StateCollector when available
                        // OPTIMIZATION: Skip state blobs for dirty transactions (from failed proxy)
                        // since they're already in the initial transfer. Only fetch for bridging
                        // transactions (from healthy proxies).
                        let mut state_blobs = std::collections::BTreeMap::new();
                        let is_from_failed_proxy =
                            record.destination_proxy == failed_proxy_id as usize;

                        // Only fetch state blobs for bridging transactions
                        // Dirty transaction states are already in initial transfer
                        if !is_from_failed_proxy {
                            for (object_id, version) in record.required_states.keys() {
                                // Skip if already sent in this replay batch
                                if sent_blobs.contains(&(*object_id, *version)) {
                                    continue;
                                }
                                // Skip if already in initial transfer
                                if initial_transfer_states.contains(&(*object_id, *version)) {
                                    continue;
                                }
                                if *version == SequenceNumber::from(2) {
                                    continue;
                                }
                                // Skip if this version will be produced by an earlier bridging transaction
                                // This prevents duplicate state blobs when TxnA produces V3 and TxnB requires V3
                                if bridging_productions.contains(&(*object_id, *version)) {
                                    tracing::debug!(
                                        "Skipping state blob fetch for {:?} v{} - will be produced by earlier bridging txn",
                                        object_id,
                                        version.value()
                                    );
                                    continue;
                                }

                                // CRITICAL: Fetch from the ORIGINAL proxy that executed this bridging transaction
                                // Using get_object_for_proxy ensures we get the exact version from the proxy's
                                // snapshot at persist_index, not the latest version from merged_state which may
                                // have advanced due to epoch commits during recovery.
                                let source_proxy = record.destination_proxy;
                                let object_opt = collector.get_object_for_proxy(
                                    object_id,
                                    *version,
                                    source_proxy,
                                );

                                if let Some(object) = object_opt {
                                    // Should always match since get_object_for_proxy returns exact version
                                    if object.version() == *version {
                                        state_blobs.insert(*object_id, object.clone());
                                        sent_blobs.insert((*object_id, *version));

                                        tracing::debug!(
                                            "Fetched BRIDGING state blob: {:?} v{} from source proxy {} \
                                             (txn digest={:?}, epoch={}, consensus_index={:?})",
                                            object_id, version.value(), source_proxy,
                                            record.txn_digest, epoch.0, record.consensus_index
                                        );
                                    } else {
                                        tracing::error!(
                                            "Version mismatch from get_object_for_proxy! {:?} - expected {:?}, got {:?}",
                                            object_id, version, object.version()
                                        );
                                    }
                                } else {
                                    tracing::debug!(
                                        "Object {:?} v{:?} not found in source proxy {} - will be regenerated by replay",
                                        object_id, version, source_proxy
                                    );
                                }
                            }
                        }

                        // CAUSALITY VALIDATION: Validate that state_blobs for THIS transaction
                        // are in increasing order per object (no blob version <= previous blob version)
                        // We do NOT check produced versions here - those will be validated by execution order.
                        for (obj_id, object) in &state_blobs {
                            let blob_version = object.version();
                            if let Some(prev_blob_version) = highest_blob_version.get(obj_id) {
                                if blob_version <= *prev_blob_version {
                                    let txn_type = if is_from_failed_proxy {
                                        "DIRTY (from failed proxy)"
                                    } else {
                                        "BRIDGING (from healthy proxy)"
                                    };
                                    tracing::error!(
                                        "STATE BLOB CAUSALITY VIOLATION DETECTED!\n\
                                         \n\
                                         Object: {:?}\n\
                                         Previous state blob version: v{}\n\
                                         Current state blob version: v{}\n\
                                         \n\
                                         State blobs must be in increasing order!\n\
                                         \n\
                                         Transaction details:\n\
                                         - Type: {}\n\
                                         - Digest: {:?}\n\
                                         - Epoch: {}\n\
                                         - Consensus index: {:?}\n\
                                         - Destination proxy: {}\n\
                                         \n\
                                         This indicates a bug in state blob fetching logic.",
                                        obj_id,
                                        prev_blob_version.value(),
                                        blob_version.value(),
                                        txn_type,
                                        record.txn_digest,
                                        epoch.0,
                                        record.consensus_index,
                                        record.destination_proxy
                                    );
                                    panic!("State blob causality violation - aborting recovery");
                                }
                            }
                            // Update tracking with this blob version
                            highest_blob_version.insert(*obj_id, blob_version);
                        }

                        replay_items.push(crate::executor::api::ReplayMsg {
                            consensus_index: record.consensus_index.unwrap_or(0),
                            transaction: Some(transaction),
                            required_versions: record.required_states.keys().cloned().collect(),
                            state_blobs,
                        });
                    }

                    let batch_size = replay_items.len();
                    sent_count += batch_size;

                    let replay_batch = crate::executor::api::ReplayBatch {
                        epoch,
                        items: replay_items,
                    };

                    // Chunk the replay batch if needed
                    let chunking_result = chunk_replay_batch(replay_batch, &chunking_config);

                    tracing::info!(
                        epoch = epoch.0,
                        batch_size,
                        sent_count,
                        num_chunks = chunking_result.num_chunks,
                        max_chunk_size = chunking_result.max_chunk_size,
                        "Sending replay batch for epoch ({} chunks)",
                        chunking_result.num_chunks
                    );

                    // Send all chunks for this epoch
                    for (chunk_idx, chunk) in chunking_result.chunks.into_iter().enumerate() {
                        let msg = crate::executor::api::PrimaryToProxyMessage::Replay(chunk);
                        if let Err(e) = proxy_tx.value().send(msg).await {
                            tracing::error!(
                                "Failed to send replay chunk {}/{} for epoch {} to replacement proxy {}: {:?}",
                                chunk_idx + 1,
                                chunking_result.num_chunks,
                                epoch.0,
                                replacement_proxy,
                                e
                            );
                            return;
                        }
                        tracing::debug!(
                            "Sent replay chunk {}/{} for epoch {} to replacement proxy {}",
                            chunk_idx + 1,
                            chunking_result.num_chunks,
                            epoch.0,
                            replacement_proxy
                        );
                    }
                }

                tracing::info!(
                    "Completed replay: sent {} transactions across {} epochs to replacement proxy {}",
                    sent_count,
                    total_epochs,
                    replacement_proxy
                );

                // CRITICAL: Update states_to_proxy to record that standby now owns versions from bridging txns
                //
                // Bridging transactions don't go through normal forwarding path, so get_missing_states_for_transaction()
                // is never called. We need to manually update states_to_proxy to record that the standby now owns
                // all versions produced by bridging transactions. This prevents unnecessary state transfers when
                // future transactions get routed to the standby.
                //
                // IMPORTANT: Calculate ExecutorIndex AFTER promotion (without standby exclusion)
                // We're about to promote the standby, so we need its ExecutorIndex in the post-promotion state.
                let replacement_proxy_index = {
                    let mut keys: Vec<usize> = proxy_connections.iter().map(|e| *e.key()).collect();
                    keys.sort_unstable();
                    // Don't exclude standby - we want the index AFTER promotion
                    keys.iter()
                        .position(|&id| id == replacement_proxy)
                        .unwrap_or(replacement_proxy)
                };

                let mut bridging_versions_added = 0;
                for record in &bridging_txns {
                    let produced_version = record.produced_version();
                    for ((obj_id, _req_version), _) in &record.required_states {
                        states_to_proxy
                            .entry((*obj_id, produced_version))
                            .or_insert_with(std::collections::HashSet::new)
                            .insert(replacement_proxy_index);
                        tracing::debug!(
                            "Updated states_to_proxy for {:?} to include {:?}",
                            (*obj_id, produced_version),
                            replacement_proxy_index
                        );
                        bridging_versions_added += 1;
                    }
                }

                tracing::info!(
                    replacement_proxy,
                    replacement_proxy_index,
                    bridging_versions_added,
                    "Updated states_to_proxy: added {} versions from bridging transactions to standby proxy",
                    bridging_versions_added
                );

                // CRITICAL FIX: Promote standby AFTER all replay messages are sent.
                //
                // Bug: If we promote the standby before replay completes, new transactions can be
                // forwarded to the standby while it's still processing replay messages. This causes
                // duplicates: the same transaction appears both as a normal CombinedTxn (from new
                // forwarding) and as a Replay message (from recovery).
                //
                // Solution: Keep standby_excluded=true during replay, then promote after sending
                // all replay messages. This ensures the standby only receives replay traffic until
                // it's fully caught up.
                standby_excluded.store(false, std::sync::atomic::Ordering::SeqCst);
                tracing::info!(
                    replacement_proxy,
                    "Standby proxy promoted to active after replay completion"
                );

                // Note on states_to_proxy updates:
                //
                // - Bridging transactions: Already updated above. Both healthy proxies AND standby now
                //   own these versions in the HashSet. This allows future transactions to execute locally
                //   on either proxy without unnecessary state transfers.
                //
                // - Dirty transactions: Will be updated naturally through normal forwarding as the standby
                //   executes new transactions. Since standby is now promoted, future transactions will call
                //   get_missing_states_for_transaction() which updates states_to_proxy.
                //
                // With HashSet-based ownership, multiple proxies can own the same version simultaneously,
                // which is correct after bridging transaction replay.
            } else {
                tracing::error!(
                    "Replacement proxy {} not found in connections",
                    replacement_proxy
                );
                // Promote standby anyway to avoid system deadlock
                standby_excluded.store(false, std::sync::atomic::Ordering::SeqCst);
                tracing::warn!(
                    replacement_proxy,
                    "Standby promoted despite replay failure to avoid deadlock"
                );
            }
        });
    }

    /// Prune epoch logger based on current persist index from state collector.
    /// This should be called periodically to clean up old epoch logs.
    ///
    /// With per-proxy persist_index, we can safely prune any epoch where the epoch ID
    /// is less than the minimum persist_index across all proxies, since that means
    /// all proxies have moved past that epoch.
    pub fn prune_epoch_logger(&self) {
        let min_persist_index = self.collector.get_persist_index();

        // Simply prune all epochs below the minimum persist index
        // Since persist_index represents epoch IDs, and all proxies report in order,
        // any epoch with ID < min_persist_index has been completed by all proxies
        let epochs_to_prune: Vec<crate::checkpoint::EpochId> = self
            .epoch_logger
            .get_segments()
            .iter()
            .filter_map(|entry| {
                let epoch = *entry.key();
                // Prune if epoch ID is less than the minimum persist index
                if epoch.0 < min_persist_index {
                    Some(epoch)
                } else {
                    None
                }
            })
            .collect();

        // Prune the identified epochs
        let pruned_count = epochs_to_prune.len();
        for epoch in epochs_to_prune {
            self.epoch_logger.prune_epoch(epoch);
            tracing::debug!(
                "Pruned epoch {} (epoch < min_persist_index {})",
                epoch.0,
                min_persist_index
            );
        }

        if pruned_count > 0 {
            tracing::info!(
                "Epoch logger pruning completed. Min persist index: {}, pruned {} epochs",
                min_persist_index,
                pruned_count
            );
        }
    }

    /// Initialize transaction processors and return the senders
    fn initialize_processors(
        &self,
    ) -> (
        Sender<Vec<RemoraTransaction<E>>>,   // owned_txn_sender
        Sender<(u64, RemoraTransaction<E>)>, // shared_txn_sender with consensus index
    ) {
        // Create channels for transactions
        let (owned_txn_sender, owned_txn_receiver) =
            tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (shared_txn_sender, shared_txn_receiver) =
            tokio::sync::mpsc::channel::<(u64, RemoraTransaction<E>)>(DEFAULT_CHANNEL_SIZE);
        let (version_assignment_sender, version_assignment_receiver) =
            tokio::sync::mpsc::channel::<(
                u64,
                RemoraTransaction<E>,
                Vec<(
                    sui_types::base_types::ObjectID,
                    sui_types::base_types::SequenceNumber,
                )>,
            )>(DEFAULT_CHANNEL_SIZE);

        // Initialize the OwnedTxnProcessor
        let mut owned_txn_processor = OwnedObjTxnForwarder::<E> {
            proxy_connections: self.proxy_connections.clone(),
            index: 0,
            proxy_mode: self.proxy_mode.clone(),
        };

        let mut version_assignment_processor = VersionAssignmentTask::<E> {
            shared_object_versions: rustc_hash::FxHashMap::default(),
            _phantom: PhantomData,
        };
        version_assignment_processor
            .shared_object_versions
            .reserve(10000000);

        // Initialize the SharedTxnProcessor
        let mut shared_txn_processor = SharedObjTxnForwarder::<E>::new(
            Arc::new(VersionedDependencyController::new()),
            self.states_to_proxy.clone(),
            self.policy.clone(),
            self.proxy_connections.clone(),
            self.proxy_mode.clone(),
            self.metrics.clone(),
            Arc::new(DashMap::with_capacity(self.proxy_connections.len())),
            (0..self.proxy_connections.len())
                .map(|_| Arc::new(DashMap::with_capacity(10000)))
                .collect(),
            Some(self.epoch_logger.clone()),
            self.current_epoch_atomic.clone(),
            self.standby_excluded.clone(),
        );

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                owned_txn_processor
                    .process_owned_txns(owned_txn_receiver)
                    .await;
            });
        });

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                version_assignment_processor
                    .process_version_assignments(shared_txn_receiver, version_assignment_sender)
                    .await;
            });
        });

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_cpus::get())
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                shared_txn_processor
                    .process_shared_txns(version_assignment_receiver)
                    .await;
            });
        });

        // Return the senders so they can be used in the run loop
        (owned_txn_sender, shared_txn_sender)
    }

    /// Run the load balancer.
    pub async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Load balancer started");

        // Initialize processors and get the transaction senders
        let (owned_txn_sender, shared_txn_sender) = self.initialize_processors();

        let mut txn_cnt = 0;
        let mut _last_consensus_index: u64 = 0;
        loop {
            tokio::select! {
                Some(transactions) = self.rx_committed_txns.recv() => {
                    txn_cnt += 1;
                    if txn_cnt == 1 {
                        self.metrics.register_start_time();
                    }

                    // Separate transactions into owned-only and shared-object transactions
                    let mut owned_txns = Vec::new();
                    let mut shared_txns = Vec::new();

                    for transaction in transactions {
                        self.metrics.update_metrics(transaction.timestamp(), "lb-ingress");
                        let shared_object_ids = transaction.shared_objects();
                        if shared_object_ids.is_empty() {
                            owned_txns.push(transaction);
                        } else {
                            shared_txns.push(transaction);
                        }
                    }

                    // Count current batch size before moving vectors
                    let batch_size = owned_txns.len() + shared_txns.len();

                    // Send owned-only transactions to the dedicated task
                    if !owned_txns.is_empty() {
                        let owned = owned_txns;
                        if let Err(e) = owned_txn_sender.send(owned).await {
                            tracing::error!("Failed to send owned transactions: {:?}", e);
                        }
                    }

                    // Send shared-object transactions to the dedicated task
                    if !shared_txns.is_empty() {
                        // Simulated consensus index (monotonic) per batch
                        let consensus_index = self.next_epoch_id /* placeholder */ * 1_000_000 + txn_cnt as u64;
                        _last_consensus_index = consensus_index;
                        for tx in shared_txns {
                            if let Err(e) = shared_txn_sender.send((consensus_index, tx)).await {
                                tracing::error!("Failed to send shared transactions: {:?}", e);
                            }
                        }
                    }

                    // Phase 2+: Broadcast checkpoint when cumulative txns reach threshold
                    self.txns_since_last_epoch = self.txns_since_last_epoch.saturating_add(batch_size);

                    const EPOCH_TXN_THRESHOLD: usize = 10_000;
                    if self.txns_since_last_epoch >= EPOCH_TXN_THRESHOLD {
                        self.txns_since_last_epoch = 0;
                        let epoch = EpochId(self.next_epoch_id);
                        // Notify collector first; if channel is full, log and continue
                        if let Err(e) = self.epoch_tx.try_send(epoch) {
                            tracing::warn!("Failed to notify collector of epoch {:?}: {:?}", epoch, e);
                        }

                        // CRITICAL FIX: Update epoch counter BEFORE broadcast to prevent transaction spillover.
                        //
                        // Bug: If we update current_epoch_atomic AFTER broadcast_checkpoint completes,
                        // transactions forwarded during the broadcast window will be tagged with the OLD
                        // epoch ID. This causes epoch logs to grow unboundedly - later epochs accumulate
                        // spillover from all previous broadcasts.
                        //
                        // Solution: Increment and store the next epoch ID immediately, so any transactions
                        // forwarded during broadcast are correctly tagged with the new epoch.
                        self.next_epoch_id += 1;
                        self.current_epoch_atomic.store(self.next_epoch_id, Ordering::SeqCst);

                        self.broadcast_checkpoint(epoch).await;

                        // Periodically prune old epochs from the logger
                        self.prune_epoch_logger();
                    }
                }

                else => Err(NodeError::ShuttingDown)?,
            }
        }
    }

    async fn broadcast_checkpoint(&mut self, epoch: EpochId) {
        // Clone keys to avoid holding references while mutating map
        let proxy_ids: Vec<ProxyId> = self.proxy_connections.iter().map(|e| *e.key()).collect();
        let proxy_count = proxy_ids.len();

        for proxy_id in proxy_ids {
            // Exclude standby proxy if standby_excluded is true
            if self.standby_excluded.load(Ordering::SeqCst) && proxy_count > 0 {
                let last_proxy_index = proxy_count - 1;
                if proxy_id == last_proxy_index {
                    continue; // Skip the standby proxy
                }
            }

            let tx_opt = self
                .proxy_connections
                .get(&proxy_id)
                .map(|e| e.value().clone());
            if let Some(tx) = tx_opt {
                let msg = PrimaryToProxyMessage::Checkpoint(epoch);
                if let Err(_e) = tx.send(msg).await {
                    tracing::warn!("Proxy {} send failed; beginning recovery", proxy_id);
                    if let Some(replacement) = self.begin_recovery(proxy_id).await {
                        tracing::info!(
                            "Proxy {} replaced by {} during recovery",
                            proxy_id,
                            replacement
                        );
                    }
                }
            }
        }
        tracing::info!("Broadcasted checkpoint for epoch {}", epoch.0);
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send,
        <E as Executor>::Transaction: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}
