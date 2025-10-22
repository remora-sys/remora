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
    /// Shared mapping of (object, version) -> proxy index, for gating
    states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), usize>>,
    /// Reference to the collector for persisted version checks
    collector: Arc<StateCollector>,
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
    ) -> Self {
        tracing::info!("LB: proxy_mode: {:?}", proxy_mode);
        let states_to_proxy = Arc::new(DashMap::with_capacity(10000000));
        let recovery_coordinator = RecoveryCoordinator::new(epoch_logger.clone());
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
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            tracing::info!(
                failed_proxy,
                "Delay complete, proceeding with dirty transaction collection"
            );

            // NEW: Compute recovery plan with bridging transactions
            let (bridging_txns, dirty_txns) =
                self.recovery_coordinator.begin_recovery_with_bridging(
                    failed_proxy as usize,
                    persist_index,
                    &self.collector,
                );

            tracing::info!(
                failed_proxy,
                standby_proxy,
                bridging_count = bridging_txns.len(),
                dirty_count = dirty_txns.len(),
                persist_index,
                "Beginning recovery with bridging transactions"
            );

            // Begin recovery process (old API for compatibility)
            let _replacement = self
                .recovery_coordinator
                .begin_recovery(failed_proxy as usize, standby_proxy as usize);

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

            // CRITICAL: Capture ALL state versions owned by failed proxy BEFORE clearing.
            // These will be transferred to the standby even if not referenced by dirty/bridging txns.
            // Use StateCollector's version_ownership tracking (comprehensive, not rotating like states_to_proxy)
            let failed_proxy_states = self.collector.get_versions_by_proxy(failed_proxy);

            tracing::info!(
                failed_proxy,
                state_count = failed_proxy_states.len(),
                "Captured {} state versions owned by failed proxy for transfer to standby",
                failed_proxy_states.len()
            );

            // Remove states that were owned by the failed proxy
            for key in &failed_proxy_states {
                self.states_to_proxy.remove(key);
                cleared_count += 1;
            }

            // Decrement indices for proxies that came after the failed proxy
            for mut entry in self.states_to_proxy.iter_mut() {
                let current_idx = *entry.value();
                if current_idx > failed_proxy {
                    *entry.value_mut() = current_idx - 1;
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
            self.start_replay_process(failed_proxy, standby_proxy, failed_proxy_states);
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
    fn start_replay_process(
        &self,
        failed_proxy: ProxyId,
        replacement_proxy: ProxyId,
        failed_proxy_states: Vec<(ObjectID, SequenceNumber)>,
    ) {
        let recovery_coordinator = self.recovery_coordinator.clone();
        let proxy_connections = self.proxy_connections.clone();
        let collector = self.collector.clone();
        let standby_excluded = self.standby_excluded.clone();

        tokio::spawn(async move {
            tracing::info!(
                failed_proxy,
                replacement_proxy,
                "Replay task spawned for failed proxy"
            );

            // Use the failed proxy's own persist_index
            let persist_index = collector.get_proxy_persist_index(failed_proxy);
            let failed_proxy_id = failed_proxy; // Capture for use in closure

            // NEW: Get complete recovery plan with bridging transactions
            let (bridging_txns, dirty_txns) = recovery_coordinator.begin_recovery_with_bridging(
                failed_proxy as usize,
                persist_index,
                &collector,
            );

            if bridging_txns.is_empty() && dirty_txns.is_empty() {
                tracing::info!(failed_proxy, persist_index, "No transactions to replay");

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
                persist_index,
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

                // Fetch state blobs for all states owned by the failed proxy
                let mut initial_state_blobs = std::collections::BTreeMap::new();
                for (object_id, version) in &failed_proxy_states {
                    if let Some(object) =
                        collector.get_object_for_proxy(object_id, *version, failed_proxy as usize)
                    {
                        // Version is guaranteed to match now - get_object_for_proxy verifies it
                        initial_state_blobs.insert(*object_id, object);
                    } else {
                        tracing::info!(
                            "Failed to fetch state {:?} @ {:?} from collector for failed proxy {}",
                            object_id,
                            version,
                            failed_proxy
                        );
                    }
                }

                tracing::info!(
                    replacement_proxy,
                    fetched_count = initial_state_blobs.len(),
                    requested_count = failed_proxy_states.len(),
                    "Fetched {} / {} state blobs for initial transfer",
                    initial_state_blobs.len(),
                    failed_proxy_states.len()
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

                    tracing::info!(
                        "Sending initial state transfer batch to replacement proxy: {:?}",
                        initial_state_blobs.clone()
                    );

                    let state_transfer_batch = crate::executor::api::ReplayBatch {
                        epoch: crate::checkpoint::EpochId(0), // Special epoch for state transfer
                        items: vec![state_transfer_msg],
                    };

                    let msg =
                        crate::executor::api::PrimaryToProxyMessage::Replay(state_transfer_batch);
                    if let Err(e) = proxy_tx.value().send(msg).await {
                        tracing::error!(
                            "Failed to send initial state transfer to replacement proxy {}: {:?}",
                            replacement_proxy,
                            e
                        );
                        return;
                    }

                    tracing::info!(
                        replacement_proxy,
                        state_count = initial_state_blobs.len(),
                        "Sent initial state transfer batch to replacement proxy"
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

                                // Fetch from persisted state only (bridging txns were on healthy proxies)
                                let object_opt = collector.get_object(object_id);

                                if let Some(object) = object_opt {
                                    // Verify version matches
                                    if object.version() == *version {
                                        state_blobs.insert(*object_id, object);
                                        sent_blobs.insert((*object_id, *version));
                                    } else {
                                        tracing::warn!(
                                            "Version mismatch for object {:?} - expected {:?}, got {:?}. \
                                            This is OK for bridging txns - will be regenerated",
                                            object_id, version, object.version()
                                        );
                                    }
                                } else {
                                    tracing::debug!(
                                        "Object {:?} v{:?} not in collector - will be regenerated by replay",
                                        object_id, version
                                    );
                                }
                            }
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

                    tracing::info!(
                        epoch = epoch.0,
                        batch_size,
                        sent_count,
                        "Sending replay batch for epoch"
                    );

                    let msg = crate::executor::api::PrimaryToProxyMessage::Replay(replay_batch);
                    if let Err(e) = proxy_tx.value().send(msg).await {
                        tracing::error!(
                            "Failed to send replay batch for epoch {} to replacement proxy {}: {:?}",
                            epoch.0,
                            replacement_proxy,
                            e
                        );
                        return;
                    }
                }

                tracing::info!(
                    "Completed replay: sent {} transactions across {} epochs to replacement proxy {}",
                    sent_count,
                    total_epochs,
                    replacement_proxy
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

                // IMPORTANT: Update states_to_proxy ONLY for dirty transactions (from failed proxy).
                //
                // Bridging transactions are from healthy proxies - they already executed there and
                // those proxies own the states. We're just replaying them on standby to regenerate
                // intermediate versions, but ownership remains with the original healthy proxies.
                //
                // Dirty transactions are from the failed proxy - after replay on standby, the standby
                // now owns those state versions. We need to update states_to_proxy to reflect this.
                //
                // Note: We do NOT pass states_to_proxy to this task because updating it from a spawned
                // task would be unsafe. Instead, we accept that future transactions will naturally
                // update the map through normal forwarding when they access these objects.
                //
                // The cleared states from the failed proxy (line 196-212 in begin_recovery) will be
                // re-established organically as new transactions touch those objects.
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
