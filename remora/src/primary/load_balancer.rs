// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
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
use sui_types::object::Object;

/// Sentinel value in states_to_proxy indicating state should be lazily fetched from primary.
/// When a proxy fails, its owned states are marked with this sentinel instead of being removed.
/// Future transactions needing these states will trigger lazy fetch from primary's persisted snapshot.
pub const PRIMARY_FETCH_SENTINEL: usize = usize::MAX;

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
    /// Current epoch counter (increments every 10k transactions)
    epoch_counter: u64,
    /// Count of transactions in current epoch
    txn_count_in_epoch: usize,
    /// Sender to notify the checkpoint collector of new epochs
    epoch_tx: tokio::sync::mpsc::Sender<EpochId>,
    /// In-memory per-epoch transaction logger
    epoch_logger: Arc<EpochLogger<E::Transaction>>,
    /// Recovery coordinator for failure handling
    recovery_coordinator: Arc<RecoveryCoordinator<E::Transaction>>,
    /// Standby exclusion toggle: when true, exclude the last proxy index from dispatch
    standby_excluded: Arc<AtomicBool>,
    /// Shared mapping of (object, version) -> set of proxy indices that own this version
    /// Multiple proxies can own the same version (e.g., after uncommitted transaction replay during recovery)
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
            chunking_config.effective_max_size(),
        );
        Self {
            _phantom: PhantomData,
            proxy_connections,
            rx_committed_txns,
            policy,
            proxy_mode,
            metrics,
            epoch_counter: 1,
            txn_count_in_epoch: 0,
            epoch_tx,
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
            // Use the failed proxy's own persist_index as epoch_id
            let persist_epoch = EpochId(self.collector.get_proxy_persist_index(failed_proxy));

            // TODO: Replace the sleep with a proper synchronization mechanism (e.g., worker pool flush or epoch barrier).
            tracing::info!(
                failed_proxy,
                "Waiting for in-flight forwarding tasks to complete epoch logger appends..."
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            tracing::info!(failed_proxy, "Delay complete, capturing atomic snapshot");

            // Collect all uncommitted transactions from the epoch logger.
            let uncommitted_txns = self
                .recovery_coordinator
                .begin_recovery_simple(persist_epoch);

            tracing::info!(
                failed_proxy,
                standby_proxy,
                uncommitted_count = uncommitted_txns.len(),
                persist_epoch = persist_epoch.0,
                "Captured atomic snapshot: {} uncommitted txns (all proxies)",
                uncommitted_txns.len()
            );

            // Remove failed proxy from connections.
            self.proxy_connections.remove(&failed_proxy);
            tracing::info!(failed_proxy, "Removed failed proxy connection");

            // Remap ownership and mark states of the failed proxy for lazy fetching.
            self.update_state_ownership_after_failure(failed_proxy);

            tracing::info!(
                "Recovery begun: failed proxy {} replaced by standby {}",
                failed_proxy,
                standby_proxy
            );

            // Start replay process for the replacement proxy.
            self.start_replay_process(failed_proxy, standby_proxy, uncommitted_txns);
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

    /// In a single pass over the `states_to_proxy` map, this function performs all necessary updates
    /// after a proxy has failed. It correctly and efficiently remaps ownership by:
    /// 1. Removing the `failed_proxy` from any owner sets.
    /// 2. If the `failed_proxy` was the sole owner, marking the state for lazy fetching from the primary.
    /// 3. Compacting indices by decrementing the index of any proxy with an ID greater than `failed_proxy`.
    fn update_state_ownership_after_failure(&self, failed_proxy: ProxyId) {
        let mut remapped_count = 0;
        let mut marked_for_lazy_fetch = 0;

        self.states_to_proxy.iter_mut().for_each(|mut entry| {
            let owners = entry.value_mut();

            // Fast path to skip entries that don't need changes.
            let failed_is_owner = owners.contains(&failed_proxy);
            let has_indices_to_remap = owners.iter().any(|&idx| idx > failed_proxy);

            if !failed_is_owner && !has_indices_to_remap {
                return;
            }

            // Build a new set of owners, remapping indices as we go.
            let mut new_owners = std::collections::HashSet::new();
            for &owner_idx in owners.iter() {
                if owner_idx == failed_proxy {
                    continue; // Remove failed proxy by not adding it.
                }

                if owner_idx > failed_proxy {
                    new_owners.insert(owner_idx - 1);
                } else {
                    new_owners.insert(owner_idx);
                }
            }

            if failed_is_owner && new_owners.is_empty() {
                new_owners.insert(PRIMARY_FETCH_SENTINEL);
                marked_for_lazy_fetch += 1;
            }

            // Count remappings for this entry for logging.
            remapped_count += owners.iter().filter(|&&idx| idx > failed_proxy).count();

            // Replace the old owner set with the new one.
            *owners = new_owners;
        });

        tracing::info!(
            failed_proxy,
            marked_for_lazy_fetch,
            remapped_count,
            "Completed ownership update: {} states marked for lazy fetch, {} indices remapped",
            marked_for_lazy_fetch,
            remapped_count
        );
    }

    /// Build replay messages with intelligent state blob attachment.
    ///
    /// This function determines which state blobs need to be attached to each replay message
    /// based on what the replacement proxy will have available:
    /// - Replacement proxy starts EMPTY (only has v2 initial versions)
    /// - Skip v2 versions (replacement has them)
    /// - Skip versions that will be produced by earlier replay transactions
    ///
    /// Returns: Vec of (LogRecord, state_blobs) tuples in consensus order
    fn build_replay_messages_with_state_blobs(
        uncommitted_txns: &[crate::recovery::LogRecord<E::Transaction>],
        collector: &StateCollector,
    ) -> Vec<(
        crate::recovery::LogRecord<E::Transaction>,
        std::collections::BTreeMap<ObjectID, Object>,
    )> {
        let mut result = Vec::new();

        // Track which object versions will be produced by replay transactions (in consensus order)
        // This prevents fetching state blobs for versions that will be created during replay
        let mut replay_produced_versions = std::collections::HashSet::new();

        // Track highest version seen per object to validate partial dependency ordering
        // Versions for the same object should be non-decreasing in consensus order
        let mut highest_version_per_object = std::collections::HashMap::new();

        tracing::debug!(
            uncommitted_count = uncommitted_txns.len(),
            "Building replay messages with intelligent state blob attachment"
        );

        for record in uncommitted_txns {
            // Validate partial dependency ordering for required versions
            for ((obj_id, version), _) in &record.required_states {
                if let Some(&prev_highest) = highest_version_per_object.get(obj_id) {
                    if *version < prev_highest {
                        tracing::error!(
                            obj_id = ?obj_id,
                            required_version = version.value(),
                            prev_highest = prev_highest.value(),
                            txn_digest = ?record.txn_digest,
                            epoch = record.epoch.0,
                            "VIOLATION: Partial dependency ordering broken - transaction requires lower version after higher version was seen for same object"
                        );
                    }
                }

                // Update highest version seen for this object (track input versions)
                highest_version_per_object
                    .entry(*obj_id)
                    .and_modify(|v| *v = (*v).max(*version))
                    .or_insert(*version);
            }

            // Intelligent state blob attachment:
            // Attach state blobs for required states that replacement proxy won't have
            let mut state_blobs = std::collections::BTreeMap::new();

            for ((obj_id, version), _) in &record.required_states {
                // Skip initial versions (v2) - replacement proxy has these
                let should_skip_attaching = *version == SequenceNumber::from(2)
                    || replay_produced_versions.contains(&(*obj_id, *version));

                if should_skip_attaching {
                    tracing::trace!(
                        "Skipping state blob for {:?} v{} (initial or produced by replay)",
                        obj_id,
                        version.value()
                    );
                } else {
                    // This state must be fetched and attached
                    if let Some(object) = collector.get_object(obj_id) {
                        if object.version() == *version {
                            state_blobs.insert(*obj_id, object);
                            tracing::debug!(
                                "Attached state blob {:?} v{} from merged_state for txn {:?}",
                                obj_id,
                                version.value(),
                                record.txn_digest
                            );
                        } else {
                            tracing::info!(
                                "Version mismatch: need {:?} v{} but merged_state has v{} for txn {:?}",
                                obj_id,
                                version.value(),
                                object.version().value(),
                                record.txn_digest
                            );
                        }
                    } else {
                        tracing::warn!(
                            "Failed to fetch state blob {:?} v{} from merged_state for txn {:?}",
                            obj_id,
                            version.value(),
                            record.txn_digest
                        );
                    }
                }
            }

            // Mark versions that will be produced by THIS transaction
            let produced_version = record.produced_version();
            for ((obj_id, _), _) in &record.required_states {
                replay_produced_versions.insert((*obj_id, produced_version));
                tracing::info!(
                    "Marked {:?} v{} as will-be-produced by replay txn {:?}",
                    obj_id,
                    produced_version.value(),
                    record.txn_digest
                );
            }

            result.push((record.clone(), state_blobs));
        }

        result
    }

    /// Start the replay process for a replacement proxy.
    /// This method spawns a task to send ALL uncommitted transactions (from all proxies) to the
    /// replacement proxy. States are fetched lazily on-demand during execution (Task 2.4).
    /// After all replay messages are sent, it promotes the standby to active.
    ///
    /// CRITICAL: Takes pre-computed recovery plan to ensure atomicity - computing it again
    /// inside the spawned task would allow epochs to commit between captures, causing version mismatches.
    fn start_replay_process(
        &self,
        failed_proxy: ProxyId,
        replacement_proxy: ProxyId,
        uncommitted_txns: Vec<crate::recovery::LogRecord<E::Transaction>>,
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
                uncommitted_count = uncommitted_txns.len(),
                "Replay task spawned with simplified recovery plan (all uncommitted txns)"
            );

            // Recovery plan (uncommitted transactions) already computed in begin_recovery()
            // to ensure atomicity. Don't recompute here!

            if uncommitted_txns.is_empty() {
                tracing::info!(failed_proxy, "No transactions to replay");

                // No replay needed, but still promote standby to active
                //standby_excluded.store(false, std::sync::atomic::Ordering::SeqCst);
                tracing::info!(
                    replacement_proxy,
                    "Standby proxy promoted to active (no replay needed)"
                );
                return;
            }

            tracing::info!(
                failed_proxy,
                replacement_proxy,
                uncommitted_count = uncommitted_txns.len(),
                "Sending uncommitted transactions to replacement proxy (Task 2.4: NO initial state transfer)"
            );

            // Convert LogRecord to ReplayMsg and send to replacement proxy
            if let Some(proxy_tx) = proxy_connections.get(&replacement_proxy) {
                // Task 2.4: NO INITIAL STATE TRANSFER!
                //
                // Old approach: Transfer all states owned by failed proxy upfront
                // New approach: States fetched lazily on-demand during execution
                //
                // Benefits:
                // - Eliminates large upfront data transfer
                // - Reduces recovery latency
                // - Only transfers states that are actually needed
                // - Relies on primary's lazy state serving (Task 3.1)

                // SIMPLIFIED RECOVERY (Task 2.3 FIX): Replay ALL uncommitted transactions with INTELLIGENT state blob attachment
                // Uses extracted helper function for testability
                let replay_messages_with_blobs =
                    Self::build_replay_messages_with_state_blobs(&uncommitted_txns, &collector);

                // Group by epoch for sending
                let mut txns_by_epoch: std::collections::BTreeMap<
                    crate::checkpoint::EpochId,
                    Vec<_>,
                > = std::collections::BTreeMap::new();

                for (record, state_blobs) in replay_messages_with_blobs {
                    txns_by_epoch
                        .entry(record.epoch)
                        .or_default()
                        .push((record, state_blobs));
                }

                let total_epochs = txns_by_epoch.len();
                let mut sent_count = 0;

                tracing::info!(
                    replacement_proxy,
                    total_epochs,
                    uncommitted_count = uncommitted_txns.len(),
                    "Sending replay messages with intelligent state blob attachment"
                );

                for (epoch, epoch_records) in txns_by_epoch {
                    let mut replay_items = Vec::new();

                    for (record, state_blobs) in epoch_records {
                        // Hydrate transaction data from LogRecord
                        let transaction = (*record.transaction).clone();

                        replay_items.push(crate::executor::api::ReplayMsg {
                            epoch_id: record.epoch,
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

                // CRITICAL: Update states_to_proxy to record that standby now owns versions from uncommitted txns
                //
                // Uncommitted transactions replayed to the standby don't go through normal forwarding path, so
                // get_missing_states_for_transaction() is never called. We need to manually update states_to_proxy
                // to record that the standby now owns all versions produced by uncommitted transactions.
                // This prevents unnecessary state transfers when future transactions get routed to the standby.
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

                let mut versions_added = 0;
                for record in &uncommitted_txns {
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
                        versions_added += 1;
                    }
                }

                tracing::info!(
                    replacement_proxy,
                    replacement_proxy_index,
                    versions_added,
                    "Updated states_to_proxy: added {} versions from uncommitted transactions to standby proxy",
                    versions_added
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
                // - Uncommitted transactions: Already updated above. Both the original proxy AND standby now
                //   own these versions in the HashSet. This allows future transactions to execute locally
                //   on either proxy without unnecessary state transfers.
                //
                // - Future transactions: Will be updated naturally through normal forwarding as the standby
                //   executes new transactions. Since standby is now promoted, future transactions will call
                //   get_missing_states_for_transaction() which updates states_to_proxy.
                //
                // With HashSet-based ownership, multiple proxies can own the same version simultaneously,
                // which is correct after uncommitted transaction replay.
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

    /// Initialize transaction processors and return the senders
    fn initialize_processors(
        &self,
    ) -> (
        Sender<(EpochId, Vec<RemoraTransaction<E>>)>, // owned_txn_sender with epoch_id
        Sender<(EpochId, RemoraTransaction<E>)>,      // shared_txn_sender with epoch_id
    ) {
        // Create channels for transactions
        let (owned_txn_sender, owned_txn_receiver) = tokio::sync::mpsc::channel::<(
            EpochId,
            Vec<RemoraTransaction<E>>,
        )>(DEFAULT_CHANNEL_SIZE);
        let (shared_txn_sender, shared_txn_receiver) =
            tokio::sync::mpsc::channel::<(EpochId, RemoraTransaction<E>)>(DEFAULT_CHANNEL_SIZE);
        let (version_assignment_sender, version_assignment_receiver) =
            tokio::sync::mpsc::channel::<(
                EpochId,
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
            epoch_logger: Some(self.epoch_logger.clone()),
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
            self.standby_excluded.clone(),
            self.collector.clone(),
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
        loop {
            tokio::select! {
                Some(transactions) = self.rx_committed_txns.recv() => {
                    txn_cnt += 1;
                    if txn_cnt == 1 {
                        self.metrics.register_start_time();
                    }

                    for transaction in transactions {
                        // Calculate current epoch per transaction to ensure precise 10k transaction epochs
                        let current_epoch = EpochId(self.epoch_counter);

                        self.metrics.update_metrics(transaction.timestamp(), "lb-ingress");

                        // Send transaction with current epoch_id
                        let shared_object_ids = transaction.shared_objects();
                        if shared_object_ids.is_empty() {
                            // Owned-only transaction - send as single-element vec to maintain interface
                            if let Err(e) = owned_txn_sender.send((current_epoch, vec![transaction])).await {
                                tracing::error!("Failed to send owned transaction: {:?}", e);
                            }
                        } else {
                            // Shared-object transaction
                            if let Err(e) = shared_txn_sender.send((current_epoch, transaction)).await {
                                tracing::error!("Failed to send shared transaction: {:?}", e);
                            }
                        }

                        // Increment transaction count for current epoch
                        self.txn_count_in_epoch += 1;

                        // Check if we've reached epoch threshold (10k transactions)
                        // When threshold is reached, the current transaction belongs to the completed epoch,
                        // and the next transaction will start a new epoch
                        const EPOCH_TXN_THRESHOLD: usize = 10_000;
                        if self.txn_count_in_epoch >= EPOCH_TXN_THRESHOLD {
                            // Broadcast checkpoint for the epoch that just completed
                            // Notify collector first; if channel is full, log and continue
                            if let Err(e) = self.epoch_tx.try_send(current_epoch) {
                                tracing::warn!("Failed to notify collector of epoch {:?}: {:?}", current_epoch, e);
                            }

                            // Increment epoch counter for the next transaction
                            // This ensures: transactions 1-10k get epoch 1, transactions 10,001-20k get epoch 2, etc.
                            self.epoch_counter += 1;
                            self.txn_count_in_epoch = 0;

                            self.broadcast_checkpoint(current_epoch).await;
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::state_collector::StateCollector;
    use crate::recovery::LogRecord;
    use std::sync::Arc;
    use sui_types::{
        base_types::{ObjectID, SequenceNumber, TransactionDigest},
        object::{MoveObject, Object},
    };

    // Use FakeExecutor's transaction type
    use crate::executor::fake::{FakeExecutor, FakeTransaction};
    type TestExecutor = FakeExecutor;

    // Helper to create a test object
    fn create_test_object(obj_id: ObjectID, version: SequenceNumber) -> Object {
        let move_obj = MoveObject::new_gas_coin(version, obj_id, 1000);
        let txn_digest = TransactionDigest::random();
        let owner =
            sui_types::object::Owner::AddressOwner(sui_types::base_types::SuiAddress::default());
        Object::new_move(move_obj, owner, txn_digest)
    }

    // Counter for generating unique watermarks
    // Start at 1 because process_snapshot skips committing when completed_up_to = 0
    static ADD_OBJECT_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    // Helper to add object to collector for a specific proxy
    // Using the public API, each proxy can have different versions in temp_state
    fn add_object_to_collector(
        collector: &StateCollector,
        proxy_id: usize,
        obj_id: ObjectID,
        version: SequenceNumber,
    ) {
        let obj = create_test_object(obj_id, version);
        let mut snapshot = std::collections::BTreeMap::new();
        snapshot.insert(obj_id, obj);

        // Use a shared epoch counter so all proxies report the same epoch
        // This triggers commits to merged_state
        let counter = ADD_OBJECT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let completed_up_to = counter;

        // Add the snapshot for this proxy
        collector.process_snapshot::<FakeTransaction>(
            proxy_id,
            completed_up_to,
            snapshot.clone(),
            3,
            None,
        );

        // Also add empty snapshots for other proxies to trigger commit
        // StateCollector needs all 3 proxies to report before committing
        for other_proxy in 0..3 {
            if other_proxy != proxy_id {
                collector.process_snapshot::<FakeTransaction>(
                    other_proxy,
                    completed_up_to,
                    std::collections::BTreeMap::new(),
                    3,
                    None,
                );
            }
        }
    }

    // Helper to create a test LogRecord
    fn create_test_log_record(
        epoch: u64,
        required_states: Vec<(ObjectID, SequenceNumber)>,
        txn_digest: TransactionDigest,
    ) -> LogRecord<FakeTransaction> {
        let required_states_map: std::collections::BTreeMap<
            (ObjectID, SequenceNumber),
            Option<usize>,
        > = required_states.into_iter().map(|k| (k, None)).collect();

        let txn = FakeTransaction::new(vec![]);
        let txn_with_ts = crate::executor::api::TransactionWithTimestamp::new(
            txn,
            0.0,
            vec![],
            std::time::Duration::from_millis(0),
            std::time::Duration::from_millis(0),
            Some(0),
        );

        LogRecord {
            epoch: EpochId(epoch),
            transaction: Arc::new(txn_with_ts),
            required_states: required_states_map,
            txn_digest,
        }
    }

    #[test]
    fn test_build_replay_messages_empty_uncommitted() {
        let collector = StateCollector::new(3);
        let uncommitted_txns = vec![];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(
            result.len(),
            0,
            "Should return empty vec for no uncommitted transactions"
        );
    }

    #[test]
    fn test_build_replay_messages_skip_v2_initial_versions() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let txn_digest = TransactionDigest::random();

        // Transaction requires object A version 2 (initial version)
        let uncommitted_txns = vec![create_test_log_record(
            10,
            vec![(obj_a, SequenceNumber::from(2))],
            txn_digest,
        )];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 1, "Should have 1 result");
        assert_eq!(
            result[0].1.len(),
            0,
            "Should skip v2 initial version - no state blobs attached"
        );
    }

    #[test]
    fn test_build_replay_messages_attach_v3_state() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let txn_digest = TransactionDigest::random();

        // Add object A v3 to collector for proxy 0
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(3));

        // Transaction requires object A version 3
        let uncommitted_txns = vec![create_test_log_record(
            10,
            vec![(obj_a, SequenceNumber::from(3))],
            txn_digest,
        )];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 1, "Should have 1 result");
        assert_eq!(result[0].1.len(), 1, "Should attach 1 state blob");
        assert!(result[0].1.contains_key(&obj_a), "Should contain object A");
        assert_eq!(result[0].1[&obj_a].version(), SequenceNumber::from(3));
    }

    #[test]
    fn test_build_replay_messages_skip_produced_versions() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let txn1_digest = TransactionDigest::random();
        let txn2_digest = TransactionDigest::random();

        // Add object A v3 to collector for proxy 0
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(3));

        // Transaction 1: requires A v3, will produce A v4
        // Transaction 2: requires A v4 (produced by txn 1)
        let uncommitted_txns = vec![
            create_test_log_record(10, vec![(obj_a, SequenceNumber::from(3))], txn1_digest),
            create_test_log_record(10, vec![(obj_a, SequenceNumber::from(4))], txn2_digest),
        ];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 2, "Should have 2 results");
        assert_eq!(result[0].1.len(), 1, "Txn 1 should attach A v3");
        assert_eq!(
            result[1].1.len(),
            0,
            "Txn 2 should skip A v4 (produced by txn 1)"
        );
    }

    #[test]
    fn test_build_replay_messages_chain_of_dependencies() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let txn1_digest = TransactionDigest::random();
        let txn2_digest = TransactionDigest::random();
        let txn3_digest = TransactionDigest::random();

        // Add object A v3 to collector for proxy 0
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(3));

        // Chain: Txn1 (requires v3) → v4, Txn2 (requires v4) → v5, Txn3 (requires v5) → v6
        let uncommitted_txns = vec![
            create_test_log_record(10, vec![(obj_a, SequenceNumber::from(3))], txn1_digest),
            create_test_log_record(10, vec![(obj_a, SequenceNumber::from(4))], txn2_digest),
            create_test_log_record(10, vec![(obj_a, SequenceNumber::from(5))], txn3_digest),
        ];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 3, "Should have 3 results");
        assert_eq!(result[0].1.len(), 1, "Txn 1 should attach A v3");
        assert_eq!(result[1].1.len(), 0, "Txn 2 should skip A v4");
        assert_eq!(result[2].1.len(), 0, "Txn 3 should skip A v5");
    }

    #[test]
    fn test_build_replay_messages_multiple_objects() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let obj_b = ObjectID::random();
        let txn_digest = TransactionDigest::random();

        // Add objects to collector - both on proxy 0 (same proxy as transaction)
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(3));
        add_object_to_collector(&collector, 0, obj_b, SequenceNumber::from(5));

        // Transaction on proxy 0 requires both A v3 and B v5
        let uncommitted_txns = vec![create_test_log_record(
            10,
            vec![
                (obj_a, SequenceNumber::from(3)),
                (obj_b, SequenceNumber::from(5)),
            ],
            txn_digest,
        )];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 1, "Should have 1 result");
        assert_eq!(result[0].1.len(), 2, "Should attach 2 state blobs");
        assert!(result[0].1.contains_key(&obj_a), "Should contain object A");
        assert!(result[0].1.contains_key(&obj_b), "Should contain object B");
    }

    #[test]
    fn test_build_replay_messages_mixed_v2_and_higher() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let obj_b = ObjectID::random();
        let txn_digest = TransactionDigest::random();

        // Add object A v3 to collector
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(3));

        // Transaction requires A v3 (should attach) and B v2 (should skip)
        let uncommitted_txns = vec![create_test_log_record(
            10,
            vec![
                (obj_a, SequenceNumber::from(3)),
                (obj_b, SequenceNumber::from(2)), // initial version
            ],
            txn_digest,
        )];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 1, "Should have 1 result");
        assert_eq!(
            result[0].1.len(),
            1,
            "Should attach only 1 state blob (not v2)"
        );
        assert!(result[0].1.contains_key(&obj_a), "Should contain object A");
        assert!(
            !result[0].1.contains_key(&obj_b),
            "Should NOT contain object B (v2)"
        );
    }

    #[test]
    fn test_build_replay_messages_missing_state_in_collector() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let txn_digest = TransactionDigest::random();

        // Don't add object to collector - it's missing

        // Transaction requires object A version 3 (not in collector)
        let uncommitted_txns = vec![create_test_log_record(
            10,
            vec![(obj_a, SequenceNumber::from(3))],
            txn_digest,
        )];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 1, "Should have 1 result");
        assert_eq!(
            result[0].1.len(),
            0,
            "Should have no state blobs (missing from collector)"
        );
    }

    #[test]
    fn test_build_replay_messages_cross_proxy_dependencies() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let txn1_digest = TransactionDigest::random();
        let txn2_digest = TransactionDigest::random();

        // Proxy 0 produces A v3
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(3));

        // Txn1 (proxy 0): requires A v3, produces A v4
        // Txn2 (proxy 1): requires A v4 (cross-proxy dependency)
        let uncommitted_txns = vec![
            create_test_log_record(10, vec![(obj_a, SequenceNumber::from(3))], txn1_digest),
            create_test_log_record(10, vec![(obj_a, SequenceNumber::from(4))], txn2_digest),
        ];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 2, "Should have 2 results");
        assert_eq!(result[0].1.len(), 1, "Txn1 should attach A v3 from proxy 0");
        assert_eq!(
            result[1].1.len(),
            0,
            "Txn2 should skip A v4 (produced by txn1)"
        );
    }

    #[test]
    fn test_build_replay_messages_preserves_consensus_order() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();

        // Add objects to collector
        for v in 3..=6 {
            add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(v));
        }

        // Create transactions in consensus order
        let uncommitted_txns = vec![
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(3))],
                TransactionDigest::random(),
            ),
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(4))],
                TransactionDigest::random(),
            ),
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(5))],
                TransactionDigest::random(),
            ),
        ];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 3, "Should have 3 results");
        // Verify epochs are preserved in order
        assert_eq!(result[0].0.epoch.0, 10);
        assert_eq!(result[1].0.epoch.0, 10);
        assert_eq!(result[2].0.epoch.0, 10);
    }

    #[test]
    fn test_build_replay_messages_same_batch_out_of_order_versions() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let obj_b = ObjectID::random();

        // SAME epoch, but out-of-order version touching
        // Different objects with different version numbers
        // Txn 1: requires A v4 → produces A v5
        // Txn 2: requires B v3 → produces B v4
        // This shows that same-epoch txns can have "out of order" version numbers for different objects
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(4));
        add_object_to_collector(&collector, 0, obj_b, SequenceNumber::from(3));

        let uncommitted_txns = vec![
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(4))],
                TransactionDigest::random(),
            ),
            create_test_log_record(
                10,
                vec![(obj_b, SequenceNumber::from(3))],
                TransactionDigest::random(),
            ),
        ];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 2, "Should have 2 results");
        // Both should attach their required states (different objects)
        assert_eq!(result[0].1.len(), 1, "Txn 1 should attach A v4");
        assert!(
            result[0].1.contains_key(&obj_a),
            "Txn 1 should have object A"
        );
        assert_eq!(result[0].1[&obj_a].version(), SequenceNumber::from(4));

        assert_eq!(result[1].1.len(), 1, "Txn 2 should attach B v3");
        assert!(
            result[1].1.contains_key(&obj_b),
            "Txn 2 should have object B"
        );
        assert_eq!(result[1].1[&obj_b].version(), SequenceNumber::from(3));
    }

    #[test]
    fn test_build_replay_messages_same_batch_dependency_chain() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();

        // Add initial version to collector
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(3));

        // SAME epoch (same batch), with dependency chain
        // Txn 1: requires v3, will produce v4
        // Txn 2: requires v4 (produced by Txn 1)
        // Txn 3: requires v5 (produced by Txn 2)
        let uncommitted_txns = vec![
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(3))],
                TransactionDigest::random(),
            ),
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(4))],
                TransactionDigest::random(),
            ),
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(5))],
                TransactionDigest::random(),
            ),
        ];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 3, "Should have 3 results");
        // Only first transaction should attach state blob
        assert_eq!(result[0].1.len(), 1, "Txn 1 should attach A v3");
        assert_eq!(
            result[1].1.len(),
            0,
            "Txn 2 should skip A v4 (produced by Txn 1)"
        );
        assert_eq!(
            result[2].1.len(),
            0,
            "Txn 3 should skip A v5 (produced by Txn 2)"
        );
    }

    #[test]
    fn test_build_replay_messages_same_batch_interleaved_objects() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let obj_b = ObjectID::random();

        // Simplified scenario: Each proxy has ONE object
        // Proxy 1: A v5
        // Proxy 0: B v6
        add_object_to_collector(&collector, 1, obj_a, SequenceNumber::from(5));
        add_object_to_collector(&collector, 0, obj_b, SequenceNumber::from(6));

        // SAME epoch, each txn needs a state from a different proxy
        // Txn 1 (from Proxy 1): requires A v5 (own proxy), will produce A v6
        // Txn 2 (from Proxy 0): requires B v6 (own proxy), will produce B v7
        // Shows that same-epoch txns can access their own proxy's states
        let uncommitted_txns = vec![
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(5))],
                TransactionDigest::random(),
            ),
            create_test_log_record(
                10,
                vec![(obj_b, SequenceNumber::from(6))],
                TransactionDigest::random(),
            ),
        ];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 2, "Should have 2 results");

        // Txn 1 should attach A v5
        assert_eq!(result[0].1.len(), 1, "Txn 1 should attach 1 state blob");
        assert!(
            result[0].1.contains_key(&obj_a),
            "Txn 1 should have object A"
        );
        assert_eq!(result[0].1[&obj_a].version(), SequenceNumber::from(5));

        // Txn 2 should attach B v6
        assert_eq!(result[1].1.len(), 1, "Txn 2 should attach 1 state blob");
        assert!(
            result[1].1.contains_key(&obj_b),
            "Txn 2 should have object B"
        );
        assert_eq!(result[1].1[&obj_b].version(), SequenceNumber::from(6));
    }

    #[test]
    fn test_build_replay_messages_same_batch_partial_production() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();
        let obj_b = ObjectID::random();
        let obj_c = ObjectID::random();

        // Realistic scenario: different proxies have different objects
        // Proxy 0 has A v3 and C v5
        // Proxy 1 has B v4
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(3));
        add_object_to_collector(&collector, 0, obj_c, SequenceNumber::from(5));
        add_object_to_collector(&collector, 1, obj_b, SequenceNumber::from(4));

        // SAME epoch
        // Txn 1: requires A v3, C v5 → produces A v6, C v6 (max(3,5)+1=6)
        // Txn 2: requires A v6 (produced by Txn 1), B v4 → produces A v7, B v7 (max(6,4)+1=7)
        // Shows that production tracking works correctly: A v6 is skipped, B v4 is attached
        let uncommitted_txns = vec![
            create_test_log_record(
                10,
                vec![
                    (obj_a, SequenceNumber::from(3)),
                    (obj_c, SequenceNumber::from(5)),
                ],
                TransactionDigest::random(),
            ),
            create_test_log_record(
                10,
                vec![
                    (obj_a, SequenceNumber::from(6)), // Produced by Txn 1, should be skipped
                    (obj_b, SequenceNumber::from(4)), // From proxy 1, should be attached
                ],
                TransactionDigest::random(),
            ),
        ];

        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        assert_eq!(result.len(), 2, "Should have 2 results");

        // Txn 1 should attach both states
        assert_eq!(result[0].1.len(), 2, "Txn 1 should attach 2 state blobs");
        assert!(result[0].1.contains_key(&obj_a));
        assert!(result[0].1.contains_key(&obj_c));

        // Txn 2 should skip A v6 (produced by Txn 1) but attach B v4
        assert_eq!(
            result[1].1.len(),
            1,
            "Txn 2 should attach 1 state blob (only B v4)"
        );
        assert!(
            !result[1].1.contains_key(&obj_a),
            "Txn 2 should NOT have object A (produced by Txn 1)"
        );
        assert!(
            result[1].1.contains_key(&obj_b),
            "Txn 2 should have object B"
        );
        assert_eq!(result[1].1[&obj_b].version(), SequenceNumber::from(4));
    }

    #[test]
    fn test_build_replay_messages_detects_version_ordering_violation() {
        let collector = StateCollector::new(3);
        let obj_a = ObjectID::random();

        // Add version 5 to merged_state (latest committed version)
        // In the new implementation, merged_state only keeps the latest version
        add_object_to_collector(&collector, 0, obj_a, SequenceNumber::from(5));

        // VIOLATION: Txn 1 (from proxy 0) requires v5, then Txn 2 (from proxy 1) requires v3
        // This violates partial dependency ordering - we're going backwards in version numbers
        let uncommitted_txns = vec![
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(5))],
                TransactionDigest::random(),
            ),
            create_test_log_record(
                10,
                vec![(obj_a, SequenceNumber::from(3))], // Lower version after higher!
                TransactionDigest::random(),
            ),
        ];

        // The function should still complete but log an error
        // (In production this would indicate a serious bug in consensus ordering)
        let result = LoadBalancer::<TestExecutor>::build_replay_messages_with_state_blobs(
            &uncommitted_txns,
            &collector,
        );

        // Function completes despite violation
        assert_eq!(result.len(), 2, "Should have 2 results even with violation");

        // Txn 1 should attach v5 (available in merged_state)
        assert_eq!(result[0].1.len(), 1);
        assert_eq!(result[0].1[&obj_a].version(), SequenceNumber::from(5));

        // Txn 2 requires v3, but merged_state only has v5 (latest version)
        // Since the exact version doesn't match, no state blob is attached
        // This is expected behavior with the new implementation
        assert_eq!(
            result[1].1.len(),
            0,
            "v3 not available in merged_state, only v5"
        );
    }
}
