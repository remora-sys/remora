// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::VecDeque, marker::PhantomData, sync::Arc, thread};
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
    /// Failed proxy id (if any) for gating decisions
    failed_proxy: Option<ProxyId>,
    /// Grey-state ordered buffer per failed proxy
    grey_queue: Arc<dashmap::DashMap<ProxyId, VecDeque<RemoraTransaction<E>>>>,
    /// Shared mapping of (object, version) -> proxy index, for gating
    states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), usize>>,
    /// Reference to the collector for persisted version checks
    collector: Arc<StateCollector>,
    /// Per-epoch acknowledgment state: Acknowledged | Pending
    epoch_ack_state: Arc<DashMap<EpochId, bool>>,
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
            epoch_tx,
            txns_since_last_epoch: 0,
            epoch_logger,
            recovery_coordinator,
            standby_excluded: Arc::new(AtomicBool::new(true)),
            failed_proxy: None,
            grey_queue: Arc::new(dashmap::DashMap::new()),
            states_to_proxy,
            collector,
            epoch_ack_state: Arc::new(DashMap::new()),
        }
    }

    /// Promote the reserved standby proxy to active dispatch.
    pub fn promote_standby(&self) {
        self.standby_excluded.store(false, Ordering::SeqCst);
        tracing::info!("Standby proxy promoted to active; exclusion disabled");
    }

    /// Set the failed proxy id to enable grey gating decisions.
    pub fn set_failed_proxy(&mut self, proxy: ProxyId) {
        self.failed_proxy = Some(proxy);
        self.grey_queue.entry(proxy).or_insert_with(VecDeque::new);
        tracing::warn!("Failed proxy set to {} for grey gating", proxy);
    }

    /// Begin recovery for a failed proxy and promote standby.
    pub fn begin_recovery(&mut self, failed_proxy: ProxyId) -> Option<ProxyId> {
        // Find the standby proxy (last proxy in connections)
        let standby_proxy = self
            .proxy_connections
            .iter()
            .map(|entry| *entry.key())
            .max()
            .unwrap_or(failed_proxy);

        if standby_proxy != failed_proxy {
            // Log dirty queue size and persist index for diagnostics
            let dq_len = self
                .recovery_coordinator
                .drain_dirty_queue(failed_proxy as usize)
                .len();
            let persist_index = self.recovery_coordinator.get_persist_index();
            tracing::info!(
                failed_proxy,
                standby_proxy,
                dirty_queue = dq_len,
                persist_index,
                "Beginning recovery: diagnostics before replay"
            );
            // Begin recovery process
            let _replacement = self
                .recovery_coordinator
                .begin_recovery(failed_proxy as usize, standby_proxy as usize);

            // Promote standby to active
            self.promote_standby();

            // Remove failed proxy from connections
            self.proxy_connections.remove(&failed_proxy);

            tracing::info!(
                "Recovery begun: failed proxy {} replaced by standby {}",
                failed_proxy,
                standby_proxy
            );

            // Start replay process for the replacement proxy
            self.start_replay_process(failed_proxy, standby_proxy);

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
        self.recovery_coordinator
            .get_next_replay_batch(failed_proxy as usize)
    }

    /// Start the replay process for a replacement proxy.
    /// This method spawns a task to send replay batches to the replacement proxy.
    fn start_replay_process(&self, failed_proxy: ProxyId, replacement_proxy: ProxyId) {
        let recovery_coordinator = self.recovery_coordinator.clone();
        let proxy_connections = self.proxy_connections.clone();
        let collector = self.collector.clone();

        tokio::spawn(async move {
            let mut batch_count = 0;
            loop {
                let next = recovery_coordinator.get_next_replay_batch(failed_proxy as usize);
                if next.is_none() {
                    let persist_index = recovery_coordinator.get_persist_index();
                    // Best-effort epoch/segment stats
                    tracing::info!(
                        failed_proxy,
                        persist_index,
                        "No replay batch available; replay loop exiting"
                    );
                    break;
                }
                let batch = next.unwrap();
                batch_count += 1;
                tracing::info!(
                    "Sending replay batch {} to replacement proxy {} ({} items)",
                    batch_count,
                    replacement_proxy,
                    batch.len()
                );

                // Convert LogRecord to ReplayMsg and send to replacement proxy
                if let Some(proxy_tx) = proxy_connections.get(&replacement_proxy) {
                    // Get the epoch from the first record before consuming the batch
                    let epoch = batch
                        .first()
                        .map(|record| record.epoch)
                        .unwrap_or(crate::checkpoint::EpochId(0));

                    let replay_items: Vec<crate::executor::api::ReplayMsg<E::Transaction>> = batch
                        .into_iter()
                        .map(|record| {
                            // Hydrate transaction data from LogRecord
                            let transaction = (*record.transaction).clone();

                            // Fetch state blobs from StateCollector
                            let mut state_blobs = std::collections::BTreeMap::new();
                            for (object_id, _version) in record.required_states.keys() {
                                if let Some(object) = collector.get_object(object_id) {
                                    state_blobs.insert(*object_id, object);
                                }
                            }

                            crate::executor::api::ReplayMsg {
                                consensus_index: record.consensus_index.unwrap_or(0),
                                transaction,
                                required_versions: record.required_states.keys().cloned().collect(),
                                state_blobs,
                            }
                        })
                        .collect();

                    let replay_batch = crate::executor::api::ReplayBatch {
                        epoch,
                        items: replay_items,
                    };

                    let msg = crate::executor::api::PrimaryToProxyMessage::Replay(replay_batch);
                    if let Err(e) = proxy_tx.value().send(msg).await {
                        tracing::error!(
                            "Failed to send replay batch {} to replacement proxy {}: {:?}",
                            batch_count,
                            replacement_proxy,
                            e
                        );
                        break;
                    }
                } else {
                    tracing::error!(
                        "Replacement proxy {} not found in connections",
                        replacement_proxy
                    );
                    break;
                }
            }

            tracing::info!(
                "Completed replay process for failed proxy {} -> replacement proxy {} ({} batches)",
                failed_proxy,
                replacement_proxy,
                batch_count
            );
        });
    }

    /// Mark an epoch as acknowledged and prune its log segment.
    pub fn acknowledge_epoch(&self, epoch: EpochId, consensus_index: u64) {
        self.epoch_ack_state.insert(epoch, true);
        self.epoch_logger.prune_epoch(epoch);
        self.collector.acknowledge_epoch(epoch);
        // Update primary persist index
        self.recovery_coordinator
            .update_persist_index(consensus_index);
        tracing::debug!(
            "Epoch {} acknowledged and pruned, persist index updated to {}",
            epoch.0,
            consensus_index
        );
    }

    /// Check for epoch completion and trigger acknowledgment if ready.
    pub fn check_epoch_completion(
        &self,
        epoch: EpochId,
        expected_proxies: usize,
        consensus_index: u64,
    ) {
        if self.collector.is_epoch_complete(epoch, expected_proxies) {
            self.acknowledge_epoch(epoch, consensus_index);
        }
    }

    /// Clear failed proxy and optionally flush queued transactions.
    pub async fn clear_failed_proxy(
        &mut self,
        flush: bool,
        shared_txn_sender: &Sender<(u64, RemoraTransaction<E>)>,
        consensus_index: u64,
    ) {
        if let Some(proxy) = self.failed_proxy.take() {
            if flush {
                if let Some(mut q) = self.grey_queue.remove(&proxy).map(|(_, v)| v) {
                    while let Some(tx) = q.pop_front() {
                        let _ = shared_txn_sender.send((consensus_index, tx)).await;
                    }
                }
            } else {
                self.grey_queue.remove(&proxy);
            }
        }
    }

    /// Determine if a transaction should be buffered due to grey state.
    fn should_buffer_grey(&self, tx: &RemoraTransaction<E>) -> bool {
        // If any required state maps to a currently unavailable proxy, buffer
        for (obj, maybe_ver) in tx.shared_objects().iter() {
            if let Some(ver) = maybe_ver {
                // persisted version in primary (from merged_state)
                let persisted_ver = self.collector.get_persisted_version(obj);
                if persisted_ver.map_or(true, |pv| pv < *ver) {
                    if let Some(owner) = self.states_to_proxy.get(&(*obj, *ver)) {
                        let owner_id = *owner.value();
                        if !self.proxy_connections.contains_key(&owner_id) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    // Grey unblocking readiness is determined externally when replacement is healthy.

    /// Unblock grey transactions for a proxy that has caught up.
    pub async fn unblock_grey_transactions(
        &mut self,
        proxy: ProxyId,
        shared_txn_sender: &Sender<(u64, RemoraTransaction<E>)>,
        consensus_index: u64,
    ) {
        if let Some(mut queue) = self.grey_queue.remove(&proxy).map(|(_, v)| v) {
            let queue_size = queue.len();
            tracing::info!(
                "Unblocking {} grey transactions for proxy {}",
                queue_size,
                proxy
            );

            // Update metrics - log unblocking activity
            tracing::info!(
                "Unblocked {} grey transactions, queue now empty",
                queue_size
            );

            while let Some(tx) = queue.pop_front() {
                if let Err(e) = shared_txn_sender.send((consensus_index, tx)).await {
                    tracing::error!("Failed to send unblocked transaction: {:?}", e);
                    break;
                }
            }
        }
    }

    /// Enqueue a transaction into the per-failed-proxy ordered grey queue.
    fn enqueue_grey(&self, tx: RemoraTransaction<E>) {
        if let Some(proxy) = self.failed_proxy {
            self.grey_queue
                .entry(proxy)
                .and_modify(|q| q.push_back(tx.clone()))
                .or_insert_with(|| {
                    let mut q = VecDeque::new();
                    q.push_back(tx.clone());
                    q
                });
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
            self.next_epoch_id,
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
                        for tx in shared_txns {
                            // Optionally buffer grey-state transactions
                            if self.should_buffer_grey(&tx) {
                                self.enqueue_grey(tx);
                                continue;
                            }
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
                        self.broadcast_checkpoint(epoch).await;
                        self.next_epoch_id += 1;
                    }
                }

                else => Err(NodeError::ShuttingDown)?,
            }
        }
    }

    async fn broadcast_checkpoint(&mut self, epoch: EpochId) {
        // Clone keys to avoid holding references while mutating map
        let proxy_ids: Vec<ProxyId> = self.proxy_connections.iter().map(|e| *e.key()).collect();
        for proxy_id in proxy_ids {
            let tx_opt = self
                .proxy_connections
                .get(&proxy_id)
                .map(|e| e.value().clone());
            if let Some(tx) = tx_opt {
                let msg = PrimaryToProxyMessage::Checkpoint(epoch);
                if let Err(_e) = tx.send(msg).await {
                    tracing::warn!("Proxy {} send failed; designating as failed", proxy_id);
                    // Designate failed: enable grey gating and begin recovery
                    self.set_failed_proxy(proxy_id);
                    if let Some(replacement) = self.begin_recovery(proxy_id) {
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
