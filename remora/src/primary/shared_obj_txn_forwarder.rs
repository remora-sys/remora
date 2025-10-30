use crate::{
    checkpoint::{state_collector::StateCollector, EpochId},
    config::{LoadBalancingPolicy, ProxyMode},
    executor::{
        api::{
            ExecutableTransaction, Executor, ExecutorIndex, PrimaryToProxyMessage,
            RemoraTransaction, RequiredStates,
        },
        versioned_dependency_controller::VersionedDependencyController,
        worker_pool::{WorkerPool, WorkerPoolConfig, WorkerTask},
    },
    metrics::Metrics,
    primary::load_balancer::PRIMARY_FETCH_SENTINEL,
    proxy::core::ProxyId,
    recovery::EpochLogger,
};
use dashmap::DashMap;
use rand::Rng;
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc, time::Duration};
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::object::Object;
use tokio::sync::mpsc::{Receiver, Sender};

/// Result of analyzing missing states for a transaction.
/// Contains both the required_states map (for inter-proxy requests) and
/// lazy-fetched state blobs (from primary's persisted snapshot).
struct MissingStatesResult {
    /// Map of (object, version) -> proxy index that owns it (for inter-proxy transfer)
    required_states: RequiredStates,
    /// State blobs lazy-fetched from primary (for states marked with PRIMARY_FETCH_SENTINEL)
    /// These are sent via Replay message and bypass inter-proxy transfer
    lazy_fetched_blobs: BTreeMap<ObjectID, Object>,
}

/// Context for forwarding tasks containing all necessary dependencies
#[derive(Clone)]
pub(crate) struct ForwardingContext<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub dependency_controller: Arc<VersionedDependencyController>,
    pub states_to_proxy:
        Arc<DashMap<(ObjectID, SequenceNumber), std::collections::HashSet<ExecutorIndex>>>,
    pub policy: LoadBalancingPolicy,
    pub proxy_connections: Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<E::Transaction>>>>,
    pub proxy_mode: ProxyMode,
    pub metrics: Arc<Metrics>,
    pub proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
    pub proxy_access_histories: Vec<Arc<DashMap<ObjectID, usize>>>,
    pub standby_excluded: Arc<AtomicBool>,
    pub collector: Arc<StateCollector>,
}

pub(crate) struct VersionAssignmentTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    // Mapping of object ID to its current version for shared objects
    pub(crate) shared_object_versions: FxHashMap<ObjectID, SequenceNumber>,
    // Epoch logger for sequential transaction recording
    pub(crate) epoch_logger: Option<Arc<EpochLogger<E::Transaction>>>,
    // PhantomData to indicate we're using the generic parameter
    pub(crate) _phantom: PhantomData<E>,
}

impl<E> VersionAssignmentTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) async fn process_version_assignments(
        &mut self,
        mut shared_txn_receiver: Receiver<(EpochId, RemoraTransaction<E>)>,
        sender: Sender<(
            EpochId,
            RemoraTransaction<E>,
            Vec<(ObjectID, SequenceNumber)>,
        )>,
    ) {
        while let Some((epoch_id, mut transaction)) = shared_txn_receiver.recv().await {
            let required_versions = self.assign_shared_object_versions(&mut transaction);

            tracing::debug!(
                "Version assignment task received transaction {:?}",
                transaction.digest()
            );

            // Record transaction sequentially in epoch logger (preserves epoch order)
            if let Some(logger) = &self.epoch_logger {
                // Build required_states map from required_versions
                let required_states: std::collections::BTreeMap<
                    (ObjectID, SequenceNumber),
                    Option<usize>,
                > = required_versions
                    .iter()
                    .map(|(obj_id, version)| ((*obj_id, *version), None))
                    .collect();

                let rec = crate::recovery::LogRecord {
                    txn_digest: *transaction.digest(),
                    transaction: Arc::new(transaction.clone()),
                    required_states,
                    epoch: epoch_id,
                };

                logger.append(epoch_id, rec);

                tracing::debug!(
                    epoch = epoch_id.0,
                    txn = ?transaction.digest(),
                    "Sequential epoch logger: appended transaction in epoch order"
                );
            }

            sender
                .send((epoch_id, transaction, required_versions))
                .await
                .unwrap();
        }
    }

    /// Assign versions to shared objects in a transaction
    ///
    /// 1. Get the shared object IDs from the transaction
    /// 2. Find the maximum version among all objects
    /// 3. Assign the next version (max + 1) to all objects
    /// 4. Return the list of (ObjectID, SequenceNumber) pairs
    pub(crate) fn assign_shared_object_versions(
        &mut self,
        transaction: &mut RemoraTransaction<E>,
    ) -> Vec<(ObjectID, SequenceNumber)> {
        // Get all shared object IDs from the transaction
        let shared_objects = transaction
            .shared_objects()
            .into_iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();

        if shared_objects.is_empty() {
            return Vec::new();
        }

        // Find the maximum version for all objects in the transaction
        let mut max_version = SequenceNumber::from(2);
        let initial_version = SequenceNumber::from(2);
        let mut result = Vec::with_capacity(shared_objects.len());

        // First collect current versions for result and find max
        for obj_id in shared_objects.iter() {
            let current_version = self
                .shared_object_versions
                .get(obj_id)
                .copied()
                .unwrap_or(initial_version);

            // Add current version to result
            result.push((*obj_id, current_version));

            // Update max version if needed
            if current_version > max_version {
                max_version = current_version;
            }
        }

        // Calculate the new version (max + 1)
        let new_version = max_version.next();

        // Update all objects to the new version
        for obj_id in shared_objects.iter() {
            self.shared_object_versions.insert(*obj_id, new_version);
        }

        // // Update the transaction's shared_objects field with current versions
        transaction.shared_objects = result
            .iter()
            .map(|(obj_id, version)| (*obj_id, Some(*version)))
            .collect();

        result
    }
}

/// Processor for transactions that involve shared objects.
/// Used only for load balancing policy selection.
pub(crate) struct SharedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) worker_pool: WorkerPool<ForwardingTask<E>>,
}

/// Task structure for worker threads
pub(crate) struct ForwardingTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub transaction: RemoraTransaction<E>,
    pub required_versions: Vec<(ObjectID, SequenceNumber)>,
    pub txn_cnt: usize,
    pub epoch_id: EpochId,
}

impl<E> WorkerTask for ForwardingTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    type Context = ForwardingContext<E>;

    async fn process(self, context: &Self::Context) {
        SharedObjTxnForwarder::<E>::process_forwarding_task(self, context).await;
    }
}

impl<E> SharedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    #[inline]
    fn resolve_proxy_id(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
        idx: ExecutorIndex,
    ) -> Option<ProxyId> {
        // This is the core fix: always resolve the index against the *current* set of active proxies.
        let mut keys: Vec<ProxyId> = proxy_connections.iter().map(|e| *e.key()).collect();
        keys.sort_unstable();

        if standby_excluded.load(Ordering::SeqCst) && keys.len() > 1 {
            keys.pop();
        }

        keys.get(idx).copied()
    }

    /// Create a new SharedObjTxnForwarder with worker pool
    pub(crate) fn new(
        dependency_controller: Arc<VersionedDependencyController>,
        states_to_proxy: Arc<
            DashMap<(ObjectID, SequenceNumber), std::collections::HashSet<ExecutorIndex>>,
        >,
        policy: LoadBalancingPolicy,
        proxy_connections: Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        proxy_mode: ProxyMode,
        metrics: Arc<Metrics>,
        proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: Vec<Arc<DashMap<ObjectID, usize>>>,
        standby_excluded: Arc<AtomicBool>,
        collector: Arc<StateCollector>,
    ) -> Self {
        let context = ForwardingContext {
            dependency_controller: dependency_controller.clone(),
            states_to_proxy: states_to_proxy.clone(),
            policy: policy.clone(),
            proxy_connections: proxy_connections.clone(),
            proxy_mode,
            metrics: metrics.clone(),
            proxy_loads: proxy_loads.clone(),
            proxy_access_histories: proxy_access_histories.clone(),
            standby_excluded,
            collector,
        };

        let config = WorkerPoolConfig {
            num_workers: Some(num_cpus::get() - 4),
            ..Default::default()
        };
        let worker_pool = WorkerPool::new(context, config);

        Self { worker_pool }
    }

    /// Process a single forwarding task
    async fn process_forwarding_task(task: ForwardingTask<E>, context: &ForwardingContext<E>) {
        let dependency_controller = &context.dependency_controller;
        let states_to_proxy = &context.states_to_proxy;
        let policy = &context.policy;
        let proxy_connections = &context.proxy_connections;
        let proxy_mode = context.proxy_mode.clone();
        let metrics = &context.metrics;
        let proxy_loads = &context.proxy_loads;
        let proxy_access_histories = &context.proxy_access_histories;
        let collector = &context.collector;
        let (prior_handles, current_handles) = match task.required_versions.is_empty() {
            true => (Vec::new(), Vec::new()),
            false => dependency_controller.get_prior_dependency_and_update(
                0,
                task.required_versions.clone(),
                false,
                false,
            ),
        };

        // Wait for prior dependencies to complete
        for prior_notify in prior_handles {
            prior_notify.notified().await;
        }

        // Remove the dependency when done
        dependency_controller.remove_dependency(task.required_versions.clone());

        let txn_duration = task.transaction.expected_stateful_duration()
            + task.transaction.verification_duration();
        let transaction_arc = Arc::new(task.transaction);

        if let Some((proxy_index, stateless_proxy_id)) = Self::get_proxy_for_shared_objects(
            policy,
            proxy_connections,
            &context.standby_excluded,
            states_to_proxy,
            task.txn_cnt,
            &task.required_versions,
            &transaction_arc.destination,
            proxy_loads,
            proxy_access_histories,
            &txn_duration,
        ) {
            // Resolve proxy ids to actual connection keys
            let resolved_stateful =
                Self::resolve_proxy_id(proxy_connections, &context.standby_excluded, proxy_index);
            let resolved_stateless = Self::resolve_proxy_id(
                proxy_connections,
                &context.standby_excluded,
                stateless_proxy_id,
            );

            // Note: Epoch logging now happens sequentially in VersionAssignmentTask
            // (preserves consensus order for correct recovery tracking)

            let missing_states_result = Self::get_missing_states_for_transaction(
                &transaction_arc,
                Some(task.required_versions),
                proxy_index,
                states_to_proxy.clone(),
                collector.clone(),
            )
            .await;

            // Send lazy-fetched state blobs via Replay message if any
            if !missing_states_result.lazy_fetched_blobs.is_empty() {
                if let Some(dest_pid) = resolved_stateful {
                    let replay_msg = crate::executor::api::ReplayMsg {
                        epoch_id: crate::checkpoint::EpochId(0),
                        transaction: None,
                        required_versions: vec![],
                        state_blobs: missing_states_result.lazy_fetched_blobs.clone(),
                    };
                    let replay_batch = crate::executor::api::ReplayBatch {
                        epoch: crate::checkpoint::EpochId(0),
                        items: vec![replay_msg],
                    };
                    let replay_message = PrimaryToProxyMessage::Replay(replay_batch);
                    Self::send_to_proxy(proxy_connections, dest_pid, replay_message).await;

                    tracing::info!(
                        "Sent {:?} lazy-fetched states to proxy {} for txn {:?}",
                        missing_states_result.lazy_fetched_blobs,
                        dest_pid,
                        transaction_arc.digest()
                    );
                }
            }

            // CRITICAL: Resolve positional indices to ProxyIds before sending to proxies
            // Proxies use these values to look up peer connections, so they must be actual ProxyIds
            let stateful_missing_states = Self::resolve_required_states_to_proxy_ids(
                missing_states_result.required_states,
                proxy_connections,
                &context.standby_excluded,
            );

            if proxy_mode == ProxyMode::Separation {
                let stateless_msg = PrimaryToProxyMessage::StatelessTxn(
                    task.epoch_id,
                    Arc::clone(&transaction_arc),
                );
                if let Some(stateless_pid) = resolved_stateless {
                    Self::send_to_proxy(proxy_connections, stateless_pid, stateless_msg).await;
                } else {
                    tracing::warn!(
                        idx = stateless_proxy_id,
                        "Stateless proxy index did not resolve to a live proxy"
                    );
                }

                let stateful_msg = PrimaryToProxyMessage::Txn(
                    task.epoch_id,
                    Arc::clone(&transaction_arc),
                    resolved_stateless.unwrap_or(usize::MAX),
                    stateful_missing_states,
                );
                if let Some(stateful_pid) = resolved_stateful {
                    Self::send_to_proxy(proxy_connections, stateful_pid, stateful_msg).await;
                } else {
                    tracing::warn!(
                        idx = proxy_index,
                        "Stateful proxy index did not resolve to a live proxy"
                    );
                }
            } else {
                let stateful_msg = PrimaryToProxyMessage::CombinedTxn(
                    task.epoch_id,
                    Arc::clone(&transaction_arc),
                    resolved_stateless.unwrap_or(usize::MAX),
                    stateful_missing_states.clone(),
                );
                tracing::info!(
                    "stateful message: {:?}, transaction_arc.digest: {:?}, destination: {:?}",
                    stateful_missing_states,
                    transaction_arc.digest(),
                    resolved_stateful
                );
                if let Some(stateful_pid) = resolved_stateful {
                    Self::send_to_proxy(proxy_connections, stateful_pid, stateful_msg).await;
                } else {
                    tracing::warn!(
                        idx = proxy_index,
                        "Combined proxy index did not resolve to a live proxy"
                    );
                }
            }

            metrics.update_metrics(transaction_arc.timestamp(), "primary-egress");
        } else {
            tracing::warn!("No proxies available for transaction with shared objects");
        }

        // Notify any dependencies waiting on this transaction
        for notify in current_handles {
            notify.notify_one();
        }
    }

    /// Convert required_states from positional indices to ProxyIds
    /// This is critical for proxies to correctly resolve peer proxy connections
    fn resolve_required_states_to_proxy_ids(
        required_states: BTreeMap<(ObjectID, SequenceNumber), Option<ExecutorIndex>>,
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
    ) -> BTreeMap<(ObjectID, SequenceNumber), Option<ProxyId>> {
        let mut resolved = BTreeMap::new();
        for ((obj_id, seq_num), maybe_idx) in required_states {
            let resolved_proxy_id = maybe_idx
                .and_then(|idx| Self::resolve_proxy_id(proxy_connections, standby_excluded, idx));
            resolved.insert((obj_id, seq_num), resolved_proxy_id);
        }
        resolved
    }

    /// Main processing method that distributes transactions to worker threads
    pub(crate) async fn process_shared_txns(
        &mut self,
        mut shared_txn_receiver: Receiver<(
            EpochId,
            RemoraTransaction<E>,
            Vec<(ObjectID, SequenceNumber)>,
        )>,
    ) {
        let mut txn_cnt = 0;
        while let Some((epoch_id, transaction, required_versions)) =
            shared_txn_receiver.recv().await
        {
            let task = ForwardingTask {
                transaction,
                required_versions,
                txn_cnt,
                epoch_id,
            };

            if let Err(e) = self.worker_pool.send_task(task).await {
                tracing::error!("Failed to send forwarding task to worker pool: {}", e);
            }

            txn_cnt += 1;
        }
    }

    /// Get assigned proxy for shared objects in a transaction.
    /// This is the main entry point for load balancing policy selection.
    fn get_proxy_for_shared_objects(
        policy: &LoadBalancingPolicy,
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
        states_to_proxy: &Arc<
            DashMap<(ObjectID, SequenceNumber), std::collections::HashSet<ExecutorIndex>>,
        >,
        txn_cnt: usize,
        required_versions: &[(ObjectID, SequenceNumber)],
        destination: &Option<ProxyId>,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
        txn_duration: &Duration,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        match policy {
            LoadBalancingPolicy::RoundRobin => Self::get_proxy_for_shared_objects_round_robin(
                proxy_connections,
                standby_excluded,
                txn_cnt,
            ),
            LoadBalancingPolicy::Zeus => Self::get_proxy_for_shared_objects_most_states(
                proxy_connections,
                standby_excluded,
                states_to_proxy,
                required_versions,
                txn_cnt,
            ),
            LoadBalancingPolicy::Random => {
                Self::get_proxy_for_shared_objects_random(proxy_connections, standby_excluded)
            }
            LoadBalancingPolicy::Hermes => {
                destination.map(|dest| (dest, dest)) // Note: Hermes still returns ProxyId, must be converted to index
            }
            LoadBalancingPolicy::LocalityLoad => Self::get_proxy_for_shared_objects_locality_load(
                proxy_connections,
                standby_excluded,
                states_to_proxy,
                required_versions,
                proxy_loads,
                txn_duration,
                txn_cnt,
            ),
            LoadBalancingPolicy::AffinityAware => {
                Self::get_proxy_for_shared_objects_affinity_aware(
                    proxy_connections,
                    standby_excluded,
                    states_to_proxy,
                    required_versions,
                    proxy_loads,
                    proxy_access_histories,
                    txn_duration,
                    txn_cnt,
                )
            }
        }
    }

    #[inline]
    fn effective_proxy_count(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
    ) -> usize {
        let count = proxy_connections.len();
        if standby_excluded.load(Ordering::SeqCst) && count > 1 {
            count - 1
        } else {
            count
        }
    }

    /// Get assigned proxy for shared objects using round-robin.
    fn get_proxy_for_shared_objects_round_robin(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = Self::effective_proxy_count(proxy_connections, standby_excluded);
        if proxy_count == 0 {
            return None;
        }

        let proxy_index = txn_cnt % proxy_count;

        Some((proxy_index, proxy_index))
    }

    fn get_proxy_for_shared_objects_random(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = Self::effective_proxy_count(proxy_connections, standby_excluded);
        if proxy_count == 0 {
            return None;
        }

        let proxy_index = rand::thread_rng().gen_range(0..proxy_count);
        Some((proxy_index, proxy_index))
    }

    /// Get assigned proxy based on which proxy hosts the most states needed by this transaction.
    fn get_proxy_for_shared_objects_most_states(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
        states_to_proxy: &Arc<
            DashMap<(ObjectID, SequenceNumber), std::collections::HashSet<ExecutorIndex>>,
        >,
        required_versions: &[(ObjectID, SequenceNumber)],
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = Self::effective_proxy_count(proxy_connections, standby_excluded);
        if proxy_count == 0 {
            return None;
        }

        if required_versions.is_empty() {
            // If no shared objects, use first proxy
            return Some((0, 0));
        }

        // Count how many objects each proxy already has
        let mut proxy_state_counts = vec![0; proxy_count];
        for (id, v) in required_versions {
            if let Some(owners) = states_to_proxy.get(&(*id, *v)) {
                // Increment count for all proxies that own this version
                for proxy_index in owners.iter() {
                    if *proxy_index < proxy_count {
                        proxy_state_counts[*proxy_index] += 1;
                    }
                }
            }
        }

        // Find the proxy with the most states
        let mut max_count = 0;
        let mut best_proxies = Vec::new();

        // First pass to find maximum count
        for (index, count) in proxy_state_counts.iter().enumerate() {
            match count.cmp(&max_count) {
                std::cmp::Ordering::Greater => {
                    max_count = *count;
                    best_proxies.clear();
                    best_proxies.push(index);
                }
                std::cmp::Ordering::Equal => {
                    best_proxies.push(index);
                }
                std::cmp::Ordering::Less => {}
            }
        }

        // Select a proxy randomly if multiple proxies have the same max count
        let proxy_index = if best_proxies.len() > 1 {
            best_proxies[txn_cnt % best_proxies.len()]
        } else {
            best_proxies[0]
        };
        Some((proxy_index, proxy_index))
    }

    fn get_proxy_for_shared_objects_locality_load(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
        states_to_proxy: &Arc<
            DashMap<(ObjectID, SequenceNumber), std::collections::HashSet<ExecutorIndex>>,
        >,
        required_versions: &[(ObjectID, SequenceNumber)],
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        txn_duration: &Duration,
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = Self::effective_proxy_count(proxy_connections, standby_excluded);
        if proxy_count == 0 {
            return None;
        }

        // Calculate locality based on current state ownership
        let mut locality_raw_counts = vec![0usize; proxy_count];
        for (id, v) in required_versions {
            if let Some(owners) = states_to_proxy.get(&(*id, *v)) {
                // Increment count for all proxies that own this version
                for proxy_index in owners.iter() {
                    if *proxy_index < proxy_count {
                        locality_raw_counts[*proxy_index] += 1;
                    }
                }
            }
        }

        Self::compute_locality_load_proxy(
            locality_raw_counts,
            required_versions,
            proxy_loads,
            None, // No access history updates for locality-based policy
            txn_duration,
            txn_cnt,
            "Locality",
        )
    }

    fn get_proxy_for_shared_objects_affinity_aware(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        standby_excluded: &Arc<AtomicBool>,
        _states_to_proxy: &Arc<
            DashMap<(ObjectID, SequenceNumber), std::collections::HashSet<ExecutorIndex>>,
        >,
        required_versions: &[(ObjectID, SequenceNumber)],
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
        txn_duration: &Duration,
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = Self::effective_proxy_count(proxy_connections, standby_excluded);
        if proxy_count == 0 {
            return None;
        }

        // Calculate affinity based on historical access patterns
        let mut locality_raw_counts = vec![0usize; proxy_count];
        for (id, _v) in required_versions {
            for proxy_index in 0..proxy_count {
                let access_count = proxy_access_histories[proxy_index]
                    .get(id)
                    .map_or(0, |r| *r.value());
                locality_raw_counts[proxy_index] += access_count;
            }
        }

        Self::compute_locality_load_proxy(
            locality_raw_counts,
            required_versions,
            proxy_loads,
            Some(proxy_access_histories), // Update access history for affinity-aware policy
            txn_duration,
            txn_cnt,
            "Affinity",
        )
    }

    /// Common logic for locality/affinity-based load balancing policies
    fn compute_locality_load_proxy(
        locality_raw_counts: Vec<usize>,
        required_versions: &[(ObjectID, SequenceNumber)],
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: Option<&Vec<Arc<DashMap<ObjectID, usize>>>>,
        txn_duration: &Duration,
        txn_cnt: usize,
        policy_name: &str,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = locality_raw_counts.len();
        if proxy_count == 0 {
            return None;
        }

        let total_required_versions = required_versions.len() as f64;
        let mut locality_scores: Vec<f64> = locality_raw_counts
            .iter()
            .map(|&count| {
                if total_required_versions == 0.0 {
                    0.0 // Avoid division by zero if there are no required versions
                } else {
                    count as f64 / total_required_versions
                }
            })
            .collect();

        tracing::debug!(
            "{} scores before normalization: {:?}",
            policy_name,
            locality_scores
        );

        const SCORE_SUM_EPSILON: f64 = 0.0; // Epsilon for sum and score comparisons
        let sum_of_locality_scores: f64 = locality_scores.iter().sum();

        if sum_of_locality_scores > SCORE_SUM_EPSILON {
            // Normalize locality_scores so they sum to 1.0 if the sum is meaningfully positive.
            locality_scores = locality_scores
                .iter()
                .map(|&score| score / sum_of_locality_scores)
                .collect();
            tracing::debug!(
                "{} scores after normalization: {:?}",
                policy_name,
                locality_scores
            );
        }

        let mut current_loads = vec![0usize; proxy_count];
        for i in 0..proxy_count {
            current_loads[i] = proxy_loads.get(&i).map_or(0, |r| *r.value());
        }

        let total_load: usize = current_loads.iter().sum();

        let load_scores: Vec<f64> = current_loads
            .iter()
            .map(|&load| {
                if total_load == 0 {
                    1.0
                } else {
                    1.0 - (load as f64 / total_load as f64)
                }
            })
            .collect();

        tracing::debug!(
            "{}_scores: {:?}",
            policy_name.to_lowercase(),
            locality_scores
        );
        tracing::debug!("proxy_loads: {:?}", proxy_loads);
        tracing::debug!("load_scores: {:?}", load_scores);

        let combined_scores: Vec<f64> = locality_scores
            .iter()
            .zip(load_scores.iter())
            .map(|(&loc_score, &ld_score)| 0.5 * loc_score + 0.5 * ld_score)
            .collect();

        let mut best_score = -1.0_f64;
        let mut best_proxies = Vec::new();

        for i in 0..proxy_count {
            if combined_scores[i] > best_score {
                best_score = combined_scores[i];
                best_proxies.clear();
                best_proxies.push(i);
            } else if (combined_scores[i] - best_score).abs() < SCORE_SUM_EPSILON {
                best_proxies.push(i);
            }
        }

        let proxy_index = if best_proxies.is_empty() {
            txn_cnt % proxy_count
        } else {
            best_proxies[txn_cnt % best_proxies.len()]
        };

        // Update stateful proxy load
        let load_inc = txn_duration.as_micros() as usize;
        if let Some(mut load) = proxy_loads.get_mut(&proxy_index) {
            *load += load_inc;
        } else {
            proxy_loads.insert(proxy_index, load_inc);
        }

        // Update access history if provided (for affinity-aware policy)
        if let Some(histories) = proxy_access_histories {
            for (obj_id, _v) in required_versions {
                histories[proxy_index]
                    .entry(*obj_id)
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }

        Some((proxy_index, proxy_index))
    }

    async fn get_missing_states_for_transaction(
        transaction: &RemoraTransaction<E>,
        required_versions: Option<Vec<(ObjectID, SequenceNumber)>>,
        proxy_index: ExecutorIndex,
        states_to_proxy: Arc<
            DashMap<(ObjectID, SequenceNumber), std::collections::HashSet<ExecutorIndex>>,
        >,
        collector: Arc<StateCollector>,
    ) -> MissingStatesResult {
        let mut required_states = BTreeMap::new();
        let mut lazy_fetched_blobs = BTreeMap::new();

        let Some(required_versions) = required_versions else {
            return MissingStatesResult {
                required_states,
                lazy_fetched_blobs,
            };
        };

        let max_version = required_versions
            .iter()
            .map(|(_, v)| v)
            .max()
            .copied()
            .unwrap_or(SequenceNumber::from(2));
        let next_version = max_version.next();

        for (object_id, seq_num) in required_versions {
            let owner = states_to_proxy
                .get(&(object_id, seq_num))
                .and_then(|owners| {
                    if owners.contains(&proxy_index) {
                        None
                    } else {
                        owners.iter().next().copied()
                    }
                });

            match owner {
                Some(PRIMARY_FETCH_SENTINEL) => {
                    if let Some(blob) = collector.get_object(&object_id) {
                        lazy_fetched_blobs.insert(object_id, blob);
                    } else {
                        tracing::warn!(
                            "Lazy fetch failed for {:?} v{} in txn {:?}",
                            object_id,
                            seq_num.value(),
                            transaction.digest()
                        );
                    }
                    required_states.insert((object_id, seq_num), None);
                }
                owner_opt => {
                    required_states.insert((object_id, seq_num), owner_opt);
                }
            }

            states_to_proxy
                .entry((object_id, next_version))
                .or_insert_with(std::collections::HashSet::new)
                .insert(proxy_index);

            states_to_proxy
                .entry((object_id, seq_num))
                .and_modify(|owners| {
                    owners.remove(&proxy_index);
                });
        }

        MissingStatesResult {
            required_states,
            lazy_fetched_blobs,
        }
    }

    /// Simplified method to send a message to a proxy
    async fn send_to_proxy(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        dest_proxy: ProxyId,
        message: PrimaryToProxyMessage<<E as Executor>::Transaction>,
    ) {
        if let Some(proxy_connection) = proxy_connections.get(&dest_proxy) {
            if proxy_connection.send(message).await.is_ok() {
                tracing::debug!("Sent transaction to proxy {}", dest_proxy);
            } else {
                tracing::debug!(
                    "Failed to send to proxy {}. Not removing here; load balancer will handle recovery.",
                    dest_proxy
                );
                // Do not remove here to avoid races with load balancer coordinated recovery.
                // Load balancer will handle designation and removal during checkpoint/recovery.
            }
        } else {
            tracing::debug!("Proxy connection {} not found", dest_proxy);
        }
    }
}
