use crate::{
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
    primary::batch_breakdown::BatchBreakdownCollector,
    proxy::core::ProxyId,
};
use dashmap::DashMap;
use rand::Rng;
use rustc_hash::FxHashMap;
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc, time::Duration};
use sui_types::base_types::{ObjectID, SequenceNumber};
use tokio::sync::mpsc::{Receiver, Sender};

/// Context for forwarding tasks containing all necessary dependencies
#[derive(Clone)]
pub(crate) struct ForwardingContext<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub dependency_controller: Arc<VersionedDependencyController>,
    pub states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
    pub policy: LoadBalancingPolicy,
    pub proxy_connections: Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<E::Transaction>>>>,
    pub proxy_mode: ProxyMode,
    pub metrics: Arc<Metrics>,
    pub batch_breakdown: Arc<BatchBreakdownCollector>,
    pub proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
    pub proxy_access_histories: Vec<Arc<DashMap<ObjectID, usize>>>,
}

pub(crate) struct VersionAssignmentTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    // Mapping of object ID to its current version for shared objects
    pub(crate) shared_object_versions: FxHashMap<ObjectID, SequenceNumber>,
    pub(crate) batch_breakdown: Arc<BatchBreakdownCollector>,
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
        mut shared_txn_receiver: Receiver<Vec<RemoraTransaction<E>>>,
        sender: Sender<(RemoraTransaction<E>, Vec<(ObjectID, SequenceNumber)>)>,
    ) {
        while let Some(transactions) = shared_txn_receiver.recv().await {
            let batch_id = self.batch_breakdown.batch_id_for_messages(&transactions);
            let start = std::time::Instant::now();
            for mut transaction in transactions {
                let required_versions = self.assign_shared_object_versions(&mut transaction);

                tracing::debug!(
                    "Version assignment task received transaction {:?}",
                    transaction.digest()
                );

                sender.send((transaction, required_versions)).await.unwrap();
            }
            if let Some(batch_id) = batch_id {
                self.batch_breakdown
                    .record_version_assignment(batch_id, start, start.elapsed());
            }
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
            .keys()
            .copied()
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
    pub enqueue_time: std::time::Instant,
}

impl<E> WorkerTask for ForwardingTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    type Context = ForwardingContext<E>;

    async fn process(self, context: &Self::Context) {
        SharedObjTxnForwarder::<E>::process_forwarding_task(
            self,
            &context.dependency_controller,
            &context.states_to_proxy,
            &context.policy,
            &context.proxy_connections,
            context.proxy_mode,
            &context.metrics,
            &context.batch_breakdown,
            &context.proxy_loads,
            &context.proxy_access_histories,
        )
        .await;
    }
}

impl<E> SharedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    /// Create a new SharedObjTxnForwarder with worker pool
    pub(crate) fn new(
        dependency_controller: Arc<VersionedDependencyController>,
        states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        policy: LoadBalancingPolicy,
        proxy_connections: Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        proxy_mode: ProxyMode,
        metrics: Arc<Metrics>,
        batch_breakdown: Arc<BatchBreakdownCollector>,
        proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: Vec<Arc<DashMap<ObjectID, usize>>>,
    ) -> Self {
        let context = ForwardingContext {
            dependency_controller: dependency_controller.clone(),
            states_to_proxy: states_to_proxy.clone(),
            policy: policy.clone(),
            proxy_connections: proxy_connections.clone(),
            proxy_mode,
            metrics: metrics.clone(),
            batch_breakdown,
            proxy_loads: proxy_loads.clone(),
            proxy_access_histories: proxy_access_histories.clone(),
        };

        let config = WorkerPoolConfig::default();
        let worker_pool = WorkerPool::new(context, config);

        Self { worker_pool }
    }

    /// Process a single forwarding task
    async fn process_forwarding_task(
        task: ForwardingTask<E>,
        dependency_controller: &Arc<VersionedDependencyController>,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        policy: &LoadBalancingPolicy,
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        proxy_mode: ProxyMode,
        metrics: &Arc<Metrics>,
        batch_breakdown: &Arc<BatchBreakdownCollector>,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
    ) {
        let dispatch_start = task.enqueue_time;
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
            states_to_proxy,
            task.txn_cnt,
            &task.required_versions,
            &transaction_arc.destination,
            proxy_loads,
            proxy_access_histories,
            &txn_duration,
        ) {
            let stateful_missing_states = Self::get_missing_states_for_transaction(
                &transaction_arc,
                Some(task.required_versions),
                proxy_index,
                states_to_proxy.clone(),
            )
            .await;

            if proxy_mode == ProxyMode::Separation {
                let stateless_msg =
                    PrimaryToProxyMessage::StatelessTxn(Arc::clone(&transaction_arc));
                Self::send_to_proxy(proxy_connections, stateless_proxy_id, stateless_msg).await;

                let stateful_msg = PrimaryToProxyMessage::Txn(
                    Arc::clone(&transaction_arc),
                    stateless_proxy_id,
                    stateful_missing_states,
                );
                Self::send_to_proxy(proxy_connections, proxy_index, stateful_msg).await;
            } else {
                let stateful_msg = PrimaryToProxyMessage::CombinedTxn(
                    Arc::clone(&transaction_arc),
                    stateless_proxy_id,
                    stateful_missing_states,
                );
                Self::send_to_proxy(proxy_connections, proxy_index, stateful_msg).await;
            }

            metrics.update_metrics(transaction_arc.timestamp(), "primary-egress");
            batch_breakdown.record_dispatch_forwarding(
                *transaction_arc.digest(),
                dispatch_start,
                dispatch_start.elapsed(),
            );
        } else {
            tracing::warn!("No proxies available for transaction with shared objects");
        }

        // Notify any dependencies waiting on this transaction
        for notify in current_handles {
            notify.notify_one();
        }
    }

    /// Main processing method that distributes transactions to worker threads
    pub(crate) async fn process_shared_txns(
        &mut self,
        mut shared_txn_receiver: Receiver<(RemoraTransaction<E>, Vec<(ObjectID, SequenceNumber)>)>,
    ) {
        let mut txn_cnt = 0;
        while let Some((transaction, required_versions)) = shared_txn_receiver.recv().await {
            let task = ForwardingTask {
                transaction,
                required_versions,
                txn_cnt,
                enqueue_time: std::time::Instant::now(),
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
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        txn_cnt: usize,
        required_versions: &[(ObjectID, SequenceNumber)],
        destination: &Option<ProxyId>,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
        txn_duration: &Duration,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        match policy {
            LoadBalancingPolicy::RoundRobin => {
                Self::get_proxy_for_shared_objects_round_robin(proxy_connections, txn_cnt)
            }
            LoadBalancingPolicy::Zeus => Self::get_proxy_for_shared_objects_most_states(
                proxy_connections,
                states_to_proxy,
                required_versions,
                txn_cnt,
            ),
            LoadBalancingPolicy::Random => {
                Self::get_proxy_for_shared_objects_random(proxy_connections)
            }
            LoadBalancingPolicy::Hermes => destination.map(|dest| (dest, dest)),
            LoadBalancingPolicy::LocalityLoad => Self::get_proxy_for_shared_objects_locality_load(
                proxy_connections,
                states_to_proxy,
                required_versions,
                proxy_loads,
                txn_duration,
                txn_cnt,
            ),
            LoadBalancingPolicy::AffinityAware => {
                Self::get_proxy_for_shared_objects_affinity_aware(
                    proxy_connections,
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

    /// Get assigned proxy for shared objects using round-robin.
    fn get_proxy_for_shared_objects_round_robin(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = proxy_connections.len();
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
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = proxy_connections.len();
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
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: &[(ObjectID, SequenceNumber)],
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = proxy_connections.len();
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
            if let Some(proxy_index) = states_to_proxy.get(&(*id, *v)) {
                if *proxy_index < proxy_count {
                    proxy_state_counts[*proxy_index] += 1;
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
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: &[(ObjectID, SequenceNumber)],
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        txn_duration: &Duration,
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        // Calculate locality based on current state ownership
        let mut locality_raw_counts = vec![0usize; proxy_count];
        for (id, v) in required_versions {
            if let Some(proxy_index_ref) = states_to_proxy.get(&(*id, *v)) {
                let proxy_index = *proxy_index_ref;
                if proxy_index < proxy_count {
                    locality_raw_counts[proxy_index] += 1;
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
        _states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: &[(ObjectID, SequenceNumber)],
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
        txn_duration: &Duration,
        txn_cnt: usize,
    ) -> Option<(ExecutorIndex, ExecutorIndex)> {
        let proxy_count = proxy_connections.len();
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

    /// Helper method to determine missing states for a transaction
    /// and update the states ownership map
    async fn get_missing_states_for_transaction(
        transaction: &RemoraTransaction<E>,
        required_versions: Option<Vec<(ObjectID, SequenceNumber)>>,
        proxy_index: ExecutorIndex,
        states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
    ) -> RequiredStates {
        let mut required_states = BTreeMap::new();

        tracing::debug!(
            "Transaction {:?} required versions: {:?}",
            transaction.digest(),
            required_versions
        );

        if let Some(required_versions) = required_versions {
            let mut max_version = SequenceNumber::from(2);
            for (_, seq_num) in &required_versions {
                if *seq_num > max_version {
                    max_version = *seq_num;
                }
            }

            // Calculate the new version (max + 1) for mapping updates
            let next_version = max_version.next();

            for (object_id, seq_num) in required_versions {
                let previous_owner = states_to_proxy.get(&(object_id, seq_num));

                let previous_owner_value = if let Some(owner) = previous_owner {
                    if *owner != proxy_index {
                        Some(*owner)
                    } else {
                        None
                    }
                } else {
                    None
                };

                required_states.insert((object_id, seq_num), previous_owner_value);

                // Update the mapping
                states_to_proxy.insert((object_id, next_version), proxy_index);
                states_to_proxy.remove(&(object_id, seq_num));
            }
        }

        tracing::debug!(
            "Transaction {:?} missing states: {:?}",
            transaction.digest(),
            required_states
        );

        required_states
    }

    /// Simplified method to send a message to a proxy
    async fn send_to_proxy(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        dest_proxy: ExecutorIndex,
        message: PrimaryToProxyMessage<<E as Executor>::Transaction>,
    ) {
        if let Some(proxy_connection) = proxy_connections.get(&dest_proxy) {
            if proxy_connection.send(message).await.is_ok() {
                tracing::debug!("Sent transaction to proxy {}", dest_proxy);
            } else {
                tracing::warn!(
                    "Failed to send transaction to proxy {}, removing connection",
                    dest_proxy
                );
            }
        } else {
            tracing::warn!("Proxy connection {} not found", dest_proxy);
        }
    }
}
