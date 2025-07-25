use crate::{
    config::{LoadBalancingPolicy, ProxyMode},
    executor::{
        api::{
            ExecutableTransaction, Executor, ExecutorIndex, PrimaryToProxyMessage,
            RemoraTransaction, RequiredStates,
        },
        versioned_dependency_controller::VersionedDependencyController,
    },
    metrics::Metrics,
    proxy::core::ProxyId,
};
use dashmap::DashMap;
use rand::Rng;
use rustc_hash::FxHashMap;
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::SystemTime,
};
use sui_types::base_types::{ObjectID, SequenceNumber};
use tokio::sync::mpsc::{Receiver, Sender};

/// Encapsulates elastic scaling logic and state for RoundRobin policy
pub(crate) struct ElasticScaler {
    /// Number of active nodes (starts at 1, scales up based on load) - atomic for spawned task access
    active_nodes: Arc<AtomicUsize>,
    /// Last time scaling check was performed (milliseconds since epoch)
    last_scale_check: u64,
    /// Count of incoming transactions in current rate window
    incoming_rate_count: usize,
    /// Start time of current rate tracking window (milliseconds since epoch)
    rate_window_start: u64,
    /// Pre-calculated per-node capacity in transactions per second (calculated once)
    per_node_capacity_tps: Option<f64>,
}

impl ElasticScaler {
    /// Create a new elastic scaler starting with 1 active node
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            active_nodes: Arc::new(AtomicUsize::new(1)),
            last_scale_check: now,
            incoming_rate_count: 0,
            rate_window_start: now,
            per_node_capacity_tps: None,
        }
    }

    /// Record an incoming transaction for rate tracking
    pub fn record_transaction(&mut self) {
        self.incoming_rate_count += 1;
    }

    /// Calculate and store per-node capacity from transaction durations
    pub fn calculate_capacity(
        &mut self,
        _verification_duration: std::time::Duration,
        _expected_stateful_duration: std::time::Duration,
    ) {
        if self.per_node_capacity_tps.is_none() {
            // let total_service_time = verification_duration + expected_stateful_duration;
            // let cores_per_node = num_cpus::get() as f64;
            // let new_capacity = cores_per_node / total_service_time.as_secs_f64();

            // tracing::info!(
            //     "Calculated per-node capacity: {:.2} tps (cores: {}, service_time: {:?})",
            //     new_capacity,
            //     cores_per_node,
            //     total_service_time
            // );

            // HARDCODE core cap with 1ms workload
            self.per_node_capacity_tps = Some(27000.0);
        }
    }

    /// Check if scaling is needed and scale up if necessary
    pub fn check_and_scale(
        &mut self,
        total_available_nodes: usize,
        metrics: &Arc<crate::metrics::Metrics>,
    ) {
        const SCALE_CHECK_INTERVAL_MS: u64 = 500; // 500ms - very responsive
        const LOAD_THRESHOLD_MULTIPLIER: f64 = 0.8;
        const RATE_WINDOW_MS: u64 = 1000; // 1000ms (1 second) rate calculation window

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Check if it's time for a scaling check
        if now.saturating_sub(self.last_scale_check) < SCALE_CHECK_INTERVAL_MS {
            return;
        }

        // Update last check time
        self.last_scale_check = now;

        let current_active_nodes = self.active_nodes.load(Ordering::Relaxed);

        // Don't scale beyond available nodes
        if current_active_nodes >= total_available_nodes {
            return;
        }

        // Calculate current incoming rate
        let window_duration = now.saturating_sub(self.rate_window_start);

        // Reset window if it's been too long or calculate rate
        let incoming_rate = if window_duration >= RATE_WINDOW_MS {
            // Start new window
            self.rate_window_start = now;
            self.incoming_rate_count = 0;
            0.0
        } else if window_duration > 0 {
            // Convert to transactions per second: count / (ms / 1000)
            self.incoming_rate_count as f64 / (window_duration as f64 / 1000.0)
        } else {
            0.0
        };

        // Calculate current total capacity - get capacity or use default if not calculated yet
        let per_node_capacity = self.per_node_capacity_tps.unwrap();
        let total_current_capacity = per_node_capacity * current_active_nodes as f64;

        tracing::debug!(
            "Scaling check ({}ms intervals): incoming_rate={:.2} tps, current_capacity={:.2} tps, active_nodes={}/{}, window_duration={}ms",
            SCALE_CHECK_INTERVAL_MS,
            incoming_rate,
            total_current_capacity,
            current_active_nodes,
            total_available_nodes,
            window_duration
        );

        // Scale up if incoming load exceeds threshold of current capacity
        if incoming_rate > total_current_capacity * LOAD_THRESHOLD_MULTIPLIER {
            let new_active_nodes = (current_active_nodes + 1).min(total_available_nodes);
            self.active_nodes.store(new_active_nodes, Ordering::Relaxed);

            // Record the scaling decision
            metrics.record_scaling_decision("scale_up");
            metrics.update_active_nodes(new_active_nodes as u64);

            tracing::info!(
                "SCALING UP: Load spike detected! Incoming rate {:.2} tps exceeds {:.0}% of capacity {:.2} tps. Active nodes: {} -> {}",
                incoming_rate,
                LOAD_THRESHOLD_MULTIPLIER * 100.0,
                total_current_capacity,
                current_active_nodes,
                new_active_nodes
            );
        }
    }
}

pub(crate) struct VersionAssignmentTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    // Mapping of object ID to its current version for shared objects
    pub(crate) shared_object_versions: FxHashMap<ObjectID, SequenceNumber>,
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
            for mut transaction in transactions {
                let required_versions = self.assign_shared_object_versions(&mut transaction);

                tracing::debug!(
                    "Version assignment task received transaction {:?}",
                    transaction.digest()
                );

                sender.send((transaction, required_versions)).await.unwrap();
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
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) policy: LoadBalancingPolicy,
    pub(crate) txn_cnt: usize,
    pub(crate) states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
    pub(crate) dependency_controller: Arc<VersionedDependencyController>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) proxy_mode: ProxyMode,
    /// Elastic scaling (only used when policy is RoundRobin)
    pub(crate) elastic_scaler: ElasticScaler,
}

impl<E> SharedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) async fn process_shared_txns(
        &mut self,
        mut shared_txn_receiver: Receiver<(RemoraTransaction<E>, Vec<(ObjectID, SequenceNumber)>)>,
    ) {
        while let Some((transaction, required_versions)) = shared_txn_receiver.recv().await {
            self.forward_shared_object_txn(transaction, required_versions)
                .await;
        }
    }

    /// Forwards transactions with shared objects to the appropriate proxy.
    pub(crate) async fn forward_shared_object_txn(
        &mut self,
        transaction: RemoraTransaction<E>,
        required_versions: Vec<(ObjectID, SequenceNumber)>,
    ) {
        // Elasticity logic for RoundRobin policy
        if self.policy == LoadBalancingPolicy::RoundRobin {
            // Record incoming transaction for rate tracking
            self.elastic_scaler.record_transaction();
            self.metrics.record_incoming_transaction();

            // Calculate per-node capacity from first transaction (if not already calculated)
            self.elastic_scaler.calculate_capacity(
                transaction.verification_duration(),
                transaction.expected_stateful_duration(),
            );

            // Check if we need to scale
            self.elastic_scaler
                .check_and_scale(self.proxy_connections.len(), &self.metrics);
        }

        // Clone all needed fields to move into the spawned task
        let dependency_controller = self.dependency_controller.clone();
        let states_to_proxy = self.states_to_proxy.clone();
        let policy = self.policy.clone();
        let proxy_connections = self.proxy_connections.clone();
        let txn_cnt = self.txn_cnt;
        self.txn_cnt += 1;
        let proxy_mode = self.proxy_mode;
        let metrics = self.metrics.clone();
        let transaction_arc = Arc::new(transaction);

        // Clone active nodes count for spawned task (only field needed for routing)
        let active_nodes = self.elastic_scaler.active_nodes.clone();

        tokio::spawn(async move {
            let (prior_handles, current_handles) = match required_versions.is_empty() {
                true => (Vec::new(), Vec::new()),
                false => dependency_controller.get_prior_dependency_and_update(
                    0,
                    required_versions.clone(),
                    false,
                    false,
                ),
            };
            // Wait for prior dependencies to complete
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }

            // Remove the dependency when done
            dependency_controller.remove_dependency(required_versions.clone());

            // Get proxy assignment using elastic logic for RoundRobin
            let proxy_assignment = if policy == LoadBalancingPolicy::RoundRobin {
                // Elastic round-robin: only use active nodes
                let total_proxy_count = proxy_connections.len();
                let active_node_count = active_nodes.load(Ordering::Relaxed);

                if total_proxy_count == 0 || active_node_count == 0 {
                    None
                } else {
                    let effective_proxy_count = active_node_count.min(total_proxy_count);
                    let proxy_index = txn_cnt % effective_proxy_count;
                    Some((proxy_index, proxy_index))
                }
            } else {
                Self::get_proxy_for_shared_objects(
                    &policy,
                    &proxy_connections,
                    &states_to_proxy,
                    txn_cnt,
                    &required_versions,
                    &transaction_arc.destination,
                )
            };

            if let Some((proxy_index, stateless_proxy_id)) = proxy_assignment {
                let stateful_missing_states = Self::get_missing_states_for_transaction(
                    &transaction_arc,
                    Some(required_versions),
                    proxy_index,
                    states_to_proxy,
                )
                .await;

                if proxy_mode == ProxyMode::Separation {
                    let stateless_msg =
                        PrimaryToProxyMessage::StatelessTxn(Arc::clone(&transaction_arc));
                    Self::send_to_proxy(&proxy_connections, stateless_proxy_id, stateless_msg)
                        .await;

                    let stateful_msg = PrimaryToProxyMessage::Txn(
                        Arc::clone(&transaction_arc),
                        stateless_proxy_id,
                        stateful_missing_states,
                    );
                    Self::send_to_proxy(&proxy_connections, proxy_index, stateful_msg).await;
                } else {
                    let stateful_msg = PrimaryToProxyMessage::CombinedTxn(
                        Arc::clone(&transaction_arc),
                        stateless_proxy_id,
                        stateful_missing_states,
                    );
                    Self::send_to_proxy(&proxy_connections, proxy_index, stateful_msg).await;
                }

                metrics.update_metrics(transaction_arc.timestamp());
            } else {
                tracing::warn!("No proxies available for transaction with shared objects");
            }

            // Notify any dependencies waiting on this transaction
            for notify in current_handles {
                notify.notify_one();
            }
        });
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
                let previous_owner_value = if proxy_index == 2 && seq_num == SequenceNumber::from(2)
                {
                    // direct the object migration from other proxies.
                    // to emulate cold start.
                    Some(rand::thread_rng().gen_range(0..proxy_index))
                } else {
                    let previous_owner = states_to_proxy.get(&(object_id, seq_num));

                    if let Some(owner) = previous_owner {
                        if *owner != proxy_index {
                            Some(*owner)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
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
