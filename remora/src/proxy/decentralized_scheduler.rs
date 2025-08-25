// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    config::{LoadBalancingPolicy, ProxyMode},
    executor::api::{Executor, ExecutorIndex, RequiredStates, TransactionWithTimestamp},
    executor::versioned_dependency_controller::VersionedDependencyController,
    metrics::Metrics,
    proxy::core::{ProxyId, ScheduledTransaction},
};
use dashmap::DashMap;
use rand::{Rng, SeedableRng};
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc, time::Duration};
use sui_types::base_types::{ObjectID, SequenceNumber};

/// Decentralized scheduler that runs on each proxy node
/// Each proxy receives the same transaction batch and makes deterministic scheduling decisions
pub struct DecentralizedScheduler<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    /// This proxy's ID
    pub proxy_id: ProxyId,
    /// Total number of proxies in the system
    pub proxy_count: usize,
    /// Load balancing policy
    pub policy: LoadBalancingPolicy,
    /// Proxy mode (separation vs no separation)
    pub proxy_mode: ProxyMode,
    /// Dependency controller
    pub dependency_controller: Arc<VersionedDependencyController>,
    /// Mapping of (ObjectID, SequenceNumber) to which proxy owns that state
    pub states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
    /// Current load on each proxy
    pub proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
    /// Access history for affinity-aware scheduling
    pub proxy_access_histories: Vec<Arc<DashMap<ObjectID, usize>>>,
    /// Metrics
    pub metrics: Arc<Metrics>,
    pub txn_cnt: usize,
    /// PhantomData for the executor type
    pub _phantom: PhantomData<E>,
}

impl<E> DecentralizedScheduler<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub fn new(
        proxy_id: ProxyId,
        proxy_count: usize,
        policy: LoadBalancingPolicy,
        proxy_mode: ProxyMode,
        metrics: Arc<Metrics>,
    ) -> Self {
        // Initialize access histories for each proxy
        let proxy_access_histories = (0..proxy_count).map(|_| Arc::new(DashMap::new())).collect();

        Self {
            proxy_id,
            proxy_count,
            policy,
            proxy_mode,
            dependency_controller: Arc::new(VersionedDependencyController::new()),
            states_to_proxy: Arc::new(DashMap::new()),
            proxy_loads: Arc::new(DashMap::new()),
            proxy_access_histories,
            metrics,
            txn_cnt: 0,
            _phantom: PhantomData,
        }
    }

    /// Process a single transaction and return scheduling decision
    /// This is the new pipeline-friendly method
    pub async fn process_single_transaction(
        policy: &LoadBalancingPolicy,
        proxy_count: usize,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: Vec<(ObjectID, SequenceNumber)>,
        transaction: TransactionWithTimestamp<E::Transaction>,
        txn_cnt: usize,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
    ) -> Option<ScheduledTransaction<E>> {
        // Make scheduling decision using the same logic as centralized approach
        let selected_proxy = Self::get_proxy_for_shared_objects(
            policy,
            proxy_count,
            states_to_proxy,
            proxy_access_histories,
            proxy_loads,
            &required_versions,
            txn_cnt,
            &transaction,
        );

        // Update states_to_proxy mapping (all proxies do this for consistency)
        let missing_states = Self::update_state_mapping(
            policy,
            states_to_proxy,
            required_versions.clone(),
            selected_proxy,
            &transaction,
            proxy_loads,
            proxy_access_histories,
        )
        .await;

        // Return the scheduling decision directly as ScheduledTransaction
        Some(ScheduledTransaction {
            transaction,
            required_versions,
            missing_states,
            assigned_proxy: selected_proxy,
        })
    }

    /// Run the scheduler as a continuous task processing individual transactions
    pub async fn run_scheduler_task(
        &mut self,
        mut rx_from_version_assignment: tokio::sync::mpsc::Receiver<
            TransactionWithTimestamp<E::Transaction>,
        >,
        tx_to_primary_processor: tokio::sync::mpsc::Sender<ScheduledTransaction<E>>,
    ) {
        let mut txn_cnt = 0;

        while let Some(transaction) = rx_from_version_assignment.recv().await {
            let dependency_controller = self.dependency_controller.clone();
            let proxy_count = self.proxy_count;
            let states_to_proxy = self.states_to_proxy.clone();
            let proxy_access_histories = self.proxy_access_histories.clone();
            let proxy_loads = self.proxy_loads.clone();
            let policy = self.policy.clone();
            let tx_to_primary_processor = tx_to_primary_processor.clone();

            let required_versions: Vec<(ObjectID, SequenceNumber)> = transaction
                .shared_objects()
                .into_iter()
                .filter_map(|(id, version_opt)| version_opt.map(|v| (*id, v)))
                .collect();

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

                if let Some(scheduled_txn) = Self::process_single_transaction(
                    &policy,
                    proxy_count,
                    &states_to_proxy,
                    required_versions,
                    transaction,
                    txn_cnt,
                    &proxy_access_histories,
                    &proxy_loads,
                )
                .await
                {
                    if let Err(e) = tx_to_primary_processor.send(scheduled_txn).await {
                        tracing::error!(
                            "Failed to send scheduled transaction to primary processor: {}",
                            e
                        );
                    }
                }
                // Notify any dependencies waiting on this transaction
                for notify in current_handles {
                    notify.notify_one();
                }
            });

            txn_cnt += 1;
        }

        tracing::info!("Scheduler task shutting down");
    }

    /// Make proxy selection decision (reused from centralized logic)
    fn get_proxy_for_shared_objects(
        policy: &LoadBalancingPolicy,
        proxy_count: usize,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        required_versions: &[(ObjectID, SequenceNumber)],
        txn_cnt: usize,
        transaction: &TransactionWithTimestamp<E::Transaction>,
    ) -> ExecutorIndex {
        let txn_duration =
            transaction.expected_stateful_duration() + transaction.verification_duration();

        let result = match policy {
            LoadBalancingPolicy::RoundRobin => Self::get_proxy_round_robin(proxy_count, txn_cnt),
            LoadBalancingPolicy::Zeus => Self::get_proxy_most_states(
                proxy_count,
                states_to_proxy,
                required_versions,
                txn_cnt,
            ),
            LoadBalancingPolicy::Random => {
                // For determinism, use txn_cnt as seed
                let mut rng = rand::rngs::StdRng::seed_from_u64(txn_cnt as u64);
                rng.gen_range(0..proxy_count)
            }
            LoadBalancingPolicy::Hermes => transaction.destination.unwrap_or(0),
            LoadBalancingPolicy::LocalityLoad => Self::get_proxy_locality_load(
                proxy_count,
                states_to_proxy,
                required_versions,
                &txn_duration,
                txn_cnt,
                proxy_loads,
            ),
            LoadBalancingPolicy::AffinityAware => Self::get_proxy_affinity_aware(
                proxy_count,
                proxy_loads,
                proxy_access_histories,
                required_versions,
                &txn_duration,
                txn_cnt,
            ),
        };

        result
    }

    // Reuse the scheduling algorithms from centralized approach
    fn get_proxy_round_robin(proxy_count: usize, txn_cnt: usize) -> ExecutorIndex {
        txn_cnt % proxy_count
    }

    fn get_proxy_most_states(
        proxy_count: usize,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: &[(ObjectID, SequenceNumber)],
        txn_cnt: usize,
    ) -> ExecutorIndex {
        if required_versions.is_empty() {
            return 0;
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

        // Select deterministically
        if best_proxies.len() > 1 {
            best_proxies[txn_cnt % best_proxies.len()]
        } else {
            best_proxies[0]
        }
    }

    fn get_proxy_locality_load(
        proxy_count: usize,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: &[(ObjectID, SequenceNumber)],
        txn_duration: &Duration,
        txn_cnt: usize,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
    ) -> ExecutorIndex {
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
            proxy_count,
            proxy_loads,
            locality_raw_counts,
            required_versions,
            None,
            txn_duration,
            txn_cnt,
            "Locality",
        )
    }

    fn get_proxy_affinity_aware(
        proxy_count: usize,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
        required_versions: &[(ObjectID, SequenceNumber)],
        txn_duration: &Duration,
        txn_cnt: usize,
    ) -> ExecutorIndex {
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
            proxy_count,
            proxy_loads,
            locality_raw_counts,
            required_versions,
            Some(&proxy_access_histories),
            txn_duration,
            txn_cnt,
            "Affinity",
        )
    }

    /// Common logic for locality/affinity-based load balancing (reused from centralized)
    fn compute_locality_load_proxy(
        proxy_count: usize,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        locality_raw_counts: Vec<usize>,
        required_versions: &[(ObjectID, SequenceNumber)],
        _proxy_access_histories: Option<&Vec<Arc<DashMap<ObjectID, usize>>>>,
        _txn_duration: &Duration,
        txn_cnt: usize,
        _policy_name: &str,
    ) -> ExecutorIndex {
        let total_required_versions = required_versions.len() as f64;
        let mut locality_scores: Vec<f64> = locality_raw_counts
            .iter()
            .map(|&count| {
                if total_required_versions == 0.0 {
                    0.0
                } else {
                    count as f64 / total_required_versions
                }
            })
            .collect();

        const SCORE_SUM_EPSILON: f64 = 0.0;
        let sum_of_locality_scores: f64 = locality_scores.iter().sum();

        if sum_of_locality_scores > SCORE_SUM_EPSILON {
            locality_scores = locality_scores
                .iter()
                .map(|&score| score / sum_of_locality_scores)
                .collect();
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

        proxy_index
    }

    /// Update state mapping and return missing states
    async fn update_state_mapping(
        policy: &LoadBalancingPolicy,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: Vec<(ObjectID, SequenceNumber)>,
        selected_proxy: ExecutorIndex,
        transaction: &TransactionWithTimestamp<E::Transaction>,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        proxy_access_histories: &Vec<Arc<DashMap<ObjectID, usize>>>,
    ) -> RequiredStates {
        let mut required_states = BTreeMap::new();

        if required_versions.is_empty() {
            return required_states;
        }

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
                if *owner != selected_proxy {
                    Some(*owner)
                } else {
                    None
                }
            } else {
                None
            };

            required_states.insert((object_id, seq_num), previous_owner_value);

            // Update the mapping (all proxies do this to stay consistent)
            states_to_proxy.insert((object_id, next_version), selected_proxy);
            states_to_proxy.remove(&(object_id, seq_num));
        }

        // Update load tracking
        let load_inc = (transaction.expected_stateful_duration()
            + transaction.verification_duration())
        .as_micros() as usize;
        if let Some(mut load) = proxy_loads.get_mut(&selected_proxy) {
            *load += load_inc;
        } else {
            proxy_loads.insert(selected_proxy, load_inc);
        }

        // Update access history if needed
        if matches!(policy, LoadBalancingPolicy::AffinityAware) {
            for (obj_id, _v) in &required_states {
                proxy_access_histories[selected_proxy]
                    .entry(obj_id.0)
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }

        required_states
    }
}
