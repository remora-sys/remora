use crate::{
    config::SeparationMode,
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
use petgraph::graph::DiGraph;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc, time::Duration};
use sui_types::base_types::{ObjectID, SequenceNumber, TransactionDigest};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum PreConsensusSchedulingPolicy {
    /// Sorted Dependency Set (locality-first for small subgraphs)
    LSDS,
    /// Sorted Dependency Set (round robin for small subgraphs)
    RSDS,
}

pub(crate) struct PreConsensusSchedTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) pre_consensus_routing_plan: Arc<DashMap<TransactionDigest, ProxyId>>,
    pub(crate) _phantom: PhantomData<E>,
    pub(crate) proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
    pub(crate) object_last_proxy: Vec<Option<ExecutorIndex>>,
}

impl<E> PreConsensusSchedTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    #[allow(dead_code)]
    pub(crate) fn new(
        proxy_connections: Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        pre_consensus_routing_plan: Arc<DashMap<TransactionDigest, ProxyId>>,
        proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
    ) -> Self {
        Self {
            proxy_connections,
            pre_consensus_routing_plan,
            _phantom: PhantomData,
            proxy_loads,
            object_last_proxy: vec![None; 10000000],
        }
    }

    pub(crate) async fn process_pre_consensus_txns(
        &mut self,
        mut rx_pre_consensus: Receiver<Vec<RemoraTransaction<E>>>,
    ) {
        while let Some(transactions) = rx_pre_consensus.recv().await {
            self.schedule_transaction_batch(transactions);
        }
    }

    fn schedule_transaction_batch(&mut self, transactions: Vec<RemoraTransaction<E>>) {
        let num_proxies = self.proxy_connections.len();
        if num_proxies == 0 {
            tracing::warn!("No proxies available for pre-consensus scheduling");
            return;
        }

        // 1. Build dependency graph
        let start = std::time::Instant::now();
        let (graph, _digest_to_node) = self.build_dependency_graph(&transactions);
        tracing::debug!("Dependency graph building took {:?}", start.elapsed());

        // 2. Decide assignment based on policy
        // 3. Update data structures accordingly
        let start = std::time::Instant::now();
        self.apply_sds_policy(&transactions, &graph);
        tracing::debug!("Pre-consensus scheduling policy took {:?}", start.elapsed());
    }

    fn apply_sds_policy(
        &mut self,
        transactions: &[RemoraTransaction<E>],
        graph: &DiGraph<TransactionDigest, ()>,
    ) {
        let mut subgraphs = self.find_all_subgraphs(graph);
        subgraphs.sort_by_key(|subgraph| std::cmp::Reverse(subgraph.len()));

        // Split subgraphs into two collections
        let (small_subgraphs, large_subgraphs): (Vec<_>, Vec<_>) = subgraphs
            .into_iter()
            .partition(|subgraph| subgraph.len() < 2);

        tracing::debug!(
            "Found {} small subgraphs (< 2 nodes)",
            small_subgraphs.len()
        );
        tracing::debug!(
            "Found {} large subgraphs (>= 2 nodes)",
            large_subgraphs.len()
        );

        if !large_subgraphs.is_empty() {
            tracing::debug!("Largest subgraph has {} nodes", large_subgraphs[0].len());
        }

        let tx_map: FxHashMap<_, _> = transactions.iter().map(|tx| (tx.digest(), tx)).collect();

        // Handle large subgraphs
        let start = std::time::Instant::now();
        if !large_subgraphs.is_empty() {
            self.handle_large_subgraphs(graph, &large_subgraphs, &tx_map);
        }
        tracing::debug!("Large subgraphs processing took {:?}", start.elapsed());

        #[cfg(debug_assertions)]
        self.log_load_summary("SDS");
    }

    fn handle_large_subgraphs(
        &mut self,
        graph: &DiGraph<TransactionDigest, ()>,
        large_subgraphs: &[Vec<petgraph::graph::NodeIndex>],
        tx_map: &FxHashMap<&TransactionDigest, &RemoraTransaction<E>>,
    ) {
        // Locality-based assignment for large subgraphs
        let assignments: Vec<_> = large_subgraphs
            .iter()
            .map(|subgraph_nodes| self.assign_large_subgraph_proxy(graph, subgraph_nodes, tx_map))
            .collect();

        // Sequential state updates
        for (proxy_id, subgraph_digests, subgraph_objects) in assignments {
            self.update_state_for_sds(&subgraph_digests, &subgraph_objects, proxy_id, tx_map);
        }
    }

    #[inline]
    fn object_id_24bit_index(object_id: &ObjectID) -> usize {
        let bytes = object_id.as_ref();
        let index = (bytes[0] as usize) | ((bytes[1] as usize) << 8) | ((bytes[2] as usize) << 16);
        index - 1
    }

    fn assign_large_subgraph_proxy(
        &self,
        graph: &DiGraph<TransactionDigest, ()>,
        subgraph_nodes: &[petgraph::graph::NodeIndex],
        tx_map: &FxHashMap<&TransactionDigest, &RemoraTransaction<E>>,
    ) -> (ExecutorIndex, FxHashSet<TransactionDigest>, Vec<ObjectID>) {
        let subgraph_digests: FxHashSet<_> = subgraph_nodes.iter().map(|&idx| graph[idx]).collect();

        let mut subgraph_objects = Vec::new();
        for digest in &subgraph_digests {
            if let Some(tx) = tx_map.get(digest) {
                for (object_id, _) in tx.shared_objects() {
                    subgraph_objects.push(*object_id);
                }
            }
        }

        let num_proxies = self.proxy_connections.len();
        let mut locality_count = vec![0usize; num_proxies];

        for obj in &subgraph_objects {
            let idx = Self::object_id_24bit_index(obj);
            if let Some(proxy_id) = self.object_last_proxy[idx] {
                locality_count[proxy_id] += 1;
            }
        }

        // Find max locality
        let max_locality = *locality_count.iter().max().unwrap_or(&0);

        // Collect all proxies with max locality
        let best_candidates: Vec<_> = (0..num_proxies)
            .filter(|&p| locality_count[p] == max_locality)
            .collect();

        // Randomly choose among the best candidates
        let best_proxy = if best_candidates.is_empty() {
            0
        } else {
            best_candidates[fastrand::usize(..best_candidates.len())]
        };

        for digest in &subgraph_digests {
            self.pre_consensus_routing_plan.insert(*digest, best_proxy);
        }

        (best_proxy, subgraph_digests, subgraph_objects)
    }

    fn update_state_for_sds(
        &mut self,
        subgraph_digests: &FxHashSet<TransactionDigest>,
        subgraph_objects: &Vec<ObjectID>,
        proxy_id: ExecutorIndex,
        tx_map: &FxHashMap<&TransactionDigest, &RemoraTransaction<E>>,
    ) {
        let mut total_weight = 0;
        for digest in subgraph_digests {
            if let Some(tx) = tx_map.get(digest) {
                total_weight += tx.expected_stateful_duration().as_micros() as usize;
            }
        }

        self.proxy_loads
            .entry(proxy_id)
            .and_modify(|load| *load += total_weight)
            .or_insert(total_weight);

        for object_id in subgraph_objects {
            self.object_last_proxy[Self::object_id_24bit_index(&object_id)] = Some(proxy_id);
        }
    }

    fn build_dependency_graph(
        &self,
        transactions: &[RemoraTransaction<E>],
    ) -> (
        DiGraph<TransactionDigest, ()>,
        FxHashMap<TransactionDigest, petgraph::graph::NodeIndex>,
    ) {
        let mut graph = DiGraph::new();
        let mut digest_to_node = FxHashMap::default();
        let mut object_to_last_accessor = FxHashMap::default();

        for tx in transactions {
            let tx_digest = *tx.digest();
            let node = graph.add_node(tx_digest);
            digest_to_node.insert(tx_digest, node);

            for (object_id, _) in tx.shared_objects() {
                if let Some(prior_node) = object_to_last_accessor.get(object_id) {
                    graph.add_edge(*prior_node, node, ());
                }
                object_to_last_accessor.insert(*object_id, node);
            }
        }
        (graph, digest_to_node)
    }

    fn find_all_subgraphs(
        &self,
        graph: &DiGraph<TransactionDigest, ()>,
    ) -> Vec<Vec<petgraph::graph::NodeIndex>> {
        let mut visited = FxHashSet::default();
        let mut components = Vec::new();

        for node_idx in graph.node_indices() {
            if !visited.contains(&node_idx) {
                let mut component_nodes = Vec::new();
                let mut stack = vec![node_idx];
                visited.insert(node_idx);

                while let Some(current_node) = stack.pop() {
                    component_nodes.push(current_node);
                    // Use neighbors_undirected to find weakly connected components
                    for neighbor in graph.neighbors_undirected(current_node) {
                        if visited.insert(neighbor) {
                            stack.push(neighbor);
                        }
                    }
                }
                components.push(component_nodes);
            }
        }
        components
    }

    fn log_load_summary(&self, policy_name: &str) {
        let total_load: usize = self.proxy_loads.iter().map(|e| *e.value()).sum();
        if total_load == 0 {
            return;
        }
        let load_ratios: BTreeMap<_, _> = self
            .proxy_loads
            .iter()
            .enumerate()
            .map(|(idx, e)| (idx, format!("{:.2}", *e.value() as f64 / total_load as f64)))
            .collect();
        tracing::debug!(
            "{} policy decided load ratios: {:?}",
            policy_name,
            load_ratios
        );
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

                if sender.send((transaction, required_versions)).await.is_err() {
                    tracing::error!("Failed to send transaction to SharedObjTxnForwarder");
                }
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
#[derive(Clone)]
pub(crate) struct SharedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) dependency_controller: Arc<VersionedDependencyController>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) pre_consensus_routing_plan: Arc<DashMap<TransactionDigest, ProxyId>>,
    pub(crate) states_to_proxy: Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
    pub(crate) stateless_forwarding_table: Arc<DashMap<TransactionDigest, ExecutorIndex>>,
    pub(crate) separation_mode: SeparationMode,
    pub(crate) policy: PreConsensusSchedulingPolicy,
    pub(crate) counter: usize,
    pub(crate) proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
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
        // Clone all needed fields to move into the spawned task
        let dependency_controller = self.dependency_controller.clone();
        let proxy_connections = self.proxy_connections.clone();
        let pre_consensus_routing_plan = self.pre_consensus_routing_plan.clone();
        let metrics = self.metrics.clone();
        let transaction_arc = Arc::new(transaction);
        let states_to_proxy = self.states_to_proxy.clone();
        let stateless_forwarding_table = self.stateless_forwarding_table.clone();
        let separation_mode = self.separation_mode;
        let counter = self.counter;
        let policy = self.policy.clone();
        let proxy_loads = self.proxy_loads.clone();
        self.counter += 1;

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

            let proxy_id = if let Some((_digest, proxy_id)) =
                pre_consensus_routing_plan.remove(transaction_arc.digest())
            {
                proxy_id
            } else {
                let fallback_proxy_id = match policy {
                    PreConsensusSchedulingPolicy::LSDS => {
                        Self::get_proxy_for_shared_objects_most_states(
                            &proxy_connections,
                            &states_to_proxy,
                            &required_versions,
                            counter,
                        )
                        .unwrap_or(counter % proxy_connections.len())
                    }
                    PreConsensusSchedulingPolicy::RSDS => counter % proxy_connections.len(),
                };

                // Calculate the load for the transaction and update proxy_loads
                let transaction_load = transaction_arc.expected_stateful_duration().as_micros() as usize;
                proxy_loads.entry(fallback_proxy_id)
                    .and_modify(|load| *load += transaction_load)
                    .or_insert(transaction_load);

                fallback_proxy_id
            };

            let stateful_missing_states = Self::get_missing_states_for_transaction(
                &transaction_arc,
                Some(required_versions),
                proxy_id,
                states_to_proxy,
            )
            .await;

            let msg = if separation_mode != SeparationMode::PrimaryPreSeparation
                && separation_mode != SeparationMode::PrimaryPostSeparation
            {
                PrimaryToProxyMessage::CombinedTxn(
                    Arc::clone(&transaction_arc),
                    proxy_id,
                    stateful_missing_states,
                )
            } else {
                let stateless_proxy_id = if separation_mode == SeparationMode::PrimaryPostSeparation
                {
                    let proxy = StatelessTxnForwarder::<E>::pick_stateless_proxy(
                        &proxy_connections,
                        &proxy_loads,
                        transaction_arc.verification_duration(),
                    );
                    Self::send_to_proxy(
                        &proxy_connections,
                        proxy,
                        PrimaryToProxyMessage::StatelessTxn(
                            *transaction_arc.digest(),
                            transaction_arc.verification_duration(),
                        ),
                    )
                    .await;
                    proxy
                } else {
                    // lookup from the stateless forwarding table
                    stateless_forwarding_table
                        .remove(&transaction_arc.digest())
                        .map(|(_k, v)| v)
                        .unwrap_or_else(|| {
                            tracing::warn!(
                                "No stateless proxy found for transaction, defaulting to 0"
                            );
                            0
                        })
                };

                PrimaryToProxyMessage::Txn(
                    Arc::clone(&transaction_arc),
                    stateless_proxy_id,
                    stateful_missing_states,
                )
            };

            Self::send_to_proxy(&proxy_connections, proxy_id, msg).await;
            metrics.update_metrics(transaction_arc.timestamp(), "primary-egress");

            // Notify any dependencies waiting on this transaction
            for notify in current_handles {
                notify.notify_one();
            }
        });
    }

    /// Get assigned proxy based on which proxy hosts the most states needed by this transaction.
    fn get_proxy_for_shared_objects_most_states(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        states_to_proxy: &Arc<DashMap<(ObjectID, SequenceNumber), ExecutorIndex>>,
        required_versions: &[(ObjectID, SequenceNumber)],
        txn_cnt: usize,
    ) -> Option<ExecutorIndex> {
        let proxy_count = proxy_connections.len();
        if proxy_count == 0 {
            return None;
        }

        if required_versions.is_empty() {
            // If no shared objects, use first proxy
            return Some(0);
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
        Some(proxy_index)
    }

    /// Get assigned proxy for shared objects in a transaction.
    /// This is the main entry point for load balancing policy selection.
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

    pub(crate) async fn send_to_proxy(
        proxy_connections: &Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        dest_proxy: ExecutorIndex,
        message: PrimaryToProxyMessage<<E as Executor>::Transaction>,
    ) {
        if let Some(proxy) = proxy_connections.get(&dest_proxy) {
            if let Err(e) = proxy.send(message).await {
                tracing::error!(
                    "Failed to send transaction to proxy {}: {:?}",
                    dest_proxy,
                    e
                );
            }
        } else {
            tracing::warn!("Proxy {} not found", dest_proxy);
        }
    }
}

pub struct StatelessTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
    pub(crate) stateless_forwarding_table: Arc<DashMap<TransactionDigest, ExecutorIndex>>,
    pub(crate) rx_stateless_txns: Receiver<(TransactionDigest, Duration)>,
}

impl<E> StatelessTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) fn pick_stateless_proxy(
        conn: &Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
        proxy_loads: &Arc<DashMap<ExecutorIndex, usize>>,
        duration: Duration,
    ) -> ExecutorIndex {
        let proxy = conn
            .iter()
            .map(|entry| *entry.key())
            .min_by_key(|&proxy_id| proxy_loads.get(&proxy_id).map_or(0, |load| *load))
            .unwrap_or(0);
        let weight = duration.as_micros() as usize;
        proxy_loads
            .entry(proxy)
            .and_modify(|load| *load += weight)
            .or_insert(weight);

        proxy
    }

    pub(crate) async fn forward_stateless_txn(&mut self) {
        while let Some((digest, duration)) = self.rx_stateless_txns.recv().await {
            let conn = self.proxy_connections.clone();
            let proxy_loads = self.proxy_loads.clone();
            let stateless_forwarding_table = self.stateless_forwarding_table.clone();
            tokio::spawn(async move {
                let proxy = Self::pick_stateless_proxy(&conn, &proxy_loads, duration);
                SharedObjTxnForwarder::<E>::send_to_proxy(
                    &conn,
                    proxy,
                    PrimaryToProxyMessage::StatelessTxn(digest, duration),
                )
                .await;
                stateless_forwarding_table.insert(digest, proxy);
            });
        }
    }
}
