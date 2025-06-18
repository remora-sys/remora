use crate::{
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
use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};
use sui_types::base_types::{ObjectID, SequenceNumber, TransactionDigest};
use tokio::sync::mpsc::{Receiver, Sender};

pub(crate) struct PreConsensusSchedTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) pre_consensus_routing_plan: Arc<DashMap<TransactionDigest, ProxyId>>,
    pub(crate) index: usize,
    pub(crate) _phantom: PhantomData<E>,
    pub(crate) proxy_loads: Arc<DashMap<ExecutorIndex, usize>>,
}

impl<E> PreConsensusSchedTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
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

        let largest_subgraph_digests = self.analyze_dependencies(&transactions);
        let (largest_component_proxy, other_proxies) = self.determine_routing(num_proxies);

        self.assign_and_update_loads(
            transactions,
            &largest_subgraph_digests,
            largest_component_proxy,
            &other_proxies,
        );
    }

    fn analyze_dependencies(
        &self,
        transactions: &[RemoraTransaction<E>],
    ) -> FxHashSet<TransactionDigest> {
        let (graph, _digest_to_node) = self.build_dependency_graph(transactions);
        let largest_subgraph_nodes = self.find_largest_subgraph(&graph);
        largest_subgraph_nodes
            .into_iter()
            .map(|node_idx| graph[node_idx])
            .collect()
    }

    fn determine_routing(&self, num_proxies: usize) -> (ExecutorIndex, Vec<ExecutorIndex>) {
        let least_loaded_proxy = self
            .proxy_loads
            .iter()
            .min_by_key(|entry| *entry.value())
            .map(|entry| *entry.key())
            .unwrap_or(0);

        let mut other_proxies: Vec<_> = (0..num_proxies)
            .filter(|id| *id != least_loaded_proxy)
            .collect();
        if other_proxies.is_empty() {
            other_proxies.push(least_loaded_proxy);
        }
        (least_loaded_proxy, other_proxies)
    }

    fn assign_and_update_loads(
        &mut self,
        transactions: Vec<RemoraTransaction<E>>,
        largest_subgraph_digests: &FxHashSet<TransactionDigest>,
        largest_component_proxy: ExecutorIndex,
        other_proxies: &[ExecutorIndex],
    ) {
        let mut other_proxy_idx = 0;
        for transaction in transactions {
            let digest = transaction.digest();
            let proxy_id = if largest_subgraph_digests.contains(digest) {
                largest_component_proxy
            } else {
                let id = other_proxies[other_proxy_idx % other_proxies.len()];
                other_proxy_idx += 1;
                id
            };

            self.pre_consensus_routing_plan.insert(*digest, proxy_id);
            let weight = transaction.expected_stateful_duration().as_micros() as usize;
            self.proxy_loads
                .entry(proxy_id)
                .and_modify(|load| *load += weight)
                .or_insert(weight);
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

    fn find_largest_subgraph(
        &self,
        graph: &DiGraph<TransactionDigest, ()>,
    ) -> Vec<petgraph::graph::NodeIndex> {
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

        // Find the largest component by number of nodes
        components
            .into_iter()
            .max_by_key(|c| c.len())
            .unwrap_or_else(Vec::new)
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

            if let Some((_digest, proxy_id)) =
                pre_consensus_routing_plan.remove(transaction_arc.digest())
            {
                // lookup from the stateless forwarding table
                let stateless_proxy_id = stateless_forwarding_table
                    .remove(&transaction_arc.digest())
                    .map(|(_k, v)| v)
                    .unwrap_or_else(|| {
                        tracing::warn!("No stateless proxy found for transaction, defaulting to 0");
                        0
                    });
                let stateful_missing_states = Self::get_missing_states_for_transaction(
                    &transaction_arc,
                    Some(required_versions),
                    proxy_id,
                    states_to_proxy,
                )
                .await;

                let stateful_msg = PrimaryToProxyMessage::Txn(
                    Arc::clone(&transaction_arc),
                    stateless_proxy_id,
                    stateful_missing_states,
                );

                Self::send_to_proxy(&proxy_connections, proxy_id, stateful_msg).await;

                metrics.update_metrics(transaction_arc.timestamp());
            } else {
                tracing::warn!(
                    "No pre-consensus routing plan found for transaction {:?}",
                    transaction_arc.digest()
                );
            }

            // Notify any dependencies waiting on this transaction
            for notify in current_handles {
                notify.notify_one();
            }
        });
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
