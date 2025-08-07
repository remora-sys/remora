// src/client/hermes_schedule.rs

use crate::executor::api::ExecutableTransaction;
use petgraph::graph::UnGraph;
use rand::seq::SliceRandom;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;

pub struct TxnInfo {
    pub id: usize,
    pub objects: Vec<ObjectID>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum AssignmentMode {
    /// Reordering mode: traverse transactions in two sets until initial one is empty
    Reordering,
    /// Sequential mode: check transactions one by one and assign to best candidate nodes
    Sequential,
}

impl Default for AssignmentMode {
    fn default() -> Self {
        AssignmentMode::Reordering
    }
}

/// A map from transaction ID (index) to its assigned node ID.
type Assignments = FxHashMap<usize, usize>;
/// A dependency graph where nodes are transaction IDs.
type DepGraph = UnGraph<usize, ()>;

pub struct ScheduleResult {
    /// The optimal order to process transactions (indices into original array)
    pub transaction_order: Vec<usize>,
    /// Destination node for each transaction (indexed by transaction ID)
    pub destinations: Vec<usize>,
}

/// High-level comment:
/// Implements a sophisticated scheduling algorithm for a batch of transactions.
/// The algorithm is divided into three main phases:
/// 1.  Initial greedy assignment: Assigns each transaction to a node to minimize
///     remote object accesses, building a dependency graph along the way.
/// 2.  Load re-balancing: Iteratively moves transactions from overloaded to
///     underloaded nodes to balance the distribution, while trying to keep
///     remote accesses low.
/// 3.  Object partition update: Updates the global object-to-node mapping based
///     on the final transaction assignments, to be used by the next batch.
pub fn schedule_transactions_hermes<T: ExecutableTransaction>(
    transactions: &[T],
    object_node_partition: &mut FxHashMap<ObjectID, usize>,
    num_nodes: usize,
    assignment_mode: AssignmentMode,
) -> ScheduleResult {
    let txn_infos: Vec<TxnInfo> = transactions
        .iter()
        .enumerate()
        .map(|(i, tx)| TxnInfo {
            id: i,
            objects: tx.shared_object_ids(),
        })
        .collect();

    if txn_infos.is_empty() {
        return ScheduleResult {
            transaction_order: Vec::new(),
            destinations: Vec::new(),
        };
    }

    // Step 1: Initial assignment
    let start_time = std::time::Instant::now();
    let (mut assignments, dep_graph, assignment_order) = match assignment_mode {
        AssignmentMode::Reordering => {
            initial_assignment(&txn_infos, object_node_partition, num_nodes)
        }
        AssignmentMode::Sequential => {
            sequential_initial_assignment(&txn_infos, object_node_partition, num_nodes)
        }
    };
    let initial_assignment_time = start_time.elapsed();
    tracing::debug!("Initial assignment time: {:?}", initial_assignment_time);

    let mut loads = vec![0; num_nodes];
    for &node_id in assignments.values() {
        loads[node_id] += 1;
    }
    tracing::info!(
        "Load distribution after initial assignment ({} txns): {:?}",
        assignments.len(),
        loads
    );

    // Step 2: Re-balancing
    rebalance_assignments(
        &mut assignments,
        &dep_graph,
        &assignment_order,
        txn_infos.len(),
        num_nodes,
    );

    let mut loads = vec![0; num_nodes];
    for &node_id in assignments.values() {
        loads[node_id] += 1;
    }
    tracing::debug!(
        "Load distribution after re-balancing ({} txns): {:?}",
        assignments.len(),
        loads
    );

    // Step 3: Update object partition (for any changes from re-balancing)
    update_object_partitions(object_node_partition, &txn_infos, &assignments);
    let update_object_partition_time = start_time.elapsed();
    tracing::debug!(
        "Update object partition time: {:?}",
        update_object_partition_time
    );

    let mut destinations = vec![0; transactions.len()];
    for (txn_id, node_id) in assignments {
        destinations[txn_id] = node_id;
    }

    ScheduleResult {
        transaction_order: assignment_order,
        destinations,
    }
}

/// 1. [Locality-aware reorder and initial assign txn]
fn initial_assignment(
    txn_infos: &[TxnInfo],
    object_node_partition: &mut FxHashMap<ObjectID, usize>,
    num_nodes: usize,
) -> (Assignments, DepGraph, Vec<usize>) {
    let mut assignments = FxHashMap::default();
    let mut dep_graph = UnGraph::new_undirected();
    let mut assignment_order = Vec::new();
    let mut unassigned_txns: FxHashSet<usize> = (0..txn_infos.len()).collect();
    let mut txn_nodes = FxHashMap::default();
    let mut object_last_txn: FxHashMap<ObjectID, usize> = FxHashMap::default();

    while !unassigned_txns.is_empty() {
        let mut best_cost = usize::MAX;
        let mut best_candidates = Vec::new();

        for &txn_id in &unassigned_txns {
            for node_id in 0..num_nodes {
                let cost = calculate_cost(&txn_infos[txn_id], node_id, object_node_partition);
                if cost < best_cost {
                    best_cost = cost;
                    best_candidates.clear();
                    best_candidates.push((txn_id, node_id));
                } else if cost == best_cost {
                    best_candidates.push((txn_id, node_id));
                }
            }
            if best_cost == 0 {
                break;
            }
        }

        let (best_txn_id, best_node_id) = *best_candidates.choose(&mut rand::thread_rng()).unwrap();

        unassigned_txns.remove(&best_txn_id);
        assignments.insert(best_txn_id, best_node_id);
        assignment_order.push(best_txn_id);
        let graph_node = dep_graph.add_node(best_txn_id);
        txn_nodes.insert(best_txn_id, graph_node);

        // Add dependency edges: only to the most recent transaction that accessed each object
        let mut dependencies = FxHashSet::default();
        for obj_id in &txn_infos[best_txn_id].objects {
            if let Some(&last_txn_id) = object_last_txn.get(obj_id) {
                dependencies.insert(last_txn_id);
            }
            // Update the last transaction that accessed this object
            object_last_txn.insert(*obj_id, best_txn_id);
        }

        // Add edges to all transactions this one depends on
        for dep_txn_id in dependencies {
            if let Some(&dep_node) = txn_nodes.get(&dep_txn_id) {
                dep_graph.add_edge(dep_node, graph_node, ());
            }
        }

        // Update object partition immediately for subsequent cost calculations
        for obj_id in &txn_infos[best_txn_id].objects {
            object_node_partition.insert(*obj_id, best_node_id);
        }
    }

    (assignments, dep_graph, assignment_order)
}

fn calculate_cost(
    txn: &TxnInfo,
    node_id: usize,
    object_node_partition: &FxHashMap<ObjectID, usize>,
) -> usize {
    txn.objects
        .iter()
        .filter(|obj_id| {
            if let Some(&home_node) = object_node_partition.get(obj_id) {
                home_node != node_id
            } else {
                // Note: assumption if the object is not in the partition, its cost is considered 0.
                false
            }
        })
        .count()
}

/// 2. [re-assign to balance loads]
fn rebalance_assignments(
    assignments: &mut Assignments,
    dep_graph: &DepGraph,
    assignment_order: &[usize],
    num_txns: usize,
    num_nodes: usize,
) {
    let overload_threshold = (num_txns as f64 / num_nodes as f64 * 2.0) as usize;
    let mut increase_tolerance = 0; // Start with strict tolerance

    // Track partition states
    let mut loads = vec![0; num_nodes];
    for &node_id in assignments.values() {
        loads[node_id] += 1;
    }

    let mut overloaded_parts: FxHashSet<usize> = (0..num_nodes)
        .filter(|&i| loads[i] > overload_threshold)
        .collect();

    // Start with all transactions from overloaded partitions as candidates
    let mut candidate_txns: Vec<usize> = assignment_order
        .iter()
        .rev() // Process most recent transactions first
        .filter(|&&txn_id| overloaded_parts.contains(&assignments[&txn_id]))
        .copied()
        .collect();

    // Outer loop: progressively increase tolerance until all partitions are balanced
    while !overloaded_parts.is_empty() {
        tracing::debug!(
            "Rebalancing with tolerance {}: overloaded_parts={:?}, loads={:?}",
            increase_tolerance,
            overloaded_parts,
            loads
        );

        candidate_txns = reroute_txns_to_underloaded_parts(
            candidate_txns,
            assignments,
            dep_graph,
            &mut loads,
            &mut overloaded_parts,
            overload_threshold,
            increase_tolerance,
            num_nodes,
        );

        increase_tolerance += 1;

        if increase_tolerance > 100 {
            tracing::error!("Rebalancing failed to converge after 100 iterations");
            break;
        }
    }
}

fn reroute_txns_to_underloaded_parts(
    candidate_txns: Vec<usize>,
    assignments: &mut Assignments,
    dep_graph: &DepGraph,
    loads: &mut Vec<usize>,
    overloaded_parts: &mut FxHashSet<usize>,
    overload_threshold: usize,
    increase_tolerance: usize,
    num_nodes: usize,
) -> Vec<usize> {
    let mut next_candidates = Vec::new();

    let mut saturated_parts: FxHashSet<usize> = (0..num_nodes)
        .filter(|&i| loads[i] == overload_threshold)
        .collect();

    for &txn_id in &candidate_txns {
        let current_part_id = assignments[&txn_id];

        // If the home partition is no longer overloaded, skip it
        if !overloaded_parts.contains(&current_part_id) {
            continue;
        }

        let current_remote_edges =
            count_remote_edges(txn_id, current_part_id, assignments, dep_graph);
        let mut best_delta = increase_tolerance as i32 + 1;
        let mut best_part_id = current_part_id;

        // Find a better partition
        for part_id in 0..num_nodes {
            // Skip home partition
            if part_id == current_part_id {
                continue;
            }

            // Skip overloaded partitions
            if overloaded_parts.contains(&part_id) {
                continue;
            }

            // Skip saturated partitions
            if saturated_parts.contains(&part_id) {
                continue;
            }

            // Count remote edges for this candidate partition
            let remote_edge_count = count_remote_edges(txn_id, part_id, assignments, dep_graph);

            // Calculate the difference
            let delta = remote_edge_count as i32 - current_remote_edges as i32;
            if delta <= increase_tolerance as i32 {
                // Prefer the partition with lower load, or lower delta if loads are equal
                if (delta < best_delta)
                    || (delta == best_delta && loads[part_id] < loads[best_part_id])
                {
                    best_delta = delta;
                    best_part_id = part_id;
                }
            }
        }

        // If there is no suitable partition, add to next candidates
        if best_part_id == current_part_id {
            next_candidates.push(txn_id);
            continue;
        }

        // Move the transaction
        assignments.insert(txn_id, best_part_id);

        // Update loads and partition states
        loads[current_part_id] -= 1;
        if loads[current_part_id] == overload_threshold {
            overloaded_parts.remove(&current_part_id);
            saturated_parts.insert(current_part_id);
        }

        loads[best_part_id] += 1;
        if loads[best_part_id] == overload_threshold {
            saturated_parts.insert(best_part_id);
        }

        // Early termination if no overloaded partitions remain
        if overloaded_parts.is_empty() {
            return Vec::new();
        }
    }

    next_candidates
}

fn count_remote_edges(
    txn_id: usize,
    node_id: usize,
    assignments: &Assignments,
    dep_graph: &DepGraph,
) -> usize {
    let txn_node_index = dep_graph
        .node_indices()
        .find(|i| dep_graph[*i] == txn_id)
        .unwrap();
    dep_graph
        .neighbors(txn_node_index)
        .filter(|&neighbor_index| {
            let neighbor_txn_id = dep_graph[neighbor_index];
            assignments[&neighbor_txn_id] != node_id
        })
        .count()
}

/// 3. [update object-partition]
fn update_object_partitions(
    object_node_partition: &mut FxHashMap<ObjectID, usize>,
    txn_infos: &[TxnInfo],
    assignments: &Assignments,
) {
    for (txn_id, node_id) in assignments {
        for obj_id in &txn_infos[*txn_id].objects {
            object_node_partition.insert(*obj_id, *node_id);
        }
    }
}

/// Non-reordering initial assignment: simply check transactions one by one and assign to best candidate nodes
fn sequential_initial_assignment(
    txn_infos: &[TxnInfo],
    object_node_partition: &mut FxHashMap<ObjectID, usize>,
    num_nodes: usize,
) -> (Assignments, DepGraph, Vec<usize>) {
    let mut assignments = FxHashMap::default();
    let mut dep_graph = UnGraph::new_undirected();
    let mut assignment_order = Vec::new();
    let mut txn_nodes = FxHashMap::default();
    let mut object_last_txn: FxHashMap<ObjectID, usize> = FxHashMap::default();

    for (txn_id, txn_info) in txn_infos.iter().enumerate() {
        let mut best_cost = usize::MAX;
        let mut best_candidates = Vec::new();

        // Find the best candidate node for this transaction
        for node_id in 0..num_nodes {
            let cost = calculate_cost(txn_info, node_id, object_node_partition);
            if cost < best_cost {
                best_cost = cost;
                best_candidates.clear();
                best_candidates.push(node_id);
            } else if cost == best_cost {
                best_candidates.push(node_id);
            }
        }

        // Select a random node from the best candidates
        let best_node_id = *best_candidates.choose(&mut rand::thread_rng()).unwrap();

        assignments.insert(txn_id, best_node_id);
        assignment_order.push(txn_id);
        let graph_node = dep_graph.add_node(txn_id);
        txn_nodes.insert(txn_id, graph_node);

        // Add dependency edges: only to the most recent transaction that accessed each object
        let mut dependencies = FxHashSet::default();
        for obj_id in &txn_info.objects {
            if let Some(&last_txn_id) = object_last_txn.get(obj_id) {
                dependencies.insert(last_txn_id);
            }
            // Update the last transaction that accessed this object
            object_last_txn.insert(*obj_id, txn_id);
        }

        // Add edges to all transactions this one depends on
        for dep_txn_id in dependencies {
            if let Some(&dep_node) = txn_nodes.get(&dep_txn_id) {
                dep_graph.add_edge(dep_node, graph_node, ());
            }
        }

        // Update object partition immediately for subsequent cost calculations
        for obj_id in &txn_info.objects {
            object_node_partition.insert(*obj_id, best_node_id);
        }
    }

    (assignments, dep_graph, assignment_order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::api::ExecutableTransaction;
    use std::hash::{Hash, Hasher};
    use sui_types::base_types::ObjectID;
    use sui_types::{digests::TransactionDigest, transaction::InputObjectKind};

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct MockTransaction {
        id: u64,
        objects: Vec<ObjectID>,
    }

    impl Hash for MockTransaction {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.id.hash(state);
        }
    }

    impl ExecutableTransaction for MockTransaction {
        fn shared_object_ids(&self) -> Vec<ObjectID> {
            self.objects.clone()
        }

        fn digest(&self) -> &TransactionDigest {
            unimplemented!()
        }

        fn input_objects(&self) -> Vec<InputObjectKind> {
            unimplemented!()
        }
    }

    fn new_mock_tx(id: u64, objects: Vec<ObjectID>) -> MockTransaction {
        MockTransaction { id, objects }
    }

    fn new_object_id(id: u8) -> ObjectID {
        ObjectID::from_bytes([id; ObjectID::LENGTH]).unwrap()
    }

    #[test]
    fn test_locality_and_rebalancing() {
        let num_nodes = 2;
        let mut object_node_partition = FxHashMap::default();

        // Node 0 has objects A, B
        // Node 1 has objects C, D
        let obj_a = new_object_id(1);
        let obj_b = new_object_id(2);
        let obj_c = new_object_id(3);
        let obj_d = new_object_id(4);
        object_node_partition.insert(obj_a, 0);
        object_node_partition.insert(obj_b, 0);
        object_node_partition.insert(obj_c, 1);
        object_node_partition.insert(obj_d, 1);

        // A list of transactions.
        // T0-T3 access only local objects on node 0.
        // T4-T5 access only local objects on node 1.
        // T6 accesses objects on both nodes (remote access).
        let transactions = vec![
            new_mock_tx(0, vec![obj_a]),        // Should go to node 0
            new_mock_tx(1, vec![obj_b]),        // Should go to node 0
            new_mock_tx(2, vec![obj_a, obj_b]), // Should go to node 0
            new_mock_tx(3, vec![obj_a]),        // Should go to node 0
            new_mock_tx(4, vec![obj_c]),        // Should go to node 1
            new_mock_tx(5, vec![obj_d]),        // Should go to node 1
            new_mock_tx(6, vec![obj_a, obj_c]), // Remote access, cost is 1 for either node
        ];

        // The overload threshold is ceil(7 / 2 * 1.5) = ceil(5.25) = 6.
        // The initial assignment will put 5 txns (T0,T1,T2,T3,T6) on node 0 and 2 on node 1 (T4,T5)
        // or 4 on node 0 and 3 on node 1, depending on where T6 goes.
        // Let's assume T6 goes to node 0.
        // Loads: Node 0: 5, Node 1: 2. No node is overloaded. So no rebalancing should happen.
        let schedule_result = schedule_transactions_hermes(
            &transactions,
            &mut object_node_partition,
            num_nodes,
            AssignmentMode::Reordering,
        );

        let mut node_loads = vec![0; num_nodes];
        for &node_id in &schedule_result.destinations {
            node_loads[node_id] += 1;
        }

        println!("Final loads: {:?}", node_loads);
        println!("Final destinations: {:?}", schedule_result.destinations);

        // Check that T0-T3 are on node 0.
        for i in 0..=3 {
            assert_eq!(schedule_result.destinations[i], 0);
        }
        // Check that T4-T5 are on node 1.
        for i in 4..=5 {
            assert_eq!(schedule_result.destinations[i], 1);
        }

        // Now, let's test re-balancing. We need to create an overloaded scenario.
        let transactions_for_rebalance = vec![
            new_mock_tx(10, vec![obj_a]), // 0
            new_mock_tx(11, vec![obj_a]), // 0
            new_mock_tx(12, vec![obj_a]), // 0
            new_mock_tx(13, vec![obj_a]), // 0
            new_mock_tx(14, vec![obj_a]), // 0 -> gets rebalanced to 1
            new_mock_tx(15, vec![obj_c]), // 1
        ]; // Total 6 txns. Overload threshold = ceil(6/2 * 1.5) = ceil(4.5) = 5.

        let schedule_result_rebalanced = schedule_transactions_hermes(
            &transactions_for_rebalance,
            &mut object_node_partition,
            num_nodes,
            AssignmentMode::Reordering,
        );
        let mut node_loads_rebalanced = vec![0; num_nodes];
        for &node_id in &schedule_result_rebalanced.destinations {
            node_loads_rebalanced[node_id] += 1;
        }
        println!("Rebalanced loads: {:?}", node_loads_rebalanced);
        println!(
            "Rebalanced destinations: {:?}",
            schedule_result_rebalanced.destinations
        );

        // Initial assignment will put 5 txns on node 0 and 1 on node 1.
        // Node 0 is not overloaded. So no rebalancing.
        // Let's add more txns to trigger overload.
        let _transactions_for_rebalance_2 = vec![
            new_mock_tx(20, vec![obj_a]), // 0
            new_mock_tx(21, vec![obj_a]), // 0
            new_mock_tx(22, vec![obj_a]), // 0
            new_mock_tx(23, vec![obj_a]), // 0
            new_mock_tx(24, vec![obj_a]), // 0
            new_mock_tx(25, vec![obj_a]), // 0
            new_mock_tx(26, vec![obj_c]), // 1
        ]; // Total 7 txns. Overload threshold = ceil(7/2 * 1.5) = 6. Node 0 gets 6 txns, which is not > 6.
           // The logic is `> overload_threshold`. Let's add one more.
        let transactions_for_rebalance_3 = vec![
            new_mock_tx(30, vec![obj_a]), // 0
            new_mock_tx(31, vec![obj_a]), // 0
            new_mock_tx(32, vec![obj_a]), // 0
            new_mock_tx(33, vec![obj_a]), // 0
            new_mock_tx(34, vec![obj_a]), // 0
            new_mock_tx(35, vec![obj_a]), // 0
            new_mock_tx(36, vec![obj_a]), // 0 -> will be moved
            new_mock_tx(37, vec![obj_c]), // 1
        ]; // Total 8 txns. Overload threshold = ceil(8/2*1.5) = 6. Node 0 will get 7 txns initially.
           // Node 0 is overloaded. One transaction should be moved to node 1.

        let schedule_result_rebalanced_3 = schedule_transactions_hermes(
            &transactions_for_rebalance_3,
            &mut object_node_partition,
            num_nodes,
            AssignmentMode::Reordering,
        );
        let mut node_loads_rebalanced_3 = vec![0; num_nodes];
        for &node_id in &schedule_result_rebalanced_3.destinations {
            node_loads_rebalanced_3[node_id] += 1;
        }

        println!("Final rebalanced loads: {:?}", node_loads_rebalanced_3);
        println!(
            "Final rebalanced destinations: {:?}",
            schedule_result_rebalanced_3.destinations
        );

        // After rebalancing, loads should be 6 and 2.
        assert_eq!(node_loads_rebalanced_3[0], 6);
        assert_eq!(node_loads_rebalanced_3[1], 2);
    }

    #[test]
    fn test_reordering() {
        let num_nodes = 2;
        let mut object_node_partition = FxHashMap::default();

        // Node 0 has object A, Node 1 has object B.
        let obj_a = new_object_id(10);
        let obj_b = new_object_id(20);
        object_node_partition.insert(obj_a, 0);
        object_node_partition.insert(obj_b, 1);

        // T0 has high remote access cost.
        // T1 and T2 have zero remote access cost if placed correctly.
        let transactions = vec![
            new_mock_tx(0, vec![obj_a, obj_b]), // High cost, should be scheduled last.
            new_mock_tx(1, vec![obj_a]),        // Zero cost on node 0, should be first.
            new_mock_tx(2, vec![obj_b]),        // Zero cost on node 1, should be second.
        ];

        let txn_infos: Vec<TxnInfo> = transactions
            .iter()
            .enumerate()
            .map(|(i, tx)| TxnInfo {
                id: i,
                objects: tx.shared_object_ids(),
            })
            .collect();

        // The greedy algorithm should select the zero-cost transactions first,
        // effectively re-ordering them before the higher-cost transaction.
        let (_assignments, _dep_graph, assignment_order) =
            initial_assignment(&txn_infos, &mut object_node_partition, num_nodes);

        println!("Initial order: {:?}", vec![0, 1, 2]);
        println!("Assignment order: {:?}", assignment_order);

        // Expected order is T1 (id 1), then T2 (id 2), then T0 (id 0).
        // The transaction with the highest cost (T0) is scheduled last.
        assert_eq!(assignment_order, vec![1, 2, 0]);
    }

    #[test]
    fn test_assignment_modes() {
        let num_nodes = 2;
        let mut object_node_partition = FxHashMap::default();

        // Node 0 has object A, Node 1 has object B.
        let obj_a = new_object_id(10);
        let obj_b = new_object_id(20);
        object_node_partition.insert(obj_a, 0);
        object_node_partition.insert(obj_b, 1);

        // T0 has high remote access cost.
        // T1 and T2 have zero remote access cost if placed correctly.
        let transactions = vec![
            new_mock_tx(0, vec![obj_a, obj_b]), // High cost, should be scheduled last in reordering mode
            new_mock_tx(1, vec![obj_a]),        // Zero cost on node 0
            new_mock_tx(2, vec![obj_b]),        // Zero cost on node 1
        ];

        // Test reordering mode
        let mut object_partition_reordering = object_node_partition.clone();
        let schedule_result_reordering = schedule_transactions_hermes(
            &transactions,
            &mut object_partition_reordering,
            num_nodes,
            AssignmentMode::Reordering,
        );

        // Test sequential mode
        let mut object_partition_sequential = object_node_partition.clone();
        let schedule_result_sequential = schedule_transactions_hermes(
            &transactions,
            &mut object_partition_sequential,
            num_nodes,
            AssignmentMode::Sequential,
        );

        println!(
            "Reordering mode destinations: {:?}",
            schedule_result_reordering.destinations
        );
        println!(
            "Sequential mode destinations: {:?}",
            schedule_result_sequential.destinations
        );

        // In reordering mode, the order should be optimized (T1, T2, T0)
        // In sequential mode, transactions are processed in order (T0, T1, T2)
        // The destinations might be different due to different processing order

        // Both modes should still assign transactions to optimal nodes
        assert_eq!(schedule_result_reordering.destinations[1], 0); // T1 should go to node 0
        assert_eq!(schedule_result_reordering.destinations[2], 1); // T2 should go to node 1
        assert_eq!(schedule_result_sequential.destinations[1], 0); // T1 should go to node 0
        assert_eq!(schedule_result_sequential.destinations[2], 1); // T2 should go to node 1
    }
}
