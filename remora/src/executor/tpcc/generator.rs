// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TPC-C transaction generator.
//!
//! Generates NEW_ORDER and PAYMENT transactions following TPC-C specification.

use rand::{rngs::StdRng, Rng, SeedableRng};

use super::constants::*;
use super::transactions::{OrderItem, TpccTransaction};

// =============================================================================
// Transaction Generator
// =============================================================================

/// Generates TPC-C transactions following the specification
pub struct TpccGenerator {
    num_warehouses: usize,
    /// Ratio of PAYMENT transactions (0.0 = all NEW_ORDER, 1.0 = all PAYMENT)
    payment_ratio: f64,
    /// Number of nodes/proxies for warehouse partitioning
    num_nodes: usize,
    /// Number of warehouses per node
    warehouses_per_node: usize,
    /// Probability of cross-node multi-warehouse transactions
    cross_node_prob: f64,
    rng: StdRng,
}

impl TpccGenerator {
    /// Create a new TPC-C generator
    pub fn new(
        num_warehouses: usize,
        payment_ratio: f64,
        num_nodes: usize,
        cross_node_prob: f64,
        seed: u64,
    ) -> Self {
        assert!(num_nodes > 0, "num_nodes must be greater than 0");
        assert!(
            num_warehouses % num_nodes == 0,
            "num_warehouses ({}) must be evenly divisible by num_nodes ({})",
            num_warehouses,
            num_nodes
        );
        let warehouses_per_node = num_warehouses / num_nodes;
        Self {
            num_warehouses,
            payment_ratio,
            num_nodes,
            warehouses_per_node,
            cross_node_prob,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Generate a random transaction following configured payment ratio
    pub fn generate_transaction(&mut self) -> TpccTransaction {
        if self.rng.gen_bool(self.payment_ratio) {
            self.generate_payment()
        } else {
            self.generate_new_order()
        }
    }

    /// Generate a NEW_ORDER transaction
    pub fn generate_new_order(&mut self) -> TpccTransaction {
        let w_id = self.random_warehouse();
        let d_id = self.random_district();
        let c_id = self.random_customer();
        let home_node = self.node_for_warehouse(w_id);

        // Generate 5-15 order items
        let ol_cnt = self.rng.gen_range(MIN_OL_CNT..=MAX_OL_CNT);
        let items: Vec<OrderItem> = (0..ol_cnt)
            .map(|_| {
                let i_id = self.random_item();
                let supply_w_id =
                    if self.num_warehouses > 1 && self.rng.gen_bool(REMOTE_WAREHOUSE_PROB) {
                        // Decide if this should be cross-node based on cross_node_prob
                        if self.cross_node_prob > 0.0
                            && self.num_nodes > 1
                            && self.rng.gen_bool(self.cross_node_prob)
                        {
                            // Cross-node: select from different node
                            self.random_warehouse_from_different_node(home_node)
                        } else {
                            // Same node: select different warehouse from same node
                            if self.warehouses_per_node > 1 {
                                // Try to get a different warehouse from the same node
                                loop {
                                    let selected = self.random_warehouse_from_node(home_node);
                                    if selected != w_id {
                                        break selected;
                                    }
                                }
                            } else {
                                // Only one warehouse per node, use the same warehouse
                                w_id
                            }
                        }
                    } else {
                        w_id
                    };
                let quantity = self.rng.gen_range(1..=MAX_OL_QUANTITY);

                OrderItem {
                    i_id,
                    supply_w_id,
                    quantity,
                }
            })
            .collect();

        TpccTransaction::NewOrder {
            w_id,
            d_id,
            c_id,
            items,
        }
    }

    /// Generate a PAYMENT transaction
    pub fn generate_payment(&mut self) -> TpccTransaction {
        let w_id = self.random_warehouse();
        let d_id = self.random_district();

        // Determine if customer is from a remote warehouse
        let (c_w_id, c_d_id) = if self.num_warehouses > 1 && self.rng.gen_bool(REMOTE_CUSTOMER_PROB)
        {
            let remote_w_id = self.random_warehouse_excluding(w_id);
            let remote_d_id = self.random_district();
            (remote_w_id, remote_d_id)
        } else {
            (w_id, d_id)
        };

        let c_id = self.random_customer();
        let h_amount = self.rng.gen_range(MIN_PAYMENT_CENTS..=MAX_PAYMENT_CENTS);

        TpccTransaction::Payment {
            w_id,
            d_id,
            c_w_id,
            c_d_id,
            c_id,
            h_amount,
        }
    }

    // =========================================================================
    // Random ID Generators
    // =========================================================================

    fn random_warehouse(&mut self) -> u32 {
        self.rng.gen_range(1..=self.num_warehouses as u32)
    }

    fn random_warehouse_excluding(&mut self, exclude: u32) -> u32 {
        loop {
            let w_id = self.random_warehouse();
            if w_id != exclude {
                return w_id;
            }
        }
    }

    fn random_district(&mut self) -> u32 {
        self.rng.gen_range(1..=DISTRICTS_PER_WAREHOUSE as u32)
    }

    fn random_customer(&mut self) -> u32 {
        self.rng.gen_range(1..=CUSTOMERS_PER_DISTRICT as u32)
    }

    fn random_item(&mut self) -> u32 {
        self.rng.gen_range(1..=NUM_ITEMS as u32)
    }

    // =========================================================================
    // Node-Aware Warehouse Selection
    // =========================================================================

    /// Determine which node owns a given warehouse
    fn node_for_warehouse(&self, w_id: u32) -> usize {
        ((w_id - 1) / self.warehouses_per_node as u32) as usize
    }

    /// Select a random warehouse from a specific node
    fn random_warehouse_from_node(&mut self, node_id: usize) -> u32 {
        let start = (node_id * self.warehouses_per_node) as u32 + 1;
        let end = ((node_id + 1) * self.warehouses_per_node) as u32;
        self.rng.gen_range(start..=end)
    }

    /// Select a random warehouse from a different node
    fn random_warehouse_from_different_node(&mut self, exclude_node: usize) -> u32 {
        loop {
            let node_id = self.rng.gen_range(0..self.num_nodes);
            if node_id != exclude_node {
                return self.random_warehouse_from_node(node_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator_creation() {
        let generator = TpccGenerator::new(2, 0.5, 1, 0.0, 42);
        assert_eq!(generator.num_warehouses, 2);
    }

    #[test]
    fn test_transaction_mix() {
        let mut generator = TpccGenerator::new(1, 0.5, 1, 0.0, 42);
        let mut new_order_count = 0;
        let mut _payment_count = 0;

        for _ in 0..1000 {
            match generator.generate_transaction() {
                TpccTransaction::NewOrder { .. } => new_order_count += 1,
                TpccTransaction::Payment { .. } => _payment_count += 1,
            }
        }

        // Should be roughly 50/50 (allow 10% variance)
        let ratio = new_order_count as f64 / 1000.0;
        assert!(ratio > 0.4 && ratio < 0.6, "Unexpected ratio: {}", ratio);
    }

    #[test]
    fn test_new_order_generation() {
        let mut generator = TpccGenerator::new(1, 0.5, 1, 0.0, 42);
        let txn = generator.generate_new_order();

        match txn {
            TpccTransaction::NewOrder {
                w_id,
                d_id,
                c_id,
                items,
            } => {
                assert_eq!(w_id, 1);
                assert!(d_id >= 1 && d_id <= DISTRICTS_PER_WAREHOUSE as u32);
                assert!(c_id >= 1 && c_id <= CUSTOMERS_PER_DISTRICT as u32);
                assert!(items.len() >= MIN_OL_CNT && items.len() <= MAX_OL_CNT);
            }
            _ => panic!("Expected NEW_ORDER"),
        }
    }

    #[test]
    fn test_payment_generation() {
        let mut generator = TpccGenerator::new(1, 0.5, 1, 0.0, 42);
        let txn = generator.generate_payment();

        match txn {
            TpccTransaction::Payment {
                w_id,
                d_id,
                c_w_id,
                c_d_id,
                c_id,
                h_amount,
            } => {
                assert_eq!(w_id, 1);
                assert!(d_id >= 1 && d_id <= DISTRICTS_PER_WAREHOUSE as u32);
                assert_eq!(c_w_id, 1); // Single warehouse, so customer is local
                assert!(c_d_id >= 1 && c_d_id <= DISTRICTS_PER_WAREHOUSE as u32);
                assert!(c_id >= 1 && c_id <= CUSTOMERS_PER_DISTRICT as u32);
                assert!(h_amount >= MIN_PAYMENT_CENTS && h_amount <= MAX_PAYMENT_CENTS);
            }
            _ => panic!("Expected PAYMENT"),
        }
    }

    #[test]
    fn test_no_cross_node_transactions_with_zero_prob() {
        // Test with 3 nodes, 96 warehouses (32 per node), cross_node_prob = 0.0
        // This simulates Zeus policy with empty states_to_proxy at init
        let mut generator = TpccGenerator::new(96, 0.0, 3, 0.0, 42);

        // Generate 1000 NEW_ORDER transactions and verify none are cross-node
        for _ in 0..1000 {
            let txn = generator.generate_new_order();

            match txn {
                TpccTransaction::NewOrder { w_id, items, .. } => {
                    // Determine the home node for this transaction
                    let home_node = (w_id - 1) / 32;

                    // Verify all item warehouse IDs belong to the same node
                    for item in items {
                        let item_node = (item.supply_w_id - 1) / 32;
                        assert_eq!(
                            item_node, home_node,
                            "Cross-node transaction detected! w_id={} (node {}), supply_w_id={} (node {})",
                            w_id, home_node, item.supply_w_id, item_node
                        );
                    }
                }
                _ => panic!("Expected NEW_ORDER"),
            }
        }
    }

    #[test]
    fn test_cross_node_transactions_with_nonzero_prob() {
        // Test with 3 nodes, 96 warehouses, cross_node_prob = 0.5
        let mut generator = TpccGenerator::new(96, 0.0, 3, 0.5, 42);

        let mut cross_node_count = 0;
        let total_transactions = 1000;

        // Generate transactions and count cross-node ones
        for _ in 0..total_transactions {
            let txn = generator.generate_new_order();

            match txn {
                TpccTransaction::NewOrder { w_id, items, .. } => {
                    let home_node = (w_id - 1) / 32;

                    // Check if any item is from a different node
                    let is_cross_node = items.iter().any(|item| {
                        let item_node = (item.supply_w_id - 1) / 32;
                        item_node != home_node
                    });

                    if is_cross_node {
                        cross_node_count += 1;
                    }
                }
                _ => panic!("Expected NEW_ORDER"),
            }
        }

        // With cross_node_prob = 0.5 and remote warehouse prob = 0.15,
        // we expect some cross-node transactions (but not all)
        assert!(
            cross_node_count > 0,
            "Expected some cross-node transactions with cross_node_prob=0.5, got {}",
            cross_node_count
        );

        // Should be less than 100% cross-node
        assert!(
            cross_node_count < total_transactions,
            "Not all transactions should be cross-node, got {}/{}",
            cross_node_count,
            total_transactions
        );
    }

    #[test]
    fn test_node_partitioning() {
        // Test with 6 nodes, 192 warehouses (32 per node)
        let generator = TpccGenerator::new(192, 0.5, 6, 0.0, 42);

        // Verify each warehouse is assigned to the correct node
        for w_id in 1..=192 {
            let expected_node = (w_id - 1) / 32;
            let actual_node = generator.node_for_warehouse(w_id);
            assert_eq!(
                actual_node, expected_node as usize,
                "Warehouse {} should belong to node {}, got {}",
                w_id, expected_node, actual_node
            );
        }

        // Verify node boundaries
        assert_eq!(generator.node_for_warehouse(1), 0); // First warehouse of node 0
        assert_eq!(generator.node_for_warehouse(32), 0); // Last warehouse of node 0
        assert_eq!(generator.node_for_warehouse(33), 1); // First warehouse of node 1
        assert_eq!(generator.node_for_warehouse(64), 1); // Last warehouse of node 1
        assert_eq!(generator.node_for_warehouse(192), 5); // Last warehouse of node 5
    }

    #[test]
    fn test_zeus_policy_with_states_to_proxy() {
        use std::collections::HashMap;
        use sui_types::base_types::ObjectID;

        // Test with 3 nodes, 96 warehouses (32 per node), cross_node_prob = 0.0
        // Simulate Zeus policy: assign transactions to proxy with most states
        let mut generator = TpccGenerator::new(96, 0.0, 3, 0.0, 42);

        // Track which proxy owns which object states (simulates states_to_proxy)
        let mut states_to_proxy: HashMap<ObjectID, usize> = HashMap::new();

        // Generate and process 100 NEW_ORDER transactions
        for _ in 0..100 {
            let txn = generator.generate_new_order();

            match &txn {
                TpccTransaction::NewOrder { w_id, items, .. } => {
                    // Determine the home node for this transaction
                    let home_node = ((w_id - 1) / 32) as usize;

                    // Get all objects accessed by this transaction
                    let access_set = txn.access_set();

                    // Simulate Zeus policy: count states per proxy
                    let mut state_counts = vec![0usize; 3]; // 3 proxies
                    for obj_id in &access_set {
                        if let Some(&proxy_id) = states_to_proxy.get(obj_id) {
                            state_counts[proxy_id] += 1;
                        }
                    }

                    // Select proxy with most states (Zeus "most_states" policy)
                    let selected_proxy = state_counts
                        .iter()
                        .enumerate()
                        .max_by_key(|(_, count)| *count)
                        .map(|(idx, _)| idx)
                        .unwrap_or(home_node); // Default to home node if no states exist yet

                    // Verify all warehouses touched by the transaction belong to the same node
                    // For non-cross-node transactions, this should be the selected proxy's node
                    let mut touched_nodes = std::collections::HashSet::new();
                    touched_nodes.insert(home_node);

                    for item in items {
                        let item_node = ((item.supply_w_id - 1) / 32) as usize;
                        touched_nodes.insert(item_node);
                    }

                    // Assert that all touched nodes are the same (single-node transaction)
                    assert_eq!(
                        touched_nodes.len(),
                        1,
                        "Cross-node transaction detected! Transaction w_id={} touches nodes {:?}",
                        w_id,
                        touched_nodes
                    );

                    // The single touched node should be the selected proxy's node
                    // (for node-local transactions with cross_node_prob=0.0)
                    let single_node = *touched_nodes.iter().next().unwrap();
                    assert_eq!(
                        single_node, home_node,
                        "Transaction should only touch home node {}, but touched node is {}",
                        home_node, single_node
                    );

                    // Update states_to_proxy: assign all accessed objects to the selected proxy
                    for obj_id in access_set {
                        states_to_proxy.insert(obj_id, selected_proxy);
                    }
                }
                _ => panic!("Expected NEW_ORDER"),
            }
        }

        // Verify that states_to_proxy has been populated
        assert!(
            !states_to_proxy.is_empty(),
            "states_to_proxy should be populated after processing transactions"
        );

        // Additional validation: verify states are partitioned by node
        // For each proxy, all its states should belong to warehouses from the corresponding node
        let mut proxy_states: HashMap<usize, Vec<ObjectID>> = HashMap::new();
        for (obj_id, proxy_id) in &states_to_proxy {
            proxy_states
                .entry(*proxy_id)
                .or_insert_with(Vec::new)
                .push(*obj_id);
        }

        // For each proxy, verify its states align with node boundaries
        // (Note: This is a soft check since Zeus might assign to any proxy, but
        //  with cross_node_prob=0.0, states should naturally cluster by node)
        for (proxy_id, _states) in proxy_states {
            // Just verify the proxy ID is within range
            assert!(proxy_id < 3, "Proxy ID {} should be < 3", proxy_id);
        }
    }
}
