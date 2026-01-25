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
    rng: StdRng,
    /// Number of nodes in the cluster
    num_nodes: usize,
    /// Hotspot percentage (0.0 = no hotspot, 1.0 = all to hotspot)
    hotspot_percentage: f64,
    /// Rotation interval in seconds (0 = no rotation)
    rotation_interval_secs: u64,
    /// Current hotspot node ID (0-indexed)
    current_hotspot_node: usize,
    /// Transaction count since last rotation (for time estimation)
    txn_count_since_rotation: u64,
    /// Expected transactions per second (for time estimation)
    expected_tps: u64,
}

impl TpccGenerator {
    /// Create a new TPC-C generator
    pub fn new(
        num_warehouses: usize,
        payment_ratio: f64,
        num_nodes: usize,
        hotspot_percentage: f64,
        rotation_interval_secs: u64,
        expected_tps: u64,
        seed: u64,
    ) -> Self {
        Self {
            num_warehouses,
            payment_ratio,
            rng: StdRng::seed_from_u64(seed),
            num_nodes: num_nodes.max(1), // At least 1 node
            hotspot_percentage: hotspot_percentage.clamp(0.0, 1.0),
            rotation_interval_secs,
            current_hotspot_node: 0,
            txn_count_since_rotation: 0,
            expected_tps,
        }
    }

    /// Generate a random transaction following configured payment ratio
    pub fn generate_transaction(&mut self) -> TpccTransaction {
        // Check if we should rotate hotspot (do this once per transaction)
        self.maybe_rotate_hotspot();

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

        // Generate 5-15 order items
        let ol_cnt = self.rng.gen_range(MIN_OL_CNT..=MAX_OL_CNT);
        let items: Vec<OrderItem> = (0..ol_cnt)
            .map(|_| {
                let i_id = self.random_item();
                let supply_w_id =
                    if self.num_warehouses > 1 && self.rng.gen_bool(REMOTE_WAREHOUSE_PROB) {
                        self.random_warehouse_excluding(w_id)
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

    /// Select a warehouse with hotspot consideration
    fn random_warehouse(&mut self) -> u32 {
        // If no hotspot configured, use uniform distribution
        if self.hotspot_percentage == 0.0 || self.num_nodes <= 1 {
            return self.rng.gen_range(1..=self.num_warehouses as u32);
        }

        // With hotspot_percentage probability, select from hotspot node
        if self.rng.gen_bool(self.hotspot_percentage) {
            self.random_warehouse_from_node(self.current_hotspot_node)
        } else {
            // Select from any warehouse uniformly
            self.rng.gen_range(1..=self.num_warehouses as u32)
        }
    }

    /// Determine which node owns a given warehouse
    /// Uses simple modulo-based partitioning
    fn warehouse_to_node(&self, warehouse_id: u32) -> usize {
        ((warehouse_id - 1) as usize) % self.num_nodes
    }

    /// Get all warehouses on a specific node
    fn warehouses_on_node(&self, node_id: usize) -> Vec<u32> {
        (1..=self.num_warehouses as u32)
            .filter(|w_id| self.warehouse_to_node(*w_id) == node_id)
            .collect()
    }

    /// Select a random warehouse from a specific node
    fn random_warehouse_from_node(&mut self, node_id: usize) -> u32 {
        let warehouses = self.warehouses_on_node(node_id);
        if warehouses.is_empty() {
            // Fallback: select any warehouse
            return self.rng.gen_range(1..=self.num_warehouses as u32);
        }
        let idx = self.rng.gen_range(0..warehouses.len());
        warehouses[idx]
    }

    /// Check if hotspot should rotate and perform rotation if needed
    fn maybe_rotate_hotspot(&mut self) {
        // Skip rotation if disabled or only one node
        if self.rotation_interval_secs == 0 || self.num_nodes <= 1 {
            return;
        }

        // Estimate elapsed time based on transaction count and expected TPS
        // This approach works well in multi-threaded scenarios
        self.txn_count_since_rotation += 1;

        let txns_per_rotation = self.expected_tps * self.rotation_interval_secs;
        if self.txn_count_since_rotation >= txns_per_rotation {
            // Rotate to next node (round-robin)
            self.current_hotspot_node = (self.current_hotspot_node + 1) % self.num_nodes;
            self.txn_count_since_rotation = 0;

            tracing::info!(
                "Hotspot rotated to node {} (warehouses: {:?})",
                self.current_hotspot_node,
                self.warehouses_on_node(self.current_hotspot_node)
            );
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator_creation() {
        let generator = TpccGenerator::new(2, 0.5, 1, 0.0, 0, 1000, 42);
        assert_eq!(generator.num_warehouses, 2);
    }

    #[test]
    fn test_transaction_mix() {
        let mut generator = TpccGenerator::new(1, 0.5, 1, 0.0, 0, 1000, 42);
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
        let mut generator = TpccGenerator::new(1, 0.5, 1, 0.0, 0, 1000, 42);
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
        let mut generator = TpccGenerator::new(1, 0.5, 1, 0.0, 0, 1000, 42);
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
    fn test_warehouse_to_node_mapping() {
        let generator = TpccGenerator::new(8, 0.5, 4, 0.0, 0, 1000, 42);

        // Verify modulo-based partitioning
        assert_eq!(generator.warehouse_to_node(1), 0);
        assert_eq!(generator.warehouse_to_node(2), 1);
        assert_eq!(generator.warehouse_to_node(3), 2);
        assert_eq!(generator.warehouse_to_node(4), 3);
        assert_eq!(generator.warehouse_to_node(5), 0);
        assert_eq!(generator.warehouse_to_node(6), 1);
        assert_eq!(generator.warehouse_to_node(7), 2);
        assert_eq!(generator.warehouse_to_node(8), 3);
    }

    #[test]
    fn test_hotspot_distribution() {
        let mut generator = TpccGenerator::new(8, 0.5, 4, 0.8, 0, 1000, 42);

        let mut hotspot_count = 0;
        let total = 1000;

        for _ in 0..total {
            let w_id = generator.random_warehouse();
            if generator.warehouse_to_node(w_id) == generator.current_hotspot_node {
                hotspot_count += 1;
            }
        }

        let hotspot_ratio = hotspot_count as f64 / total as f64;
        // Should be approximately 80% (allow 10% variance)
        assert!(
            hotspot_ratio > 0.7 && hotspot_ratio < 0.9,
            "Hotspot ratio: {}",
            hotspot_ratio
        );
    }

    #[test]
    fn test_hotspot_rotation() {
        let mut generator = TpccGenerator::new(8, 0.5, 4, 0.8, 20, 100, 42);

        assert_eq!(generator.current_hotspot_node, 0);

        // Generate 2000 transactions (20s * 100 TPS)
        for _ in 0..2000 {
            generator.generate_transaction();
        }

        // Should have rotated to node 1
        assert_eq!(generator.current_hotspot_node, 1);

        // Generate another 2000 transactions
        for _ in 0..2000 {
            generator.generate_transaction();
        }

        // Should have rotated to node 2
        assert_eq!(generator.current_hotspot_node, 2);
    }

    #[test]
    fn test_no_hotspot_uniform_distribution() {
        let mut generator = TpccGenerator::new(8, 0.5, 4, 0.0, 0, 1000, 42);

        let mut node_counts = vec![0; 4];
        let total = 1000;

        for _ in 0..total {
            let w_id = generator.random_warehouse();
            let node = generator.warehouse_to_node(w_id);
            node_counts[node] += 1;
        }

        // With no hotspot, distribution should be roughly uniform across nodes
        // Each node should get around 250 ±100 transactions
        for count in node_counts {
            let ratio = count as f64 / total as f64;
            assert!(ratio > 0.15 && ratio < 0.35, "Node ratio: {}", ratio);
        }
    }
}
