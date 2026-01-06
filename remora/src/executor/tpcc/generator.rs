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
}

impl TpccGenerator {
    /// Create a new TPC-C generator
    pub fn new(num_warehouses: usize, payment_ratio: f64, seed: u64) -> Self {
        Self {
            num_warehouses,
            payment_ratio,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator_creation() {
        let generator = TpccGenerator::new(2, 0.5, 42);
        assert_eq!(generator.num_warehouses, 2);
    }

    #[test]
    fn test_transaction_mix() {
        let mut generator = TpccGenerator::new(1, 0.5, 42);
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
        let mut generator = TpccGenerator::new(1, 0.5, 42);
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
        let mut generator = TpccGenerator::new(1, 0.5, 42);
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
}
