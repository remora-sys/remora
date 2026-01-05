// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TPC-C data model and state management.
//!
//! Maps TPC-C tables to Remora's Object model with deterministic ObjectIDs.

use dashmap::DashMap;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use sui_types::base_types::ObjectID;

use super::constants::*;

// =============================================================================
// Table Records
// =============================================================================

/// Warehouse record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Warehouse {
    pub w_id: u32,
    pub w_ytd: f64,
    pub w_tax: f64,
    pub w_name: String,
}

/// District record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct District {
    pub d_id: u32,
    pub d_w_id: u32,
    pub d_ytd: f64,
    pub d_tax: f64,
    pub d_next_o_id: u32,
    pub d_name: String,
}

/// Customer record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Customer {
    pub c_id: u32,
    pub c_d_id: u32,
    pub c_w_id: u32,
    pub c_balance: f64,
    pub c_ytd_payment: f64,
    pub c_payment_cnt: u32,
    pub c_discount: f64,
    pub c_first: String,
    pub c_last: String,
}

/// Stock record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stock {
    pub s_i_id: u32,
    pub s_w_id: u32,
    pub s_quantity: i32,
    pub s_ytd: u32,
    pub s_order_cnt: u32,
    pub s_remote_cnt: u32,
}

/// Item record (read-only)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Item {
    pub i_id: u32,
    pub i_price: f64,
    pub i_name: String,
    pub i_data: String,
}

// =============================================================================
// TPC-C State
// =============================================================================

/// Complete TPC-C database state for a given number of warehouses.
/// Uses DashMap for concurrent mutable access during transaction execution.
#[derive(Clone, Debug)]
pub struct TpccState {
    pub num_warehouses: usize,
    pub warehouses: DashMap<u32, Warehouse>,
    pub districts: DashMap<(u32, u32), District>,
    pub customers: DashMap<(u32, u32, u32), Customer>,
    pub stock: DashMap<(u32, u32), Stock>,
    pub items: HashMap<u32, Item>, // Items are read-only, no need for DashMap
    /// Per-district atomic counters for order IDs (FastIds optimization)
    /// Key: (warehouse_id, district_id) -> atomic counter
    order_id_counters: HashMap<(u32, u32), Arc<AtomicU32>>,
}

impl TpccState {
    /// Create a new TPC-C state with the given number of warehouses.
    pub fn new(num_warehouses: usize) -> Self {
        let mut rng = StdRng::seed_from_u64(42);
        let mut state = Self {
            num_warehouses,
            warehouses: DashMap::new(),
            districts: DashMap::new(),
            customers: DashMap::new(),
            stock: DashMap::new(),
            items: HashMap::new(),
            order_id_counters: HashMap::new(),
        };

        // Generate items (shared across all warehouses)
        for i_id in 1..=NUM_ITEMS as u32 {
            state.items.insert(
                i_id,
                Item {
                    i_id,
                    i_price: rng.gen_range(MIN_PRICE..=MAX_PRICE),
                    i_name: format!("Item_{}", i_id),
                    i_data: format!("ItemData_{}", i_id),
                },
            );
        }

        // Generate per-warehouse data
        for w_id in 1..=num_warehouses as u32 {
            // Warehouse
            state.warehouses.insert(
                w_id,
                Warehouse {
                    w_id,
                    w_ytd: INITIAL_W_YTD,
                    w_tax: rng.gen_range(MIN_TAX..=MAX_TAX),
                    w_name: format!("Warehouse_{}", w_id),
                },
            );

            // Districts
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE as u32 {
                state.districts.insert(
                    (w_id, d_id),
                    District {
                        d_id,
                        d_w_id: w_id,
                        d_ytd: INITIAL_D_YTD,
                        d_tax: rng.gen_range(MIN_TAX..=MAX_TAX),
                        d_next_o_id: INITIAL_NEXT_O_ID,
                        d_name: format!("District_{}_{}", w_id, d_id),
                    },
                );

                // Initialize atomic order ID counter for this district (FastIds)
                state
                    .order_id_counters
                    .insert((w_id, d_id), Arc::new(AtomicU32::new(INITIAL_NEXT_O_ID)));

                // Customers
                for c_id in 1..=CUSTOMERS_PER_DISTRICT as u32 {
                    state.customers.insert(
                        (w_id, d_id, c_id),
                        Customer {
                            c_id,
                            c_d_id: d_id,
                            c_w_id: w_id,
                            c_balance: INITIAL_BALANCE,
                            c_ytd_payment: INITIAL_YTD_PAYMENT,
                            c_payment_cnt: INITIAL_PAYMENT_CNT,
                            c_discount: rng.gen_range(MIN_DISCOUNT..=MAX_DISCOUNT),
                            c_first: format!("First_{}", c_id),
                            c_last: format!("Last_{}", c_id),
                        },
                    );
                }
            }

            // Stock
            for i_id in 1..=STOCK_PER_WAREHOUSE as u32 {
                state.stock.insert(
                    (w_id, i_id),
                    Stock {
                        s_i_id: i_id,
                        s_w_id: w_id,
                        s_quantity: rng.gen_range(MIN_QUANTITY..=MAX_QUANTITY),
                        s_ytd: 0,
                        s_order_cnt: 0,
                        s_remote_cnt: 0,
                    },
                );
            }
        }

        state
    }

    // =========================================================================
    // FastIds: Atomic Order ID Generation
    // =========================================================================

    /// Get the next order ID for a district using atomic fetch-and-add.
    /// This is the "FastIds" optimization from Silo/Caracal.
    pub fn next_order_id(&self, w_id: u32, d_id: u32) -> u32 {
        self.order_id_counters
            .get(&(w_id, d_id))
            .expect("Order ID counter not found for district")
            .fetch_add(1, Ordering::SeqCst)
    }

    // =========================================================================
    // ObjectID Generation (deterministic based on primary key)
    // =========================================================================

    /// Generate ObjectID for a warehouse
    pub fn object_id_for_warehouse(w_id: u32) -> ObjectID {
        Self::make_object_id(&format!("W_{}", w_id))
    }

    /// Generate ObjectID for a district
    pub fn object_id_for_district(w_id: u32, d_id: u32) -> ObjectID {
        Self::make_object_id(&format!("D_{}_{}", w_id, d_id))
    }

    /// Generate ObjectID for a customer
    pub fn object_id_for_customer(w_id: u32, d_id: u32, c_id: u32) -> ObjectID {
        Self::make_object_id(&format!("C_{}_{}_{}", w_id, d_id, c_id))
    }

    /// Generate ObjectID for a stock record
    pub fn object_id_for_stock(w_id: u32, i_id: u32) -> ObjectID {
        Self::make_object_id(&format!("S_{}_{}", w_id, i_id))
    }

    /// Generate ObjectID for an item
    pub fn object_id_for_item(i_id: u32) -> ObjectID {
        Self::make_object_id(&format!("I_{}", i_id))
    }

    /// Create a deterministic ObjectID from a string key
    fn make_object_id(key: &str) -> ObjectID {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        let mut bytes = [0u8; ObjectID::LENGTH];
        let hash_bytes = hash.to_le_bytes();
        bytes[..8].copy_from_slice(&hash_bytes);
        // Fill remaining bytes with a pattern based on key length
        for (i, byte) in bytes[8..].iter_mut().enumerate() {
            *byte = ((hash >> (i % 8)) & 0xFF) as u8;
        }

        ObjectID::from_bytes(bytes).expect("Invalid ObjectID bytes")
    }

    // =========================================================================
    // Object ID Collections
    // =========================================================================

    /// Get all warehouse ObjectIDs
    pub fn all_warehouse_ids(&self) -> Vec<ObjectID> {
        (1..=self.num_warehouses as u32)
            .map(Self::object_id_for_warehouse)
            .collect()
    }

    /// Get all district ObjectIDs
    pub fn all_district_ids(&self) -> Vec<ObjectID> {
        self.districts
            .iter()
            .map(|entry| Self::object_id_for_district(entry.key().0, entry.key().1))
            .collect()
    }

    /// Get all customer ObjectIDs
    pub fn all_customer_ids(&self) -> Vec<ObjectID> {
        self.customers
            .iter()
            .map(|entry| Self::object_id_for_customer(entry.key().0, entry.key().1, entry.key().2))
            .collect()
    }

    /// Get all stock ObjectIDs
    pub fn all_stock_ids(&self) -> Vec<ObjectID> {
        self.stock
            .iter()
            .map(|entry| Self::object_id_for_stock(entry.key().0, entry.key().1))
            .collect()
    }

    /// Get all item ObjectIDs
    pub fn all_item_ids(&self) -> Vec<ObjectID> {
        self.items
            .keys()
            .map(|i_id| Self::object_id_for_item(*i_id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_creation() {
        let state = TpccState::new(1);

        assert_eq!(state.warehouses.len(), 1);
        assert_eq!(state.districts.len(), DISTRICTS_PER_WAREHOUSE);
        assert_eq!(
            state.customers.len(),
            DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT
        );
        assert_eq!(state.stock.len(), STOCK_PER_WAREHOUSE);
        assert_eq!(state.items.len(), NUM_ITEMS);
    }

    #[test]
    fn test_object_id_deterministic() {
        let id1 = TpccState::object_id_for_warehouse(1);
        let id2 = TpccState::object_id_for_warehouse(1);
        assert_eq!(id1, id2);

        let id3 = TpccState::object_id_for_warehouse(2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_object_id_uniqueness() {
        let w_id = TpccState::object_id_for_warehouse(1);
        let d_id = TpccState::object_id_for_district(1, 1);
        let c_id = TpccState::object_id_for_customer(1, 1, 1);
        let s_id = TpccState::object_id_for_stock(1, 1);
        let i_id = TpccState::object_id_for_item(1);

        // All IDs should be unique
        let ids = vec![w_id, d_id, c_id, s_id, i_id];
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                assert_ne!(ids[i], ids[j], "IDs at {} and {} should be different", i, j);
            }
        }
    }
}
