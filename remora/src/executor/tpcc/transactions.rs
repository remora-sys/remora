// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TPC-C transaction types and their access patterns.
//!
//! Only NEW_ORDER and PAYMENT are implemented (static read/write sets).

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;

use super::data::TpccState;

// =============================================================================
// Order Item (for NEW_ORDER transaction)
// =============================================================================

/// An item in a new order
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderItem {
    /// Item ID
    pub i_id: u32,
    /// Supply warehouse ID (may differ from order's warehouse for remote orders)
    pub supply_w_id: u32,
    /// Quantity ordered
    pub quantity: u32,
}

// =============================================================================
// Transaction Types
// =============================================================================

/// TPC-C transaction parameters
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TpccTransaction {
    /// NEW_ORDER transaction: creates a new order with 5-15 items
    NewOrder {
        /// Warehouse ID
        w_id: u32,
        /// District ID
        d_id: u32,
        /// Customer ID
        c_id: u32,
        /// Order items (5-15 items)
        items: Vec<OrderItem>,
    },

    /// PAYMENT transaction: records a customer payment
    Payment {
        /// Warehouse ID
        w_id: u32,
        /// District ID
        d_id: u32,
        /// Customer's warehouse ID (may differ for remote customers)
        c_w_id: u32,
        /// Customer's district ID
        c_d_id: u32,
        /// Customer ID
        c_id: u32,
        /// Payment amount
        h_amount: f64,
    },
}

impl TpccTransaction {
    /// Get the transaction type name
    pub fn name(&self) -> &'static str {
        match self {
            TpccTransaction::NewOrder { .. } => "NEW_ORDER",
            TpccTransaction::Payment { .. } => "PAYMENT",
        }
    }

    /// Get all ObjectIDs that this transaction will read.
    pub fn read_set(&self) -> Vec<ObjectID> {
        match self {
            TpccTransaction::NewOrder {
                w_id,
                d_id,
                c_id,
                items,
            } => {
                let mut reads = Vec::new();

                // Read warehouse (for tax)
                reads.push(TpccState::object_id_for_warehouse(*w_id));

                // Read customer (for discount)
                reads.push(TpccState::object_id_for_customer(*w_id, *d_id, *c_id));

                // Read items and stock for each order line
                for item in items {
                    reads.push(TpccState::object_id_for_item(item.i_id));
                    reads.push(TpccState::object_id_for_stock(item.supply_w_id, item.i_id));
                }

                reads
            }

            TpccTransaction::Payment {
                w_id,
                d_id,
                c_w_id,
                c_d_id,
                c_id,
                ..
            } => {
                vec![
                    TpccState::object_id_for_warehouse(*w_id),
                    TpccState::object_id_for_district(*w_id, *d_id),
                    TpccState::object_id_for_customer(*c_w_id, *c_d_id, *c_id),
                ]
            }
        }
    }

    /// Get all ObjectIDs that this transaction will write.
    pub fn write_set(&self) -> Vec<ObjectID> {
        match self {
            TpccTransaction::NewOrder {
                w_id, d_id, items, ..
            } => {
                let mut writes = Vec::new();

                // Write district (increment next_o_id)
                writes.push(TpccState::object_id_for_district(*w_id, *d_id));

                // Write stock for each order line
                for item in items {
                    writes.push(TpccState::object_id_for_stock(item.supply_w_id, item.i_id));
                }

                writes
            }

            TpccTransaction::Payment {
                w_id,
                d_id,
                c_w_id,
                c_d_id,
                c_id,
                ..
            } => {
                vec![
                    TpccState::object_id_for_warehouse(*w_id),
                    TpccState::object_id_for_district(*w_id, *d_id),
                    TpccState::object_id_for_customer(*c_w_id, *c_d_id, *c_id),
                ]
            }
        }
    }

    /// Get all unique ObjectIDs accessed by this transaction (union of read and write sets).
    pub fn access_set(&self) -> Vec<ObjectID> {
        let mut ids = self.read_set();
        ids.extend(self.write_set());

        // Deduplicate while preserving order
        let mut seen = std::collections::HashSet::new();
        ids.retain(|id| seen.insert(*id));
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_order_access_set() {
        let txn = TpccTransaction::NewOrder {
            w_id: 1,
            d_id: 1,
            c_id: 100,
            items: vec![
                OrderItem {
                    i_id: 1,
                    supply_w_id: 1,
                    quantity: 5,
                },
                OrderItem {
                    i_id: 2,
                    supply_w_id: 1,
                    quantity: 3,
                },
            ],
        };

        let reads = txn.read_set();
        let writes = txn.write_set();

        // Reads: warehouse, customer, 2 items, 2 stocks = 6
        assert_eq!(reads.len(), 6);

        // Writes: district, 2 stocks = 3
        assert_eq!(writes.len(), 3);
    }

    #[test]
    fn test_payment_access_set() {
        let txn = TpccTransaction::Payment {
            w_id: 1,
            d_id: 1,
            c_w_id: 1,
            c_d_id: 1,
            c_id: 100,
            h_amount: 100.0,
        };

        let reads = txn.read_set();
        let writes = txn.write_set();

        // Reads: warehouse, district, customer = 3
        assert_eq!(reads.len(), 3);

        // Writes: warehouse, district, customer = 3
        assert_eq!(writes.len(), 3);
    }

    #[test]
    fn test_access_set_deduplication() {
        let txn = TpccTransaction::Payment {
            w_id: 1,
            d_id: 1,
            c_w_id: 1,
            c_d_id: 1,
            c_id: 100,
            h_amount: 100.0,
        };

        let access = txn.access_set();

        // Should be deduplicated: warehouse, district, customer = 3
        assert_eq!(access.len(), 3);
    }
}
