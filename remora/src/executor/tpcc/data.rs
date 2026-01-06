// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TPC-C data model and object encoding.
//!
//! Maps TPC-C tables to Remora's Object model with deterministic ObjectIDs.
//! Business data is stored in MoveObject contents (BCS), with ObjectID first.

use move_core_types::account_address::AccountAddress;
use move_core_types::identifier::Identifier;
use move_core_types::language_storage::StructTag;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sui_types::base_types::{MoveObjectType, ObjectID, SequenceNumber};
use sui_types::digests::TransactionDigest;
use sui_types::object::{MoveObject, Object, Owner};

use super::constants::*;

const TPCC_MODULE_NAME: &str = "tpcc";
const MAX_TPCC_OBJECT_SIZE: u64 = 16 * 1024;

// =============================================================================
// Table Records
// =============================================================================

/// Warehouse record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Warehouse {
    pub w_id: u32,
    pub w_ytd: i64,
    pub w_tax: u32,
    pub w_name: String,
}

/// District record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct District {
    pub d_id: u32,
    pub d_w_id: u32,
    pub d_ytd: i64,
    pub d_tax: u32,
    pub d_next_o_id: u32,
    pub d_name: String,
}

/// Customer record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Customer {
    pub c_id: u32,
    pub c_d_id: u32,
    pub c_w_id: u32,
    pub c_balance: i64,
    pub c_ytd_payment: i64,
    pub c_payment_cnt: u32,
    pub c_discount: u32,
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
    pub i_price: u32,
    pub i_name: String,
    pub i_data: String,
}

// =============================================================================
// Object Encoding
// =============================================================================

/// Wrapper that ensures ObjectID is first in BCS encoding.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TpccObject<T> {
    pub id: ObjectID,
    pub data: T,
}

#[derive(Clone, Copy, Debug)]
pub enum TpccObjectKind {
    Warehouse,
    District,
    Customer,
    Stock,
    Item,
}

fn tpcc_struct_tag(kind: TpccObjectKind) -> StructTag {
    let name = match kind {
        TpccObjectKind::Warehouse => "Warehouse",
        TpccObjectKind::District => "District",
        TpccObjectKind::Customer => "Customer",
        TpccObjectKind::Stock => "Stock",
        TpccObjectKind::Item => "Item",
    };

    StructTag {
        address: AccountAddress::ZERO,
        module: Identifier::new(TPCC_MODULE_NAME).expect("valid module name"),
        name: Identifier::new(name).expect("valid struct name"),
        type_params: Vec::new(),
    }
}

fn tpcc_move_object_type(kind: TpccObjectKind) -> MoveObjectType {
    MoveObjectType::from(tpcc_struct_tag(kind))
}

pub fn encode_tpcc_object<T: Serialize>(
    id: ObjectID,
    version: SequenceNumber,
    kind: TpccObjectKind,
    data: T,
) -> Object {
    let payload = TpccObject { id, data };
    let contents = bcs::to_bytes(&payload).expect("Failed to serialize TPCC object");
    let move_obj = unsafe {
        MoveObject::new_from_execution_with_limit(
            tpcc_move_object_type(kind),
            true,
            version,
            contents,
            MAX_TPCC_OBJECT_SIZE,
        )
        .expect("Failed to build TPCC MoveObject")
    };

    let owner = Owner::Shared {
        initial_shared_version: version,
    };
    Object::new_move(move_obj, owner, TransactionDigest::genesis_marker())
}

pub fn decode_tpcc_object<T: DeserializeOwned>(object: &Object) -> TpccObject<T> {
    let move_obj = object
        .as_inner()
        .data
        .try_as_move()
        .expect("TPCC object must be a Move object");
    bcs::from_bytes(move_obj.contents()).expect("Failed to deserialize TPCC object")
}

pub fn decode_tpcc_data<T: DeserializeOwned>(object: &Object, kind: TpccObjectKind) -> T {
    let move_obj = object
        .as_inner()
        .data
        .try_as_move()
        .expect("TPCC object must be a Move object");
    debug_assert_eq!(move_obj.type_(), &tpcc_move_object_type(kind));

    let decoded: TpccObject<T> =
        bcs::from_bytes(move_obj.contents()).expect("Failed to deserialize TPCC object");
    debug_assert_eq!(decoded.id, object.id());
    decoded.data
}

// =============================================================================
// TPC-C State (ObjectID helpers only)
// =============================================================================

/// TPC-C state helper for deterministic ObjectID generation.
#[derive(Clone, Debug)]
pub struct TpccState {
    pub num_warehouses: usize,
}

impl TpccState {
    /// Create a new TPC-C state helper with the given number of warehouses.
    pub fn new(num_warehouses: usize) -> Self {
        Self { num_warehouses }
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
        let mut ids = Vec::with_capacity(self.num_warehouses * DISTRICTS_PER_WAREHOUSE);
        for w_id in 1..=self.num_warehouses as u32 {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE as u32 {
                ids.push(Self::object_id_for_district(w_id, d_id));
            }
        }
        ids
    }

    /// Get all customer ObjectIDs
    pub fn all_customer_ids(&self) -> Vec<ObjectID> {
        let mut ids = Vec::with_capacity(
            self.num_warehouses * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT,
        );
        for w_id in 1..=self.num_warehouses as u32 {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE as u32 {
                for c_id in 1..=CUSTOMERS_PER_DISTRICT as u32 {
                    ids.push(Self::object_id_for_customer(w_id, d_id, c_id));
                }
            }
        }
        ids
    }

    /// Get all stock ObjectIDs
    pub fn all_stock_ids(&self) -> Vec<ObjectID> {
        let mut ids = Vec::with_capacity(self.num_warehouses * STOCK_PER_WAREHOUSE);
        for w_id in 1..=self.num_warehouses as u32 {
            for i_id in 1..=STOCK_PER_WAREHOUSE as u32 {
                ids.push(Self::object_id_for_stock(w_id, i_id));
            }
        }
        ids
    }

    /// Get all item ObjectIDs
    pub fn all_item_ids(&self) -> Vec<ObjectID> {
        (1..=NUM_ITEMS as u32)
            .map(Self::object_id_for_item)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_creation() {
        let state = TpccState::new(1);

        assert_eq!(state.all_warehouse_ids().len(), 1);
        assert_eq!(state.all_district_ids().len(), DISTRICTS_PER_WAREHOUSE);
        assert_eq!(
            state.all_customer_ids().len(),
            DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT
        );
        assert_eq!(state.all_stock_ids().len(), STOCK_PER_WAREHOUSE);
        assert_eq!(state.all_item_ids().len(), NUM_ITEMS);
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

    // =========================================================================
    // encode_tpcc_object / decode_tpcc_data Round-Trip Tests
    // =========================================================================

    #[test]
    fn test_warehouse_encode_decode_round_trip() {
        let id = TpccState::object_id_for_warehouse(42);
        let version = SequenceNumber::from_u64(1);
        let warehouse = Warehouse {
            w_id: 42,
            w_ytd: 300_000_00,
            w_tax: 1500,
            w_name: "Test Warehouse".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Warehouse, warehouse.clone());

        // Decode and verify round-trip
        let decoded: Warehouse = decode_tpcc_data(&object, TpccObjectKind::Warehouse);
        assert_eq!(decoded.w_id, warehouse.w_id);
        assert_eq!(decoded.w_ytd, warehouse.w_ytd);
        assert_eq!(decoded.w_tax, warehouse.w_tax);
        assert_eq!(decoded.w_name, warehouse.w_name);
    }

    #[test]
    fn test_district_encode_decode_round_trip() {
        let id = TpccState::object_id_for_district(1, 5);
        let version = SequenceNumber::from_u64(2);
        let district = District {
            d_id: 5,
            d_w_id: 1,
            d_ytd: 30_000_00,
            d_tax: 1000,
            d_next_o_id: 3001,
            d_name: "Test District".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::District, district.clone());

        let decoded: District = decode_tpcc_data(&object, TpccObjectKind::District);
        assert_eq!(decoded.d_id, district.d_id);
        assert_eq!(decoded.d_w_id, district.d_w_id);
        assert_eq!(decoded.d_ytd, district.d_ytd);
        assert_eq!(decoded.d_tax, district.d_tax);
        assert_eq!(decoded.d_next_o_id, district.d_next_o_id);
        assert_eq!(decoded.d_name, district.d_name);
    }

    #[test]
    fn test_customer_encode_decode_round_trip() {
        let id = TpccState::object_id_for_customer(1, 2, 100);
        let version = SequenceNumber::from_u64(3);
        let customer = Customer {
            c_id: 100,
            c_d_id: 2,
            c_w_id: 1,
            c_balance: -10_00,
            c_ytd_payment: 10_00,
            c_payment_cnt: 1,
            c_discount: 500,
            c_first: "John".to_string(),
            c_last: "Doe".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Customer, customer.clone());

        let decoded: Customer = decode_tpcc_data(&object, TpccObjectKind::Customer);
        assert_eq!(decoded.c_id, customer.c_id);
        assert_eq!(decoded.c_d_id, customer.c_d_id);
        assert_eq!(decoded.c_w_id, customer.c_w_id);
        assert_eq!(decoded.c_balance, customer.c_balance);
        assert_eq!(decoded.c_ytd_payment, customer.c_ytd_payment);
        assert_eq!(decoded.c_payment_cnt, customer.c_payment_cnt);
        assert_eq!(decoded.c_discount, customer.c_discount);
        assert_eq!(decoded.c_first, customer.c_first);
        assert_eq!(decoded.c_last, customer.c_last);
    }

    #[test]
    fn test_stock_encode_decode_round_trip() {
        let id = TpccState::object_id_for_stock(1, 5000);
        let version = SequenceNumber::from_u64(4);
        let stock = Stock {
            s_i_id: 5000,
            s_w_id: 1,
            s_quantity: 50,
            s_ytd: 0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Stock, stock.clone());

        let decoded: Stock = decode_tpcc_data(&object, TpccObjectKind::Stock);
        assert_eq!(decoded.s_i_id, stock.s_i_id);
        assert_eq!(decoded.s_w_id, stock.s_w_id);
        assert_eq!(decoded.s_quantity, stock.s_quantity);
        assert_eq!(decoded.s_ytd, stock.s_ytd);
        assert_eq!(decoded.s_order_cnt, stock.s_order_cnt);
        assert_eq!(decoded.s_remote_cnt, stock.s_remote_cnt);
    }

    #[test]
    fn test_item_encode_decode_round_trip() {
        let id = TpccState::object_id_for_item(42);
        let version = SequenceNumber::from_u64(1);
        let item = Item {
            i_id: 42,
            i_price: 99_99,
            i_name: "Test Item".to_string(),
            i_data: "Original data".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Item, item.clone());

        let decoded: Item = decode_tpcc_data(&object, TpccObjectKind::Item);
        assert_eq!(decoded.i_id, item.i_id);
        assert_eq!(decoded.i_price, item.i_price);
        assert_eq!(decoded.i_name, item.i_name);
        assert_eq!(decoded.i_data, item.i_data);
    }

    #[test]
    fn test_decode_tpcc_object_preserves_id() {
        let id = TpccState::object_id_for_warehouse(99);
        let version = SequenceNumber::from_u64(1);
        let warehouse = Warehouse {
            w_id: 99,
            w_ytd: 0,
            w_tax: 0,
            w_name: "ID Test".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Warehouse, warehouse);

        // Use decode_tpcc_object to get the full TpccObject wrapper
        let decoded: TpccObject<Warehouse> = decode_tpcc_object(&object);
        assert_eq!(
            decoded.id, id,
            "Decoded ObjectID must match encoded ObjectID"
        );
        assert_eq!(decoded.data.w_id, 99);
    }

    // =========================================================================
    // ID-First Encoding Layout Validation
    // =========================================================================

    #[test]
    fn test_id_first_encoding_layout() {
        // Validates that ObjectID is BCS-encoded first in the MoveObject contents.
        // This is critical for Move object compatibility where `id` field must be first.
        let id = TpccState::object_id_for_warehouse(1);
        let version = SequenceNumber::from_u64(1);
        let warehouse = Warehouse {
            w_id: 1,
            w_ytd: 0,
            w_tax: 0,
            w_name: "Layout Test".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Warehouse, warehouse);
        let move_obj = object.as_inner().data.try_as_move().unwrap();
        let contents = move_obj.contents();

        // ObjectID is 32 bytes, and BCS encodes it as raw bytes (no length prefix for fixed-size)
        // The first 32 bytes of contents should be the ObjectID
        assert!(
            contents.len() >= ObjectID::LENGTH,
            "Contents must be at least ObjectID::LENGTH bytes"
        );
        let id_bytes: [u8; ObjectID::LENGTH] = contents[..ObjectID::LENGTH].try_into().unwrap();
        let extracted_id = ObjectID::from_bytes(id_bytes).unwrap();
        assert_eq!(
            extracted_id, id,
            "First 32 bytes of encoded content must be the ObjectID"
        );
    }

    // =========================================================================
    // Type Tag Validation After Move Object Migration
    // =========================================================================

    #[test]
    fn test_type_tag_warehouse() {
        let id = TpccState::object_id_for_warehouse(1);
        let version = SequenceNumber::from_u64(1);
        let warehouse = Warehouse {
            w_id: 1,
            w_ytd: 0,
            w_tax: 0,
            w_name: "Type Test".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Warehouse, warehouse);
        let move_obj = object.as_inner().data.try_as_move().unwrap();

        let expected_type = tpcc_move_object_type(TpccObjectKind::Warehouse);
        assert_eq!(
            move_obj.type_(),
            &expected_type,
            "MoveObject type must be Warehouse"
        );
    }

    #[test]
    fn test_type_tag_district() {
        let id = TpccState::object_id_for_district(1, 1);
        let version = SequenceNumber::from_u64(1);
        let district = District {
            d_id: 1,
            d_w_id: 1,
            d_ytd: 0,
            d_tax: 0,
            d_next_o_id: 1,
            d_name: "Type Test".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::District, district);
        let move_obj = object.as_inner().data.try_as_move().unwrap();

        let expected_type = tpcc_move_object_type(TpccObjectKind::District);
        assert_eq!(
            move_obj.type_(),
            &expected_type,
            "MoveObject type must be District"
        );
    }

    #[test]
    fn test_type_tag_customer() {
        let id = TpccState::object_id_for_customer(1, 1, 1);
        let version = SequenceNumber::from_u64(1);
        let customer = Customer {
            c_id: 1,
            c_d_id: 1,
            c_w_id: 1,
            c_balance: 0,
            c_ytd_payment: 0,
            c_payment_cnt: 0,
            c_discount: 0,
            c_first: "Test".to_string(),
            c_last: "Customer".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Customer, customer);
        let move_obj = object.as_inner().data.try_as_move().unwrap();

        let expected_type = tpcc_move_object_type(TpccObjectKind::Customer);
        assert_eq!(
            move_obj.type_(),
            &expected_type,
            "MoveObject type must be Customer"
        );
    }

    #[test]
    fn test_type_tag_stock() {
        let id = TpccState::object_id_for_stock(1, 1);
        let version = SequenceNumber::from_u64(1);
        let stock = Stock {
            s_i_id: 1,
            s_w_id: 1,
            s_quantity: 10,
            s_ytd: 0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Stock, stock);
        let move_obj = object.as_inner().data.try_as_move().unwrap();

        let expected_type = tpcc_move_object_type(TpccObjectKind::Stock);
        assert_eq!(
            move_obj.type_(),
            &expected_type,
            "MoveObject type must be Stock"
        );
    }

    #[test]
    fn test_type_tag_item() {
        let id = TpccState::object_id_for_item(1);
        let version = SequenceNumber::from_u64(1);
        let item = Item {
            i_id: 1,
            i_price: 100,
            i_name: "Test".to_string(),
            i_data: "Data".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Item, item);
        let move_obj = object.as_inner().data.try_as_move().unwrap();

        let expected_type = tpcc_move_object_type(TpccObjectKind::Item);
        assert_eq!(
            move_obj.type_(),
            &expected_type,
            "MoveObject type must be Item"
        );
    }

    #[test]
    fn test_object_id_matches_after_encode() {
        // Verifies that the Object's id() matches what was passed to encode_tpcc_object
        let id = TpccState::object_id_for_district(5, 3);
        let version = SequenceNumber::from_u64(10);
        let district = District {
            d_id: 3,
            d_w_id: 5,
            d_ytd: 1000,
            d_tax: 500,
            d_next_o_id: 100,
            d_name: "ID Match Test".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::District, district);

        assert_eq!(
            object.id(),
            id,
            "Object ID must match the ID passed to encode_tpcc_object"
        );
    }

    #[test]
    fn test_version_preserved_after_encode() {
        let id = TpccState::object_id_for_stock(2, 100);
        let version = SequenceNumber::from_u64(42);
        let stock = Stock {
            s_i_id: 100,
            s_w_id: 2,
            s_quantity: 25,
            s_ytd: 10,
            s_order_cnt: 5,
            s_remote_cnt: 1,
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Stock, stock);

        assert_eq!(
            object.version(),
            version,
            "Object version must match the version passed to encode_tpcc_object"
        );
    }

    #[test]
    fn test_shared_owner_with_initial_version() {
        let id = TpccState::object_id_for_warehouse(7);
        let version = SequenceNumber::from_u64(99);
        let warehouse = Warehouse {
            w_id: 7,
            w_ytd: 0,
            w_tax: 0,
            w_name: "Shared Owner Test".to_string(),
        };

        let object = encode_tpcc_object(id, version, TpccObjectKind::Warehouse, warehouse);

        match object.as_inner().owner {
            Owner::Shared {
                initial_shared_version,
            } => {
                assert_eq!(
                    initial_shared_version, version,
                    "initial_shared_version must match the version passed to encode"
                );
            }
            other => panic!("Expected Owner::Shared, got {:?}", other),
        }
    }
}
