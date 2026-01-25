// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! TPC-C specification constants.
//!
//! Based on the TPC-C benchmark specification and py-tpcc reference implementation.

// =============================================================================
// Scale Factors
// =============================================================================

/// Number of items in the ITEM table (fixed, not scaled by warehouse)
pub const NUM_ITEMS: usize = 100_000;

/// Number of districts per warehouse
pub const DISTRICTS_PER_WAREHOUSE: usize = 10;

/// Number of customers per district
pub const CUSTOMERS_PER_DISTRICT: usize = 3_000;

/// Number of stock records per warehouse (same as NUM_ITEMS)
pub const STOCK_PER_WAREHOUSE: usize = 100_000;

/// Initial number of orders per district
pub const INITIAL_ORDERS_PER_DISTRICT: usize = 3_000;

/// Initial number of new orders per district
pub const INITIAL_NEW_ORDERS_PER_DISTRICT: usize = 900;

// =============================================================================
// Order Line Constraints
// =============================================================================

/// Minimum number of order lines per order
pub const MIN_OL_CNT: usize = 5;

/// Maximum number of order lines per order
pub const MAX_OL_CNT: usize = 15;

/// Maximum quantity per order line
pub const MAX_OL_QUANTITY: u32 = 10;

// =============================================================================
// Transaction Mix (NEW_ORDER and PAYMENT only)
// =============================================================================

/// NEW_ORDER transaction weight (50%)
pub const NEW_ORDER_WEIGHT: u32 = 50;

/// PAYMENT transaction weight (50%)
pub const PAYMENT_WEIGHT: u32 = 50;

// =============================================================================
// Remote Warehouse Probability
// =============================================================================

/// Probability that an order line is from a remote warehouse (15%)
pub const REMOTE_WAREHOUSE_PROB: f64 = 0.15;

/// Probability that a payment is to a remote customer (15%)
pub const REMOTE_CUSTOMER_PROB: f64 = 0.15;

// =============================================================================
// Price and Amount Ranges
// =============================================================================

/// Fixed-point scale for monetary values (cents)
pub const MONEY_SCALE: i64 = 100;

/// Minimum item price
pub const MIN_PRICE: f64 = 1.00;

/// Maximum item price
pub const MAX_PRICE: f64 = 100.00;

/// Minimum item price (cents)
pub const MIN_PRICE_CENTS: u32 = 100;

/// Maximum item price (cents)
pub const MAX_PRICE_CENTS: u32 = 10_000;

/// Minimum payment amount
pub const MIN_PAYMENT: f64 = 1.00;

/// Maximum payment amount
pub const MAX_PAYMENT: f64 = 5000.00;

/// Minimum payment amount (cents)
pub const MIN_PAYMENT_CENTS: i64 = 100;

/// Maximum payment amount (cents)
pub const MAX_PAYMENT_CENTS: i64 = 500_000;

// =============================================================================
// Tax and Discount Ranges
// =============================================================================

/// Fixed-point scale for rates (basis points)
pub const RATE_SCALE: u32 = 10_000;

/// Minimum tax rate
pub const MIN_TAX: f64 = 0.0;

/// Maximum tax rate
pub const MAX_TAX: f64 = 0.2;

/// Minimum tax rate (basis points)
pub const MIN_TAX_BPS: u32 = 0;

/// Maximum tax rate (basis points)
pub const MAX_TAX_BPS: u32 = 2_000;

/// Minimum discount
pub const MIN_DISCOUNT: f64 = 0.0;

/// Maximum discount
pub const MAX_DISCOUNT: f64 = 0.5;

/// Minimum discount (basis points)
pub const MIN_DISCOUNT_BPS: u32 = 0;

/// Maximum discount (basis points)
pub const MAX_DISCOUNT_BPS: u32 = 5_000;

// =============================================================================
// Stock Quantity
// =============================================================================

/// Minimum stock quantity
pub const MIN_QUANTITY: i32 = 10;

/// Maximum stock quantity
pub const MAX_QUANTITY: i32 = 100;

// =============================================================================
// Initial Values
// =============================================================================

/// Initial warehouse YTD
pub const INITIAL_W_YTD: f64 = 300_000.00;

/// Initial district YTD
pub const INITIAL_D_YTD: f64 = 30_000.00;

/// Initial customer balance
pub const INITIAL_BALANCE: f64 = -10.00;

/// Initial customer YTD payment
pub const INITIAL_YTD_PAYMENT: f64 = 10.00;

/// Initial warehouse YTD (cents)
pub const INITIAL_W_YTD_CENTS: i64 = 30_000_000;

/// Initial district YTD (cents)
pub const INITIAL_D_YTD_CENTS: i64 = 3_000_000;

/// Initial customer balance (cents)
pub const INITIAL_BALANCE_CENTS: i64 = -1_000;

/// Initial customer YTD payment (cents)
pub const INITIAL_YTD_PAYMENT_CENTS: i64 = 1_000;

/// Initial customer payment count
pub const INITIAL_PAYMENT_CNT: u32 = 1;

/// Initial next order ID for districts
pub const INITIAL_NEXT_O_ID: u32 = 3001;
