// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Observer for connection-level network traffic.
pub trait ConnectionStats: Send + Sync {
    fn record_rx_bytes(&self, bytes: usize);
    fn record_tx_bytes(&self, bytes: usize);
}
