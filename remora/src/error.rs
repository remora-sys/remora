// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::proxy::core::ProxyId;

pub type NodeResult<T> = Result<T, NodeError>;

#[derive(thiserror::Error, Debug)]
pub enum NodeError {
    #[error("Node shutting down")]
    ShuttingDown,
    #[error("Proxy connection not found for proxy {0}")]
    ProxyConnectionNotFound(ProxyId),
    #[error("Failed to send replay batches: {0}")]
    FailedToReplayBatches(String),
}
