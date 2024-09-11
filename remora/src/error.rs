// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub type NodeResult<T> = Result<T, NodeError>;

#[derive(thiserror::Error, Debug)]
pub enum NodeError {
    #[error("Node shutting down")]
    ShuttingDown,
}
