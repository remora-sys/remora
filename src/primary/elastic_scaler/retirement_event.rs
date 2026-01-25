// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Retirement event types for communication between PrimaryNode and LoadBalancer.

use crate::checkpoint::{EpochId, EpochObjectStates};
use crate::proxy::core::ProxyId;

/// Events that the retirement coordinator needs to handle.
/// These are sent from the PrimaryNode (snapshot receiver) to LoadBalancer (retirement coordinator).
#[derive(Debug, Clone)]
pub enum RetirementEvent {
    /// Snapshot received from a proxy during retirement.
    /// The LoadBalancer should check if this proxy is in retirement and call on_snapshot_received.
    Snapshot {
        proxy_id: ProxyId,
        epoch: EpochId,
        snapshot: EpochObjectStates,
    },
    /// An epoch has been sealed (committed by all proxies).
    /// The LoadBalancer should call on_epoch_sealed if awaiting next epoch seal.
    EpochSealed { epoch: EpochId },
}
