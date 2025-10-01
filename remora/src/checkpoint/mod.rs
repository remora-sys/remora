pub mod primary;
pub mod proxy;
pub mod state_collector;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use sui_types::base_types::{ObjectID, SequenceNumber};

/// Object versions modified within an epoch
pub type EpochObjectVersions = BTreeMap<ObjectID, SequenceNumber>;

/// Identifier for an epoch (monotonic per-primary)
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct EpochId(pub u64);
