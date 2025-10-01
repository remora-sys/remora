pub mod primary;
pub mod proxy;
pub mod state_collector;
pub mod storage;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use sui_types::base_types::ObjectID;
use sui_types::object::Object;

/// Object versions modified within an epoch
pub type EpochObjectVersions = BTreeMap<ObjectID, SequenceNumber>;

/// Object states modified within an epoch
pub type EpochObjectStates = BTreeMap<ObjectID, Object>;

/// Identifier for an epoch (monotonic per-primary)
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct EpochId(pub u64);
