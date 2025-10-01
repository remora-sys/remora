pub mod primary;
pub mod proxy;

use sui_types::base_types::{ObjectID, SequenceNumber};
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;

/// Object versions modified within an epoch
pub type EpochObjectVersions = BTreeMap<ObjectID, SequenceNumber>;

/// Identifier for an epoch (monotonic per-primary)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EpochId(pub u64);


