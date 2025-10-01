use crate::checkpoint::EpochId;
use std::sync::atomic::{AtomicU64, Ordering};

/// Manages epoch boundaries on the primary.
/// Phase 1: in-memory counter and APIs; integrations added later.
pub struct EpochManager {
    next_epoch_id: AtomicU64,
}

impl Default for EpochManager {
    fn default() -> Self {
        Self { next_epoch_id: AtomicU64::new(1) }
    }
}

impl EpochManager {
    pub fn new() -> Self { Self::default() }

    /// Get a new epoch id and advance the counter.
    pub fn end_epoch_and_begin_next(&self) -> EpochId {
        let id = self.next_epoch_id.fetch_add(1, Ordering::SeqCst);
        EpochId(id)
    }

    /// Peek next epoch id without incrementing.
    pub fn peek_next_epoch(&self) -> EpochId {
        EpochId(self.next_epoch_id.load(Ordering::SeqCst))
    }
}


