use crate::checkpoint::EpochId;
use std::sync::atomic::{AtomicU64, Ordering};

/// Manages epoch boundaries on the primary.
/// Phase 1: in-memory counter and APIs; integrations added later.
pub struct EpochManager {
    next_epoch_id: AtomicU64,
}

impl Default for EpochManager {
    fn default() -> Self {
        Self {
            next_epoch_id: AtomicU64::new(1),
        }
    }
}

impl EpochManager {
    pub fn new() -> Self {
        Self::default()
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_manager_default() {
        let manager = EpochManager::new();
        assert_eq!(manager.peek_next_epoch(), EpochId(1));
    }

    #[test]
    fn test_epoch_manager_advance() {
        let manager = EpochManager::new();

        // First epoch
        let epoch1 = manager.end_epoch_and_begin_next();
        assert_eq!(epoch1, EpochId(1));
        assert_eq!(manager.peek_next_epoch(), EpochId(2));

        // Second epoch
        let epoch2 = manager.end_epoch_and_begin_next();
        assert_eq!(epoch2, EpochId(2));
        assert_eq!(manager.peek_next_epoch(), EpochId(3));
    }

    #[test]
    fn test_epoch_manager_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let manager = Arc::new(EpochManager::new());
        let mut handles = vec![];

        // Spawn multiple threads to advance epochs concurrently
        for _ in 0..10 {
            let manager = Arc::clone(&manager);
            let handle = thread::spawn(move || manager.end_epoch_and_begin_next());
            handles.push(handle);
        }

        // Collect all epoch IDs
        let mut epochs: Vec<EpochId> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        epochs.sort();

        // Should have unique epochs from 1 to 10
        assert_eq!(epochs.len(), 10);
        for (i, epoch) in epochs.iter().enumerate() {
            assert_eq!(epoch.0, (i + 1) as u64);
        }
    }
}
