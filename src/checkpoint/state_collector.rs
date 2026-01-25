use crate::checkpoint::{EpochId, EpochObjectStates};
use crate::primary::pause_barrier::PauseBarrier;
use crate::proxy::core::ProxyId;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::object::Object;

/// Concurrent in-memory state for snapshots and merged objects
pub struct StateCollector {
    /// Persisted state: Objects acknowledged by ALL proxies (stable, safe for recovery)
    /// This is the "commit point" - all proxies have reached consensus on these versions
    pub merged_state: DashMap<ObjectID, Object>,

    /// Temporary state, grouped by epoch: (EpochId -> (ProxyId, ObjectID) -> Object)
    /// This structure isolates epochs to prevent race conditions during commits.
    /// An epoch's data is atomically removed from here and processed by `commit_epoch`.
    temp_state_by_epoch: DashMap<EpochId, DashMap<(ProxyId, ObjectID), Object>>,

    /// Per-proxy persist index: tracks the last acknowledged epoch for each proxy
    /// Key: ProxyId, Value: last acknowledged epoch ID
    pub(crate) per_proxy_persist_index: DashMap<ProxyId, AtomicU64>,

    /// Number of expected proxies (default, for determining when all have acknowledged)
    expected_proxies: usize,

    /// Per-epoch expected proxies: tracks the number of active proxies for each epoch.
    /// This supports elastic scaling where the number of proxies changes at epoch boundaries.
    /// Key: epoch number, Value: expected proxy count for that epoch
    expected_proxies_per_epoch: DashMap<u64, usize>,

    /// The last epoch that was successfully committed to merged_state.
    last_committed_epoch: AtomicU64,
    /// Barrier to pause this worker during recovery.
    pause_barrier: Option<Arc<PauseBarrier>>,
}

impl StateCollector {
    pub fn new(expected_proxies: usize) -> Self {
        Self {
            merged_state: DashMap::new(),
            temp_state_by_epoch: DashMap::new(),
            per_proxy_persist_index: DashMap::new(),
            expected_proxies,
            expected_proxies_per_epoch: DashMap::new(),
            last_committed_epoch: AtomicU64::new(0),
            pause_barrier: None,
        }
    }

    /// Set the pause barrier for the state collector.
    pub fn with_barrier(mut self, barrier: Arc<PauseBarrier>) -> Self {
        self.pause_barrier = Some(barrier);
        self
    }

    /// Process a state snapshot from a proxy
    ///
    /// Two-phase commit protocol:
    /// 1. Store snapshot in temp_state_by_epoch (per-proxy temporary storage)
    /// 2. Check if ALL proxies have reported for this epoch
    /// 3. If yes, promote temp states to merged_state (persisted, stable)
    ///
    /// This ensures merged_state only contains versions acknowledged by all proxies,
    /// eliminating version gaps during recovery.
    pub async fn process_snapshot<T>(
        &self,
        proxy_id: ProxyId,
        completed_up_to: u64,
        snapshot: EpochObjectStates,
        _expected_proxies: usize,
        epoch_logger: Option<&crate::recovery::EpochLogger<T>>,
    ) where
        T: crate::executor::api::ExecutableTransaction + Clone,
    {
        // Enter the barrier, pausing if recovery is in progress.
        // Important: make sure the _guard lives the whole fn scope
        let _guard = if let Some(barrier) = &self.pause_barrier {
            Some(barrier.enter().await)
        } else {
            None
        };

        let last_committed = self.last_committed_epoch.load(Ordering::SeqCst);
        let is_stale = completed_up_to > 0 && completed_up_to <= last_committed;

        if is_stale {
            tracing::warn!(
                "Received stale snapshot for already-committed epoch {}. Last committed epoch is {}. Discarding snapshot data.",
                completed_up_to,
                last_committed
            );
        } else if completed_up_to > 0 {
            // Only store snapshot data if it's for a future, non-stale epoch.
            let epoch = EpochId(completed_up_to);
            let epoch_state = self.temp_state_by_epoch.entry(epoch).or_default();
            for (obj_id, obj) in snapshot.into_iter() {
                tracing::debug!(
                    "Storing temp snapshot for proxy {} in epoch {} with obj_id {}: {}",
                    proxy_id,
                    epoch.0,
                    obj_id,
                    obj.version().value()
                );
                epoch_state.insert((proxy_id, obj_id), obj);
            }
        }

        // Atomically update the persist index for the reporting proxy.
        self.per_proxy_persist_index
            .entry(proxy_id)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_max(completed_up_to, Ordering::SeqCst);

        // ALWAYS run the commit loop. The index update (even from a stale snapshot)
        // might be what completes the next sequential epoch.
        loop {
            let current_last_committed = self.last_committed_epoch.load(Ordering::SeqCst);
            let next_epoch_to_commit = current_last_committed + 1;

            // If the next epoch isn't ready to commit, we're done.
            if !self.is_epoch_complete(EpochId(next_epoch_to_commit), self.expected_proxies) {
                break;
            }

            if !self.commit_epoch(
                EpochId(next_epoch_to_commit),
                current_last_committed,
                epoch_logger,
            ) {
                break;
            }
        }
    }

    /// Commit an epoch: promote all temp states to merged_state
    /// This happens when ALL proxies have acknowledged the epoch
    fn commit_epoch<T>(
        &self,
        epoch: EpochId,
        current_last_committed: u64,
        epoch_logger: Option<&crate::recovery::EpochLogger<T>>,
    ) -> bool
    where
        T: crate::executor::api::ExecutableTransaction + Clone,
    {
        // Atomically remove the epoch's data to ensure we are the only ones processing it.
        if let Some((_, epoch_state)) = self.temp_state_by_epoch.remove(&epoch) {
            let mut committed_count = 0;

            // Collect all object IDs and their latest versions from the isolated epoch state.
            let mut objects_by_id: std::collections::HashMap<
                ObjectID,
                (SequenceNumber, Object, ProxyId),
            > = std::collections::HashMap::new();

            // Scan all temp states for this epoch to find the latest version of each object.
            for entry in epoch_state.iter() {
                let ((proxy_id, obj_id), obj) = entry.pair();
                let version = obj.version();

                objects_by_id
                    .entry(*obj_id)
                    .and_modify(|(current_version, current_obj, writer_proxy)| {
                        if version > *current_version {
                            *current_version = version;
                            *current_obj = obj.clone();
                            *writer_proxy = *proxy_id;
                        }
                    })
                    .or_insert((version, obj.clone(), *proxy_id));
            }

            // Commit all latest versions to merged_state.
            for (obj_id, (version, obj, _writer_proxy)) in objects_by_id {
                if let Some(existing_obj) = self.merged_state.get(&obj_id) {
                    if existing_obj.version() >= version {
                        tracing::warn!(
                            "Stale version update rejected for obj_id {}: existing_version={}, new_version={}",
                            obj_id,
                            existing_obj.version(),
                            version
                        );
                        continue; // Skip insertion
                    }
                }
                tracing::debug!(
                    "Inserting new latest version for obj_id {}: {}",
                    obj_id,
                    version.value()
                );
                self.merged_state.insert(obj_id, obj);
                committed_count += 1;
            }

            tracing::info!(
                "Committed epoch {} to merged_state: {} objects promoted",
                epoch.0,
                committed_count
            );

            // Prune the epoch logger after successful commit.
            if let Some(logger) = epoch_logger {
                logger.prune_epoch(epoch);
                tracing::info!("Pruned epoch {} from epoch logger after commit", epoch.0);
            }

            // Try to atomically advance the committed epoch. If this fails, it's okay:
            // it means another thread just advanced it, and the next iteration of the
            // commit loop will simply try to commit the following epoch.
            if let Ok(_) = self.last_committed_epoch.compare_exchange(
                current_last_committed,
                epoch.0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                tracing::info!("Successfully advanced last committed epoch to {}", epoch.0);
            } else {
                let last_committed = self.last_committed_epoch.load(Ordering::SeqCst);
                tracing::warn!(
                    "Failed to advance. Last committed epoch is now {}",
                    last_committed
                );
            }
            true
        } else {
            false
        }
    }

    /// Get an object from the in-memory store (persisted state only).
    pub fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.merged_state.get(object_id).map(|e| e.clone())
    }

    /// Get the persisted version for an object without cloning the entire object.
    pub fn get_persisted_version(&self, object_id: &ObjectID) -> Option<SequenceNumber> {
        self.merged_state.get(object_id).map(|e| e.version())
    }

    /// Current number of objects in memory.
    pub fn merged_state_len(&self) -> usize {
        self.merged_state.len()
    }

    /// Check if an epoch is complete (all proxies have reported at least this epoch).
    /// Returns true if ALL proxies (including failed ones) have a persist_index >= the epoch.
    /// Uses per-epoch expected proxy count if available, otherwise falls back to default.
    pub fn is_epoch_complete(&self, epoch: EpochId, _expected_proxies: usize) -> bool {
        // Use per-epoch expected proxies if available, otherwise fall back to default
        let expected = self.get_expected_proxies_for_epoch(epoch);

        if self.per_proxy_persist_index.len() < expected {
            tracing::debug!(
                "Epoch {} not complete: only {}/{} proxies have reported",
                epoch.0,
                self.per_proxy_persist_index.len(),
                expected
            );
            return false;
        }

        let min_persist_index = self
            .per_proxy_persist_index
            .iter()
            .map(|entry| entry.value().load(Ordering::SeqCst))
            .min()
            .unwrap_or(0);

        let complete = min_persist_index >= epoch.0;

        tracing::debug!(
            "Epoch {} completion check: min_persist_index={}, expected_proxies={}, complete={}",
            epoch.0,
            min_persist_index,
            expected,
            complete
        );
        complete
    }

    /// Get the current primary persist index (replay cut).
    ///
    /// This returns the last epoch that was successfully committed to merged_state,
    /// which represents the stable "commit point" for the entire system. It is the
    /// safe point for pruning or recovery.
    pub fn get_persist_index(&self) -> u64 {
        self.last_committed_epoch.load(Ordering::SeqCst)
    }

    /// Get the persist index for a specific proxy (for debugging/diagnostics).
    pub fn get_proxy_persist_index(&self, proxy_id: ProxyId) -> u64 {
        self.per_proxy_persist_index
            .get(&proxy_id)
            .map(|atomic| atomic.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Set the expected number of proxies for a specific epoch.
    /// Called by the load balancer at epoch boundaries when scaling happens.
    pub fn set_expected_proxies_for_epoch(&self, epoch: EpochId, count: usize) {
        tracing::debug!("Setting expected proxies for epoch {}: {}", epoch.0, count);
        self.expected_proxies_per_epoch.insert(epoch.0, count);
    }

    /// Get the expected number of proxies for a specific epoch.
    /// Returns the per-epoch count if set, otherwise falls back to the default.
    pub fn get_expected_proxies_for_epoch(&self, epoch: EpochId) -> usize {
        self.expected_proxies_per_epoch
            .get(&epoch.0)
            .map(|v| *v)
            .unwrap_or(self.expected_proxies)
    }

    /// Clean up per-epoch expected proxy count for committed epochs.
    /// Called after epoch commit to free memory.
    pub fn cleanup_expected_proxies_up_to(&self, epoch: EpochId) {
        for e in 1..=epoch.0 {
            self.expected_proxies_per_epoch.remove(&e);
        }
    }

    /// Remove a proxy from persist index tracking after retirement completes.
    ///
    /// This is critical for scale-in: without removal, the retired proxy's frozen
    /// persist index would cause `is_epoch_complete()` to return false forever,
    /// blocking all future epoch commits.
    pub fn remove_proxy_persist_index(&self, proxy_id: ProxyId) {
        if self.per_proxy_persist_index.remove(&proxy_id).is_some() {
            tracing::info!(
                proxy_id,
                "Removed retired proxy from persist index tracking"
            );
        } else {
            tracing::warn!(
                proxy_id,
                "Attempted to remove non-existent proxy from persist index"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::base_types::ObjectID;
    use sui_types::object::Object;

    fn create_test_object(id: ObjectID) -> Object {
        Object::immutable_with_id_for_testing(id)
    }

    fn create_test_object_with_version(id: ObjectID, version: u64) -> Object {
        use sui_types::object::MoveObject;
        let v = SequenceNumber::from(version);
        let o = MoveObject::new_gas_coin(v, id, 100);
        Object::new_move(o, sui_types::object::Owner::Immutable, Default::default())
    }

    #[tokio::test]
    async fn test_state_collector_new() {
        let collector = StateCollector::new(3);
        assert_eq!(collector.merged_state_len(), 0);
    }

    #[tokio::test]
    async fn test_state_collector_with_store() {
        let collector = StateCollector::new(2);
        assert_eq!(collector.merged_state_len(), 0);
    }

    #[tokio::test]
    async fn test_state_collector_process_snapshot() {
        let collector = StateCollector::new(2);

        // Create test objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj1 = create_test_object(obj_id1);
        let obj2 = create_test_object(obj_id2);

        let mut snapshot = EpochObjectStates::new();
        snapshot.insert(obj_id1, obj1);
        snapshot.insert(obj_id2, obj2);

        // Process snapshot from proxy 1 with completed_up_to = 5
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                1,
                1,
                snapshot.clone(),
                2,
                None,
            )
            .await;
        // Check proxy 1's persist index
        assert_eq!(collector.get_proxy_persist_index(1), 1);
        // merged_state should be EMPTY until all proxies report (two-phase commit)
        assert_eq!(collector.merged_state_len(), 0);

        // Process snapshot from proxy 2
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 1, snapshot, 2, None)
            .await;
        // Check proxy 2's persist index
        assert_eq!(collector.get_proxy_persist_index(2), 1);
        // Minimum should be 5
        assert_eq!(collector.get_persist_index(), 1);
        // NOW merged_state should contain both objects (all proxies reported)
        assert_eq!(collector.merged_state_len(), 2);
    }

    #[tokio::test]
    async fn test_state_collector_multiple_epochs() {
        let collector = StateCollector::new(2);

        // Process snapshots for different batches - no ordering required
        let snapshot = EpochObjectStates::new();
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                1,
                2,
                snapshot.clone(),
                2,
                None,
            )
            .await;

        // Proxy 1 should be at completed_up_to 2
        assert_eq!(collector.get_proxy_persist_index(1), 2);

        // Process snapshot for batch 1 from proxy 2 - out of order is fine
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 1, snapshot, 2, None)
            .await;

        // Proxy 2 should be at completed_up_to 1
        assert_eq!(collector.get_proxy_persist_index(2), 1);
        // The minimum completed epoch is 1, so the persist index should be 1.
        assert_eq!(collector.get_persist_index(), 1);
    }

    #[tokio::test]
    async fn test_state_collector_per_proxy_tracking() {
        let collector = StateCollector::new(3);

        // Process snapshots from different proxies at different completed_up_to values
        let snapshot = EpochObjectStates::new();

        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                3,
                snapshot.clone(),
                3,
                None,
            )
            .await;
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                1,
                1,
                snapshot.clone(),
                3,
                None,
            )
            .await;
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 2, snapshot, 3, None)
            .await;

        // Check individual persist indices
        assert_eq!(collector.get_proxy_persist_index(0), 3);
        assert_eq!(collector.get_proxy_persist_index(1), 1);
        assert_eq!(collector.get_proxy_persist_index(2), 2);

        // Global persist index should be minimum (5)
        assert_eq!(collector.get_persist_index(), 1);
    }

    #[tokio::test]
    async fn test_state_collector_merge_snapshots() {
        let collector = StateCollector::new(2);

        // Create snapshots with overlapping objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj_id3 = ObjectID::random();

        let obj1_v1 = create_test_object(obj_id1);
        let obj1_v2 = create_test_object(obj_id1); // Same ID, different object
        let obj2 = create_test_object(obj_id2);
        let obj3 = create_test_object(obj_id3);

        // Proxy 1 snapshot
        let mut snapshot1 = EpochObjectStates::new();
        snapshot1.insert(obj_id1, obj1_v1);
        snapshot1.insert(obj_id2, obj2);

        // Proxy 2 snapshot (overlaps with obj_id1)
        let mut snapshot2 = EpochObjectStates::new();
        snapshot2.insert(obj_id1, obj1_v2); // Different version of obj_id1
        snapshot2.insert(obj_id3, obj3);

        // Process both snapshots
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(1, 1, snapshot1, 2, None)
            .await;
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 1, snapshot2, 2, None)
            .await;

        // Both proxies should be at completed_up_to 5
        assert_eq!(collector.get_proxy_persist_index(1), 1);
        assert_eq!(collector.get_proxy_persist_index(2), 1);
        // All 3 objects should be in merged state (last-writer-wins for obj_id1)
        assert_eq!(collector.merged_state_len(), 3);
    }

    #[tokio::test]
    async fn test_per_proxy_independent_progress() {
        // Test that each proxy can progress independently through batches
        let collector = StateCollector::new(3);
        let snapshot = EpochObjectStates::new();

        // Proxy 0 reports up to batch 10
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                3,
                snapshot.clone(),
                3,
                None,
            )
            .await;

        // Proxy 1 reports up to batch 5
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                1,
                1,
                snapshot.clone(),
                3,
                None,
            )
            .await;

        // Proxy 2 reports up to batch 7
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 2, snapshot, 3, None)
            .await;

        // Each proxy should track its own progress
        assert_eq!(collector.get_proxy_persist_index(0), 3);
        assert_eq!(collector.get_proxy_persist_index(1), 1);
        assert_eq!(collector.get_proxy_persist_index(2), 2);

        // Global persist index is the minimum (safe point for pruning)
        assert_eq!(collector.get_persist_index(), 1);
    }

    #[tokio::test]
    async fn test_initial_snapshot_with_zero_completed_up_to() {
        // Test that initial snapshots with completed_up_to = 0 don't trigger commit
        let collector = StateCollector::new(2);

        // Create test objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj1 = create_test_object(obj_id1);
        let obj2 = create_test_object(obj_id2);

        let mut snapshot = EpochObjectStates::new();
        snapshot.insert(obj_id1, obj1);
        snapshot.insert(obj_id2, obj2);

        // Both proxies send initial snapshots with completed_up_to = 0
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                0,
                snapshot.clone(),
                2,
                None,
            )
            .await;

        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(1, 0, snapshot, 2, None)
            .await;

        // Both proxies should be at completed_up_to 0
        assert_eq!(collector.get_proxy_persist_index(0), 0);
        assert_eq!(collector.get_proxy_persist_index(1), 0);
        assert_eq!(collector.get_persist_index(), 0);

        // IMPORTANT: merged_state should be EMPTY because completed_up_to = 0
        // means no batches have been completed yet, so we shouldn't commit
        assert_eq!(collector.merged_state_len(), 0);
    }

    #[tokio::test]
    async fn test_initial_then_real_snapshots() {
        // Test the progression: initial snapshot (0) -> real snapshot (1+)
        let collector = StateCollector::new(2);

        let obj_id1 = ObjectID::random();
        let obj1_v1 = create_test_object(obj_id1);
        let obj1_v2 = create_test_object(obj_id1);

        // Initial snapshots with completed_up_to = 0
        let mut initial_snapshot = EpochObjectStates::new();
        initial_snapshot.insert(obj_id1, obj1_v1);

        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                0,
                initial_snapshot.clone(),
                2,
                None,
            )
            .await;
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                1,
                0,
                initial_snapshot,
                2,
                None,
            )
            .await;

        // No commit should happen
        assert_eq!(collector.merged_state_len(), 0);

        // Now proxies complete batch 1 and send real snapshots
        let mut real_snapshot = EpochObjectStates::new();
        real_snapshot.insert(obj_id1, obj1_v2);

        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                1,
                real_snapshot.clone(),
                2,
                None,
            )
            .await;

        // Still no commit (only 1 proxy reported batch 1)
        assert_eq!(collector.merged_state_len(), 0);

        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                1,
                1,
                real_snapshot,
                2,
                None,
            )
            .await;

        // NOW commit should happen (both proxies at batch 1)
        assert_eq!(collector.get_persist_index(), 1);
        assert_eq!(collector.merged_state_len(), 1);
    }

    #[tokio::test]
    async fn test_mixed_zero_and_nonzero_snapshots() {
        // Test when one proxy is at 0 and another is ahead
        let collector = StateCollector::new(2);

        let snapshot = EpochObjectStates::new();

        // Proxy 0 sends initial snapshot (completed_up_to = 0)
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                0,
                snapshot.clone(),
                2,
                None,
            )
            .await;

        // Proxy 1 has already completed batch 5
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(1, 5, snapshot, 2, None)
            .await;

        // Persist indices should be tracked independently
        assert_eq!(collector.get_proxy_persist_index(0), 0);
        assert_eq!(collector.get_proxy_persist_index(1), 5);

        // Global persist index should be the minimum (0)
        assert_eq!(collector.get_persist_index(), 0);

        // No commit should happen (can't commit batch 0)
        assert_eq!(collector.merged_state_len(), 0);
    }

    #[tokio::test]
    async fn test_epoch_commit_is_isolated() {
        let collector = StateCollector::new(2);

        // --- Epoch 5 Setup ---
        let obj_id_1 = ObjectID::random();
        let obj_1 = create_test_object(obj_id_1);
        let mut snapshot_1 = EpochObjectStates::new();
        snapshot_1.insert(obj_id_1, obj_1);

        // --- Epoch 6 Setup ---
        let obj_id_2 = ObjectID::random();
        let obj_2 = create_test_object(obj_id_2);
        let mut snapshot_2 = EpochObjectStates::new();
        snapshot_2.insert(obj_id_2, obj_2);

        // --- Simulate Race ---

        // 1. Proxy 1 reports for epoch 1. This should not trigger a commit.
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                1,
                1,
                snapshot_1.clone(),
                2,
                None,
            )
            .await;
        assert_eq!(collector.merged_state_len(), 0);
        assert!(collector.temp_state_by_epoch.contains_key(&EpochId(1)));

        // 2. Before epoch 1 is committed, proxy 1 reports for epoch 2.
        // This should also not trigger a commit.
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                1,
                2,
                snapshot_2.clone(),
                2,
                None,
            )
            .await;
        assert_eq!(collector.merged_state_len(), 0);
        assert!(collector.temp_state_by_epoch.contains_key(&EpochId(2)));

        // 3. Now, proxy 2 reports for epoch 1. This SHOULD trigger the commit for epoch 1.
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                2,
                1,
                snapshot_1.clone(),
                2,
                None,
            )
            .await;

        // --- Assertions ---

        // The commit for epoch 1 should be complete.
        assert_eq!(collector.get_persist_index(), 1);

        // merged_state should contain ONLY the object from epoch 1.
        assert_eq!(collector.merged_state_len(), 1);
        assert!(collector.get_object(&obj_id_1).is_some());
        assert!(collector.get_object(&obj_id_2).is_none()); // Crucial check

        // The temp state for epoch 1 should be gone.
        assert!(!collector.temp_state_by_epoch.contains_key(&EpochId(1)));

        // The temp state for epoch 2 should still be there.
    }

    #[tokio::test]
    async fn test_out_of_order_commit_preserves_latest_version() {
        let collector = StateCollector::new(2);
        let obj_id = ObjectID::random();

        // 1. Simulate commit of a newer version (v4 from epoch 3)
        let epoch_3_state = DashMap::new();
        let obj_v4 = create_test_object_with_version(obj_id, 4);
        epoch_3_state.insert((0, obj_id), obj_v4);
        collector
            .temp_state_by_epoch
            .insert(EpochId(3), epoch_3_state);

        collector.commit_epoch::<crate::executor::fake::FakeTransaction>(EpochId(3), 2, None);

        // Assert that merged_state has v4
        assert_eq!(
            collector.get_persisted_version(&obj_id),
            Some(SequenceNumber::from(4))
        );

        // 2. Simulate a delayed, out-of-order commit of an older version (v3 from epoch 2)
        let epoch_2_state = DashMap::new();
        let obj_v3 = create_test_object_with_version(obj_id, 3);
        epoch_2_state.insert((0, obj_id), obj_v3);
        collector
            .temp_state_by_epoch
            .insert(EpochId(2), epoch_2_state);

        collector.commit_epoch::<crate::executor::fake::FakeTransaction>(EpochId(2), 1, None);

        // 3. Assert that the stale update was rejected
        // The version in merged_state should still be v4, not overwritten by v3.
        assert_eq!(
            collector.get_persisted_version(&obj_id),
            Some(SequenceNumber::from(4)),
            "Stale version v3 should have been rejected"
        );
    }

    #[tokio::test]
    async fn test_late_duplicate_snapshot_is_ignored() {
        let collector = StateCollector::new(1);
        let obj_id = ObjectID::random();

        // First, process a snapshot for epoch 1 to satisfy sequential commit logic.
        let obj_v1 = create_test_object_with_version(obj_id, 1);
        let mut snapshot_1 = EpochObjectStates::new();
        snapshot_1.insert(obj_id, obj_v1.clone());
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                1,
                snapshot_1.clone(),
                1,
                None,
            )
            .await;
        assert_eq!(collector.get_persist_index(), 1);

        let obj_v2 = create_test_object_with_version(obj_id, 2);
        let mut snapshot_2 = EpochObjectStates::new();
        snapshot_2.insert(obj_id, obj_v2);

        // 1. Process a snapshot for epoch 2.
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(0, 2, snapshot_2, 1, None)
            .await;

        // Assert that the proxy's index is 2 and epoch 2 was committed.
        assert_eq!(collector.get_proxy_persist_index(0), 2);
        assert_eq!(collector.last_committed_epoch.load(Ordering::SeqCst), 2);
        assert!(collector.temp_state_by_epoch.is_empty()); // Should be cleaned up after commit.

        // 2. Process a late/duplicate snapshot for epoch 1.
        let obj_v1_again = create_test_object_with_version(obj_id, 1);
        let mut snapshot_1_again = EpochObjectStates::new();
        snapshot_1_again.insert(obj_id, obj_v1_again);
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                1,
                snapshot_1_again,
                1,
                None,
            )
            .await;

        // 3. Assert that the state has not changed.
        // The proxy's index should NOT regress to 1.
        assert_eq!(collector.get_proxy_persist_index(0), 2);
        // The last committed epoch should still be 2.
        assert_eq!(collector.last_committed_epoch.load(Ordering::SeqCst), 2);
        // No new temp state for epoch 1 should have been created.
        assert!(!collector.temp_state_by_epoch.contains_key(&EpochId(1)));
    }

    #[tokio::test]
    async fn test_commit_is_atomic_with_pause() {
        // This test simulates a race between a commit operation in `process_snapshot`
        // and a recovery snapshot in `begin_recovery`. It verifies that the pause
        // barrier correctly waits for the entire commit operation (including the
        // final update to `last_committed_epoch`) to complete before allowing
        // the snapshot to proceed.
        let barrier = PauseBarrier::new();
        let collector = Arc::new(StateCollector::new(2).with_barrier(barrier.clone()));
        let snapshot = EpochObjectStates::new();

        // The first snapshot doesn't trigger a commit.
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                1,
                snapshot.clone(),
                2,
                None,
            )
            .await;
        assert_eq!(collector.get_persist_index(), 0);

        // Spawn a task that will trigger the commit for epoch 1.
        let collector_clone = collector.clone();
        let committer = tokio::spawn(async move {
            collector_clone
                .process_snapshot::<crate::executor::fake::FakeTransaction>(
                    1,
                    1,
                    snapshot.clone(),
                    2,
                    None,
                )
                .await;
        });

        // Spawn a task that immediately pauses and takes a snapshot.
        let pauser = tokio::spawn(async move {
            // Yield to give the committer a chance to start.
            tokio::task::yield_now().await;
            // This should wait until the committer task is *completely* finished.
            let _guard = barrier.pause_and_wait().await;
            // Read the persist index *after* the pause is complete.
            collector.get_persist_index()
        });

        let (_, persist_index_at_pause) = tokio::join!(committer, pauser);

        // The persist index read by the pauser should be 1. If it's 0, it means
        // `pause_and_wait` returned prematurely, before `last_committed_epoch` was
        // updated, which is the race condition we are investigating.
        assert_eq!(
            persist_index_at_pause.unwrap(),
            1,
            "Snapshot was taken before commit operation completed."
        );
    }

    #[tokio::test]
    async fn test_remove_proxy_persist_index_basic() {
        // Test that removing a proxy from persist index tracking works correctly
        let collector = StateCollector::new(3);
        let snapshot = EpochObjectStates::new();

        // Simulate 3 proxies reporting epoch 1
        for proxy_id in 0..3 {
            collector
                .process_snapshot::<crate::executor::fake::FakeTransaction>(
                    proxy_id,
                    1,
                    snapshot.clone(),
                    3,
                    None,
                )
                .await;
        }

        // Epoch 1 should be committed
        assert_eq!(collector.get_persist_index(), 1);
        assert_eq!(collector.per_proxy_persist_index.len(), 3);

        // Remove proxy 2 (simulating retirement)
        collector.remove_proxy_persist_index(2);

        // Should now only have 2 proxies tracked
        assert_eq!(collector.per_proxy_persist_index.len(), 2);
        assert!(collector.per_proxy_persist_index.get(&0).is_some());
        assert!(collector.per_proxy_persist_index.get(&1).is_some());
        assert!(collector.per_proxy_persist_index.get(&2).is_none());
    }

    #[tokio::test]
    async fn test_remove_proxy_allows_epoch_completion() {
        // Test that removing a retired proxy allows future epochs to commit
        // without waiting for the (now frozen) retired proxy
        let collector = StateCollector::new(3);
        let snapshot = EpochObjectStates::new();

        // Epoch 1: all 3 proxies report
        for proxy_id in 0..3 {
            collector
                .process_snapshot::<crate::executor::fake::FakeTransaction>(
                    proxy_id,
                    1,
                    snapshot.clone(),
                    3,
                    None,
                )
                .await;
        }
        assert_eq!(collector.get_persist_index(), 1);

        // Proxy 2 retires after epoch 1 - remove from tracking
        collector.remove_proxy_persist_index(2);

        // Set expected proxies for epoch 2 to reflect the new count
        collector.set_expected_proxies_for_epoch(EpochId(2), 2);

        // Epoch 2: only remaining 2 proxies report
        for proxy_id in 0..2 {
            collector
                .process_snapshot::<crate::executor::fake::FakeTransaction>(
                    proxy_id,
                    2,
                    snapshot.clone(),
                    2,
                    None,
                )
                .await;
        }

        // Epoch 2 should now be committed (only 2 proxies needed)
        assert_eq!(
            collector.get_persist_index(),
            2,
            "Epoch 2 should commit with only the remaining 2 proxies"
        );
    }

    #[tokio::test]
    async fn test_retired_proxy_blocks_without_removal() {
        // Test that without removing the retired proxy, epoch completion is blocked
        let collector = StateCollector::new(3);
        let snapshot = EpochObjectStates::new();

        // Epoch 1: all 3 proxies report
        for proxy_id in 0..3 {
            collector
                .process_snapshot::<crate::executor::fake::FakeTransaction>(
                    proxy_id,
                    1,
                    snapshot.clone(),
                    3,
                    None,
                )
                .await;
        }
        assert_eq!(collector.get_persist_index(), 1);

        // Proxy 2 retires but is NOT removed (bug scenario)
        // Set expected proxies for epoch 2, but DON'T remove proxy 2
        collector.set_expected_proxies_for_epoch(EpochId(2), 2);

        // Epoch 2: only remaining 2 proxies report
        for proxy_id in 0..2 {
            collector
                .process_snapshot::<crate::executor::fake::FakeTransaction>(
                    proxy_id,
                    2,
                    snapshot.clone(),
                    2,
                    None,
                )
                .await;
        }

        // Epoch 2 should NOT be committed because min(persist_indices) still includes
        // proxy 2 at epoch 1
        assert_eq!(
            collector.get_persist_index(),
            1,
            "Bug confirmed: retired proxy blocks epoch commit when not removed"
        );

        // Now remove the retired proxy
        collector.remove_proxy_persist_index(2);

        // Re-trigger epoch check by processing a no-op snapshot
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(
                0,
                2,
                snapshot.clone(),
                2,
                None,
            )
            .await;

        // Now epoch 2 should be committed
        assert_eq!(
            collector.get_persist_index(),
            2,
            "Epoch 2 should commit after removing retired proxy"
        );
    }
}
