// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Retirement Coordinator Module
//!
//! Handles graceful epoch-boundary aligned retirement of proxies during scale-in.
//! The retirement follows a state machine:
//!
//! 1. `Idle` - No retirement in progress
//! 2. `PendingEpochBoundary` - Load drop detected, waiting for epoch boundary
//! 3. `AwaitingSnapshot` - At epoch boundary, waiting for proxy's final snapshot
//! 4. `AwaitingNextEpochSeal` - Snapshot received, waiting for next epoch to seal
//!
//! Key invariants:
//! - Retired proxy continues serving inter-proxy requests until snapshot received
//! - Ownership transfer is versioned (only update if newer)
//! - Always retire highest proxy ID to minimize round-robin disruption

use crate::checkpoint::{state_collector::StateCollector, EpochId, EpochObjectStates};
use crate::proxy::core::ProxyId;
use std::sync::Arc;

/// Retirement phases (epoch-boundary aligned state machine)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetirementPhase {
    /// No retirement in progress
    Idle,
    /// Load drop detected, waiting for epoch boundary to initiate
    PendingEpochBoundary { proxy_id: ProxyId },
    /// At epoch boundary: stop dispatching, waiting for proxy's final snapshot
    AwaitingSnapshot { proxy_id: ProxyId, epoch: EpochId },
    /// Snapshot received, waiting for next epoch to seal before full retirement
    AwaitingNextEpochSeal { proxy_id: ProxyId },
}

/// Actions that LoadBalancer should execute based on coordinator state transitions.
///
/// Note: Dispatch exclusion is handled by calling `is_proxy_retiring()` rather than
/// an explicit action, which is more natural for the forwarder to check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetirementAction {
    /// Send retirement signal to proxy (stop new txns, continue serving requests)
    SendRetirementSignal { proxy_id: ProxyId, epoch: EpochId },
    /// Update ownership map with versioned check (after snapshot received)
    UpdateOwnership { proxy_id: ProxyId },
    /// Complete retirement: remove proxy and decrease active nodes
    CompleteRetirement { proxy_id: ProxyId },
}

/// Coordinates the graceful retirement of proxies during scale-in.
///
/// This is a pure state machine that produces actions for LoadBalancer to execute.
/// It does not directly modify LoadBalancer state, maintaining clean separation.
pub struct RetirementCoordinator {
    /// Current phase of retirement
    phase: RetirementPhase,
    /// Reference to state collector for ownership updates
    collector: Arc<StateCollector>,
}

impl RetirementCoordinator {
    /// Create a new retirement coordinator.
    pub fn new(collector: Arc<StateCollector>) -> Self {
        Self {
            phase: RetirementPhase::Idle,
            collector,
        }
    }

    /// Get current phase (for debugging/testing).
    pub fn phase(&self) -> &RetirementPhase {
        &self.phase
    }

    /// Check if currently in a retirement process.
    pub fn is_retiring(&self) -> bool {
        !matches!(self.phase, RetirementPhase::Idle)
    }

    /// Check if a specific proxy is in the retirement process.
    pub fn is_proxy_retiring(&self, proxy_id: ProxyId) -> bool {
        match &self.phase {
            RetirementPhase::Idle => false,
            RetirementPhase::PendingEpochBoundary { proxy_id: pid } => *pid == proxy_id,
            RetirementPhase::AwaitingSnapshot { proxy_id: pid, .. } => *pid == proxy_id,
            RetirementPhase::AwaitingNextEpochSeal { proxy_id: pid } => *pid == proxy_id,
        }
    }

    /// Initiate retirement of a proxy.
    ///
    /// Call this when scale-in is detected. The retirement will activate
    /// at the next epoch boundary.
    ///
    /// Returns false if a retirement is already in progress.
    pub fn initiate(&mut self, proxy_id: ProxyId) -> bool {
        if self.is_retiring() {
            tracing::warn!(
                proxy_id,
                "Cannot initiate retirement: another retirement already in progress"
            );
            return false;
        }

        tracing::info!(
            proxy_id,
            "Initiating retirement (will activate at next epoch boundary)"
        );
        self.phase = RetirementPhase::PendingEpochBoundary { proxy_id };
        true
    }

    /// Cancel a pending retirement (before epoch boundary).
    ///
    /// Can only cancel if in PendingEpochBoundary phase.
    pub fn cancel(&mut self) -> bool {
        if let RetirementPhase::PendingEpochBoundary { proxy_id } = &self.phase {
            tracing::info!(proxy_id, "Retirement cancelled");
            self.phase = RetirementPhase::Idle;
            true
        } else {
            false
        }
    }

    /// Called at epoch boundary to advance the state machine.
    ///
    /// Returns action for LoadBalancer to execute, if any.
    /// Dispatch exclusion is handled by forwarders calling `is_proxy_retiring()`.
    pub fn on_epoch_boundary(&mut self, epoch: EpochId) -> Option<RetirementAction> {
        match &self.phase {
            RetirementPhase::PendingEpochBoundary { proxy_id } => {
                let proxy_id = *proxy_id;
                tracing::info!(
                    proxy_id,
                    epoch = epoch.0,
                    "Epoch boundary reached: activating retirement"
                );

                // Transition to AwaitingSnapshot
                self.phase = RetirementPhase::AwaitingSnapshot { proxy_id, epoch };

                // Only action needed: send retirement signal
                // Dispatch exclusion is handled by forwarders checking is_proxy_retiring()
                Some(RetirementAction::SendRetirementSignal { proxy_id, epoch })
            }
            _ => None,
        }
    }

    /// Called when snapshot is received from the retiring proxy.
    ///
    /// Returns action for LoadBalancer to execute, if any.
    pub fn on_snapshot_received(
        &mut self,
        proxy_id: ProxyId,
        epoch: EpochId,
        snapshot: &EpochObjectStates,
    ) -> Option<RetirementAction> {
        if let RetirementPhase::AwaitingSnapshot {
            proxy_id: expected_id,
            epoch: expected_epoch,
        } = &self.phase
        {
            if proxy_id != *expected_id {
                tracing::warn!(
                    proxy_id,
                    expected = expected_id,
                    "Received snapshot from unexpected proxy"
                );
                return None;
            }
            if epoch != *expected_epoch {
                tracing::warn!(
                    epoch = epoch.0,
                    expected = expected_epoch.0,
                    "Received snapshot for unexpected epoch"
                );
                return None;
            }

            tracing::info!(
                proxy_id,
                epoch = epoch.0,
                object_count = snapshot.len(),
                "Received retirement snapshot"
            );

            // Merge snapshot into StateCollector (versioned)
            self.merge_retirement_snapshot(proxy_id, epoch, snapshot);

            // Transition to AwaitingNextEpochSeal
            self.phase = RetirementPhase::AwaitingNextEpochSeal { proxy_id };

            Some(RetirementAction::UpdateOwnership { proxy_id })
        } else {
            tracing::warn!(
                proxy_id,
                epoch = epoch.0,
                "Received snapshot but not in AwaitingSnapshot phase"
            );
            None
        }
    }

    /// Called when an epoch is sealed (persisted).
    ///
    /// If we're awaiting next epoch seal after snapshot, complete the retirement.
    pub fn on_epoch_sealed(&mut self, _epoch: EpochId) -> Option<RetirementAction> {
        if let RetirementPhase::AwaitingNextEpochSeal { proxy_id } = &self.phase {
            let proxy_id = *proxy_id;
            tracing::info!(proxy_id, "Epoch sealed: completing retirement");

            // Reset to Idle
            self.phase = RetirementPhase::Idle;

            Some(RetirementAction::CompleteRetirement { proxy_id })
        } else {
            None
        }
    }

    /// Merge retirement snapshot into StateCollector with versioned check.
    fn merge_retirement_snapshot(
        &self,
        proxy_id: ProxyId,
        epoch: EpochId,
        snapshot: &EpochObjectStates,
    ) {
        // For each object in snapshot, update StateCollector only if newer
        for (object_id, object) in snapshot.iter() {
            let snapshot_version = object.version();

            // Check if we already have a newer version
            if let Some(existing_version) = self.collector.get_persisted_version(object_id) {
                if existing_version >= snapshot_version {
                    tracing::debug!(
                        ?object_id,
                        existing = existing_version.value(),
                        snapshot = snapshot_version.value(),
                        "Skipping snapshot object: existing version is not older"
                    );
                    continue;
                }
            }

            // Update merged_state with the newer snapshot object
            self.collector
                .merged_state
                .insert(*object_id, object.clone());
            tracing::debug!(
                ?object_id,
                version = snapshot_version.value(),
                proxy_id,
                epoch = epoch.0,
                "Merged retirement snapshot object"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_collector() -> Arc<StateCollector> {
        Arc::new(StateCollector::new(3))
    }

    #[test]
    fn test_initial_state_is_idle() {
        let coordinator = RetirementCoordinator::new(create_test_collector());
        assert_eq!(*coordinator.phase(), RetirementPhase::Idle);
        assert!(!coordinator.is_retiring());
    }

    #[test]
    fn test_initiate_retirement() {
        let mut coordinator = RetirementCoordinator::new(create_test_collector());

        assert!(coordinator.initiate(2));
        assert!(coordinator.is_retiring());
        assert!(coordinator.is_proxy_retiring(2));
        assert!(!coordinator.is_proxy_retiring(1));
    }

    #[test]
    fn test_cannot_initiate_twice() {
        let mut coordinator = RetirementCoordinator::new(create_test_collector());

        assert!(coordinator.initiate(2));
        assert!(!coordinator.initiate(1)); // Should fail
        assert!(coordinator.is_proxy_retiring(2)); // Original still pending
    }

    #[test]
    fn test_cancel_pending_retirement() {
        let mut coordinator = RetirementCoordinator::new(create_test_collector());

        coordinator.initiate(2);
        assert!(coordinator.cancel());
        assert!(!coordinator.is_retiring());
    }

    #[test]
    fn test_epoch_boundary_transitions_state() {
        let mut coordinator = RetirementCoordinator::new(create_test_collector());

        coordinator.initiate(2);
        let action = coordinator.on_epoch_boundary(EpochId(5));

        assert!(matches!(
            action,
            Some(RetirementAction::SendRetirementSignal {
                proxy_id: 2,
                epoch: EpochId(5)
            })
        ));

        assert!(matches!(
            coordinator.phase(),
            RetirementPhase::AwaitingSnapshot {
                proxy_id: 2,
                epoch: EpochId(5)
            }
        ));
    }

    #[test]
    fn test_snapshot_received_transitions_state() {
        let mut coordinator = RetirementCoordinator::new(create_test_collector());

        coordinator.initiate(2);
        coordinator.on_epoch_boundary(EpochId(5));

        let snapshot = EpochObjectStates::new();
        let action = coordinator.on_snapshot_received(2, EpochId(5), &snapshot);

        assert!(matches!(
            action,
            Some(RetirementAction::UpdateOwnership { proxy_id: 2 })
        ));

        assert!(matches!(
            coordinator.phase(),
            RetirementPhase::AwaitingNextEpochSeal { proxy_id: 2 }
        ));
    }

    #[test]
    fn test_epoch_sealed_completes_retirement() {
        let mut coordinator = RetirementCoordinator::new(create_test_collector());

        coordinator.initiate(2);
        coordinator.on_epoch_boundary(EpochId(5));
        coordinator.on_snapshot_received(2, EpochId(5), &EpochObjectStates::new());

        let action = coordinator.on_epoch_sealed(EpochId(6));

        assert!(matches!(
            action,
            Some(RetirementAction::CompleteRetirement { proxy_id: 2 })
        ));

        assert!(!coordinator.is_retiring());
    }

    #[test]
    fn test_full_retirement_flow() {
        let mut coordinator = RetirementCoordinator::new(create_test_collector());

        // Phase 1: Initiate
        assert!(coordinator.initiate(2));
        assert!(matches!(
            coordinator.phase(),
            RetirementPhase::PendingEpochBoundary { proxy_id: 2 }
        ));

        // Phase 2: Epoch boundary
        let action1 = coordinator.on_epoch_boundary(EpochId(10));
        assert!(action1.is_some());

        // Phase 3: Snapshot received
        let action2 = coordinator.on_snapshot_received(2, EpochId(10), &EpochObjectStates::new());
        assert!(action2.is_some());

        // Phase 4: Next epoch sealed
        let action3 = coordinator.on_epoch_sealed(EpochId(11));
        assert!(action3.is_some());

        // Back to idle
        assert!(!coordinator.is_retiring());
    }
}

/// Property-based tests for the retirement state machine using rand.
/// These tests verify invariants hold across randomly generated inputs.
#[cfg(test)]
mod property_tests {
    use super::*;
    use rand::Rng;

    fn create_test_collector() -> Arc<StateCollector> {
        Arc::new(StateCollector::new(3))
    }

    const NUM_ITERATIONS: usize = 100;

    #[test]
    fn prop_initiate_always_sets_retiring() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let proxy_id: ProxyId = rng.gen_range(0..100);
            let mut coordinator = RetirementCoordinator::new(create_test_collector());
            coordinator.initiate(proxy_id);
            assert!(
                coordinator.is_retiring(),
                "Failed for proxy_id={}",
                proxy_id
            );
        }
    }

    #[test]
    fn prop_double_initiation_fails() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let proxy_id1: ProxyId = rng.gen_range(0..100);
            let proxy_id2: ProxyId = rng.gen_range(0..100);
            let mut coordinator = RetirementCoordinator::new(create_test_collector());
            let first = coordinator.initiate(proxy_id1);
            let second = coordinator.initiate(proxy_id2);
            assert!(first, "First initiation should succeed");
            assert!(!second, "Second initiation should fail");
        }
    }

    #[test]
    fn prop_cancel_always_returns_to_idle() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let proxy_id: ProxyId = rng.gen_range(0..100);
            let mut coordinator = RetirementCoordinator::new(create_test_collector());
            coordinator.initiate(proxy_id);
            coordinator.cancel();
            assert!(!coordinator.is_retiring());
            assert_eq!(*coordinator.phase(), RetirementPhase::Idle);
        }
    }

    #[test]
    fn prop_full_flow_terminates_in_idle() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let proxy_id: ProxyId = rng.gen_range(0..100);
            let epoch1 = EpochId(rng.gen_range(1..500));
            let epoch2 = EpochId(epoch1.0 + rng.gen_range(1..10));

            let mut coordinator = RetirementCoordinator::new(create_test_collector());
            coordinator.initiate(proxy_id);
            coordinator.on_epoch_boundary(epoch1);
            coordinator.on_snapshot_received(proxy_id, epoch1, &EpochObjectStates::new());
            coordinator.on_epoch_sealed(epoch2);

            assert!(
                !coordinator.is_retiring(),
                "Failed for proxy={}, epoch1={:?}, epoch2={:?}",
                proxy_id,
                epoch1,
                epoch2
            );
        }
    }

    #[test]
    fn prop_epoch_boundary_on_idle_is_noop() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let epoch = EpochId(rng.gen_range(1..1000));
            let mut coordinator = RetirementCoordinator::new(create_test_collector());
            let action = coordinator.on_epoch_boundary(epoch);
            assert!(action.is_none());
            assert!(!coordinator.is_retiring());
        }
    }

    #[test]
    fn prop_snapshot_from_wrong_proxy_ignored() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let retiring_proxy: ProxyId = rng.gen_range(0..50);
            let wrong_proxy: ProxyId = rng.gen_range(50..100);
            let epoch = EpochId(rng.gen_range(1..1000));

            let mut coordinator = RetirementCoordinator::new(create_test_collector());
            coordinator.initiate(retiring_proxy);
            coordinator.on_epoch_boundary(epoch);

            let action =
                coordinator.on_snapshot_received(wrong_proxy, epoch, &EpochObjectStates::new());
            assert!(
                action.is_none(),
                "Snapshot from wrong proxy should be ignored"
            );
            assert!(coordinator.is_retiring(), "Should still be retiring");
        }
    }

    #[test]
    fn prop_snapshot_wrong_epoch_ignored() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let proxy_id: ProxyId = rng.gen_range(0..100);
            let signal_epoch = EpochId(rng.gen_range(1..500));
            let wrong_epoch = EpochId(signal_epoch.0 + rng.gen_range(1..100));

            let mut coordinator = RetirementCoordinator::new(create_test_collector());
            coordinator.initiate(proxy_id);
            coordinator.on_epoch_boundary(signal_epoch);

            let action =
                coordinator.on_snapshot_received(proxy_id, wrong_epoch, &EpochObjectStates::new());
            assert!(
                action.is_none(),
                "Snapshot with wrong epoch should be ignored"
            );
            assert!(coordinator.is_retiring());
        }
    }

    #[test]
    fn prop_state_advances_monotonically() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let proxy_id: ProxyId = rng.gen_range(0..100);
            let epoch1 = EpochId(rng.gen_range(1..500));
            let epoch2 = EpochId(epoch1.0 + 1);

            let mut coordinator = RetirementCoordinator::new(create_test_collector());

            coordinator.initiate(proxy_id);
            assert!(matches!(
                coordinator.phase(),
                RetirementPhase::PendingEpochBoundary { .. }
            ));

            coordinator.on_epoch_boundary(epoch1);
            assert!(matches!(
                coordinator.phase(),
                RetirementPhase::AwaitingSnapshot { .. }
            ));

            coordinator.on_snapshot_received(proxy_id, epoch1, &EpochObjectStates::new());
            assert!(matches!(
                coordinator.phase(),
                RetirementPhase::AwaitingNextEpochSeal { .. }
            ));

            coordinator.on_epoch_sealed(epoch2);
            assert_eq!(*coordinator.phase(), RetirementPhase::Idle);
        }
    }

    #[test]
    fn prop_any_epoch_seal_completes_after_snapshot() {
        // Note: The retirement coordinator completes on ANY epoch seal after snapshot,
        // regardless of actual epoch number. This tests that behavior.
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let proxy_id: ProxyId = rng.gen_range(0..100);
            let signal_epoch = EpochId(rng.gen_range(1..500));
            let seal_epoch = EpochId(rng.gen_range(1..1000)); // Any epoch

            let mut coordinator = RetirementCoordinator::new(create_test_collector());
            coordinator.initiate(proxy_id);
            coordinator.on_epoch_boundary(signal_epoch);
            coordinator.on_snapshot_received(proxy_id, signal_epoch, &EpochObjectStates::new());

            // Any epoch seal should complete retirement when in AwaitingNextEpochSeal phase
            let action = coordinator.on_epoch_sealed(seal_epoch);
            assert!(
                matches!(action, Some(RetirementAction::CompleteRetirement { .. })),
                "Any epoch seal should complete retirement after snapshot"
            );
            assert!(!coordinator.is_retiring());
        }
    }

    #[test]
    fn prop_random_event_sequence_never_panics() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let mut coordinator = RetirementCoordinator::new(create_test_collector());

            for _ in 0..20 {
                let event_type = rng.gen_range(0..6);
                let proxy_id: ProxyId = rng.gen_range(0..10);
                let epoch = EpochId(rng.gen_range(1..100));

                match event_type {
                    0 => {
                        coordinator.initiate(proxy_id);
                    }
                    1 => {
                        coordinator.cancel();
                    }
                    2 => {
                        coordinator.on_epoch_boundary(epoch);
                    }
                    3 => {
                        coordinator.on_snapshot_received(
                            proxy_id,
                            epoch,
                            &EpochObjectStates::new(),
                        );
                    }
                    4 => {
                        coordinator.on_epoch_sealed(epoch);
                    }
                    5 => {
                        coordinator.is_retiring();
                    }
                    _ => {}
                }
            }
        }
    }
}
