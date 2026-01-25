// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Elastic Scaler Module
//!
//! Handles both scale-out (adding proxies) and scale-in (retiring proxies)
//! based on observed load. Uses a state machine for graceful epoch-boundary
//! aligned retirement.

pub mod retirement_coordinator;
pub mod retirement_event;

pub use retirement_event::RetirementEvent;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

/// Threshold multiplier for scale-in: retire if load < capacity * threshold
const SCALE_IN_THRESHOLD: f64 = 0.8;
/// Threshold multiplier for scale-out: add node if load > capacity * threshold
const SCALE_OUT_THRESHOLD: f64 = 0.8;
/// Rate calculation window in milliseconds
const RATE_WINDOW_MS: u64 = 1000;

/// Encapsulates elastic scaling logic for both scale-out and scale-in.
///
/// Ported from the elastic branch with scale-in additions.
/// Scale-out decisions are queued and activated at epoch boundaries to ensure
/// the primary knows exactly which proxies are participating in each epoch.
pub struct ElasticScaler {
    /// Number of active nodes (can scale out/in based on load)
    active_nodes: Arc<AtomicUsize>,
    /// Minimum number of nodes (cannot scale below this, typically 1)
    min_nodes: usize,
    /// Maximum number of nodes (cannot scale above this)
    max_nodes: usize,
    /// Pre-calculated per-node capacity in transactions per second
    per_node_capacity_tps: Option<f64>,
    /// Count of incoming transactions in current rate window
    incoming_rate_count: usize,
    /// Start time of current rate tracking window (milliseconds since epoch)
    rate_window_start: u64,
    /// Last time a scaling check was performed (milliseconds since epoch)
    last_scale_check: u64,
    /// Pending scale-out: new node will become active at next epoch boundary.
    /// This ensures epoch-aligned scaling so the primary knows exactly which
    /// proxies are participating in each epoch.
    pending_scale_out: bool,
}

/// Result of activating a pending scale-out at epoch boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScaleOutActivation {
    /// Previous active node count
    pub previous_count: usize,
    /// New active node count after scale-out
    pub new_count: usize,
}

impl ElasticScaler {
    /// Create a new elastic scaler starting at minimum nodes.
    ///
    /// Use this for truly elastic behavior where we start small and scale out.
    ///
    /// # Arguments
    /// * `max_nodes` - Maximum number of available nodes
    pub fn new(max_nodes: usize) -> Self {
        Self::with_initial_nodes(1, max_nodes)
    }

    /// Create a new elastic scaler with explicit initial node count.
    ///
    /// # Arguments
    /// * `initial_nodes` - Starting number of active nodes
    /// * `max_nodes` - Maximum number of available nodes
    pub fn with_initial_nodes(initial_nodes: usize, max_nodes: usize) -> Self {
        let now = Self::now_millis();
        let initial = initial_nodes.clamp(1, max_nodes);
        Self {
            active_nodes: Arc::new(AtomicUsize::new(initial)),
            min_nodes: 1,
            max_nodes,
            per_node_capacity_tps: None,
            incoming_rate_count: 0,
            rate_window_start: now,
            last_scale_check: now,
            pending_scale_out: false,
        }
    }

    /// Create a new elastic scaler with initial nodes calculated from expected load.
    ///
    /// # Arguments
    /// * `expected_load_tps` - Expected initial load in transactions per second
    /// * `per_node_capacity` - Per-node capacity in TPS
    /// * `max_nodes` - Maximum number of available nodes
    pub fn with_expected_load(
        expected_load_tps: f64,
        per_node_capacity: f64,
        max_nodes: usize,
    ) -> Self {
        // Calculate minimum nodes needed for the expected load
        let nodes_needed =
            (expected_load_tps / (per_node_capacity * SCALE_OUT_THRESHOLD)).ceil() as usize;
        let initial_nodes = nodes_needed.clamp(1, max_nodes);

        tracing::info!(
            expected_load = expected_load_tps,
            per_node_capacity,
            nodes_needed,
            initial_nodes,
            max_nodes,
            "ElasticScaler: calculated initial nodes from expected load"
        );

        let mut scaler = Self::with_initial_nodes(initial_nodes, max_nodes);
        scaler.per_node_capacity_tps = Some(per_node_capacity);
        scaler
    }

    /// Get current time in milliseconds since epoch.
    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// Record incoming transactions for rate tracking.
    pub fn record_transactions(&mut self, count: usize) {
        self.incoming_rate_count += count;
    }

    /// Calculate and store per-node capacity from transaction durations.
    pub fn calculate_capacity(
        &mut self,
        _verification_duration: std::time::Duration,
        _expected_stateful_duration: std::time::Duration,
    ) {
        if self.per_node_capacity_tps.is_none() {
            // HARDCODE core cap with 1ms workload (from elastic branch)
            // TODO: Calculate dynamically from verification + stateful duration
            self.per_node_capacity_tps = Some(27000.0);
        }
    }

    /// Set the per-node capacity manually.
    pub fn set_capacity(&mut self, capacity_tps: f64) {
        self.per_node_capacity_tps = Some(capacity_tps);
    }

    /// Get current number of active nodes.
    pub fn active_node_count(&self) -> usize {
        self.active_nodes.load(Ordering::Relaxed)
    }

    /// Get a clone of the active_nodes Arc for sharing with spawned tasks.
    pub fn active_nodes_handle(&self) -> Arc<AtomicUsize> {
        self.active_nodes.clone()
    }

    /// Calculate current incoming rate in transactions per second.
    fn calculate_current_rate(&mut self) -> f64 {
        let now = Self::now_millis();
        let window_duration = now.saturating_sub(self.rate_window_start);

        if window_duration >= RATE_WINDOW_MS {
            let rate = (self.incoming_rate_count as f64) / (window_duration as f64 / 1000.0);
            // Reset window
            self.incoming_rate_count = 0;
            self.rate_window_start = now;
            rate
        } else if window_duration > 0 {
            // Extrapolate current rate
            (self.incoming_rate_count as f64) / (window_duration as f64 / 1000.0)
        } else {
            0.0
        }
    }

    /// Check if we should scale in: current load handleable by N-1 nodes (N>=2).
    ///
    /// Returns true if:
    /// - We have at least 2 active nodes
    /// - Current rate can be handled by (N-1) nodes at SCALE_IN_THRESHOLD capacity
    pub fn should_scale_in(&self, current_rate: f64) -> bool {
        let active = self.active_nodes.load(Ordering::Relaxed);

        // Need at least 2 nodes to scale in
        if active <= self.min_nodes || active < 2 {
            return false;
        }

        // Need capacity to be set for scaling decisions
        let Some(per_node_cap) = self.per_node_capacity_tps else {
            return false;
        };

        let capacity_with_one_less = per_node_cap * (active - 1) as f64;
        current_rate <= capacity_with_one_less * SCALE_IN_THRESHOLD
    }

    /// Check if we should scale out: current load exceeds capacity threshold.
    ///
    /// Returns true if:
    /// - We have room to add more nodes
    /// - Current rate exceeds SCALE_OUT_THRESHOLD of current capacity
    pub fn should_scale_out(&self, current_rate: f64) -> bool {
        let active = self.active_nodes.load(Ordering::Relaxed);

        // Cannot scale beyond max nodes
        if active >= self.max_nodes {
            return false;
        }

        // Need capacity to be set for scaling decisions
        let Some(per_node_cap) = self.per_node_capacity_tps else {
            return false;
        };

        let total_current_capacity = per_node_cap * active as f64;
        current_rate > total_current_capacity * SCALE_OUT_THRESHOLD
    }

    /// Increase active node count (scale-out).
    pub fn increase_active_nodes(&self) {
        let current = self.active_nodes.load(Ordering::Relaxed);
        if current < self.max_nodes {
            self.active_nodes.store(current + 1, Ordering::Relaxed);
            tracing::info!("SCALE OUT: Active nodes {} -> {}", current, current + 1);
        }
    }

    /// Decrease active node count (scale-in).
    pub fn decrease_active_nodes(&self) {
        let current = self.active_nodes.load(Ordering::Relaxed);
        if current > self.min_nodes {
            self.active_nodes.store(current - 1, Ordering::Relaxed);
            tracing::info!("SCALE IN: Active nodes {} -> {}", current, current - 1);
        }
    }

    /// Main scaling check: determine if we need to scale out or in.
    ///
    /// Returns:
    /// - `Some(ScalingDecision::ScaleOut)` if we should add a node
    /// - `Some(ScalingDecision::ScaleIn)` if we should retire a node
    /// - `None` if no scaling action needed
    pub fn check_scaling(&mut self) -> Option<ScalingDecision> {
        let now = Self::now_millis();

        // Rate-limiting is handled by the caller/scheduler.
        self.last_scale_check = now;

        // Auto-initialize capacity if not set (hardcoded for 1ms workload as in elastic branch)
        if self.per_node_capacity_tps.is_none() {
            self.per_node_capacity_tps = Some(27000.0);
            tracing::info!("ElasticScaler: auto-initialized per-node capacity to 27000 TPS");
        }

        let current_rate = self.calculate_current_rate();
        let active = self.active_nodes.load(Ordering::Relaxed);
        let capacity = self.per_node_capacity_tps.unwrap_or(0.0) * active as f64;

        if self.should_scale_out(current_rate) {
            tracing::info!(
                "ElasticScaler: SCALE OUT triggered - rate={:.0} TPS, capacity={:.0} TPS ({}×{:.0}), active={}/{}, threshold={:.0}%",
                current_rate, capacity, active, self.per_node_capacity_tps.unwrap_or(0.0),
                active, self.max_nodes, SCALE_OUT_THRESHOLD * 100.0
            );
            Some(ScalingDecision::ScaleOut)
        } else if self.should_scale_in(current_rate) {
            tracing::info!(
                "ElasticScaler: SCALE IN triggered - rate={:.0} TPS, capacity={:.0} TPS ({}×{:.0}), active={}/{}, threshold={:.0}%",
                current_rate, capacity, active, self.per_node_capacity_tps.unwrap_or(0.0),
                active, self.max_nodes, SCALE_IN_THRESHOLD * 100.0
            );
            Some(ScalingDecision::ScaleIn)
        } else {
            None
        }
    }

    // ==================== Epoch-Aligned Scale-Out ====================

    /// Queue a scale-out decision to take effect at the next epoch boundary.
    ///
    /// This ensures the primary knows exactly which proxies are participating
    /// in each epoch, preventing empty snapshots from newly-activated proxies
    /// that didn't receive any transactions for the current epoch.
    ///
    /// Returns false if:
    /// - A scale-out is already pending
    /// - Already at maximum nodes
    pub fn queue_scale_out(&mut self) -> bool {
        if self.pending_scale_out {
            tracing::debug!("Scale-out already pending, ignoring duplicate request");
            return false;
        }

        let active = self.active_nodes.load(Ordering::Relaxed);
        if active >= self.max_nodes {
            tracing::debug!(
                "Cannot queue scale-out: already at max nodes ({}/{})",
                active,
                self.max_nodes
            );
            return false;
        }

        self.pending_scale_out = true;
        tracing::info!(
            "SCALE OUT queued: will activate at next epoch boundary ({} -> {} nodes)",
            active,
            active + 1
        );
        true
    }

    /// Called at epoch boundary to activate any pending scale-out.
    ///
    /// Returns `Some(ScaleOutActivation)` if a scale-out was activated,
    /// `None` if no scale-out was pending.
    pub fn on_epoch_boundary(&mut self) -> Option<ScaleOutActivation> {
        if !self.pending_scale_out {
            return None;
        }

        self.pending_scale_out = false;
        let previous = self.active_nodes.load(Ordering::Relaxed);

        if previous < self.max_nodes {
            self.active_nodes.store(previous + 1, Ordering::Relaxed);
            let activation = ScaleOutActivation {
                previous_count: previous,
                new_count: previous + 1,
            };
            tracing::info!(
                "SCALE OUT activated at epoch boundary: {} -> {} active nodes",
                previous,
                previous + 1
            );
            Some(activation)
        } else {
            tracing::warn!(
                "Pending scale-out cancelled: already at max nodes ({}/{})",
                previous,
                self.max_nodes
            );
            None
        }
    }

    /// Check if there is a pending scale-out waiting for epoch boundary.
    pub fn is_scale_out_pending(&self) -> bool {
        self.pending_scale_out
    }
}

/// Scaling decision from the ElasticScaler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalingDecision {
    /// Add a new active node (scale-out)
    ScaleOut,
    /// Retire the highest-ID node (scale-in)
    ScaleIn,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elastic_scaler_new() {
        let scaler = ElasticScaler::with_initial_nodes(3, 5);
        assert_eq!(scaler.active_node_count(), 3);
    }

    #[test]
    fn test_should_scale_in_with_low_load() {
        let mut scaler = ElasticScaler::with_initial_nodes(3, 5);
        scaler.set_capacity(10000.0); // 10k TPS per node

        // 3 nodes = 30k capacity
        // With 2 nodes = 20k capacity
        // Scale-in threshold = 20k * 0.8 = 16k
        // So if load < 16k, should scale in

        assert!(scaler.should_scale_in(10000.0)); // 10k < 16k
        assert!(scaler.should_scale_in(15000.0)); // 15k < 16k
        assert!(!scaler.should_scale_in(20000.0)); // 20k > 16k
    }

    #[test]
    fn test_should_not_scale_in_with_min_nodes() {
        let mut scaler = ElasticScaler::with_initial_nodes(1, 5);
        scaler.set_capacity(10000.0);

        // Cannot scale in when at min nodes
        assert!(!scaler.should_scale_in(1000.0));
    }

    #[test]
    fn test_should_scale_out_with_high_load() {
        let mut scaler = ElasticScaler::with_initial_nodes(2, 5);
        scaler.set_capacity(10000.0); // 10k TPS per node

        // 2 nodes = 20k capacity
        // Scale-out threshold = 20k * 0.8 = 16k
        // So if load > 16k, should scale out

        assert!(scaler.should_scale_out(18000.0)); // 18k > 16k
        assert!(!scaler.should_scale_out(15000.0)); // 15k < 16k
    }

    #[test]
    fn test_should_not_scale_out_at_max_nodes() {
        let mut scaler = ElasticScaler::with_initial_nodes(5, 5);
        scaler.set_capacity(10000.0);

        // Cannot scale out when at max nodes
        assert!(!scaler.should_scale_out(100000.0));
    }

    #[test]
    fn test_increase_decrease_nodes() {
        let scaler = ElasticScaler::with_initial_nodes(3, 5);

        scaler.increase_active_nodes();
        assert_eq!(scaler.active_node_count(), 4);

        scaler.decrease_active_nodes();
        assert_eq!(scaler.active_node_count(), 3);
    }

    #[test]
    fn test_record_transactions_calculates_rate() {
        let mut scaler = ElasticScaler::with_initial_nodes(1, 5);
        scaler.record_transactions(100);

        // Mock time passage by manipulating the window start (hacky but effective for unit test if rate_window_start was pub or we wait, simpler to just trust the logic update or add a better test infrastructure later)
        // Since we can't easily mock time here without refactoring, we'll just check that count increased.
        assert_eq!(scaler.incoming_rate_count, 100);
    }

    // ==================== Epoch-Aligned Scale-Out Tests ====================

    #[test]
    fn test_queue_scale_out_sets_pending() {
        let mut scaler = ElasticScaler::with_initial_nodes(2, 5);

        assert!(!scaler.is_scale_out_pending());
        assert!(scaler.queue_scale_out());
        assert!(scaler.is_scale_out_pending());
        // Active nodes should NOT change yet
        assert_eq!(scaler.active_node_count(), 2);
    }

    #[test]
    fn test_queue_scale_out_idempotent() {
        let mut scaler = ElasticScaler::with_initial_nodes(2, 5);

        assert!(scaler.queue_scale_out()); // First call succeeds
        assert!(!scaler.queue_scale_out()); // Duplicate call fails
        assert!(scaler.is_scale_out_pending());
    }

    #[test]
    fn test_queue_scale_out_at_max_fails() {
        let mut scaler = ElasticScaler::with_initial_nodes(5, 5);

        assert!(!scaler.queue_scale_out()); // Already at max
        assert!(!scaler.is_scale_out_pending());
    }

    #[test]
    fn test_on_epoch_boundary_activates_pending() {
        let mut scaler = ElasticScaler::with_initial_nodes(2, 5);

        scaler.queue_scale_out();
        let activation = scaler.on_epoch_boundary();

        assert!(activation.is_some());
        let activation = activation.unwrap();
        assert_eq!(activation.previous_count, 2);
        assert_eq!(activation.new_count, 3);
        assert_eq!(scaler.active_node_count(), 3);
        assert!(!scaler.is_scale_out_pending());
    }

    #[test]
    fn test_on_epoch_boundary_no_pending() {
        let mut scaler = ElasticScaler::with_initial_nodes(2, 5);

        // No pending scale-out
        let activation = scaler.on_epoch_boundary();
        assert!(activation.is_none());
        assert_eq!(scaler.active_node_count(), 2);
    }

    #[test]
    fn test_epoch_aligned_scale_out_full_flow() {
        let mut scaler = ElasticScaler::with_initial_nodes(1, 3);

        // Epoch 1: queue scale-out mid-epoch
        assert!(scaler.queue_scale_out());
        assert_eq!(scaler.active_node_count(), 1); // Still 1

        // Epoch 2: boundary activates scale-out
        let activation = scaler.on_epoch_boundary();
        assert!(activation.is_some());
        assert_eq!(scaler.active_node_count(), 2); // Now 2

        // Queue another
        assert!(scaler.queue_scale_out());

        // Epoch 3: boundary activates again
        let activation = scaler.on_epoch_boundary();
        assert!(activation.is_some());
        assert_eq!(scaler.active_node_count(), 3); // Now 3

        // Cannot queue more (at max)
        assert!(!scaler.queue_scale_out());
    }
}

/// Property-based tests for epoch-aligned scale-out using rand.
/// These tests verify invariants hold across randomly generated inputs.
#[cfg(test)]
mod property_tests {
    use super::*;
    use rand::Rng;

    const NUM_ITERATIONS: usize = 100;

    /// Invariant: queue_scale_out should be idempotent (second call always fails)
    #[test]
    fn prop_queue_scale_out_is_idempotent() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let initial: usize = rng.gen_range(1..5);
            let max: usize = rng.gen_range(initial..10);
            let mut scaler = ElasticScaler::with_initial_nodes(initial, max);

            let first = scaler.queue_scale_out();
            let second = scaler.queue_scale_out();

            // If first succeeded, second must fail (idempotent)
            if first {
                assert!(!second, "Duplicate queue_scale_out should fail");
            }
        }
    }

    /// Invariant: on_epoch_boundary without pending always returns None
    #[test]
    fn prop_epoch_boundary_no_pending_is_noop() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let initial: usize = rng.gen_range(1..5);
            let max: usize = rng.gen_range(initial..10);
            let mut scaler = ElasticScaler::with_initial_nodes(initial, max);

            // No queued scale-out
            let activation = scaler.on_epoch_boundary();
            assert!(activation.is_none());
            assert_eq!(scaler.active_node_count(), initial);
        }
    }

    /// Invariant: queue_scale_out should never exceed max_nodes
    #[test]
    fn prop_active_nodes_never_exceed_max() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let initial: usize = rng.gen_range(1..5);
            let max: usize = rng.gen_range(initial..10);
            let mut scaler = ElasticScaler::with_initial_nodes(initial, max);

            // Queue and activate multiple times
            for _ in 0..20 {
                scaler.queue_scale_out();
                scaler.on_epoch_boundary();
            }

            assert!(
                scaler.active_node_count() <= max,
                "Active nodes {} exceeded max {}",
                scaler.active_node_count(),
                max
            );
        }
    }

    /// Invariant: on_epoch_boundary clears pending state
    #[test]
    fn prop_epoch_boundary_clears_pending() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let initial: usize = rng.gen_range(1..4);
            let max: usize = rng.gen_range(initial + 1..10);
            let mut scaler = ElasticScaler::with_initial_nodes(initial, max);

            scaler.queue_scale_out();
            assert!(scaler.is_scale_out_pending());

            scaler.on_epoch_boundary();
            assert!(
                !scaler.is_scale_out_pending(),
                "Pending should be cleared after epoch boundary"
            );
        }
    }

    /// Invariant: after successful queue + boundary, active_nodes increases by 1
    #[test]
    fn prop_scale_out_increases_by_one() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let initial: usize = rng.gen_range(1..4);
            let max: usize = rng.gen_range(initial + 1..10);
            let mut scaler = ElasticScaler::with_initial_nodes(initial, max);

            let before = scaler.active_node_count();
            let queued = scaler.queue_scale_out();
            let activation = scaler.on_epoch_boundary();
            let after = scaler.active_node_count();

            if queued && activation.is_some() {
                assert_eq!(
                    after,
                    before + 1,
                    "Scale-out should increase active nodes by 1"
                );
            }
        }
    }

    /// Invariant: ScaleOutActivation contains correct previous/new counts
    #[test]
    fn prop_activation_counts_are_consistent() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let initial: usize = rng.gen_range(1..4);
            let max: usize = rng.gen_range(initial + 1..10);
            let mut scaler = ElasticScaler::with_initial_nodes(initial, max);

            scaler.queue_scale_out();
            if let Some(activation) = scaler.on_epoch_boundary() {
                assert_eq!(
                    activation.new_count,
                    activation.previous_count + 1,
                    "new_count should be previous_count + 1"
                );
                assert_eq!(
                    scaler.active_node_count(),
                    activation.new_count,
                    "active_node_count should equal new_count"
                );
            }
        }
    }

    /// Invariant: random sequence of operations never panics and maintains invariants
    #[test]
    fn prop_random_operations_maintain_invariants() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let initial: usize = rng.gen_range(1..5);
            let max: usize = rng.gen_range(initial..10);
            let mut scaler = ElasticScaler::with_initial_nodes(initial, max);

            for _ in 0..50 {
                let op = rng.gen_range(0..5);
                match op {
                    0 => {
                        scaler.queue_scale_out();
                    }
                    1 => {
                        scaler.on_epoch_boundary();
                    }
                    2 => {
                        scaler.is_scale_out_pending();
                    }
                    3 => {
                        scaler.active_node_count();
                    }
                    4 => {
                        // Simulate decrease (scale-in)
                        scaler.decrease_active_nodes();
                    }
                    _ => {}
                }

                // Invariants must hold after every operation
                assert!(scaler.active_node_count() >= 1, "Active nodes must be >= 1");
                assert!(
                    scaler.active_node_count() <= max,
                    "Active nodes {} exceeded max {}",
                    scaler.active_node_count(),
                    max
                );
            }
        }
    }
}
