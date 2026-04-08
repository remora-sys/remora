// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io::{self, Write},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use dashmap::DashMap;
use sui_types::digests::TransactionDigest;

use crate::executor::api::{
    ExecutableTransaction, InterProxyReply, InterProxyRequest, PrimaryToProxyMessage,
    ProxyToProxyMessage, StatelessVerificationRequest, TransactionWithTimestamp,
};

pub trait MeasuredMessage {
    fn measurement_digest(&self) -> Option<TransactionDigest>;
}

impl MeasuredMessage for () {
    fn measurement_digest(&self) -> Option<TransactionDigest> {
        None
    }
}

impl MeasuredMessage for String {
    fn measurement_digest(&self) -> Option<TransactionDigest> {
        None
    }
}

impl MeasuredMessage for u32 {
    fn measurement_digest(&self) -> Option<TransactionDigest> {
        None
    }
}

impl<T> MeasuredMessage for T
where
    T: ExecutableTransaction + Clone,
{
    fn measurement_digest(&self) -> Option<TransactionDigest> {
        None
    }
}

impl<T> MeasuredMessage for TransactionWithTimestamp<T>
where
    T: ExecutableTransaction + Clone,
{
    fn measurement_digest(&self) -> Option<TransactionDigest> {
        (!self.shared_objects().is_empty()).then_some(*self.digest())
    }
}

impl MeasuredMessage for StatelessVerificationRequest {
    fn measurement_digest(&self) -> Option<TransactionDigest> {
        None
    }
}

impl<T> MeasuredMessage for PrimaryToProxyMessage<T>
where
    T: ExecutableTransaction + Clone,
{
    fn measurement_digest(&self) -> Option<TransactionDigest> {
        match self {
            Self::Txn(transaction, ..) | Self::CombinedTxn(transaction, ..) => {
                (!transaction.shared_objects().is_empty()).then_some(*transaction.digest())
            }
            Self::StatelessTxn(_) => None,
        }
    }
}

impl MeasuredMessage for ProxyToProxyMessage {
    fn measurement_digest(&self) -> Option<TransactionDigest> {
        match self {
            Self::Request(InterProxyRequest::Stateless(_, digest))
            | Self::Reply(InterProxyReply::Stateless(digest, _)) => Some(*digest),
            Self::Request(InterProxyRequest::Stateful(_, _))
            | Self::Reply(InterProxyReply::Stateful(_, _)) => None,
        }
    }
}

#[derive(Default)]
struct StageTiming {
    total_ns: u64,
    completed: usize,
    first_start: Option<Instant>,
    last_end: Option<Instant>,
}

impl StageTiming {
    fn record(&mut self, start: Instant, elapsed: Duration) {
        self.total_ns += duration_to_ns(elapsed);
        self.completed += 1;
        self.first_start = Some(match self.first_start {
            Some(existing) => existing.min(start),
            None => start,
        });

        let end = start.checked_add(elapsed).unwrap_or(start);
        self.last_end = Some(match self.last_end {
            Some(existing) => existing.max(end),
            None => end,
        });
    }

    fn merge(&mut self, other: StageTiming) {
        self.total_ns += other.total_ns;
        self.completed += other.completed;
        self.first_start = merge_min_instant(self.first_start, other.first_start);
        self.last_end = merge_max_instant(self.last_end, other.last_end);
    }

    fn sum_ms(&self) -> f64 {
        ns_to_ms(self.total_ns)
    }

    fn avg_ms(&self) -> f64 {
        if self.completed == 0 {
            0.0
        } else {
            self.sum_ms() / self.completed as f64
        }
    }

    fn makespan_ms(&self) -> f64 {
        match (self.first_start, self.last_end) {
            (Some(start), Some(end)) => duration_to_ms(end.duration_since(start)),
            _ => 0.0,
        }
    }
}

#[derive(Default)]
struct BatchBreakdownState {
    digests: Vec<TransactionDigest>,
    tx_count: usize,
    network_rx_deser: StageTiming,
    batch_scheduling: StageTiming,
    version_assignment: StageTiming,
    dispatch_forwarding: StageTiming,
    network_tx_serialize: StageTiming,
    scheduling_recorded: bool,
    version_assignment_recorded: bool,
}

#[derive(Default)]
pub(crate) struct BatchBreakdownCollector {
    next_batch_id: AtomicU64,
    digest_to_batch: DashMap<TransactionDigest, u64>,
    pending_network_rx_deser: DashMap<TransactionDigest, StageTiming>,
    batches: DashMap<u64, BatchBreakdownState>,
}

impl BatchBreakdownCollector {
    pub(crate) fn register_shared_batch<T>(
        &self,
        transactions: &[TransactionWithTimestamp<T>],
    ) -> Option<u64>
    where
        T: ExecutableTransaction + Clone,
    {
        let digests: Vec<_> = transactions
            .iter()
            .filter_map(MeasuredMessage::measurement_digest)
            .collect();
        if digests.is_empty() {
            return None;
        }

        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let mut state = BatchBreakdownState {
            tx_count: digests.len(),
            digests: digests.clone(),
            ..Default::default()
        };

        for digest in &digests {
            self.digest_to_batch.insert(*digest, batch_id);
            if let Some((_, pending_timing)) = self.pending_network_rx_deser.remove(digest) {
                state.network_rx_deser.merge(pending_timing);
            }
        }

        self.batches.insert(batch_id, state);
        Some(batch_id)
    }

    pub(crate) fn batch_id_for_transactions<T>(
        &self,
        transactions: &[TransactionWithTimestamp<T>],
    ) -> Option<u64>
    where
        T: ExecutableTransaction + Clone,
    {
        transactions.iter().find_map(|transaction| {
            transaction
                .measurement_digest()
                .and_then(|digest| self.digest_to_batch.get(&digest).map(|batch_id| *batch_id))
        })
    }

    pub(crate) fn record_network_rx_deser(
        &self,
        digest: TransactionDigest,
        start: Instant,
        elapsed: Duration,
    ) {
        if let Some(batch_id) = self.batch_id_for_digest(&digest) {
            self.update_batch(batch_id, |state| {
                state.network_rx_deser.record(start, elapsed);
            });
            return;
        }

        self.pending_network_rx_deser
            .entry(digest)
            .and_modify(|timing| timing.record(start, elapsed))
            .or_insert_with(|| {
                let mut timing = StageTiming::default();
                timing.record(start, elapsed);
                timing
            });
    }

    pub(crate) fn record_batch_scheduling(&self, batch_id: u64, start: Instant, elapsed: Duration) {
        self.update_batch(batch_id, |state| {
            state.batch_scheduling.record(start, elapsed);
            state.scheduling_recorded = true;
        });
    }

    pub(crate) fn record_version_assignment(
        &self,
        batch_id: u64,
        start: Instant,
        elapsed: Duration,
    ) {
        self.update_batch(batch_id, |state| {
            state.version_assignment.record(start, elapsed);
            state.version_assignment_recorded = true;
        });
    }

    pub(crate) fn record_dispatch_forwarding(
        &self,
        digest: TransactionDigest,
        start: Instant,
        elapsed: Duration,
    ) {
        let Some(batch_id) = self.batch_id_for_digest(&digest) else {
            return;
        };

        self.update_batch(batch_id, |state| {
            state.dispatch_forwarding.record(start, elapsed);
        });
        self.maybe_finish_batch(batch_id);
    }

    pub(crate) fn record_network_tx_serialize(
        &self,
        digest: TransactionDigest,
        start: Instant,
        elapsed: Duration,
    ) {
        let Some(batch_id) = self.batch_id_for_digest(&digest) else {
            return;
        };

        self.update_batch(batch_id, |state| {
            state.network_tx_serialize.record(start, elapsed);
        });
        self.maybe_finish_batch(batch_id);
    }

    fn update_batch(&self, batch_id: u64, update: impl FnOnce(&mut BatchBreakdownState)) {
        if let Some(mut state) = self.batches.get_mut(&batch_id) {
            update(&mut state);
        }
    }

    fn batch_id_for_digest(&self, digest: &TransactionDigest) -> Option<u64> {
        self.digest_to_batch.get(digest).map(|batch_id| *batch_id)
    }

    fn maybe_finish_batch(&self, batch_id: u64) {
        let ready = self
            .batches
            .get(&batch_id)
            .map(|state| {
                state.scheduling_recorded
                    && state.version_assignment_recorded
                    && state.dispatch_forwarding.completed == state.tx_count
                    && state.network_tx_serialize.completed == state.tx_count
            })
            .unwrap_or(false);

        if !ready {
            return;
        }

        let Some((_, state)) = self.batches.remove(&batch_id) else {
            return;
        };

        for digest in &state.digests {
            self.digest_to_batch.remove(digest);
            self.pending_network_rx_deser.remove(digest);
        }

        let summary = summarize_batch(batch_id, &state);

        // Print directly to stdout so the measurement remains visible in `--release`
        // regardless of tracing subscriber configuration or log level.
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "{}", format_summary(&summary))
            .expect("batch breakdown write to stdout should succeed");
    }
}

#[derive(Debug, PartialEq)]
struct StageSummary {
    sum_ms: f64,
    avg_ms: f64,
    makespan_ms: f64,
}

#[derive(Debug, PartialEq)]
struct BatchBreakdownSummary {
    batch_id: u64,
    tx_count: usize,
    network_rx_deser: StageSummary,
    batch_scheduling_ms: f64,
    version_assignment_ms: f64,
    dispatch_forwarding: StageSummary,
    network_tx_serialize: StageSummary,
    batch_observed_wall_ms: f64,
}

fn summarize_stage(stage: &StageTiming) -> StageSummary {
    StageSummary {
        sum_ms: stage.sum_ms(),
        avg_ms: stage.avg_ms(),
        makespan_ms: stage.makespan_ms(),
    }
}

fn summarize_batch(batch_id: u64, state: &BatchBreakdownState) -> BatchBreakdownSummary {
    let batch_start = [
        state.network_rx_deser.first_start,
        state.batch_scheduling.first_start,
        state.version_assignment.first_start,
        state.dispatch_forwarding.first_start,
        state.network_tx_serialize.first_start,
    ]
    .into_iter()
    .flatten()
    .min();
    let batch_end = [
        state.network_rx_deser.last_end,
        state.batch_scheduling.last_end,
        state.version_assignment.last_end,
        state.dispatch_forwarding.last_end,
        state.network_tx_serialize.last_end,
    ]
    .into_iter()
    .flatten()
    .max();

    BatchBreakdownSummary {
        batch_id,
        tx_count: state.tx_count,
        network_rx_deser: summarize_stage(&state.network_rx_deser),
        batch_scheduling_ms: state.batch_scheduling.sum_ms(),
        version_assignment_ms: state.version_assignment.sum_ms(),
        dispatch_forwarding: summarize_stage(&state.dispatch_forwarding),
        network_tx_serialize: summarize_stage(&state.network_tx_serialize),
        batch_observed_wall_ms: match (batch_start, batch_end) {
            (Some(start), Some(end)) => duration_to_ms(end.duration_since(start)),
            _ => 0.0,
        },
    }
}

fn format_summary(summary: &BatchBreakdownSummary) -> String {
    format!(
        "[primary-batch-breakdown] batch_id={} txns={} network_rx_deser_sum_ms={:.3} network_rx_deser_avg_ms={:.3} network_rx_deser_makespan_ms={:.3} batch_scheduling_ms={:.3} version_assignment_ms={:.3} dispatch_forwarding_sum_ms={:.3} dispatch_forwarding_avg_ms={:.3} dispatch_forwarding_makespan_ms={:.3} network_tx_serialize_sum_ms={:.3} network_tx_serialize_avg_ms={:.3} network_tx_serialize_makespan_ms={:.3} batch_observed_wall_ms={:.3}",
        summary.batch_id,
        summary.tx_count,
        summary.network_rx_deser.sum_ms,
        summary.network_rx_deser.avg_ms,
        summary.network_rx_deser.makespan_ms,
        summary.batch_scheduling_ms,
        summary.version_assignment_ms,
        summary.dispatch_forwarding.sum_ms,
        summary.dispatch_forwarding.avg_ms,
        summary.dispatch_forwarding.makespan_ms,
        summary.network_tx_serialize.sum_ms,
        summary.network_tx_serialize.avg_ms,
        summary.network_tx_serialize.makespan_ms,
        summary.batch_observed_wall_ms,
    )
}

fn merge_min_instant(lhs: Option<Instant>, rhs: Option<Instant>) -> Option<Instant> {
    match (lhs, rhs) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn merge_max_instant(lhs: Option<Instant>, rhs: Option<Instant>) -> Option<Instant> {
    match (lhs, rhs) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn duration_to_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn ns_to_ms(duration_ns: u64) -> f64 {
    duration_ns as f64 / 1_000_000.0
}

fn duration_to_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_ms_eq(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 0.001,
            "expected {expected:.3}ms, got {actual:.3}ms"
        );
    }

    #[test]
    fn stage_timing_tracks_sum_average_and_makespan() {
        let base = Instant::now();
        let mut timing = StageTiming::default();
        timing.record(base, Duration::from_millis(2));
        timing.record(base + Duration::from_millis(1), Duration::from_millis(3));

        assert_ms_eq(timing.sum_ms(), 5.0);
        assert_ms_eq(timing.avg_ms(), 2.5);
        assert_ms_eq(timing.makespan_ms(), 4.0);
        assert_eq!(timing.completed, 2);
    }

    #[test]
    fn batch_summary_reports_wall_clock_without_summing_overlaps() {
        let base = Instant::now();
        let mut state = BatchBreakdownState {
            tx_count: 2,
            ..Default::default()
        };

        state
            .network_rx_deser
            .record(base, Duration::from_millis(1));
        state
            .network_rx_deser
            .record(base + Duration::from_millis(1), Duration::from_millis(1));
        state
            .batch_scheduling
            .record(base + Duration::from_millis(2), Duration::from_millis(5));
        state
            .version_assignment
            .record(base + Duration::from_millis(20), Duration::from_millis(2));
        state
            .dispatch_forwarding
            .record(base + Duration::from_millis(22), Duration::from_millis(3));
        state
            .dispatch_forwarding
            .record(base + Duration::from_millis(23), Duration::from_millis(4));
        state
            .network_tx_serialize
            .record(base + Duration::from_millis(27), Duration::from_millis(1));
        state
            .network_tx_serialize
            .record(base + Duration::from_millis(28), Duration::from_millis(1));

        let summary = summarize_batch(7, &state);

        assert_eq!(summary.batch_id, 7);
        assert_eq!(summary.tx_count, 2);
        assert_ms_eq(summary.network_rx_deser.sum_ms, 2.0);
        assert_ms_eq(summary.network_rx_deser.avg_ms, 1.0);
        assert_ms_eq(summary.network_rx_deser.makespan_ms, 2.0);
        assert_ms_eq(summary.batch_scheduling_ms, 5.0);
        assert_ms_eq(summary.version_assignment_ms, 2.0);
        assert_ms_eq(summary.dispatch_forwarding.sum_ms, 7.0);
        assert_ms_eq(summary.dispatch_forwarding.avg_ms, 3.5);
        assert_ms_eq(summary.dispatch_forwarding.makespan_ms, 5.0);
        assert_ms_eq(summary.network_tx_serialize.sum_ms, 2.0);
        assert_ms_eq(summary.network_tx_serialize.avg_ms, 1.0);
        assert_ms_eq(summary.network_tx_serialize.makespan_ms, 2.0);
        assert_ms_eq(summary.batch_observed_wall_ms, 29.0);
    }

    #[test]
    fn summary_format_uses_explicit_sum_avg_and_wall_labels() {
        let summary = BatchBreakdownSummary {
            batch_id: 3,
            tx_count: 4,
            network_rx_deser: StageSummary {
                sum_ms: 1.0,
                avg_ms: 0.25,
                makespan_ms: 0.5,
            },
            batch_scheduling_ms: 0.6,
            version_assignment_ms: 0.7,
            dispatch_forwarding: StageSummary {
                sum_ms: 8.0,
                avg_ms: 2.0,
                makespan_ms: 3.0,
            },
            network_tx_serialize: StageSummary {
                sum_ms: 0.9,
                avg_ms: 0.225,
                makespan_ms: 0.4,
            },
            batch_observed_wall_ms: 9.1,
        };

        let line = format_summary(&summary);

        assert!(line.contains("network_rx_deser_sum_ms=1.000"));
        assert!(line.contains("dispatch_forwarding_avg_ms=2.000"));
        assert!(line.contains("batch_observed_wall_ms=9.100"));
        assert!(!line.contains("total_measured_ms"));
    }
}
