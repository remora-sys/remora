// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io::{self, Write},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
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
            | Self::Reply(InterProxyReply::Stateful(_)) => None,
        }
    }
}

#[derive(Default)]
struct BatchBreakdownState {
    digests: Vec<TransactionDigest>,
    tx_count: usize,
    network_rx_deser_ns: u64,
    batch_scheduling_ns: u64,
    version_assignment_ns: u64,
    dispatch_forwarding_ns: u64,
    network_tx_serialize_ns: u64,
    dispatch_completed: usize,
    network_tx_serialize_completed: usize,
    scheduling_recorded: bool,
    version_assignment_recorded: bool,
}

#[derive(Default)]
pub(crate) struct BatchBreakdownCollector {
    next_batch_id: AtomicU64,
    digest_to_batch: DashMap<TransactionDigest, u64>,
    pending_network_rx_deser_ns: DashMap<TransactionDigest, u64>,
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
            if let Some((_, elapsed_ns)) = self.pending_network_rx_deser_ns.remove(digest) {
                state.network_rx_deser_ns += elapsed_ns;
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

    pub(crate) fn record_network_rx_deser(&self, digest: TransactionDigest, elapsed: Duration) {
        let elapsed_ns = duration_to_ns(elapsed);
        if let Some(batch_id) = self.batch_id_for_digest(&digest) {
            self.update_batch(batch_id, |state| {
                state.network_rx_deser_ns += elapsed_ns;
            });
            return;
        }

        self.pending_network_rx_deser_ns
            .entry(digest)
            .and_modify(|total| *total += elapsed_ns)
            .or_insert(elapsed_ns);
    }

    pub(crate) fn record_batch_scheduling(&self, batch_id: u64, elapsed: Duration) {
        self.update_batch(batch_id, |state| {
            state.batch_scheduling_ns += duration_to_ns(elapsed);
            state.scheduling_recorded = true;
        });
    }

    pub(crate) fn record_version_assignment(&self, batch_id: u64, elapsed: Duration) {
        self.update_batch(batch_id, |state| {
            state.version_assignment_ns += duration_to_ns(elapsed);
            state.version_assignment_recorded = true;
        });
    }

    pub(crate) fn record_dispatch_forwarding(&self, digest: TransactionDigest, elapsed: Duration) {
        let Some(batch_id) = self.batch_id_for_digest(&digest) else {
            return;
        };

        self.update_batch(batch_id, |state| {
            state.dispatch_forwarding_ns += duration_to_ns(elapsed);
            state.dispatch_completed += 1;
        });
        self.maybe_finish_batch(batch_id);
    }

    pub(crate) fn record_network_tx_serialize(&self, digest: TransactionDigest, elapsed: Duration) {
        let Some(batch_id) = self.batch_id_for_digest(&digest) else {
            return;
        };

        self.update_batch(batch_id, |state| {
            state.network_tx_serialize_ns += duration_to_ns(elapsed);
            state.network_tx_serialize_completed += 1;
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
                    && state.dispatch_completed == state.tx_count
                    && state.network_tx_serialize_completed == state.tx_count
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
            self.pending_network_rx_deser_ns.remove(digest);
        }

        let total_measured_ns = state.network_rx_deser_ns
            + state.batch_scheduling_ns
            + state.version_assignment_ns
            + state.dispatch_forwarding_ns
            + state.network_tx_serialize_ns;

        // Print directly to stdout so the measurement remains visible in `--release`
        // regardless of tracing subscriber configuration or log level.
        let mut stdout = io::stdout().lock();
        writeln!(
            stdout,
            "[primary-batch-breakdown] batch_id={} txns={} network_rx_deser_ms={:.3} batch_scheduling_ms={:.3} version_assignment_ms={:.3} dispatch_forwarding_ms={:.3} network_tx_serialize_ms={:.3} total_measured_ms={:.3}",
            batch_id,
            state.tx_count,
            ns_to_ms(state.network_rx_deser_ns),
            ns_to_ms(state.batch_scheduling_ns),
            ns_to_ms(state.version_assignment_ns),
            ns_to_ms(state.dispatch_forwarding_ns),
            ns_to_ms(state.network_tx_serialize_ns),
            ns_to_ms(total_measured_ns),
        )
        .expect("batch breakdown write to stdout should succeed");
    }
}

fn duration_to_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn ns_to_ms(duration_ns: u64) -> f64 {
    duration_ns as f64 / 1_000_000.0
}
