// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashSet,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use rand::{rngs::StdRng, Rng, SeedableRng};
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    transaction::InputObjectKind,
};
use tokio::sync::mpsc;

use crate::{
    executor::{
        api::{
            ExecutableTransaction, PrimaryToProxyMessage, RemoraTransaction,
            TransactionWithTimestamp,
        },
        fake::FakeTransaction,
    },
    primary::{
        batch_breakdown::BatchBreakdownCollector, shared_obj_txn_forwarder::PreConsensusSchedTask,
    },
    proxy::core::ProxyId,
};

const FIXED_OBJECT_SPACE: usize = 1_000_000;
const DEFAULT_BATCH_SIZES: &[usize] = &[128, 512, 2_048];
const DEFAULT_PROXY_COUNTS: &[usize] = &[2, 4, 8, 16];
const DEFAULT_SHARED_OBJECTS_PER_TXS: &[usize] = &[1, 2, 4, 8, 16];
const DEFAULT_BATCHES_PER_WORKER: usize = 20;
const DEFAULT_WARMUP_BATCHES: usize = 5;
const FIXED_WORKER_COUNT: usize = 16;
const DEFAULT_STATEFUL_DURATION_US: u64 = 200;
const DEFAULT_VERIFICATION_DURATION_US: u64 = 50;
const MAX_OBJECT_TRACKING_SPACE: usize = (1 << 24) - 1;

#[derive(Clone, Debug)]
struct RsdsBenchSpec {
    object_space: usize,
    batch_sizes: Vec<usize>,
    proxy_counts: Vec<usize>,
    shared_objects_per_txs: Vec<usize>,
    batches_per_worker: usize,
    warmup_batches: usize,
    worker_count: usize,
    stateful_duration: Duration,
    verification_duration: Duration,
}

#[derive(Clone, Debug)]
struct RsdsBenchCase {
    object_space: usize,
    batch_size: usize,
    worker_count: usize,
    batches_per_worker: usize,
    warmup_batches: usize,
    shared_objects_per_tx: usize,
    proxy_count: usize,
    stateful_duration: Duration,
    verification_duration: Duration,
}

#[derive(Debug)]
struct RsdsBenchStats {
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    total_wall_ms: f64,
    throughput_batches_per_s: f64,
    throughput_tx_per_s: f64,
}

impl RsdsBenchSpec {
    fn from_env() -> Self {
        Self {
            object_space: FIXED_OBJECT_SPACE,
            batch_sizes: parse_usize_list_env("RSDS_BENCH_BATCH_SIZES", DEFAULT_BATCH_SIZES),
            proxy_counts: parse_usize_list_env("RSDS_BENCH_PROXY_COUNTS", DEFAULT_PROXY_COUNTS),
            shared_objects_per_txs: parse_shared_objects_per_txs(),
            batches_per_worker: parse_usize_env(
                "RSDS_BENCH_BATCHES_PER_WORKER",
                DEFAULT_BATCHES_PER_WORKER,
            ),
            warmup_batches: parse_usize_env("RSDS_BENCH_WARMUP_BATCHES", DEFAULT_WARMUP_BATCHES),
            worker_count: FIXED_WORKER_COUNT,
            stateful_duration: Duration::from_micros(parse_u64_env(
                "RSDS_BENCH_STATEFUL_DURATION_US",
                DEFAULT_STATEFUL_DURATION_US,
            )),
            verification_duration: Duration::from_micros(parse_u64_env(
                "RSDS_BENCH_VERIFICATION_DURATION_US",
                DEFAULT_VERIFICATION_DURATION_US,
            )),
        }
    }
}

#[test]
fn benchmark_rsds_matrix() {
    let spec = RsdsBenchSpec::from_env();
    assert!(
        spec.object_space > 0 && spec.object_space <= MAX_OBJECT_TRACKING_SPACE,
        "object_space must be in 1..={MAX_OBJECT_TRACKING_SPACE}, got {}",
        spec.object_space
    );
    println!(
        "[rsds-bench-config] worker_count={} fixed independent RSDS schedulers object_space={} fixed batch_sizes={:?} proxy_counts={:?} shared_objects_per_txs={:?} batches_per_worker={} warmup_batches={} stateful_duration_us={} verification_duration_us={}",
        spec.worker_count,
        spec.object_space,
        spec.batch_sizes,
        spec.proxy_counts,
        spec.shared_objects_per_txs,
        spec.batches_per_worker,
        spec.warmup_batches,
        spec.stateful_duration.as_micros(),
        spec.verification_duration.as_micros(),
    );

    for &batch_size in &spec.batch_sizes {
        for &proxy_count in &spec.proxy_counts {
            for &shared_objects_per_tx in &spec.shared_objects_per_txs {
                let case = RsdsBenchCase {
                    object_space: spec.object_space,
                    batch_size,
                    worker_count: spec.worker_count,
                    batches_per_worker: spec.batches_per_worker,
                    warmup_batches: spec.warmup_batches,
                    shared_objects_per_tx: shared_objects_per_tx.min(spec.object_space),
                    proxy_count,
                    stateful_duration: spec.stateful_duration,
                    verification_duration: spec.verification_duration,
                };

                let stats = run_case(case.clone());
                println!(
                    "[rsds-bench] object_space={} batch_size={} worker_count={} shared_objects_per_tx={} proxy_count={} warmup_batches={} measured_batches={} avg_ms={:.3} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3} total_wall_ms={:.3} throughput_batches_per_s={:.3} throughput_tx_per_s={:.3}",
                    case.object_space,
                    case.batch_size,
                    case.worker_count,
                    case.shared_objects_per_tx,
                    case.proxy_count,
                    case.warmup_batches,
                    case.batches_per_worker,
                    stats.avg_ms,
                    stats.p50_ms,
                    stats.p95_ms,
                    stats.p99_ms,
                    stats.max_ms,
                    stats.total_wall_ms,
                    stats.throughput_batches_per_s,
                    stats.throughput_tx_per_s,
                );
            }
        }
    }
}

fn run_case(case: RsdsBenchCase) -> RsdsBenchStats {
    let mut worker_batches = Vec::with_capacity(case.worker_count);
    for worker_idx in 0..case.worker_count {
        worker_batches.push(generate_worker_batches(
            &case,
            0xC0FFEE_u64 + worker_idx as u64,
        ));
    }

    let total_measured_batches = case.worker_count * case.batches_per_worker;
    let total_measured_txns = total_measured_batches * case.batch_size;

    let wall_start = Instant::now();
    let handles: Vec<_> = worker_batches
        .into_iter()
        .enumerate()
        .map(|(worker_idx, batches)| {
            let case = case.clone();
            thread::spawn(move || run_worker(worker_idx, &case, batches))
        })
        .collect();

    let mut all_samples = Vec::with_capacity(total_measured_batches);
    for handle in handles {
        all_samples.extend(handle.join().expect("RSDS worker thread panicked"));
    }
    let total_wall = wall_start.elapsed();

    summarize_samples(
        &all_samples,
        total_wall,
        total_measured_batches,
        total_measured_txns,
    )
}

fn run_worker(
    worker_idx: usize,
    case: &RsdsBenchCase,
    batches: Vec<Vec<RemoraTransaction<crate::executor::fake::FakeExecutor>>>,
) -> Vec<Duration> {
    let proxy_connections = make_proxy_connections(case.proxy_count);
    let pre_consensus_routing_plan = Arc::new(DashMap::new());
    let proxy_loads = Arc::new(DashMap::new());
    let batch_breakdown = Arc::new(BatchBreakdownCollector::default());

    let mut scheduler =
        PreConsensusSchedTask::<crate::executor::fake::FakeExecutor>::new_for_benchmark(
            proxy_connections,
            pre_consensus_routing_plan,
            proxy_loads,
            batch_breakdown,
            case.object_space,
        );

    let mut samples = Vec::with_capacity(case.batches_per_worker);
    for (batch_idx, batch) in batches.into_iter().enumerate() {
        let elapsed = scheduler.benchmark_schedule_transaction_batch(batch);
        if batch_idx >= case.warmup_batches {
            samples.push(elapsed);
        }
    }

    println!(
        "[rsds-bench-worker] worker_id={} object_space={} batch_size={} worker_count={} proxy_count={} measured_batches={}",
        worker_idx,
        case.object_space,
        case.batch_size,
        case.worker_count,
        case.proxy_count,
        samples.len()
    );

    samples
}

fn generate_worker_batches(
    case: &RsdsBenchCase,
    seed: u64,
) -> Vec<Vec<RemoraTransaction<crate::executor::fake::FakeExecutor>>> {
    let total_batches = case.warmup_batches + case.batches_per_worker;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut batches = Vec::with_capacity(total_batches);

    for _ in 0..total_batches {
        let mut batch = Vec::with_capacity(case.batch_size);
        for _ in 0..case.batch_size {
            batch.push(build_shared_transaction(
                &mut rng,
                case.object_space,
                case.shared_objects_per_tx,
                case.stateful_duration,
                case.verification_duration,
            ));
        }
        batches.push(batch);
    }

    batches
}

fn build_shared_transaction(
    rng: &mut StdRng,
    object_space: usize,
    shared_objects_per_tx: usize,
    stateful_duration: Duration,
    verification_duration: Duration,
) -> RemoraTransaction<crate::executor::fake::FakeExecutor> {
    let requested_objects = shared_objects_per_tx.max(1).min(object_space);
    let object_indices = sample_unique_object_indices(rng, object_space, requested_objects);
    let inputs = object_indices
        .iter()
        .map(|&index| InputObjectKind::SharedMoveObject {
            id: object_id_from_index(index),
            initial_shared_version: SequenceNumber::from(2),
            mutable: true,
        })
        .collect();

    let transaction = FakeTransaction::new(inputs);
    let shared_object_ids = transaction.shared_object_ids();
    TransactionWithTimestamp::new(
        transaction,
        0.0,
        shared_object_ids,
        verification_duration,
        stateful_duration,
    )
}

fn sample_unique_object_indices(rng: &mut StdRng, object_space: usize, count: usize) -> Vec<usize> {
    let mut seen = HashSet::with_capacity(count);
    let mut indices = Vec::with_capacity(count);

    while indices.len() < count {
        let candidate = rng.gen_range(1..=object_space);
        if seen.insert(candidate) {
            indices.push(candidate);
        }
    }

    indices
}

fn object_id_from_index(index: usize) -> ObjectID {
    assert!(
        index > 0 && index <= MAX_OBJECT_TRACKING_SPACE,
        "object index must be in 1..={MAX_OBJECT_TRACKING_SPACE}, got {}",
        index
    );

    let mut bytes = [0u8; ObjectID::LENGTH];
    let index_bytes = index.to_le_bytes();
    let copy_len = index_bytes.len().min(ObjectID::LENGTH);
    bytes[..copy_len].copy_from_slice(&index_bytes[..copy_len]);
    ObjectID::from_bytes(bytes).expect("failed to build synthetic object id")
}

fn make_proxy_connections(
    proxy_count: usize,
) -> Arc<DashMap<ProxyId, tokio::sync::mpsc::Sender<PrimaryToProxyMessage<FakeTransaction>>>> {
    let proxy_connections = Arc::new(DashMap::new());
    for proxy_id in 0..proxy_count.max(1) {
        let (tx, _rx) = mpsc::channel(1);
        proxy_connections.insert(proxy_id, tx);
    }
    proxy_connections
}

fn summarize_samples(
    samples: &[Duration],
    total_wall: Duration,
    total_measured_batches: usize,
    total_measured_txns: usize,
) -> RsdsBenchStats {
    assert!(!samples.is_empty(), "benchmark produced no samples");

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();

    let total_sample_secs: f64 = samples.iter().map(Duration::as_secs_f64).sum();
    let avg_ms = (total_sample_secs / samples.len() as f64) * 1_000.0;

    RsdsBenchStats {
        avg_ms,
        p50_ms: percentile_ms(&sorted, 0.50),
        p95_ms: percentile_ms(&sorted, 0.95),
        p99_ms: percentile_ms(&sorted, 0.99),
        max_ms: sorted.last().unwrap().as_secs_f64() * 1_000.0,
        total_wall_ms: total_wall.as_secs_f64() * 1_000.0,
        throughput_batches_per_s: total_measured_batches as f64 / total_wall.as_secs_f64(),
        throughput_tx_per_s: total_measured_txns as f64 / total_wall.as_secs_f64(),
    }
}

fn percentile_ms(sorted: &[Duration], percentile: f64) -> f64 {
    let rank = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[rank].as_secs_f64() * 1_000.0
}

fn parse_usize_list_env(name: &str, default: &[usize]) -> Vec<usize> {
    match std::env::var(name) {
        Ok(value) => {
            let mut parsed: Vec<_> = value
                .split(',')
                .filter_map(|entry| entry.trim().parse::<usize>().ok())
                .filter(|value| *value > 0)
                .collect();

            if parsed.is_empty() {
                println!(
                    "[rsds-bench-config] {}={} is invalid, falling back to {:?}",
                    name, value, default
                );
                return default.to_vec();
            }

            parsed.sort_unstable();
            parsed.dedup();
            parsed
        }
        Err(_) => default.to_vec(),
    }
}

fn parse_shared_objects_per_txs() -> Vec<usize> {
    if std::env::var("RSDS_BENCH_SHARED_OBJECTS_PER_TXS").is_ok() {
        parse_usize_list_env(
            "RSDS_BENCH_SHARED_OBJECTS_PER_TXS",
            DEFAULT_SHARED_OBJECTS_PER_TXS,
        )
    } else if std::env::var("RSDS_BENCH_SHARED_OBJECTS_PER_TX").is_ok() {
        vec![parse_usize_env(
            "RSDS_BENCH_SHARED_OBJECTS_PER_TX",
            DEFAULT_SHARED_OBJECTS_PER_TXS[1],
        )]
    } else {
        DEFAULT_SHARED_OBJECTS_PER_TXS.to_vec()
    }
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|parsed| *parsed > 0)
            .unwrap_or_else(|| {
                println!(
                    "[rsds-bench-config] {}={} is invalid, falling back to {}",
                    name, value, default
                );
                default
            }),
        Err(_) => default,
    }
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value.parse::<u64>().ok().unwrap_or_else(|| {
            println!(
                "[rsds-bench-config] {}={} is invalid, falling back to {}",
                name, value, default
            );
            default
        }),
        Err(_) => default,
    }
}
