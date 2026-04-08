// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use rand::{rngs::StdRng, Rng, SeedableRng};
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    transaction::InputObjectKind,
};

use crate::{
    executor::{
        api::{ExecutableTransaction, RemoraTransaction, TransactionWithTimestamp},
        fake::{FakeExecutor, FakeTransaction},
    },
    primary::shared_obj_txn_forwarder::VersionAssignmentTask,
};

const FIXED_OBJECT_SPACE: usize = 1_000_000;
const DEFAULT_BATCH_SIZES: &[usize] = &[512, 1024, 2_048];
const DEFAULT_SHARED_OBJECTS_PER_TXS: &[usize] = &[2, 4, 8, 16];
const DEFAULT_MEASURED_BATCHES: usize = 100;
const DEFAULT_WARMUP_BATCHES: usize = 5;
const DEFAULT_STATEFUL_DURATION_US: u64 = 200;
const DEFAULT_VERIFICATION_DURATION_US: u64 = 50;
const MAX_OBJECT_TRACKING_SPACE: usize = (1 << 24) - 1;

#[derive(Clone, Debug)]
struct VersionAssignmentBenchSpec {
    object_space: usize,
    batch_sizes: Vec<usize>,
    shared_objects_per_txs: Vec<usize>,
    measured_batches: usize,
    warmup_batches: usize,
    stateful_duration: Duration,
    verification_duration: Duration,
}

#[derive(Clone, Debug)]
struct VersionAssignmentBenchCase {
    object_space: usize,
    batch_size: usize,
    shared_objects_per_tx: usize,
    measured_batches: usize,
    warmup_batches: usize,
    stateful_duration: Duration,
    verification_duration: Duration,
}

#[derive(Debug)]
struct BenchStats {
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    total_wall_ms: f64,
    throughput_batches_per_s: f64,
    throughput_tx_per_s: f64,
}

impl VersionAssignmentBenchSpec {
    fn from_env() -> Self {
        Self {
            object_space: FIXED_OBJECT_SPACE,
            batch_sizes: parse_usize_list_env(
                "VERSION_ASSIGNMENT_BENCH_BATCH_SIZES",
                DEFAULT_BATCH_SIZES,
            ),
            shared_objects_per_txs: parse_shared_objects_per_txs(),
            measured_batches: parse_usize_env(
                "VERSION_ASSIGNMENT_BENCH_MEASURED_BATCHES",
                DEFAULT_MEASURED_BATCHES,
            ),
            warmup_batches: parse_usize_env(
                "VERSION_ASSIGNMENT_BENCH_WARMUP_BATCHES",
                DEFAULT_WARMUP_BATCHES,
            ),
            stateful_duration: Duration::from_micros(parse_u64_env(
                "VERSION_ASSIGNMENT_BENCH_STATEFUL_DURATION_US",
                DEFAULT_STATEFUL_DURATION_US,
            )),
            verification_duration: Duration::from_micros(parse_u64_env(
                "VERSION_ASSIGNMENT_BENCH_VERIFICATION_DURATION_US",
                DEFAULT_VERIFICATION_DURATION_US,
            )),
        }
    }
}

#[test]
fn benchmark_version_assignment_matrix() {
    let spec = VersionAssignmentBenchSpec::from_env();
    assert!(
        spec.object_space > 0 && spec.object_space <= MAX_OBJECT_TRACKING_SPACE,
        "object_space must be in 1..={MAX_OBJECT_TRACKING_SPACE}, got {}",
        spec.object_space
    );
    println!(
        "[version-assignment-bench-config] assignment_tasks=1 fixed object_space={} fixed batch_sizes={:?} shared_objects_per_txs={:?} measured_batches={} warmup_batches={} stateful_duration_us={} verification_duration_us={}",
        spec.object_space,
        spec.batch_sizes,
        spec.shared_objects_per_txs,
        spec.measured_batches,
        spec.warmup_batches,
        spec.stateful_duration.as_micros(),
        spec.verification_duration.as_micros(),
    );

    for &batch_size in &spec.batch_sizes {
        for &shared_objects_per_tx in &spec.shared_objects_per_txs {
            let case = VersionAssignmentBenchCase {
                object_space: spec.object_space,
                batch_size,
                shared_objects_per_tx: shared_objects_per_tx.min(spec.object_space),
                measured_batches: spec.measured_batches,
                warmup_batches: spec.warmup_batches,
                stateful_duration: spec.stateful_duration,
                verification_duration: spec.verification_duration,
            };

            let stats = run_case(&case);
            println!(
                "[version-assignment-bench] object_space={} batch_size={} shared_objects_per_tx={} warmup_batches={} measured_batches={} avg_ms={:.3} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3} total_wall_ms={:.3} throughput_batches_per_s={:.3} throughput_tx_per_s={:.3}",
                case.object_space,
                case.batch_size,
                case.shared_objects_per_tx,
                case.warmup_batches,
                case.measured_batches,
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

fn run_case(case: &VersionAssignmentBenchCase) -> BenchStats {
    let batches = generate_batches(case, 0xC0FFEE_u64);
    let mut task = VersionAssignmentTask::<FakeExecutor>::new_for_benchmark(case.object_space);
    let mut samples = Vec::with_capacity(case.measured_batches);
    let mut measured_start = None;

    for (batch_idx, batch) in batches.into_iter().enumerate() {
        if batch_idx == case.warmup_batches {
            measured_start = Some(Instant::now());
        }

        let elapsed = task.benchmark_assign_transaction_batch(batch);
        if batch_idx >= case.warmup_batches {
            samples.push(elapsed);
        }
    }

    let total_wall = measured_start
        .map(|start| start.elapsed())
        .expect("benchmark produced no measured batches");

    summarize_samples(
        &samples,
        total_wall,
        case.measured_batches,
        case.measured_batches * case.batch_size,
    )
}

fn generate_batches(
    case: &VersionAssignmentBenchCase,
    seed: u64,
) -> Vec<Vec<RemoraTransaction<FakeExecutor>>> {
    let total_batches = case.warmup_batches + case.measured_batches;
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
) -> RemoraTransaction<FakeExecutor> {
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

fn summarize_samples(
    samples: &[Duration],
    total_wall: Duration,
    total_measured_batches: usize,
    total_measured_txns: usize,
) -> BenchStats {
    assert!(!samples.is_empty(), "benchmark produced no samples");

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();

    let total_sample_secs: f64 = samples.iter().map(Duration::as_secs_f64).sum();
    let avg_ms = (total_sample_secs / samples.len() as f64) * 1_000.0;

    BenchStats {
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
                    "[version-assignment-bench-config] {}={} is invalid, falling back to {:?}",
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
    if std::env::var("VERSION_ASSIGNMENT_BENCH_SHARED_OBJECTS_PER_TXS").is_ok() {
        parse_usize_list_env(
            "VERSION_ASSIGNMENT_BENCH_SHARED_OBJECTS_PER_TXS",
            DEFAULT_SHARED_OBJECTS_PER_TXS,
        )
    } else if std::env::var("VERSION_ASSIGNMENT_BENCH_SHARED_OBJECTS_PER_TX").is_ok() {
        vec![parse_usize_env(
            "VERSION_ASSIGNMENT_BENCH_SHARED_OBJECTS_PER_TX",
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
                    "[version-assignment-bench-config] {}={} is invalid, falling back to {}",
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
                "[version-assignment-bench-config] {}={} is invalid, falling back to {}",
                name, value, default
            );
            default
        }),
        Err(_) => default,
    }
}
