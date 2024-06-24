// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, num::NonZeroU64, path::PathBuf, time::Duration, collections::HashMap};

use bytes::Bytes;
use clap::Parser;
use itertools::Itertools;
use network::SimpleSender;
use remora::{
    metrics::{ErrorType, Metrics},
    types::{TransactionWithEffects, NetworkMessage, RemoraMessage, GlobalConfig, UniqueId},
};
use sui_single_node_benchmark::{
    benchmark_context::BenchmarkContext,
    command::{Component, WorkloadKind},
    workload::Workload,
};
use sui_types::transaction::CertifiedTransaction;
use tokio::time::{interval, Instant, MissedTickBehavior};

/// Default workload for the load generator.
const DEFAULT_WORKLOAD: WorkloadKind = WorkloadKind::PTB {
    num_transfers: 0,
    num_dynamic_fields: 0,
    use_batch_mint: false,
    computation: 0,
    use_native_transfer: false,
    num_mints: 0,
    num_shared_objects: 0,
    nft_size: 32,
};

/// The load generator generates transactions at a specified rate and submits them to the system.
pub struct LoadGenerator {
    /// Number of transactions per second to submit to the system.
    load: u64,
    /// Duration of the load test.
    duration: Duration,
    /// The network target to send transactions to.
    targets: HashMap<UniqueId, SocketAddr>,
    /// A best effort network sender.
    network: SimpleSender,
    /// Metrics for the load generator.
    metrics: Metrics,
    /// Forwarding policy to executors.
    policy: ForwardingPolicy,
}

const DEFAULT_CONFIG_PATH: &str = "src/configs/1pri1pre.json";

#[derive(Clone, Debug, Parser)]
pub enum ForwardingPolicy {
    /// Directly forward to a single target machine.
    SingleTarget {
        #[arg(long)]
        target: SocketAddr,
    },
    /// Forward to primary and one of pre-executors (round-robin).
    RoundRobin {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config_path: PathBuf,
    }
}

impl LoadGenerator {
    /// Create a new load generator.
    pub fn new(load: u64, duration: Duration, targets: HashMap<UniqueId, SocketAddr>, metrics: Metrics, policy: ForwardingPolicy) -> Self {
        LoadGenerator {
            load,
            duration,
            targets,
            network: SimpleSender::new(),
            metrics,
            policy,
        }
    }

    /// Initialize the load generator. This will generate all required genesis objects and all transactions upfront.
    // TODO: This may be problematic if the number of transactions is very large. We may need to
    // generate transactions on the fly.
    pub async fn initialize(&self) -> Vec<CertifiedTransaction> {
        let pre_generation = self.load * self.duration.as_secs();

        // Create genesis.
        println!("Creating genesis for {pre_generation} transactions...");
        let start_time = Instant::now();
        let workload = Workload::new(pre_generation, DEFAULT_WORKLOAD);
        let component = Component::PipeTxsToChannel;
        let mut ctx = BenchmarkContext::new(workload.clone(), component, true).await;
        let elapsed = start_time.elapsed();
        tracing::debug!(
            "Genesis created {} accounts/s in {} ms",
            workload.num_accounts() as f64 / elapsed.as_secs_f64(),
            elapsed.as_millis(),
        );

        // Pre-generate all transactions.
        tracing::debug!("Generating all transactions...");
        let start_time = Instant::now();
        let tx_generator = workload.create_tx_generator(&mut ctx).await;
        let transactions = ctx.generate_transactions(tx_generator).await;
        let transactions = ctx.certify_transactions(transactions, false).await;
        let elapsed = start_time.elapsed();
        println!(
            "Generated {} txs in {} ms",
            transactions.len(),
            elapsed.as_millis(),
        );

        transactions
    }

    /// Run the load generator. This will submit transactions to the system at the specified rate
    /// until all transactions are submitted.
    pub async fn run(&mut self, transactions: Vec<CertifiedTransaction>) {
        let precision = if self.load > 1000 { 20 } else { 1 };
        let burst_duration = Duration::from_millis(1000 / precision);
        let mut interval = interval(burst_duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Burst);
        let mut pre_id = 1;

        let chunks_size = (self.load / precision) as usize;
        let chunks = &transactions.into_iter().chunks(chunks_size);
        for (counter, chunk) in chunks.into_iter().enumerate() {
            if counter % 1000 == 0 && counter != 0 {
                tracing::debug!("Submitted {} txs", counter * chunks_size);
            }

            let now = Instant::now();
            let timestamp = Metrics::now().as_secs_f64();
            for tx in chunk {
                let full_tx = TransactionWithEffects {
                    tx,
                    ground_truth_effects: None,
                    child_inputs: None,
                    checkpoint_seq: None,
                    timestamp,
                };
                let msg = NetworkMessage {
                    src: 0,
                    dst: vec![1], // placeholder, not used
                    payload: RemoraMessage::ProposeExec(full_tx.clone()),
                };
                let bytes = bincode::serialize(&msg).expect("serialization failed");
                match self.policy {
                    ForwardingPolicy::SingleTarget{target} => {
                        self.network.send(target, Bytes::from(bytes.clone())).await;
                    },
                    ForwardingPolicy::RoundRobin{..} => {
                        // send to PRI
                        let primary_addr = self.targets.get(&0).unwrap();
                        self.network.send(*primary_addr, Bytes::from(bytes.clone())).await;
                        // send to one of PRE
                        let pre_addr = self.targets.get(&pre_id).unwrap();
                        self.network.send(*pre_addr, Bytes::from(bytes.clone())).await;
                        pre_id += 1;
                        if pre_id as usize >= self.targets.len() {
                            pre_id = 1; // Reset to the first PRE
                        }
                    }
                }
            }

            if now.elapsed() > burst_duration {
                tracing::warn!("Transaction rate too high for this client");
                self.metrics
                    .register_error(ErrorType::TransactionRateTooHigh);
            }

            interval.tick().await;
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Remora load generator", long_about = None)]
struct Args {
    /// The number of transactions per second to submit to the system.
    #[clap(long, default_value = "2")]
    load: NonZeroU64,
    /// The duration of the load test.
    #[clap(long, value_parser = parse_duration, default_value = "10")]
    duration: Duration,
    /// The address to expose metrics on.
    #[clap(long)]
    metrics_address: SocketAddr,
    /// Forwarding policy to executors.
    #[clap(subcommand)]
    policy: ForwardingPolicy,
}

fn parse_duration(arg: &str) -> Result<Duration, std::num::ParseIntError> {
    let seconds = arg.parse()?;
    Ok(Duration::from_secs(seconds))
}

/// The main function for the load generator.
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let _ = tracing_subscriber::fmt::try_init();
    let registry = mysten_metrics::start_prometheus_server(args.metrics_address);
    let metrics = Metrics::new(&registry.default_registry());

    // Create genesis and generate transactions.
    let load = args.load.get();
    let mut load_generator: LoadGenerator;

    match args.policy {
        ForwardingPolicy::SingleTarget { target: _ } => {
            load_generator = LoadGenerator::new(load, args.duration, HashMap::new(), metrics, args.policy.clone());
        },
        ForwardingPolicy::RoundRobin { ref config_path } => {
            let global_config = GlobalConfig::from_path(config_path);
            let mut target_vec = HashMap::new();
            for (id, entry) in global_config.iter() {
                target_vec.insert(*id, SocketAddr::new(entry.ip_addr, entry.port));
            }
            assert!(target_vec.len() > 1, "Need at least two targets.");
            load_generator = LoadGenerator::new(load, args.duration, target_vec, metrics, args.policy.clone());
        }
    }

    let transactions = load_generator.initialize().await;

    // Submit transactions to the server.
    load_generator.run(transactions).await;
}

#[cfg(test)]
mod test {
    use std::{net::SocketAddr, time::Duration};

    use bytes::Bytes;
    use futures::{sink::SinkExt, stream::StreamExt};
    use prometheus::Registry;
    use remora::{metrics::Metrics, types::TransactionWithEffects};
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    use crate::{LoadGenerator, ForwardingPolicy};

    /// Create a network listener that will receive a single message and return it.
    fn listener(address: SocketAddr) -> JoinHandle<Bytes> {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&address).await.unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            match reader.next().await {
                Some(Ok(received)) => {
                    writer.send(Bytes::from("Ack")).await.unwrap();
                    return received.freeze();
                }
                _ => panic!("Failed to receive network message"),
            }
        })
    }

    #[tokio::test]
    async fn generate_transactions() {
        // Boot a test server to receive transactions.
        // TODO: Implement a better way to get a port for tests
        let target = SocketAddr::from(([127, 0, 0, 1], 18181));
        let handle = listener(target);
        tokio::task::yield_now().await;

        // Create genesis and generate transactions.
        let metrics = Metrics::new(&Registry::new());
        let mut load_generator = LoadGenerator::new(1, Duration::from_secs(1), target, metrics, ForwardingPolicy::SingleTarget);
        let transactions = load_generator.initialize().await;

        // Submit transactions to the server.
        let now = Metrics::now().as_secs_f64();
        load_generator.run(transactions).await;

        // Check that the transactions were received.
        let received = handle.await.unwrap();
        let transaction: TransactionWithEffects = bincode::deserialize(&received).unwrap();
        let timestamp = transaction.timestamp;
        assert!(timestamp > now);
    }
}
