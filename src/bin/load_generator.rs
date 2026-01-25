// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, path::PathBuf};

use anyhow::{anyhow, Context};
use clap::Parser;
use remora::{
    //executor::sui::{check_logs_for_shared_object, import_from_files},
    client::{
        hermes_schedule::AssignmentMode,
        load_generator::{default_metrics_address, LoadGenerator},
    },
    config::{BenchmarkParameters, ImportExport, LoadBalancingPolicy, ValidatorConfig},
    executor::api::Executor,
    executor::fake::FakeExecutor,
    executor::sui::SuiExecutor,
    executor::tpcc::TpccExecutor,
};

#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Remora load generator", long_about = None)]
struct Args {
    /// The path to the validator configuration.
    #[clap(long, value_name = "FILE")]
    validator_config: PathBuf,
    /// The path to the configuration for the benchmark.
    #[clap(long, value_name = "FILE")]
    benchmark_config: Option<PathBuf>,
    /// The address to expose metrics on.
    #[clap(long, value_name = "ADDRESS", default_value_t = default_metrics_address())]
    metrics_address: SocketAddr,
    /// The assignment mode for transaction scheduling.
    #[clap(long, value_enum, default_value = "reordering")]
    assignment_mode: AssignmentModeArg,
    /// The command to run.
    #[command(subcommand)]
    command: Option<Command>,
}

/// Mode used in hermes_schedule.rs
#[derive(clap::ValueEnum, Clone, Debug)]
enum AssignmentModeArg {
    /// Reordering mode: traverse transactions in two sets until initial one is empty
    Reordering,
    /// Sequential mode: check transactions one by one and assign to best candidate nodes
    Sequential,
}

impl From<AssignmentModeArg> for AssignmentMode {
    fn from(arg: AssignmentModeArg) -> Self {
        match arg {
            AssignmentModeArg::Reordering => AssignmentMode::Reordering,
            AssignmentModeArg::Sequential => AssignmentMode::Sequential,
        }
    }
}

#[derive(Parser, Debug)]
enum Command {
    /// Run the load generator to submit transactions.
    Run,
    /// Generate a transaction log for replay.
    GenerateLog,
}

const HERMES_LOG_PATH: &str = "./hermes-txn.log";

/// The main function for the load generator.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let validator_config =
        ValidatorConfig::load(&args.validator_config).context("Failed to load validator config")?;
    let mut benchmark_config = match args.benchmark_config {
        Some(path) => BenchmarkParameters::load(path).context("Failed to load benchmark config")?,
        None => BenchmarkParameters::default(),
    };

    // Override assignment mode from CLI
    benchmark_config.assignment_mode = args.assignment_mode.into();
    let metrics_address = args.metrics_address;

    tracing::info!("Load generator exposing metrics on {metrics_address}");
    tracing_subscriber::fmt::try_init().map_err(|e| anyhow!("{e}"))?;
    let _registry = mysten_metrics::start_prometheus_server(metrics_address);

    // Create genesis and generate transactions.
    let primary_address = validator_config.client_server_address;
    let policy = validator_config.validator_parameters.load_balancing_policy;

    // Initialize based on executor type
    if benchmark_config.workload.is_fake() {
        let load_generator =
            LoadGenerator::<FakeExecutor>::new(benchmark_config.clone(), primary_address);
        match args.command {
            Some(Command::Run) => run_load_generator(load_generator, policy).await?,
            Some(Command::GenerateLog) => generate_log(load_generator).await?,
            None => run_load_generator(load_generator, policy).await?,
        }
    } else if benchmark_config.workload.is_tpcc() {
        let load_generator =
            LoadGenerator::<TpccExecutor>::new(benchmark_config.clone(), primary_address);
        match args.command {
            Some(Command::Run) => run_load_generator(load_generator, policy).await?,
            Some(Command::GenerateLog) => generate_log(load_generator).await?,
            None => run_load_generator(load_generator, policy).await?,
        }
    } else {
        let load_generator =
            LoadGenerator::<SuiExecutor>::new(benchmark_config.clone(), primary_address);
        match args.command {
            Some(Command::Run) => run_load_generator(load_generator, policy).await?,
            Some(Command::GenerateLog) => generate_log(load_generator).await?,
            None => run_load_generator(load_generator, policy).await?,
        }
    }

    Ok(())
}

async fn generate_log<E>(load_generator: LoadGenerator<E>) -> anyhow::Result<()>
where
    E: Executor + Send + Sync + 'static,
    <E as Executor>::Transaction: Send + Sync,
{
    load_generator
        .generate_schedule_and_log(HERMES_LOG_PATH)
        .await;
    Ok(())
}

async fn run_load_generator<E>(
    mut load_generator: LoadGenerator<E>,
    policy: LoadBalancingPolicy,
) -> anyhow::Result<()>
where
    E: Executor + Send + Sync + 'static,
    <E as Executor>::Transaction: Send + Sync,
{
    let path = if policy == LoadBalancingPolicy::Hermes {
        Some(HERMES_LOG_PATH)
    } else {
        None
    };

    let transactions = load_generator.initialize(path).await;

    // Submit transactions to the server.
    load_generator.run(transactions).await;

    Ok(())
}
