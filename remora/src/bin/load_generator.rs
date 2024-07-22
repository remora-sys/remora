// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;
use remora::{
    config::{BenchmarkConfig, ImportExport, ValidatorConfig},
    executor::SuiExecutor,
    load_generator::LoadGenerator,
    metrics::Metrics,
};

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Remora load generator", long_about = None)]
struct Args {
    /// The configuration for the benchmark.
    #[clap(long, value_name = "FILE")]
    benchmark_config: PathBuf,
    /// The configuration for the validator.
    #[clap(long, value_name = "FILE")]
    validator_config: PathBuf,
    /// The address to expose metrics on.
    #[clap(long)]
    metrics_address: SocketAddr,
}

/// The main function for the load generator.
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let benchmark_config =
        BenchmarkConfig::load(&args.benchmark_config).expect("Failed to load benchmark config");
    let validator_config =
        ValidatorConfig::load(&args.validator_config).expect("Failed to load validator config");

    let _ = tracing_subscriber::fmt::try_init();
    let registry = mysten_metrics::start_prometheus_server(args.metrics_address);
    let metrics = Metrics::new(&registry.default_registry());

    //
    let executor = SuiExecutor::new(&benchmark_config).await;

    // Create genesis and generate transactions.
    let mut load_generator = LoadGenerator::new(
        benchmark_config,
        validator_config.address,
        executor,
        metrics,
    );
    let transactions = load_generator.initialize().await;

    // Submit transactions to the server.
    load_generator.run(transactions).await;
}
