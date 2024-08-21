// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use clap::Parser;
use remora::{
    config::{BenchmarkConfig, ImportExport},
    load_generator::{default_metrics_address, LoadGenerator},
    metrics::Metrics,
};

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Remora load generator", long_about = None)]
struct Args {
    /// The configuration for the benchmark.
    #[clap(long, value_name = "FILE")]
    benchmark_config: Option<PathBuf>,
    /// The address of the primary.
    #[clap(long, value_name = "ADDRESS")]
    primary_address: SocketAddr,
    /// The address to expose metrics on.
    #[clap(long, value_name = "ADDRESS", default_value_t = default_metrics_address())]
    metrics_address: SocketAddr,
}

/// The main function for the load generator.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let benchmark_config = match args.benchmark_config {
        Some(path) => BenchmarkConfig::load(path).context("Failed to load benchmark config")?,
        None => BenchmarkConfig::default(),
    };
    let primary_address = args.primary_address;

    let _ = tracing_subscriber::fmt::try_init();
    let registry = mysten_metrics::start_prometheus_server(args.metrics_address);
    let metrics = Metrics::new(&registry.default_registry());

    // Create genesis and generate transactions.
    let mut load_generator = LoadGenerator::new(benchmark_config, primary_address, metrics);
    let transactions = load_generator.initialize().await;

    // Submit transactions to the server.
    load_generator.run(transactions).await;

    Ok(())
}
