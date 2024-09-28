// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, path::PathBuf};

use anyhow::{anyhow, Context};
use clap::Parser;
use remora::{
    config::{BenchmarkParameters, ImportExport, ValidatorConfig, WorkloadType},
    executor::sui::{import_from_files, LOG_DIR},
    load_generator::{default_metrics_address, LoadGenerator},
    metrics::Metrics,
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
}

/// The main function for the load generator.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let validator_config =
        ValidatorConfig::load(&args.validator_config).context("Failed to load validator config")?;
    let benchmark_config = match args.benchmark_config {
        Some(path) => BenchmarkParameters::load(path).context("Failed to load benchmark config")?,
        None => BenchmarkParameters::default(),
    };
    let metrics_address = args.metrics_address;

    tracing::info!("Load generator exposing metrics on {metrics_address}");
    tracing_subscriber::fmt::try_init().map_err(|e| anyhow!("{e}"))?;
    let registry = mysten_metrics::start_prometheus_server(metrics_address);
    let metrics = Metrics::new(&registry.default_registry());

    // Create genesis and generate transactions.
    let primary_address = validator_config.client_server_address;
    let mut load_generator = LoadGenerator::new(benchmark_config.clone(), primary_address, metrics);

    let transactions;
    match benchmark_config.workload {
        WorkloadType::Transfers => {
            transactions = load_generator.initialize().await;
        }
        WorkloadType::SharedObjects => {
            (_, transactions) = import_from_files(LOG_DIR.into());
        }
    };

    // Submit transactions to the server.
    load_generator.run(transactions).await;

    Ok(())
}
