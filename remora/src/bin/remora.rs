// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use clap::Parser;
use remora::{
    config::{BenchmarkConfig, ImportExport, ValidatorConfig},
    executor::sui::SuiExecutor,
    metrics::{periodically_print_metrics, Metrics},
    primary::node::PrimaryNode,
    proxy::{core::ProxyId, node::ProxyNode},
};

#[derive(Parser)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Remora load generator", long_about = None)]
struct Args {
    /// The configuration for the benchmark.
    #[clap(long, value_name = "FILE")]
    benchmark_config: Option<PathBuf>,
    /// The configuration for the validator.
    #[clap(long, value_name = "FILE")]
    validator_config: Option<PathBuf>,
    /// The role of the node (primary or proxy).
    #[clap(subcommand)]
    role: Role,
}

#[derive(Parser)]
enum Role {
    Primary,
    Proxy { proxy_id: ProxyId },
}

/// The main function for remora testbed.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let benchmark_config = match args.benchmark_config {
        Some(path) => BenchmarkConfig::load(path).context("Failed to load benchmark config")?,
        None => BenchmarkConfig::default(),
    };
    let validator_config = match args.validator_config {
        Some(path) => ValidatorConfig::load(path).context("Failed to load validator config")?,
        None => ValidatorConfig::default(),
    };

    // Start the metrics server.
    let _ = tracing_subscriber::fmt::try_init();
    let registry = mysten_metrics::start_prometheus_server(validator_config.metrics_address);
    let metrics = Arc::new(Metrics::new(&registry.default_registry()));
    tracing::info!("Exposing metrics on {}", validator_config.metrics_address);

    // Periodically print metrics.
    let workload = "default".to_string();
    let print_period = Duration::from_secs(5);
    let _ = periodically_print_metrics(validator_config.metrics_address, workload, print_period);

    // Build the executor.
    tracing::info!("Loading executor");
    let executor = SuiExecutor::new(&benchmark_config).await;

    // Start the node.
    match args.role {
        Role::Primary => {
            tracing::info!("Starting primary on {}", validator_config.validator_address);
            PrimaryNode::start(executor, &validator_config, metrics)
                .await
                .collect_results()
                .await;
        }
        Role::Proxy { proxy_id } => {
            tracing::info!("Starting proxy on {}", validator_config.validator_address);
            ProxyNode::start(proxy_id, executor, &validator_config, metrics)
                .await
                .await_completion()
                .await;
        }
    }

    Ok(())
}
