// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context};
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
    /// The configuration for the validator.
    #[clap(long, value_name = "FILE")]
    validator_config: Option<PathBuf>,
    /// The configuration for the benchmark.
    #[clap(long, value_name = "FILE")]
    benchmark_config: Option<PathBuf>,
    /// The address of the primary.
    #[clap(long, value_name = "ADDRESS")]
    primary_address: SocketAddr,
    /// The address to expose metrics on.
    #[clap(long, value_name = "ADDRESS")]
    metrics_address: SocketAddr,
    /// The ip address to bind the server to. This value overrides the configuration file.
    /// If not provided, the server will bind to the address specified in the configuration file.
    /// This is useful to control the exposure of the server to the external network.
    #[clap(long, value_name = "ADDRESS")]
    binding_address: Option<IpAddr>,
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
    let validator_config = match args.validator_config {
        Some(path) => ValidatorConfig::load(path).context("Failed to load validator config")?,
        None => ValidatorConfig::default(),
    };
    let benchmark_config = match args.benchmark_config {
        Some(path) => BenchmarkConfig::load(path).context("Failed to load benchmark config")?,
        None => BenchmarkConfig::default(),
    };
    let mut primary_address = args.primary_address;
    let mut metrics_address = args.metrics_address;
    if let Some(binding_address) = args.binding_address {
        primary_address.set_ip(binding_address);
        metrics_address.set_ip(binding_address);
    }

    // Start the metrics server.
    let _ = tracing_subscriber::fmt::try_init().map_err(|e| anyhow!("{e}"))?;
    let registry = mysten_metrics::start_prometheus_server(metrics_address);
    let metrics = Arc::new(Metrics::new(&registry.default_registry()));
    tracing::info!("Exposing metrics on {metrics_address}");

    // Periodically print metrics.
    let workload = "default".to_string();
    let print_period = Duration::from_secs(5);
    let _ = periodically_print_metrics(metrics_address, workload, print_period);

    // Build the executor.
    tracing::info!("Loading executor");
    let executor = SuiExecutor::new(&benchmark_config).await;

    // Start the node.
    match args.role {
        Role::Primary => {
            tracing::info!("Starting primary on {primary_address}");
            PrimaryNode::start(executor, &validator_config, primary_address, metrics)
                .await
                .collect_results()
                .await;
        }
        Role::Proxy { proxy_id } => {
            tracing::info!("Starting proxy on {primary_address}");
            ProxyNode::start(
                proxy_id,
                executor,
                &validator_config,
                primary_address,
                metrics,
            )
            .await
            .await_completion()
            .await;
        }
    }

    Ok(())
}
