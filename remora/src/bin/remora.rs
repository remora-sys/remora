// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::IpAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, Context};
use clap::Parser;
use remora::{
    config::{BenchmarkParameters, ImportExport, ValidatorConfig},
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
    validator_config: PathBuf,
    /// The configuration for the benchmark.
    #[clap(long, value_name = "FILE")]
    benchmark_config: Option<PathBuf>,
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
    let mut validator_config =
        ValidatorConfig::load(args.validator_config).context("Failed to load validator config")?;
    let benchmark_config = match args.benchmark_config {
        Some(path) => BenchmarkParameters::load(path).context("Failed to load benchmark config")?,
        None => BenchmarkParameters::default(),
    };

    // Start the metrics server.
    let _ = tracing_subscriber::fmt::try_init().map_err(|e| anyhow!("{e}"))?;
    if let Some(binding_address) = args.binding_address {
        validator_config.metrics_address.set_ip(binding_address);
    }
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
            tracing::info!(
                "Primary listening for proxies on {}",
                validator_config.proxy_server_address
            );
            tracing::info!(
                "Primary listening for client transactions on {}",
                validator_config.client_server_address
            );
            if let Some(binding_address) = args.binding_address {
                validator_config
                    .proxy_server_address
                    .set_ip(binding_address);
                validator_config
                    .client_server_address
                    .set_ip(binding_address);
            }
            PrimaryNode::start(executor, &validator_config, metrics)
                .await
                .collect_results()
                .await;
        }
        Role::Proxy { proxy_id } => {
            tracing::info!(
                "Starting proxy targeting {}",
                validator_config.proxy_server_address
            );
            ProxyNode::start(proxy_id, executor, &validator_config, metrics)
                .await_completion()
                .await;
        }
    }

    Ok(())
}
