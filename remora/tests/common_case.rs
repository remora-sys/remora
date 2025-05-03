// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use remora::{
    client::load_generator::LoadGenerator,
    config::{BenchmarkParameters, ValidatorConfig, ValidatorParameters},
    executor::{api::Executor, fake::FakeExecutor, sui::SuiExecutor},
    metrics::Metrics,
    primary::node::PrimaryNode,
    proxy::node::ProxyNode,
};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;

async fn remote_proxy_common<E: Executor + Send + Sync + 'static>(
    executor: E,
    benchmark_config: BenchmarkParameters,
) where
    <E as Executor>::ExecutionResults: Send + Sync + Serialize + DeserializeOwned,
    <E as Executor>::Transaction: Send + Sync,
    <E as Executor>::ExecutionContext: Send + Sync,
    <E as Executor>::Store: Send + Sync,
{
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let validator_parameters = ValidatorParameters {
        ..ValidatorParameters::new_for_tests()
    };
    let validator_config = ValidatorConfig {
        validator_parameters,
        ..ValidatorConfig::new_for_tests()
    };
    let primary_address = validator_config.client_server_address;

    // Start the primary.
    let validator_metrics = Arc::new(Metrics::new_for_tests());
    let _primary = PrimaryNode::start(
        executor.clone(),
        &validator_config,
        validator_metrics.clone(),
    )
    .await;
    tokio::task::yield_now().await;

    // Note: the following proxy hasn't called the await_completion() method,
    // so there might be some issues with early termination of the proxy.
    // Start two remote proxies.
    let proxy_id_1 = 0;
    let _proxy1 = ProxyNode::start(
        proxy_id_1,
        executor.clone(),
        &validator_config,
        validator_metrics.clone(),
    )
    .await;
    tokio::task::yield_now().await;

    let proxy_id_2 = 1;
    let _proxy2 =
        ProxyNode::start(proxy_id_2, executor, &validator_config, validator_metrics).await;
    tokio::task::yield_now().await;

    // Generate transactions.
    let mut load_generator = LoadGenerator::<E>::new(benchmark_config, primary_address);
    let transactions = load_generator.initialize().await;
    load_generator.run(transactions).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn remote_proxy_sui() {
    let config = BenchmarkParameters::new_for_contention_tests();
    let executor = SuiExecutor::new(&config).await;
    remote_proxy_common::<SuiExecutor>(executor, config.clone()).await;
    tokio::time::sleep(Duration::from_secs(config.duration.as_secs() * 4)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[tracing_test::traced_test]
#[ignore = "currently fake txns are not supported"]
async fn remote_proxy_fake_txn() {
    let config = BenchmarkParameters::new_for_fake_tests();
    let executor = FakeExecutor::new(&config).await;
    remote_proxy_common::<FakeExecutor>(executor, config).await;
}
