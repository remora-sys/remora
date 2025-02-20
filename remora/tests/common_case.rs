// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use remora::{
    client::load_generator::LoadGenerator,
    config::{BenchmarkParameters, CollocatedPreExecutors, ValidatorConfig, ValidatorParameters},
    executor::{api::Executor, fake::FakeExecutor, sui::SuiExecutor},
    metrics::Metrics,
    primary::node::PrimaryNode,
    proxy::node::ProxyNode,
};
use serde::{de::DeserializeOwned, Serialize};

async fn remote_proxy_common<E: Executor + Send + Sync + 'static>(
    executor: E,
    benchmark_config: BenchmarkParameters,
) where
    <E as Executor>::ExecutionResults: Send + Sync + Serialize + DeserializeOwned,
    <E as Executor>::Transaction: Send + Sync,
    <E as Executor>::ExecutionContext: Send + Sync,
    <E as Executor>::Store: Send + Sync,
{
    let validator_parameters = ValidatorParameters {
        collocated_pre_executors: CollocatedPreExecutors {
            primary: 0,
            proxy: 1,
        },
        ..ValidatorParameters::new_for_tests()
    };
    let validator_config = ValidatorConfig {
        validator_parameters,
        ..ValidatorConfig::new_for_tests()
    };
    let primary_address = validator_config.client_server_address;

    // Start the primary.
    let validator_metrics = Arc::new(Metrics::new_for_tests());
    let mut primary = PrimaryNode::start(
        executor.clone(),
        &validator_config,
        validator_metrics.clone(),
    )
    .await;
    tokio::task::yield_now().await;

    // Start a remote proxy.
    let proxy_id = 0.to_string();
    let _proxy = ProxyNode::start(proxy_id, executor, &validator_config, validator_metrics).await;
    tokio::task::yield_now().await;

    // Generate transactions.
    let mut load_generator = LoadGenerator::<E>::new(benchmark_config, primary_address);
    let transactions = load_generator.initialize().await;
    let total_transactions = transactions.len();
    load_generator.run(transactions).await;

    // Wait for all transactions to be processed.
    for _ in 0..total_transactions {
        let (_ts, result) = primary.rx_output.recv().await.unwrap();
        assert!(result.success());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[tracing_test::traced_test]
async fn remote_proxy_sui() {
    let config = BenchmarkParameters::new_for_tests();
    let executor = SuiExecutor::new(&config).await;
    remote_proxy_common::<SuiExecutor>(executor, config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[tracing_test::traced_test]
async fn remote_proxy_fake_txn() {
    let config = BenchmarkParameters::new_for_fake_tests();
    let executor = FakeExecutor::new(&config).await;
    remote_proxy_common::<FakeExecutor>(executor, config).await;
}
