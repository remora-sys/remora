// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::Arc,
    time::Duration,
};

use remora::{
    client::load_generator::LoadGenerator,
    config::{BenchmarkParameters, CollocatedPreExecutors, ValidatorConfig, ValidatorParameters},
    executor::{sui::SuiExecutor, fake::FakeExecutor, fake::FakeExecutionContext},
    metrics::Metrics,
    primary::node::PrimaryNode,
    proxy::node::ProxyNode,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[tracing_test::traced_test]
async fn remote_proxy() {
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
    let benchmark_config = BenchmarkParameters::new_for_tests();
    let primary_address = validator_config.client_server_address;

    // Create a Sui executor.
    let executor = SuiExecutor::new(&benchmark_config).await;

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
    let mut load_generator = LoadGenerator::<SuiExecutor>::new(benchmark_config, primary_address);
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
async fn remote_proxy_fake_txn() {
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
    let benchmark_config = BenchmarkParameters::new_for_fake_tests();
    let primary_address = validator_config.client_server_address;

    // Create a Sui executor.
    let execution_duration = Duration::from_micros(500);
    let checks_duration = Duration::from_micros(500);
    let execution_context = FakeExecutionContext::new(execution_duration, checks_duration);

    let executor = FakeExecutor::new(execution_context);

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
    let mut load_generator = LoadGenerator::<FakeExecutor>::new(benchmark_config, primary_address);
    let transactions = load_generator.initialize().await;
    let total_transactions = transactions.len();
    load_generator.run(transactions).await;

    // Wait for all transactions to be processed.
    for _ in 0..total_transactions {
        let (_ts, result) = primary.rx_output.recv().await.unwrap();
        assert!(result.success());
    }
}
