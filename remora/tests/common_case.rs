// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use remora::{
    config::{get_test_address, BenchmarkConfig, CollocatedPreExecutors, ValidatorConfig},
    executor::sui::SuiExecutor,
    load_generator::LoadGenerator,
    metrics::Metrics,
    primary::node::PrimaryNode,
    proxy::node::ProxyNode,
};

#[tokio::test]
#[tracing_test::traced_test]
async fn remote_proxy() {
    let primary_address = get_test_address();
    let benchmark_config = BenchmarkConfig::new_for_tests();
    let config = ValidatorConfig {
        collocated_pre_executors: CollocatedPreExecutors {
            primary: 0,
            proxy: 1,
        },
        ..ValidatorConfig::new_for_tests()
    };

    // Create a Sui executor.
    let executor = SuiExecutor::new(&benchmark_config).await;

    // Start the primary.
    let validator_metrics = Arc::new(Metrics::new_for_tests());
    let mut primary = PrimaryNode::start(
        executor.clone(),
        &config,
        primary_address,
        validator_metrics.clone(),
    )
    .await;
    tokio::task::yield_now().await;

    // Start a remote proxy.
    let proxy_id = 0.to_string();
    let _proxy = ProxyNode::start(
        proxy_id,
        executor,
        &config,
        primary_address,
        validator_metrics,
    )
    .await;
    tokio::task::yield_now().await;

    // Generate transactions.
    let load_generator_metrics = Metrics::new_for_tests();
    let mut load_generator =
        LoadGenerator::new(benchmark_config, primary_address, load_generator_metrics);
    let transactions = load_generator.initialize().await;
    let total_transactions = transactions.len();
    load_generator.run(transactions).await;

    // Wait for all transactions to be processed.
    for _ in 0..total_transactions {
        let (_tx, result) = primary.rx_output.recv().await.unwrap();
        assert!(result.success());
    }
}
