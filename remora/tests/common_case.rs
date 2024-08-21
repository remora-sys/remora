// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use remora::{
    config::{BenchmarkConfig, ValidatorConfig},
    executor::sui::SuiExecutor,
    load_generator::LoadGenerator,
    metrics::Metrics,
    primary::node::PrimaryNode,
    proxy::node::ProxyNode,
};

#[tokio::test]
#[tracing_test::traced_test]
async fn remote_proxy() {
    let config = ValidatorConfig {
        local_proxies: 0,
        ..ValidatorConfig::new_for_tests()
    };
    let validator_address = config.validator_address;
    let benchmark_config = BenchmarkConfig::new_for_tests();

    // Create a Sui executor.
    let executor = SuiExecutor::new(&benchmark_config).await;

    // Start the primary.
    let validator_metrics = Arc::new(Metrics::new_for_tests());
    let mut primary =
        PrimaryNode::start(executor.clone(), &config, validator_metrics.clone()).await;
    tokio::task::yield_now().await;

    // Start a remote proxy.
    let proxy_id = 0;
    let _proxy = ProxyNode::start(proxy_id, executor, &config, validator_metrics).await;
    tokio::task::yield_now().await;

    // Generate transactions.
    let load_generator_metrics = Metrics::new_for_tests();
    let mut load_generator =
        LoadGenerator::new(benchmark_config, validator_address, load_generator_metrics);
    let transactions = load_generator.initialize().await;
    let total_transactions = transactions.len();
    load_generator.run(transactions).await;

    // Wait for all transactions to be processed.
    for _ in 0..total_transactions {
        let (_tx, result) = primary.rx_output.recv().await.unwrap();
        assert!(result.success());
    }
}
