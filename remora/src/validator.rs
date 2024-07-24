// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{error::Error, sync::Arc};

use axum::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use network::{MessageHandler, Writer};
use tokio::sync::mpsc::{self, Sender};

use crate::{
    config::ValidatorConfig,
    executor::{SuiExecutor, SuiTransactionWithTimestamp},
    load_balancer::LoadBalancer,
    metrics::Metrics,
    mock_consensus::{models::FixedDelay, MockConsensus},
    primary::PrimaryExecutor,
    proxy::Proxy,
};

const DEFAULT_CHANNEL_SIZE: usize = 100;

pub struct SingleMachineValidator;

impl SingleMachineValidator {
    pub async fn start(executor: SuiExecutor, config: &ValidatorConfig, metrics: Arc<Metrics>) {
        let (tx_client_transactions, rx_client_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_load_balancer_load, rx_load_balancer_load) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_commits, rx_commits) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_output, mut rx_output) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let mut handles = Vec::new();
        let mut proxy_senders = Vec::new();
        for i in 0..config.num_proxies {
            let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let store = executor.create_in_memory_store();
            let proxy = Proxy::new(i, executor.clone(), store, rx, tx_proxy_results.clone());
            handles.push(proxy.spawn());
            proxy_senders.push(tx);
        }

        let consensus_handle = MockConsensus::new(
            FixedDelay::default(),
            config.consensus_parameters.clone(),
            rx_load_balancer_load,
            tx_commits,
        )
        .spawn();
        handles.push(consensus_handle);

        let store = executor.create_in_memory_store();
        let primary_handle = PrimaryExecutor::new(
            executor,
            store,
            rx_commits,
            rx_proxy_results,
            tx_output,
            metrics,
        )
        .spawn();
        handles.push(primary_handle);

        let load_balancer_handle =
            LoadBalancer::new(rx_client_transactions, tx_load_balancer_load, proxy_senders).spawn();
        handles.push(load_balancer_handle);

        let network_handler = SingleMachineValidatorHandler {
            deliver: tx_client_transactions,
        };
        network::Receiver::spawn(config.address, network_handler);

        tokio::select! {
            _ = join_all(handles) => (),
            Some(result) = rx_output.recv() => {
                tracing::debug!("Received output: {:?}", result);
            }
            else => (),
        }
    }
}

#[derive(Clone)]
struct SingleMachineValidatorHandler {
    deliver: Sender<SuiTransactionWithTimestamp>,
}

#[async_trait]
impl MessageHandler for SingleMachineValidatorHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        let message = bincode::deserialize(&message).expect("Failed to deserialize transaction");
        self.deliver.send(message).await.unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use crate::{
        config::{BenchmarkConfig, ValidatorConfig},
        executor::SuiExecutor,
        load_generator::LoadGenerator,
        metrics::Metrics,
        mock_consensus::MockConsensusParameters,
        validator::SingleMachineValidator,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn execute_transactions() {
        let address = SocketAddr::from(([127, 0, 0, 1], 18588));
        let metrics_address = SocketAddr::from(([127, 0, 0, 1], 18589));
        let config = ValidatorConfig {
            address,
            metrics_address,
            num_proxies: 1,
            consensus_parameters: MockConsensusParameters::default(),
        };
        let benchmark_config = BenchmarkConfig::new_for_tests();

        //
        let executor = SuiExecutor::new(&benchmark_config).await;

        //
        let metrics = Arc::new(Metrics::new_for_tests());
        let _m = metrics.clone();
        let _e = executor.clone();
        tokio::spawn(async move {
            SingleMachineValidator::start(_e, &config, _m).await;
        });

        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(5000)).await;

        //
        let metrics_2 = Metrics::new_for_tests();
        // let executor = SuiExecutor::new(&benchmark_config).await;
        let mut load_generator = LoadGenerator::new(benchmark_config, address, executor, metrics_2);
        let transactions = load_generator.initialize().await;
        load_generator.run(transactions).await;

        loop {
            let x = metrics
                .latency_s
                .get_metric_with_label_values(&["default"])
                .unwrap()
                .get_sample_count();

            if x > 0 {
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}
