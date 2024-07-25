// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{error::Error, sync::Arc};

use axum::async_trait;
use bytes::Bytes;
use network::{MessageHandler, Writer};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    config::ValidatorConfig,
    executor::{SuiExecutionEffects, SuiExecutor, SuiTransactionWithTimestamp},
    load_balancer::LoadBalancer,
    metrics::Metrics,
    mock_consensus::MockConsensus,
    primary::PrimaryExecutor,
    proxy::Proxy,
};

/// Default channel size for communication between components.
const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// The single machine validator is a simple validator that runs all components.
pub struct SingleMachineValidator {
    /// The handles for all components.
    pub handles: Vec<JoinHandle<()>>,
    /// The receiver for the final execution results.
    pub rx_output: Receiver<(SuiTransactionWithTimestamp, SuiExecutionEffects)>,
    /// The metrics for the validator.
    pub metrics: Arc<Metrics>,
}

impl SingleMachineValidator {
    /// Start the single machine validator.
    pub async fn start(
        executor: SuiExecutor,
        config: &ValidatorConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (tx_client_transactions, rx_client_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_load_balancer_load, rx_load_balancer_load) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_commits, rx_commits) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_output, rx_output) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        // Boot the proxies.
        let mut handles = Vec::new();
        let mut proxy_senders = Vec::new();
        for i in 0..config.num_proxies {
            let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let store = executor.create_in_memory_store();
            let proxy = Proxy::new(i, executor.clone(), store, rx, tx_proxy_results.clone());
            handles.push(proxy.spawn());
            proxy_senders.push(tx);
        }

        // Boot the consensus.
        let consensus_handle = MockConsensus::new(
            config.consensus_delay_model.clone(),
            config.consensus_parameters.clone(),
            rx_load_balancer_load,
            tx_commits,
        )
        .spawn();
        handles.push(consensus_handle);

        // Boot the primary executor.
        let store = executor.create_in_memory_store();
        let primary_handle =
            PrimaryExecutor::new(executor, store, rx_commits, rx_proxy_results, tx_output).spawn();
        handles.push(primary_handle);

        // Boot the load balancer.
        let load_balancer_handle = LoadBalancer::<SuiExecutor>::new(
            rx_client_transactions,
            tx_load_balancer_load,
            proxy_senders,
        )
        .spawn();
        handles.push(load_balancer_handle);

        let network_handler = SingleMachineValidatorHandler {
            deliver: tx_client_transactions,
        };
        network::Receiver::spawn(config.validator_address, network_handler);

        Self {
            handles,
            rx_output,
            metrics,
        }
    }

    /// Collect the results from the validator.
    pub async fn collect_results(mut self) {
        while let Some((tx, result)) = self.rx_output.recv().await {
            tracing::debug!("Received output: {:?}", result);
            assert!(result.success());
            let submit_timestamp = tx.timestamp();
            // TODO: Record transactions success and failure.
            self.metrics.update_metrics(submit_timestamp);
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
        mock_consensus::{models::FixedDelay, MockConsensusParameters},
        validator::SingleMachineValidator,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn execute_transactions() {
        // TODO: Implement a better way to get a port for tests
        let validator_address = SocketAddr::from(([127, 0, 0, 1], 18588));
        let metrics_address = SocketAddr::from(([127, 0, 0, 1], 18589));
        let config = ValidatorConfig {
            validator_address,
            metrics_address,
            num_proxies: 1,
            consensus_delay_model: FixedDelay::default(),
            consensus_parameters: MockConsensusParameters::default(),
        };
        let benchmark_config = BenchmarkConfig::new_for_tests();

        // Create a Sui executor.
        let executor = SuiExecutor::new(&benchmark_config).await;

        // Start the validator.
        let validator_metrics = Arc::new(Metrics::new_for_tests());
        let mut validator =
            SingleMachineValidator::start(executor.clone(), &config, validator_metrics).await;
        tokio::task::yield_now().await;

        // Generate transactions.
        let load_generator_metrics = Metrics::new_for_tests();
        let mut load_generator = LoadGenerator::new(
            benchmark_config,
            validator_address,
            executor,
            load_generator_metrics,
        );
        let transactions = load_generator.initialize().await;
        let total_transactions = transactions.len();
        load_generator.run(transactions).await;

        // Wait for all transactions to be processed.
        for _ in 0..total_transactions {
            let (_tx, result) = validator.rx_output.recv().await.unwrap();
            assert!(result.success());
        }
    }
}
