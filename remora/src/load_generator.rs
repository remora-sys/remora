// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use itertools::Itertools;
use sui_types::transaction::CertifiedTransaction;
use tokio::{
    sync::mpsc::{self, Sender},
    time::{interval, Instant, MissedTickBehavior},
};

use crate::{
    config::BenchmarkParameters,
    executor::{
        api::TransactionWithTimestamp,
        sui::{generate_transactions, SuiTransaction},
    },
    metrics::{ErrorType, Metrics},
    networking::client::NetworkClient,
};

/// The load generator generates transactions at a specified rate and submits them to the system.
pub struct LoadGenerator {
    /// The benchmark configurations.
    config: BenchmarkParameters,
    /// The target socket address.
    target: SocketAddr,
    /// Metrics for the load generator.
    metrics: Metrics,
}

impl LoadGenerator {
    /// Create a new load generator.
    pub fn new(config: BenchmarkParameters, target: SocketAddr, metrics: Metrics) -> Self {
        LoadGenerator {
            config,
            target,
            metrics,
        }
    }

    /// Initialize the load generator. This will generate all required genesis objects and all transactions upfront.
    pub async fn initialize(&mut self) -> Vec<CertifiedTransaction> {
        generate_transactions(&self.config).await
    }

    // Function to run the transaction submission at a specific load
    async fn submit_transactions(
        &mut self,
        transactions: Vec<CertifiedTransaction>,
        load: u64,
        precision: u64,
        burst_duration: Duration,
        sender: Sender<SuiTransaction>,
    ) {
        let mut interval = interval(burst_duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let chunks_size = (load / precision) as usize;
        let chunks = &transactions.into_iter().chunks(chunks_size);

        for (counter, chunk) in chunks.into_iter().enumerate() {
            if counter % 1000 == 0 && counter != 0 {
                tracing::debug!("Submitted {} txs", counter * chunks_size);
            }

            let now = Instant::now();
            let timestamp = Metrics::now().as_secs_f64();
            for tx in chunk {
                let full_tx = TransactionWithTimestamp::new(tx, timestamp);
                sender.send(full_tx).await.expect("Cannot send transaction");
            }

            if now.elapsed() > burst_duration {
                tracing::warn!("Transaction rate too high for this client");
                self.metrics
                    .register_error(ErrorType::TransactionRateTooHigh);
            }

            interval.tick().await;
        }
    }

    async fn connect_and_spawn_network_client(&mut self) -> mpsc::Sender<SuiTransaction> {
        let (tx_unused, _rx_unused) = mpsc::channel(1);
        let (tx_transactions, rx_transactions) = mpsc::channel(100_000);
        let client = NetworkClient::<(), _>::new(self.target, tx_unused, rx_transactions);

        match client.connect().await {
            Ok(stream) => {
                client.spawn_after_connect(stream);
            }
            Err(e) => {
                tracing::error!("Failed to connect to server: {}", e);
            }
        }

        tx_transactions
    }

    pub async fn run(&mut self, transactions: Vec<CertifiedTransaction>) {
        let tx_transactions = self.connect_and_spawn_network_client().await;

        let warm_up_load = 2_000;
        let real_load = self.config.load;

        // If the real load is less than or equal to the warm-up load, skip the warm-up
        // used for test cases
        if real_load <= warm_up_load {
            tracing::info!(
                "Skipping warm-up phase as real load ({}) <= warm-up load ({})",
                real_load,
                warm_up_load
            );
            self.real_run(transactions, tx_transactions).await;
        } else {
            tracing::info!("Starting warm-up and real run phases...");
            self.warm_up_and_real_run(transactions, warm_up_load, tx_transactions)
                .await;
        }
    }

    async fn warm_up_and_real_run(
        &mut self,
        transactions: Vec<CertifiedTransaction>,
        warm_up_load: u64,
        sender: Sender<SuiTransaction>,
    ) {
        let warm_up_duration = Duration::from_secs(1);

        // Warm-up configuration
        tracing::info!("Starting warm-up phase at {} load...", warm_up_load);
        let warm_up_precision = if warm_up_load > 1_000 { 20 } else { 1 };
        let warm_up_burst_duration = Duration::from_millis(1_000 / warm_up_precision);

        // Calculate how many transactions are needed for the warm-up phase
        let warm_up_chunk_size = (warm_up_load / warm_up_precision) as usize;
        let warm_up_tx_count = warm_up_chunk_size
            * (warm_up_duration.as_secs_f64() * warm_up_precision as f64) as usize;

        tracing::info!(
            "warm-up len {}, total_len {}",
            warm_up_tx_count,
            transactions.len(),
        );

        // Split the transactions into warm-up and real run transactions
        let (warm_up_transactions, remaining_transactions) =
            transactions.split_at(warm_up_tx_count);

        let warm_up_future = self.submit_transactions(
            warm_up_transactions.to_vec(), // Use the warm-up transactions
            warm_up_load,
            warm_up_precision,
            warm_up_burst_duration,
            sender.clone(),
        );

        // Use a timeout to limit the warm-up phase duration
        let _ = tokio::time::timeout(warm_up_duration, warm_up_future).await;

        // After warm-up, proceed to the real run
        self.real_run(remaining_transactions.to_vec(), sender).await;
    }

    async fn real_run(
        &mut self,
        transactions: Vec<CertifiedTransaction>,
        sender: Sender<SuiTransaction>,
    ) {
        let real_load = self.config.load;
        tracing::info!("Starting real run at {} load...", real_load);

        let precision = if real_load > 1_000 { 20 } else { 1 };
        let burst_duration = Duration::from_millis(1_000 / precision);

        self.submit_transactions(transactions, real_load, precision, burst_duration, sender)
            .await;
    }
}

#[cfg(test)]
pub mod tests {

    use tokio::sync::mpsc;

    use crate::{
        config::{get_test_address, BenchmarkParameters},
        executor::sui::SuiTransaction,
        load_generator::LoadGenerator,
        metrics::Metrics,
        networking::server::NetworkServer,
    };

    #[tokio::test]
    async fn test_generate_transactions() {
        let target = get_test_address();

        // Boot a test server to receive transactions.
        let (tx_client_connections, _rx_client_connections) = mpsc::channel(1);
        let (tx_transactions, mut rx_transactions) = mpsc::channel(100);
        let _handle = NetworkServer::<SuiTransaction, ()>::new(
            target,
            tx_client_connections,
            tx_transactions,
        )
        .spawn();
        tokio::task::yield_now().await;

        // Create genesis and generate transactions.
        let metrics = Metrics::new_for_tests();
        let config = BenchmarkParameters::new_for_tests();
        let mut load_generator = LoadGenerator::new(config, target, metrics);
        let transactions = load_generator.initialize().await;

        // Submit transactions to the server.
        let now = Metrics::now().as_secs_f64();
        load_generator.run(transactions).await;

        // Check that the transactions were received.
        let transaction = rx_transactions.recv().await.unwrap();
        assert!(transaction.timestamp() > now);
    }
}

/// The default metrics address.
pub fn default_metrics_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18600)
}
