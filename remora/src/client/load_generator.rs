// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use rand::Rng;
use rand_mt::Mt64;
use sui_types::transaction::Transaction;
use tokio::{
    sync::mpsc::{self, Sender},
    time::{sleep, Instant},
};

use crate::{
    client::request_arrival_distribution::Distribution,
    config::BenchmarkParameters,
    executor::{
        api::TransactionWithTimestamp,
        sui::{generate_transactions, SuiTransaction},
    },
    metrics::Metrics,
    networking::client::NetworkClient,
};

/// The load generator generates transactions at a specified rate and submits them to the system.
pub struct LoadGenerator {
    /// The benchmark configurations.
    config: BenchmarkParameters,
    /// The target socket address.
    target: SocketAddr,
    /// Request inter arrival distribution.
    arrival: Distribution,
}

const NUM_CLIENTS: usize = 8;

impl LoadGenerator {
    /// Create a new load generator.
    pub fn new(config: BenchmarkParameters, target: SocketAddr) -> Self {
        let ns_per_packet = 1_000_000_000 / config.load * NUM_CLIENTS as u64;
        LoadGenerator {
            config,
            target,
            arrival: Distribution::Exponential(ns_per_packet as f64),
        }
    }

    /// Initialize the load generator. This will generate all required genesis objects and all transactions upfront.
    pub async fn initialize(&mut self) -> Vec<Transaction> {
        generate_transactions(&self.config, None).await
    }

    /// Generate inter-arrival interval
    pub fn gen_inter_arrival(rng: &mut Mt64, arrival: Distribution) -> u64 {
        arrival.sample(rng)
    }

    // Function to run the transaction submission at a specific load
    async fn submit_transactions(
        transactions: Vec<Transaction>,
        sender: Sender<SuiTransaction>,
        arrival: Distribution,
    ) {
        let mut rng: Mt64 = Mt64::new(rand::thread_rng().gen::<u64>());
        let mut next_ts = Instant::now();

        for (counter, tx) in transactions.into_iter().enumerate() {
            // Wait until the next interval before sending the next transaction
            while Instant::now() < next_ts {
                std::hint::spin_loop();
            }

            // Get the current timestamp for metrics
            let timestamp = Metrics::now().as_secs_f64();
            let full_tx = TransactionWithTimestamp::new(tx, timestamp);

            // Send the transaction
            if sender.send(full_tx).await.is_err() {
                tracing::error!("Failed to send transaction");
                break; // Exit the loop on send failure
            }

            // Increment transaction counter for logging
            if counter > 0 && counter % 1000 == 0 {
                tracing::debug!("Submitted {} transactions", counter);
            }

            // Calculate the next interval
            next_ts += Duration::from_nanos(Self::gen_inter_arrival(&mut rng, arrival));
        }
    }

    async fn connect_and_spawn_network_client(&mut self) -> Vec<mpsc::Sender<SuiTransaction>> {
        let mut senders = Vec::with_capacity(NUM_CLIENTS);

        for _ in 0..NUM_CLIENTS {
            let (tx_unused, _rx_unused) = mpsc::channel(1);
            let (tx_transactions, rx_transactions) = mpsc::channel(1_000_000);
            let client = NetworkClient::<(), _>::new(self.target, tx_unused, rx_transactions);

            match client.connect().await {
                Ok(stream) => {
                    client.spawn_after_connect(stream);
                    senders.push(tx_transactions);
                }
                Err(e) => {
                    tracing::error!("Failed to connect to server: {}", e);
                }
            }
        }

        senders
    }

    pub async fn run(&mut self, transactions: Vec<Transaction>) {
        let tx_transactions = self.connect_and_spawn_network_client().await;
        self.real_run(transactions, tx_transactions).await;
    }

    pub fn split_transactions(&self, transactions: Vec<Transaction>) -> Vec<Vec<Transaction>> {
        let chunk_size = (transactions.len() + NUM_CLIENTS - 1) / NUM_CLIENTS; // Ceiling division
        transactions
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    }

    async fn real_run(
        &mut self,
        transactions: Vec<Transaction>,
        senders: Vec<Sender<SuiTransaction>>,
    ) {
        let real_load = self.config.load;
        tracing::info!("Starting run at {} load...", real_load);

        // split the transactions
        let split = self.split_transactions(transactions);

        // spawn for each client
        let mut handles = vec![];
        for (tx, tx_chunk) in senders.into_iter().zip(split.into_iter()) {
            let arrival = self.arrival.clone();
            let handle = tokio::spawn(async move {
                sleep(Duration::from_secs(1)).await;
                Self::submit_transactions(tx_chunk, tx, arrival).await;
            });
            handles.push(handle);
        }

        for handle in handles {
            if let Err(e) = handle.await {
                tracing::error!("Task error: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {

    use tokio::sync::mpsc;

    use crate::{
        client::load_generator::LoadGenerator,
        config::{get_test_address, BenchmarkParameters},
        executor::sui::SuiTransaction,
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
        let config = BenchmarkParameters::new_for_tests();
        let mut load_generator = LoadGenerator::new(config, target);
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
