// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use itertools::Itertools;
use network::SimpleSender;
use sui_types::transaction::CertifiedTransaction;
use tokio::time::{interval, Instant, MissedTickBehavior};

use crate::{
    config::BenchmarkConfig,
    executor::{generate_transactions, TransactionWithTimestamp},
    metrics::{ErrorType, Metrics},
};

/// The load generator generates transactions at a specified rate and submits them to the system.
pub struct LoadGenerator {
    /// The benchmark configurations.
    config: BenchmarkConfig,
    /// The network target to send transactions to.
    target: SocketAddr,
    /// A best effort network sender.
    network: SimpleSender,
    /// Metrics for the load generator.
    metrics: Metrics,
}

impl LoadGenerator {
    /// Create a new load generator.
    pub fn new(config: BenchmarkConfig, target: SocketAddr, metrics: Metrics) -> Self {
        LoadGenerator {
            config,
            target,
            network: SimpleSender::new(),
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
                let serialized = bincode::serialize(&full_tx).expect("serialization failed");
                let bytes = Bytes::from(serialized);
                self.network.send(self.target, bytes).await;
            }

            if now.elapsed() > burst_duration {
                tracing::warn!("Transaction rate too high for this client");
                self.metrics
                    .register_error(ErrorType::TransactionRateTooHigh);
            }

            interval.tick().await;
        }
    }

    pub async fn run(&mut self, transactions: Vec<CertifiedTransaction>) {
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
            self.real_run(transactions).await;
        } else {
            tracing::info!("Starting warm-up and real run phases...");
            self.warm_up_and_real_run(transactions, warm_up_load).await;
        }
    }

    async fn warm_up_and_real_run(
        &mut self,
        transactions: Vec<CertifiedTransaction>,
        warm_up_load: u64,
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
        );

        // Use a timeout to limit the warm-up phase duration
        let _ = tokio::time::timeout(warm_up_duration, warm_up_future).await;

        // After warm-up, proceed to the real run
        self.real_run(remaining_transactions.to_vec()).await;
    }

    async fn real_run(&mut self, transactions: Vec<CertifiedTransaction>) {
        let real_load = self.config.load;
        tracing::info!("Starting real run at {} load...", real_load);

        let precision = if real_load > 1_000 { 20 } else { 1 };
        let burst_duration = Duration::from_millis(1_000 / precision);

        self.submit_transactions(transactions, real_load, precision, burst_duration)
            .await;
    }
}

#[cfg(test)]
pub mod tests {
    use std::net::SocketAddr;

    use bytes::Bytes;
    use futures::{sink::SinkExt, stream::StreamExt};
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    use crate::{
        config::BenchmarkConfig, executor::SuiTransactionWithTimestamp,
        load_generator::LoadGenerator, metrics::Metrics,
    };

    /// Create a network listener that will receive a single message and return it.
    fn listener(address: SocketAddr) -> JoinHandle<Bytes> {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&address).await.unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            match reader.next().await {
                Some(Ok(received)) => {
                    writer.send(Bytes::from("Ack")).await.unwrap();
                    return received.freeze();
                }
                _ => panic!("Failed to receive network message"),
            }
        })
    }

    #[tokio::test]
    async fn test_generate_transactions() {
        // Boot a test server to receive transactions.
        // TODO: Implement a better way to get a port for tests
        let target = SocketAddr::from(([127, 0, 0, 1], 18181));
        let handle = listener(target);
        tokio::task::yield_now().await;

        // Create genesis and generate transactions.
        let metrics = Metrics::new_for_tests();
        let config = BenchmarkConfig::new_for_tests();
        let mut load_generator = LoadGenerator::new(config, target, metrics);
        let transactions = load_generator.initialize().await;

        // Submit transactions to the server.
        let now = Metrics::now().as_secs_f64();
        load_generator.run(transactions).await;

        // Check that the transactions were received.
        let received = handle.await.unwrap();
        let tx: SuiTransactionWithTimestamp = bincode::deserialize(&received).unwrap();
        assert!(tx.timestamp() > now);
    }
}
