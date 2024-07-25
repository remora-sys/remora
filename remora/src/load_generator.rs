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
    executor::{Executor, TransactionWithTimestamp},
    metrics::{ErrorType, Metrics},
};

/// The load generator generates transactions at a specified rate and submits them to the system.
pub struct LoadGenerator<E> {
    /// The benchmark configurations.
    config: BenchmarkConfig,
    /// The network target to send transactions to.
    target: SocketAddr,
    /// The executor for generating transactions.
    executor: E,
    /// A best effort network sender.
    network: SimpleSender,
    /// Metrics for the load generator.
    metrics: Metrics,
}

impl<E: Executor> LoadGenerator<E> {
    /// Create a new load generator.
    pub fn new(config: BenchmarkConfig, target: SocketAddr, executor: E, metrics: Metrics) -> Self {
        LoadGenerator {
            config,
            target,
            executor,
            network: SimpleSender::new(),
            metrics,
        }
    }

    /// Initialize the load generator. This will generate all required genesis objects and all transactions upfront.
    pub async fn initialize(&mut self) -> Vec<E::Transaction> {
        self.executor.generate_transactions().await
    }

    /// Run the load generator. This will submit transactions to the system at the specified rate
    /// until all transactions are submitted.
    pub async fn run(&mut self, transactions: Vec<CertifiedTransaction>) {
        let precision = if self.config.load > 1000 { 20 } else { 1 };
        let burst_duration = Duration::from_millis(1000 / precision);
        let mut interval = interval(burst_duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let chunks_size = (self.config.load / precision) as usize;
        let chunks = &transactions.into_iter().chunks(chunks_size);
        tracing::info!("Submitting transactions...");
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
}

#[cfg(test)]
pub mod tests {
    use std::net::SocketAddr;

    use bytes::Bytes;
    use futures::{sink::SinkExt, stream::StreamExt};
    use tokio::{net::TcpListener, task::JoinHandle};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    use crate::{
        config::BenchmarkConfig,
        executor::{SuiExecutor, SuiTransactionWithTimestamp},
        load_generator::LoadGenerator,
        metrics::Metrics,
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
    async fn generate_transactions() {
        // Boot a test server to receive transactions.
        // TODO: Implement a better way to get a port for tests
        let target = SocketAddr::from(([127, 0, 0, 1], 18181));
        let handle = listener(target);
        tokio::task::yield_now().await;

        // Create genesis and generate transactions.
        let metrics = Metrics::new_for_tests();
        let config = BenchmarkConfig::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        let mut load_generator = LoadGenerator::new(config, target, executor, metrics);
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
