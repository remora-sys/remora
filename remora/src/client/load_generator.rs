// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    marker::PhantomData,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use rand::Rng;
use rand_mt::Mt64;
use tokio::{
    sync::mpsc::{self, Sender},
    time::{sleep, Instant},
};

use crate::{
    client::request_arrival_distribution::Distribution,
    config::{BenchmarkParameters, LoadConfig},
    executor::api::{ExecutableTransaction, Executor, RemoraTransaction, TransactionWithTimestamp},
    metrics::Metrics,
    networking::client::NetworkClient,
};

/// The load generator generates transactions at a specified rate and submits them to the system.
pub struct LoadGenerator<Executor> {
    /// The executor for the transactions.
    _phantom: PhantomData<Executor>,
    /// The benchmark configurations.
    config: BenchmarkParameters,
    /// The target socket address.
    target: SocketAddr,
    /// Request inter arrival distribution (for initial load).
    arrival: Distribution,
    /// The effective load configuration.
    load_config: LoadConfig,
}

const NUM_CLIENTS: usize = 16;

impl<E: Executor> LoadGenerator<E> {
    /// Create a new load generator.
    pub fn new(config: BenchmarkParameters, target: SocketAddr) -> Self {
        let load_config = config.effective_load_config();
        let initial_load = load_config.initial_load();
        let ns_per_packet = 1_000_000_000 / initial_load * NUM_CLIENTS as u64;

        LoadGenerator {
            _phantom: PhantomData,
            config,
            target,
            arrival: Distribution::Exponential(ns_per_packet as f64),
            load_config,
        }
    }

    /// Initialize the load generator. This will generate all required genesis objects and all transactions upfront.
    pub async fn initialize(
        &mut self,
        path: Option<&str>,
    ) -> Vec<TransactionWithTimestamp<E::Transaction>> {
        if let Some(log_path) = path {
            tracing::info!("Reading transactions from {}", log_path);
            let serialized_log =
                fs::read(log_path).expect("Failed to read transaction log from file");
            let scheduled_txs: Vec<TransactionWithTimestamp<E::Transaction>> =
                bincode::deserialize(&serialized_log)
                    .expect("Failed to deserialize transaction log");

            scheduled_txs
        } else {
            let transactions = E::generate_transactions(&self.config, None).await;
            transactions
                .into_iter()
                .enumerate()
                .map(|(_, tx)| {
                    TransactionWithTimestamp::new(
                        tx.clone(),
                        0.0,                    // Timestamp is not used yet.
                        tx.shared_object_ids(), // Use actual shared objects
                        self.config.verification_duration,
                        self.config.expected_stateful_duration,
                        None,
                    )
                })
                .collect()
        }
    }

    /// Generate inter-arrival interval
    pub fn gen_inter_arrival(rng: &mut Mt64, arrival: Distribution) -> u64 {
        arrival.sample(rng)
    }

    /// Calculate arrival distribution for a given load rate.
    fn calculate_arrival_distribution(load: u64) -> Distribution {
        let ns_per_packet = 1_000_000_000 / load * NUM_CLIENTS as u64;
        Distribution::Exponential(ns_per_packet as f64)
    }

    // Function to run the transaction submission with dynamic load support
    async fn submit_transactions_dynamic(
        transactions: Vec<TransactionWithTimestamp<E::Transaction>>,
        sender: Sender<RemoraTransaction<E>>,
        load_config: LoadConfig,
    ) where
        <E as Executor>::Transaction: std::marker::Send + 'static,
    {
        let mut rng: Mt64 = Mt64::new(rand::thread_rng().gen::<u64>());
        let start_time = Instant::now();
        let mut next_ts = start_time;

        for (counter, tx_with_timestamp) in transactions.into_iter().enumerate() {
            // Wait until the next interval before sending the next transaction
            while Instant::now() < next_ts {
                std::hint::spin_loop();
            }

            // Calculate the current load based on elapsed time
            let elapsed_secs = start_time.elapsed().as_secs();
            let current_load = load_config.get_load_at_time(elapsed_secs);

            // If load is 0, we've gone past the configured intervals - stop sending
            if current_load == 0 {
                tracing::info!("Dynamic load configuration ended, stopping transaction submission");
                break;
            }

            // Get the current timestamp for metrics and create new transaction with updated timestamp
            let timestamp = Metrics::now().as_secs_f64();
            let updated_tx = TransactionWithTimestamp::new(
                tx_with_timestamp.transaction.clone(),
                timestamp,
                tx_with_timestamp.shared_object_ids(),
                tx_with_timestamp.verification_duration(),
                tx_with_timestamp.expected_stateful_duration(),
                tx_with_timestamp.destination,
            );

            // Send the transaction
            if sender.send(updated_tx).await.is_err() {
                tracing::error!("Failed to send transaction");
                break; // Exit the loop on send failure
            }

            // Increment transaction counter for logging
            if counter > 0 && counter % 1000 == 0 {
                tracing::debug!("Submitted {} transactions", counter);
            }

            // Calculate the next interval based on current load
            let current_arrival = Self::calculate_arrival_distribution(current_load);
            next_ts += Duration::from_nanos(Self::gen_inter_arrival(&mut rng, current_arrival));
        }
    }

    // Function to run the transaction submission at a specific load (backward compatibility)
    async fn submit_transactions(
        transactions: Vec<TransactionWithTimestamp<E::Transaction>>,
        sender: Sender<RemoraTransaction<E>>,
        arrival: Distribution,
    ) where
        <E as Executor>::Transaction: std::marker::Send + 'static,
    {
        let mut rng: Mt64 = Mt64::new(rand::thread_rng().gen::<u64>());
        let mut next_ts = Instant::now();

        for (counter, tx_with_timestamp) in transactions.into_iter().enumerate() {
            // Wait until the next interval before sending the next transaction
            while Instant::now() < next_ts {
                std::hint::spin_loop();
            }

            // Get the current timestamp for metrics and create new transaction with updated timestamp
            let timestamp = Metrics::now().as_secs_f64();
            let updated_tx = TransactionWithTimestamp::new(
                tx_with_timestamp.transaction.clone(),
                timestamp,
                tx_with_timestamp.shared_object_ids(),
                tx_with_timestamp.verification_duration(),
                tx_with_timestamp.expected_stateful_duration(),
                tx_with_timestamp.destination,
            );

            // Send the transaction
            if sender.send(updated_tx).await.is_err() {
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

    async fn connect_and_spawn_network_client(&mut self) -> Vec<Sender<RemoraTransaction<E>>>
    where
        <E as Executor>::Transaction: std::marker::Send + 'static,
    {
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

    pub async fn run(&mut self, transactions: Vec<TransactionWithTimestamp<E::Transaction>>)
    where
        <E as Executor>::Transaction: std::marker::Send + 'static,
    {
        let tx_transactions = self.connect_and_spawn_network_client().await;
        self.real_run(transactions, tx_transactions).await;
    }

    pub fn split_transactions(
        &self,
        transactions: Vec<TransactionWithTimestamp<E::Transaction>>,
    ) -> Vec<Vec<TransactionWithTimestamp<E::Transaction>>> {
        let chunk_size = (transactions.len() + NUM_CLIENTS - 1) / NUM_CLIENTS; // Ceiling division
        transactions
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    }

    async fn real_run(
        &mut self,
        transactions: Vec<TransactionWithTimestamp<E::Transaction>>,
        senders: Vec<Sender<RemoraTransaction<E>>>,
    ) where
        <E as Executor>::Transaction: std::marker::Send + 'static,
    {
        let initial_load = self.load_config.initial_load();
        let is_dynamic = self.load_config.is_dynamic();

        if is_dynamic {
            tracing::info!(
                "Starting dynamic load run with initial load: {} TPS",
                initial_load
            );
        } else {
            tracing::info!("Starting constant load run at {} TPS", initial_load);
        }

        // split the transactions
        let split = self.split_transactions(transactions);

        // spawn for each client
        let mut handles = vec![];
        for (tx, tx_chunk) in senders.into_iter().zip(split.into_iter()) {
            let load_config = self.load_config.clone();
            let arrival = self.arrival;
            let handle = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    sleep(Duration::from_secs(1)).await;
                    if load_config.is_dynamic() {
                        Self::submit_transactions_dynamic(tx_chunk, tx, load_config).await;
                    } else {
                        Self::submit_transactions(tx_chunk, tx, arrival).await;
                    }
                });
            });
            handles.push(handle);
        }

        for handle in handles {
            if let Err(e) = handle.await {
                tracing::error!("Task error: {:?}", e);
            }
        }
    }

    /// This function takes a batch of transactions, computes a routing schedule, and writes the resulting transaction log to a file.
    /// The current scheduling logic is a placeholder and can be replaced with a more sophisticated algorithm.
    pub async fn generate_schedule_and_log(&self, path: &str)
    where
        <E as Executor>::Transaction: std::marker::Send + 'static,
    {
        let transactions = E::generate_transactions(&self.config, None).await;

        use crate::client::hermes_schedule;
        use rustc_hash::FxHashMap;
        use sui_types::base_types::ObjectID;

        let mut scheduled_transactions = Vec::new();
        const BATCH_SIZE: usize = 1000;
        let mut object_node_partition: FxHashMap<ObjectID, usize> = FxHashMap::default();

        for batch in transactions.chunks(BATCH_SIZE) {
            let schedule_result = hermes_schedule::schedule_transactions_hermes(
                batch,
                &mut object_node_partition,
                3, // NOTE: hardcoded with 3 proxies
                self.config.assignment_mode,
            );

            // Process transactions in the optimal order determined by the scheduler
            for &tx_idx in &schedule_result.transaction_order {
                let tx = &batch[tx_idx];
                let destination = schedule_result.destinations[tx_idx];
                let full_tx = TransactionWithTimestamp::new(
                    tx.clone(),
                    0.0, // Timestamp is not used in offline mode.
                    tx.shared_object_ids(),
                    self.config.verification_duration,
                    self.config.expected_stateful_duration,
                    Some(destination),
                );
                scheduled_transactions.push(full_tx);
            }
        }

        // Serialize and write the transaction log to the specified file.
        let serialized_log = bincode::serialize(&scheduled_transactions)
            .expect("Failed to serialize transaction log");
        fs::write(path, serialized_log).expect("Failed to write transaction log to file");

        tracing::info!("Transaction schedule generated and written to {}", path);
    }
}

#[cfg(test)]
pub mod tests {

    use tokio::sync::mpsc;

    use crate::{
        client::load_generator::LoadGenerator,
        config::{
            get_test_address, BenchmarkParameters, DynamicLoadConfig, LoadConfig, LoadInterval,
        },
        executor::sui::{SuiExecutor, SuiTransaction},
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
        let mut load_generator: LoadGenerator<SuiExecutor> = LoadGenerator::new(config, target);
        let transactions = load_generator.initialize(None).await;

        // Submit transactions to the server.
        let now = Metrics::now().as_secs_f64();
        load_generator.run(transactions).await;

        // Check that the transactions were received.
        let transaction = rx_transactions.recv().await.unwrap();
        assert!(transaction.timestamp() > now);
    }

    #[tokio::test]
    async fn test_dynamic_load_configuration() {
        // Create a dynamic load configuration
        let dynamic_config = DynamicLoadConfig {
            total_duration_secs: 6,
            intervals: vec![
                LoadInterval {
                    start_time_secs: 0,
                    end_time_secs: 2,
                    target_load: 1000,
                },
                LoadInterval {
                    start_time_secs: 2,
                    end_time_secs: 4,
                    target_load: 2000,
                },
                LoadInterval {
                    start_time_secs: 4,
                    end_time_secs: 6,
                    target_load: 1500,
                },
            ],
        };

        // Validate the configuration
        assert!(dynamic_config.validate().is_ok());

        // Test getting load at different times
        assert_eq!(dynamic_config.get_load_at_time(0), Some(1000));
        assert_eq!(dynamic_config.get_load_at_time(1), Some(1000));
        assert_eq!(dynamic_config.get_load_at_time(2), Some(2000));
        assert_eq!(dynamic_config.get_load_at_time(3), Some(2000));
        assert_eq!(dynamic_config.get_load_at_time(4), Some(1500));
        assert_eq!(dynamic_config.get_load_at_time(5), Some(1500));
        assert_eq!(dynamic_config.get_load_at_time(6), None); // Past the end

        // Test LoadConfig wrapper
        let load_config = LoadConfig::Dynamic(dynamic_config);
        assert!(load_config.is_dynamic());
        assert_eq!(load_config.initial_load(), 1000);
        assert_eq!(load_config.get_load_at_time(2), 2000);
    }

    #[tokio::test]
    async fn test_dynamic_load_validation() {
        // Test invalid configuration - gap in intervals
        let invalid_config = DynamicLoadConfig {
            total_duration_secs: 10,
            intervals: vec![
                LoadInterval {
                    start_time_secs: 0,
                    end_time_secs: 3,
                    target_load: 1000,
                },
                LoadInterval {
                    start_time_secs: 5, // Gap from 3 to 5
                    end_time_secs: 10,
                    target_load: 2000,
                },
            ],
        };

        assert!(invalid_config.validate().is_err());

        // Test invalid configuration - overlap
        let invalid_config2 = DynamicLoadConfig {
            total_duration_secs: 10,
            intervals: vec![
                LoadInterval {
                    start_time_secs: 0,
                    end_time_secs: 6,
                    target_load: 1000,
                },
                LoadInterval {
                    start_time_secs: 5, // Overlap at 5-6
                    end_time_secs: 10,
                    target_load: 2000,
                },
            ],
        };

        assert!(invalid_config2.validate().is_err());

        // Test valid continuous configuration
        let valid_config = DynamicLoadConfig {
            total_duration_secs: 10,
            intervals: vec![
                LoadInterval {
                    start_time_secs: 0,
                    end_time_secs: 5,
                    target_load: 1000,
                },
                LoadInterval {
                    start_time_secs: 5,
                    end_time_secs: 10,
                    target_load: 2000,
                },
            ],
        };

        assert!(valid_config.validate().is_ok());
    }

    #[test]
    fn test_from_intervals() {
        // Test the convenience method for creating dynamic load configs
        let interval_map = vec![(0, 1000), (10, 2000), (20, 1500)];
        let config = DynamicLoadConfig::from_intervals(30, &interval_map);

        assert_eq!(config.total_duration_secs, 30);
        assert_eq!(config.intervals.len(), 3);

        assert_eq!(config.intervals[0].start_time_secs, 0);
        assert_eq!(config.intervals[0].end_time_secs, 10);
        assert_eq!(config.intervals[0].target_load, 1000);

        assert_eq!(config.intervals[1].start_time_secs, 10);
        assert_eq!(config.intervals[1].end_time_secs, 20);
        assert_eq!(config.intervals[1].target_load, 2000);

        assert_eq!(config.intervals[2].start_time_secs, 20);
        assert_eq!(config.intervals[2].end_time_secs, 30);
        assert_eq!(config.intervals[2].target_load, 1500);

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_dynamic_load_transaction_calculation() {
        use crate::config::{BenchmarkParameters, DynamicLoadConfig, LoadConfig, LoadInterval};
        use std::time::Duration;

        // Test that transaction calculation works correctly for dynamic loads
        let dynamic_config = DynamicLoadConfig {
            total_duration_secs: 30,
            intervals: vec![
                LoadInterval {
                    start_time_secs: 0,
                    end_time_secs: 10,
                    target_load: 1000, // 1000 TPS for 10s = 10,000 transactions
                },
                LoadInterval {
                    start_time_secs: 10,
                    end_time_secs: 20,
                    target_load: 2000, // 2000 TPS for 10s = 20,000 transactions
                },
                LoadInterval {
                    start_time_secs: 20,
                    end_time_secs: 30,
                    target_load: 1500, // 1500 TPS for 10s = 15,000 transactions
                },
            ],
        };

        let config = BenchmarkParameters {
            load_config: LoadConfig::Dynamic(dynamic_config),
            load: None,
            duration: Duration::from_secs(30),
            ..BenchmarkParameters::new_for_tests()
        };

        // Should calculate total: 10,000 + 20,000 + 15,000 = 45,000 transactions
        assert_eq!(config.calculate_total_transactions(), 45000);

        // Compare with old method which would be incorrect: 1000 * 30 = 30,000
        assert_eq!(
            config.get_initial_load() * config.effective_duration().as_secs(),
            30000
        );

        // Verify the new method generates more transactions than the old method
        assert!(
            config.calculate_total_transactions()
                > config.get_initial_load() * config.effective_duration().as_secs()
        );
    }

    #[test]
    fn test_constant_load_transaction_calculation() {
        use crate::config::{BenchmarkParameters, LoadConfig};
        use std::time::Duration;

        // Test that constant load calculation remains the same
        let config = BenchmarkParameters {
            load_config: LoadConfig::Constant(5000),
            load: None,
            duration: Duration::from_secs(60),
            ..BenchmarkParameters::new_for_tests()
        };

        // Should be 5000 * 60 = 300,000 transactions
        assert_eq!(config.calculate_total_transactions(), 300000);
        assert_eq!(
            config.get_initial_load() * config.effective_duration().as_secs(),
            300000
        );
    }
}

/// The default metrics address.
pub fn default_metrics_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18600)
}
