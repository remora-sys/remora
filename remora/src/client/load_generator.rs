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
}

const NUM_CLIENTS: usize = 16;

impl<E: Executor> LoadGenerator<E> {
    /// Create a new load generator.
    pub fn new(config: BenchmarkParameters, target: SocketAddr) -> Self {
        // Log the load configuration
        if config.load_config.is_dynamic() {
            tracing::info!("Load generator using DYNAMIC load config");
        } else {
            let initial_load = config.load_config.get_load_at(0).max(config.load);
            tracing::info!("Load generator using static load: {} TPS", initial_load);
        }
        LoadGenerator {
            _phantom: PhantomData,
            config,
            target,
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

    /// Calculate arrival distribution for a given load (TPS per client)
    fn arrival_for_load(load_per_client: u64) -> Distribution {
        if load_per_client == 0 {
            // Zero load means very slow (1 per second per client)
            Distribution::Exponential(1_000_000_000.0)
        } else {
            let ns_per_packet = 1_000_000_000 / load_per_client;
            Distribution::Exponential(ns_per_packet as f64)
        }
    }

    // Function to run the transaction submission at a specific load
    // Now supports dynamic load via load_config
    async fn submit_transactions(
        transactions: Vec<TransactionWithTimestamp<E::Transaction>>,
        sender: Sender<RemoraTransaction<E>>,
        load_config: LoadConfig,
        num_clients: usize,
    ) where
        <E as Executor>::Transaction: std::marker::Send + 'static,
    {
        let mut rng: Mt64 = Mt64::new(rand::thread_rng().gen::<u64>());
        let start_time = std::time::Instant::now();
        let mut next_ts = Instant::now();
        let mut last_load_check_sec: i64 = -1; // Start at -1 to log at second 0
        let initial_load = load_config.get_load_at(0);
        let mut current_arrival = Self::arrival_for_load(initial_load / num_clients as u64);

        // Log initial load
        tracing::info!(
            "Client starting: initial load {} TPS total, {} TPS per client",
            initial_load,
            initial_load / num_clients as u64
        );

        for (counter, tx_with_timestamp) in transactions.into_iter().enumerate() {
            // Wait until the next interval before sending the next transaction
            while Instant::now() < next_ts {
                std::hint::spin_loop();
            }

            // Check if we need to update the load (every second)
            let elapsed_secs = start_time.elapsed().as_secs() as i64;
            if elapsed_secs != last_load_check_sec {
                last_load_check_sec = elapsed_secs;
                let target_load = load_config.get_load_at(elapsed_secs as u64);
                let load_per_client = target_load / num_clients as u64;
                current_arrival = Self::arrival_for_load(load_per_client);

                // Log every second for visibility
                tracing::info!(
                    "Dynamic load at {}s: {} TPS total, {} TPS per client",
                    elapsed_secs,
                    target_load,
                    load_per_client
                );

                // If load is 0, we should stop sending
                if target_load == 0 {
                    tracing::info!(
                        "Load dropped to 0 at {}s, stopping submission",
                        elapsed_secs
                    );
                    break;
                }
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
            next_ts += Duration::from_nanos(Self::gen_inter_arrival(&mut rng, current_arrival));
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
        // Log the load configuration being used
        if self.config.load_config.is_dynamic() {
            tracing::info!("Starting run with DYNAMIC load config...");
        } else {
            let initial_load = self.config.load_config.get_load_at(0).max(self.config.load);
            tracing::info!("Starting run at {} load (static)...", initial_load);
        }

        // split the transactions
        let split = self.split_transactions(transactions);

        // spawn for each client
        let mut handles = vec![];
        for (tx, tx_chunk) in senders.into_iter().zip(split.into_iter()) {
            let load_config = self.config.load_config.clone();
            let handle = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    sleep(Duration::from_secs(1)).await;
                    Self::submit_transactions(tx_chunk, tx, load_config, NUM_CLIENTS).await;
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
        config::{get_test_address, BenchmarkParameters},
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
}

/// The default metrics address.
pub fn default_metrics_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18600)
}
