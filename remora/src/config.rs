// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    error::Error,
    fmt::{Debug, Display},
    fs, io,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    client::hermes_schedule::AssignmentMode,
    primary::mock_consensus::{models::FixedDelay, MockConsensusParameters},
    proxy::core::ProxyId,
};

/// Defines different load balancing policies for distributing transactions.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum LoadBalancingPolicy {
    /// Simple round-robin distribution
    RoundRobin,
    /// Random selection of proxy
    Random,
    /// Send to proxy that already has most of the required states
    Zeus,
    /// Hermes schedule
    Hermes,
}

/// Default channel size for communication between components.
pub const DEFAULT_CHANNEL_SIZE: usize = 100_000;

/// Return a socket address on the local machine with a random port.
/// This is useful for tests.
pub fn get_test_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to a random port")
        .local_addr()
        .expect("Failed to get local address")
}

/// A trait for importing and exporting configuration objects.
pub trait ImportExport: Serialize + DeserializeOwned {
    /// Load the configuration object from a file in YAML format.
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let content = fs::read_to_string(&path)?;
        let object =
            serde_yaml::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(object)
    }

    /// Print the configuration object to a file in YAML format.
    fn print<P: AsRef<Path>>(&self, path: P) -> Result<(), io::Error> {
        let content = serde_yaml::to_string(self).expect("Failed to serialize to YAML string");
        fs::write(&path, content)
    }
}

/// The default address of the primary server where the proxies connect.
pub fn default_primary_address_for_proxies() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18500)
}
/// The default address of the primary server where the clients connect.
pub fn default_primary_address_for_clients() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18501)
}
/// The default address for the metrics server.
pub fn default_metrics_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18502)
}

/// The parameters for the validator.
#[derive(Serialize, Deserialize, Clone)]
pub struct ValidatorParameters {
    /// The consensus delay model.
    #[serde(default = "default_validator_config::default_consensus_delay_model")]
    pub consensus_delay_model: FixedDelay,
    /// The consensus parameters.
    #[serde(default = "default_validator_config::default_consensus_parameters")]
    pub consensus_parameters: MockConsensusParameters,
    /// The load balancing policy.
    #[serde(default = "default_validator_config::default_load_balancing_policy")]
    pub load_balancing_policy: LoadBalancingPolicy,
    /// The proxy mode (separation or no separation)
    #[serde(default = "default_validator_config::default_proxy_mode")]
    pub proxy_mode: ProxyMode,
}

impl ValidatorParameters {
    /// Create a new validator parameters for tests.
    pub fn new_for_tests() -> Self {
        Self::default()
    }
}

mod default_validator_config {
    use crate::config::{LoadBalancingPolicy, ProxyMode};
    use crate::primary::mock_consensus::{models::FixedDelay, MockConsensusParameters};

    pub fn default_consensus_delay_model() -> FixedDelay {
        FixedDelay::default()
    }

    pub fn default_consensus_parameters() -> MockConsensusParameters {
        MockConsensusParameters::default()
    }

    pub fn default_load_balancing_policy() -> LoadBalancingPolicy {
        LoadBalancingPolicy::RoundRobin
    }

    pub fn default_proxy_mode() -> ProxyMode {
        ProxyMode::Separation
    }
}

impl Default for ValidatorParameters {
    fn default() -> Self {
        ValidatorParameters {
            consensus_delay_model: default_validator_config::default_consensus_delay_model(),
            consensus_parameters: default_validator_config::default_consensus_parameters(),
            load_balancing_policy: default_validator_config::default_load_balancing_policy(),
            proxy_mode: default_validator_config::default_proxy_mode(),
        }
    }
}

impl ImportExport for ValidatorParameters {}

/// Represents a proxy's fixed configuration.
#[derive(Serialize, Deserialize, Clone)]
pub struct ProxyInfo {
    /// Unique identifier for the proxy.
    pub proxy_id: ProxyId,
    /// The listening address for the proxy's P2P network.
    pub listen_proxy_address: SocketAddr,
    /// The listening address for the primary connection.
    pub listen_primary_address: SocketAddr,
    /// The address for metrics
    pub metrics_address: SocketAddr,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ProxyMode {
    /// The proxy separates the stateful and stateless transactions.
    Separation,
    /// The proxy does not separate the stateful and stateless transactions.
    NoSeparation,
}

/// The configuration for the validator, containing network addresses.
/// Note: This now includes a vector of proxy configurations.
#[derive(Serialize, Deserialize)]
pub struct ValidatorConfig {
    /// The address of the primary server where the proxies connect.
    pub proxy_server_address: SocketAddr,
    /// The address of the primary server where the clients connect.
    pub client_server_address: SocketAddr,
    /// Fixed configuration for all proxy instances.
    pub proxies: Vec<ProxyInfo>,
    /// The address of the primary server where validator exposes metrics.
    pub metrics_address: SocketAddr,
    /// The parameters for the validator.
    pub validator_parameters: ValidatorParameters,
}

impl ValidatorConfig {
    /// Create a new validator configuration for tests.
    pub fn new_for_tests() -> Self {
        // Example: setting up 3 proxies with incremental port numbers.
        let base_addr = get_test_address(); // assuming get_test_address() returns e.g., "127.0.0.1:8000"
        let proxies = vec![
            ProxyInfo {
                proxy_id: 0,
                // Derive a unique port number from the base address.
                listen_proxy_address: SocketAddr::new(base_addr.ip(), base_addr.port() + 1),
                listen_primary_address: SocketAddr::new(base_addr.ip(), base_addr.port() + 2),
                metrics_address: SocketAddr::new(base_addr.ip(), base_addr.port() + 3),
            },
            ProxyInfo {
                proxy_id: 1,
                listen_proxy_address: SocketAddr::new(base_addr.ip(), base_addr.port() + 4),
                listen_primary_address: SocketAddr::new(base_addr.ip(), base_addr.port() + 5),
                metrics_address: SocketAddr::new(base_addr.ip(), base_addr.port() + 6),
            },
        ];

        ValidatorConfig {
            proxy_server_address: get_test_address(),
            client_server_address: get_test_address(),
            proxies,
            metrics_address: get_test_address(),
            validator_parameters: ValidatorParameters::new_for_tests(),
        }
    }
}

impl ImportExport for ValidatorConfig {}

/// The workload type to generate.
#[derive(Serialize, Deserialize, Clone)]
pub enum WorkloadType {
    Transfers,
    SharedObjects {
        #[serde(default = "default_cont_level_for_shared_obj")]
        txs_per_counter: usize,
    },
    SolanaTransactions,
    EthereumTransfers,
    EthereumNftMint,
    UniswapNormal,
    UniswapPeak,
    Zipfian {
        #[serde(default = "default_zipfian_alpha")]
        alpha: f64,
        #[serde(default = "default_number_of_inputs")]
        number_of_inputs: usize,
    },
    FakeSolanaTransactions {
        #[serde(default = "default_fake_execution_duration")]
        execution_duration: Duration,
    },
    FakeEthereumTransfers {
        #[serde(default = "default_fake_execution_duration")]
        execution_duration: Duration,
    },
    FakeEthereumNftMint {
        #[serde(default = "default_fake_execution_duration")]
        execution_duration: Duration,
    },
    FakeUniswapNormal {
        #[serde(default = "default_fake_execution_duration")]
        execution_duration: Duration,
    },
    FakeUniswapPeak {
        #[serde(default = "default_fake_execution_duration")]
        execution_duration: Duration,
    },
    FakeZipfian {
        #[serde(default = "default_fake_execution_duration")]
        execution_duration: Duration,
        #[serde(default = "default_zipfian_alpha")]
        alpha: f64,
        #[serde(default = "default_number_of_inputs")]
        number_of_inputs: usize,
    },
}

impl WorkloadType {
    pub fn is_fake(&self) -> bool {
        match self {
            WorkloadType::FakeSolanaTransactions { .. }
            | WorkloadType::FakeEthereumTransfers { .. }
            | WorkloadType::FakeEthereumNftMint { .. }
            | WorkloadType::FakeUniswapNormal { .. }
            | WorkloadType::FakeUniswapPeak { .. }
            | WorkloadType::FakeZipfian { .. } => true,
            _ => false,
        }
    }
}

fn default_cont_level_for_shared_obj() -> usize {
    2
}

pub fn default_fake_execution_duration() -> Duration {
    Duration::from_micros(500)
}

pub fn default_zipfian_alpha() -> f64 {
    0.5
}

pub fn default_number_of_inputs() -> usize {
    2
}

impl Debug for WorkloadType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadType::Transfers => write!(f, "transfers"),
            WorkloadType::SharedObjects { .. } => write!(f, "shared objects"),
            WorkloadType::SolanaTransactions => write!(f, "Solana transactions"),
            WorkloadType::EthereumTransfers => write!(f, "Ethereum transfers"),
            WorkloadType::EthereumNftMint => write!(f, "Ethereum NFT mint"),
            WorkloadType::UniswapNormal => write!(f, "Uniswap normal"),
            WorkloadType::UniswapPeak => write!(f, "Uniswap peak"),
            WorkloadType::Zipfian { .. } => write!(f, "Zipfian"),
            WorkloadType::FakeSolanaTransactions { .. } => write!(f, "Fake Solana transactions"),
            WorkloadType::FakeEthereumTransfers { .. } => write!(f, "Fake Ethereum transfers"),
            WorkloadType::FakeEthereumNftMint { .. } => write!(f, "Fake Ethereum NFT mint"),
            WorkloadType::FakeUniswapNormal { .. } => write!(f, "Fake Uniswap normal"),
            WorkloadType::FakeUniswapPeak { .. } => write!(f, "Fake Uniswap peak"),
            WorkloadType::FakeZipfian { .. } => write!(f, "Fake Zipfian"),
        }
    }
}

#[derive(Debug)]
pub enum ConfigErrorType {
    InvalidWorkload,
}

impl Display for ConfigErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigErrorType::InvalidWorkload => write!(f, "Invalid workload type provided."),
        }
    }
}

impl Error for ConfigErrorType {}

/// Represents a load interval with timing and target load rate.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LoadInterval {
    /// Start time for this interval in seconds from benchmark start
    pub start_time_secs: u64,
    /// End time for this interval in seconds from benchmark start
    pub end_time_secs: u64,
    /// Target load rate in transactions per second for this interval
    pub target_load: u64,
}

/// Configuration for dynamic load rates with time-based intervals.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DynamicLoadConfig {
    /// Total duration of the benchmark in seconds
    pub total_duration_secs: u64,
    /// List of load intervals defining the load pattern
    pub intervals: Vec<LoadInterval>,
}

impl DynamicLoadConfig {
    /// Create a new dynamic load configuration from the user's format.
    /// Example input format:
    /// duration: 30
    /// intervals:
    ///   0: 10000
    ///   10: 20000
    ///   20: 10000
    pub fn from_intervals(duration_secs: u64, interval_map: &[(u64, u64)]) -> Self {
        let mut intervals = Vec::new();
        let mut sorted_intervals: Vec<_> = interval_map.iter().collect();
        sorted_intervals.sort_by_key(|(start, _)| *start);

        for i in 0..sorted_intervals.len() {
            let (start_time, target_load) = sorted_intervals[i];
            let end_time = if i + 1 < sorted_intervals.len() {
                sorted_intervals[i + 1].0
            } else {
                duration_secs
            };

            intervals.push(LoadInterval {
                start_time_secs: *start_time,
                end_time_secs: end_time,
                target_load: *target_load,
            });
        }

        DynamicLoadConfig {
            total_duration_secs: duration_secs,
            intervals,
        }
    }

    /// Get the target load rate for a given time offset from benchmark start.
    pub fn get_load_at_time(&self, elapsed_secs: u64) -> Option<u64> {
        for interval in &self.intervals {
            if elapsed_secs >= interval.start_time_secs && elapsed_secs < interval.end_time_secs {
                return Some(interval.target_load);
            }
        }
        None
    }

    /// Validate that the configuration is well-formed.
    pub fn validate(&self) -> Result<(), String> {
        if self.intervals.is_empty() {
            return Err("Dynamic load config must have at least one interval".to_string());
        }

        let mut prev_end = 0;
        for interval in &self.intervals {
            if interval.start_time_secs != prev_end {
                return Err(format!(
                    "Gap or overlap in intervals: expected start {} but got {}",
                    prev_end, interval.start_time_secs
                ));
            }
            if interval.end_time_secs <= interval.start_time_secs {
                return Err(format!(
                    "Invalid interval: end time {} <= start time {}",
                    interval.end_time_secs, interval.start_time_secs
                ));
            }
            prev_end = interval.end_time_secs;
        }

        if prev_end != self.total_duration_secs {
            return Err(format!(
                "Intervals don't cover full duration: {} vs {}",
                prev_end, self.total_duration_secs
            ));
        }

        Ok(())
    }
}

/// Load configuration that supports both constant and dynamic load rates.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum LoadConfig {
    /// Constant load rate throughout the benchmark
    Constant(u64),
    /// Dynamic load rates with time-based intervals
    Dynamic(DynamicLoadConfig),
}

impl LoadConfig {
    /// Get the load rate for a given time offset, or constant load if not dynamic.
    pub fn get_load_at_time(&self, elapsed_secs: u64) -> u64 {
        match self {
            LoadConfig::Constant(load) => *load,
            LoadConfig::Dynamic(config) => config.get_load_at_time(elapsed_secs).unwrap_or(0),
        }
    }

    /// Get the initial load rate.
    pub fn initial_load(&self) -> u64 {
        match self {
            LoadConfig::Constant(load) => *load,
            LoadConfig::Dynamic(config) => {
                config.intervals.first().map(|i| i.target_load).unwrap_or(0)
            }
        }
    }

    /// Check if this is a dynamic load configuration.
    pub fn is_dynamic(&self) -> bool {
        matches!(self, LoadConfig::Dynamic(_))
    }

    /// Get total duration for this load configuration.
    pub fn get_duration(&self) -> Option<Duration> {
        match self {
            LoadConfig::Constant(_) => None,
            LoadConfig::Dynamic(config) => Some(Duration::from_secs(config.total_duration_secs)),
        }
    }

    /// Calculate the total number of transactions needed for the entire load profile.
    /// For constant loads, uses load * duration. For dynamic loads, sums up all intervals.
    pub fn calculate_total_transactions(&self, fallback_duration: Duration) -> u64 {
        match self {
            LoadConfig::Constant(load) => load * fallback_duration.as_secs(),
            LoadConfig::Dynamic(config) => config
                .intervals
                .iter()
                .map(|interval| {
                    let duration_secs = interval.end_time_secs - interval.start_time_secs;
                    interval.target_load * duration_secs
                })
                .sum(),
        }
    }
}

impl Default for LoadConfig {
    fn default() -> Self {
        LoadConfig::Constant(default_benchmark_config::default_load())
    }
}

/// The configuration for the benchmark.
#[derive(Serialize, Deserialize, Clone)]
pub struct BenchmarkParameters {
    /// The load to generate - either constant or dynamic.
    #[serde(default = "default_load_config")]
    pub load_config: LoadConfig,
    /// The load to generate in transactions per second (deprecated, use load_config instead).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load: Option<u64>,
    /// The duration to run the benchmark for.
    #[serde(default = "default_benchmark_config::default_duration")]
    pub duration: Duration,
    /// The workload to generate.
    #[serde(default = "default_benchmark_config::default_workload")]
    pub workload: WorkloadType,
    /// The verification duration.
    #[serde(default = "default_benchmark_config::default_fake_verification_duration")]
    pub verification_duration: Duration,
    /// The expected stateful duration, used for LB.
    #[serde(default = "default_benchmark_config::default_fake_verification_duration")]
    pub expected_stateful_duration: Duration,
    /// The assignment mode for transaction scheduling.
    #[serde(default)]
    pub assignment_mode: AssignmentMode,
}

fn default_load_config() -> LoadConfig {
    LoadConfig::Constant(default_benchmark_config::default_load())
}

impl BenchmarkParameters {
    /// Get the effective load configuration, handling backward compatibility.
    pub fn effective_load_config(&self) -> LoadConfig {
        if let Some(legacy_load) = self.load {
            LoadConfig::Constant(legacy_load)
        } else {
            self.load_config.clone()
        }
    }

    /// Get the initial load rate for backward compatibility.
    pub fn get_initial_load(&self) -> u64 {
        self.effective_load_config().initial_load()
    }

    /// Get the effective duration, considering dynamic load configuration.
    pub fn effective_duration(&self) -> Duration {
        let load_config = self.effective_load_config();
        load_config.get_duration().unwrap_or(self.duration)
    }

    /// Calculate the total number of transactions needed for the complete load profile.
    /// This properly handles dynamic loads by calculating transactions for each interval.
    pub fn calculate_total_transactions(&self) -> u64 {
        let load_config = self.effective_load_config();
        load_config.calculate_total_transactions(self.duration)
    }

    /// Create a new benchmark configuration for tests.
    pub fn new_for_tests() -> Self {
        BenchmarkParameters {
            load_config: LoadConfig::Constant(10),
            load: Some(10),
            duration: Duration::from_secs(1),
            workload: WorkloadType::Transfers,
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
            assignment_mode: AssignmentMode::default(),
        }
    }

    /// Create a new benchmark configuration for contention tests using SharedObjects workload.
    pub fn new_for_contention_tests() -> Self {
        BenchmarkParameters {
            load_config: LoadConfig::Constant(10),
            load: Some(10),
            duration: Duration::from_secs(1),
            workload: WorkloadType::SharedObjects {
                txs_per_counter: default_cont_level_for_shared_obj(),
            },
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
            assignment_mode: AssignmentMode::default(),
        }
    }

    /// Create a new benchmark configuration for contention tests using Ethereum workload.
    pub fn new_for_ethereum_tests() -> Self {
        BenchmarkParameters {
            load_config: LoadConfig::Constant(100),
            load: Some(100),
            duration: Duration::from_secs(5),
            workload: WorkloadType::EthereumTransfers,
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
            assignment_mode: AssignmentMode::default(),
        }
    }

    /// Create a new benchmark configuration for fake txn tests.
    pub fn new_for_fake_tests() -> Self {
        BenchmarkParameters {
            load_config: LoadConfig::Constant(10),
            load: Some(10),
            duration: Duration::from_secs(1),
            workload: WorkloadType::FakeZipfian {
                execution_duration: default_fake_execution_duration(),
                alpha: 0.0,
                number_of_inputs: 2,
            },
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
            assignment_mode: AssignmentMode::default(),
        }
    }

    /// Create a new benchmark configuration for fake txn tests.
    pub fn new_for_fake_contention_tests() -> Self {
        BenchmarkParameters {
            load_config: LoadConfig::Constant(10),
            load: Some(10),
            duration: Duration::from_secs(1),
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            workload: WorkloadType::FakeZipfian {
                execution_duration: default_fake_execution_duration(),
                alpha: 0.5,
                number_of_inputs: 2,
            },
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
            assignment_mode: AssignmentMode::default(),
        }
    }
}

mod default_benchmark_config {
    use std::time::Duration;

    use super::WorkloadType;

    pub fn default_load() -> u64 {
        10_000
    }

    pub fn default_duration() -> Duration {
        Duration::from_secs(30)
    }

    pub fn default_workload() -> WorkloadType {
        WorkloadType::Transfers
    }

    pub fn default_fake_verification_duration() -> Duration {
        Duration::from_micros(2000)
    }
}

impl Default for BenchmarkParameters {
    fn default() -> Self {
        BenchmarkParameters {
            load_config: LoadConfig::Constant(default_benchmark_config::default_load()),
            load: Some(default_benchmark_config::default_load()),
            duration: default_benchmark_config::default_duration(),
            workload: default_benchmark_config::default_workload(),
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
            assignment_mode: AssignmentMode::default(),
        }
    }
}

impl ImportExport for BenchmarkParameters {}
