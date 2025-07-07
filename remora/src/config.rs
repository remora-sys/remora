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
    primary::{
        mock_consensus::{models::FixedDelay, MockConsensusParameters},
        shared_obj_txn_forwarder::PreConsensusSchedulingPolicy,
    },
    proxy::core::ProxyId,
};

/// Defines different load balancing policies for distributing transactions.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum LoadBalancingPolicy {
    /// Simple round-robin distribution
    RoundRobin,
    /// Send to proxy that already has most of the required states
    Zeus,
    /// Dedicated: one proxy for stateless, another for stateful
    Dedicated,
    /// Combined: one proxy for both stateless and stateful
    Combined,
    /// Two-tier: separate policies for stateless and stateful
    TwoTier,
    /// LocalityLoad: Choose proxy based on 50% locality and 50% load score
    LocalityLoad,
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
    pub load_balancing_policy: PreConsensusSchedulingPolicy,
    /// The proxy mode (separation or no separation)
    #[serde(default = "default_validator_config::default_separation_mode")]
    pub separation_mode: SeparationMode,
}

impl ValidatorParameters {
    /// Create a new validator parameters for tests.
    pub fn new_for_tests() -> Self {
        Self::default()
    }
}

mod default_validator_config {
    use crate::config::SeparationMode;
    use crate::primary::{
        mock_consensus::{models::FixedDelay, MockConsensusParameters},
        shared_obj_txn_forwarder::PreConsensusSchedulingPolicy,
    };

    pub fn default_consensus_delay_model() -> FixedDelay {
        FixedDelay::default()
    }

    pub fn default_consensus_parameters() -> MockConsensusParameters {
        MockConsensusParameters::default()
    }

    pub fn default_load_balancing_policy() -> PreConsensusSchedulingPolicy {
        PreConsensusSchedulingPolicy::SDS
    }

    pub fn default_separation_mode() -> SeparationMode {
        SeparationMode::NoSeparation
    }
}

impl Default for ValidatorParameters {
    fn default() -> Self {
        ValidatorParameters {
            consensus_delay_model: default_validator_config::default_consensus_delay_model(),
            consensus_parameters: default_validator_config::default_consensus_parameters(),
            load_balancing_policy: default_validator_config::default_load_balancing_policy(),
            separation_mode: default_validator_config::default_separation_mode(),
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
pub enum SeparationMode {
    /// The proxy does not separate the stateful and stateless transactions.
    NoSeparation,
    /// The proxy separates the stateful and stateless transactions.
    ProxySeparation,
    /// The load balancer separates the stateful and stateless transactions
    /// and proxies also separate the stateful and stateless transactions.
    PrimarySeparation,
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

/// The configuration for the benchmark.
#[derive(Serialize, Deserialize, Clone)]
pub struct BenchmarkParameters {
    /// The load to generate in transactions per second.
    #[serde(default = "default_benchmark_config::default_load")]
    pub load: u64,
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
}

impl BenchmarkParameters {
    /// Create a new benchmark configuration for tests.
    pub fn new_for_tests() -> Self {
        BenchmarkParameters {
            load: 10,
            duration: Duration::from_secs(1),
            workload: WorkloadType::Transfers,
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
        }
    }

    /// Create a new benchmark configuration for contention tests using SharedObjects workload.
    pub fn new_for_contention_tests() -> Self {
        BenchmarkParameters {
            load: 10,
            duration: Duration::from_secs(1),
            workload: WorkloadType::SharedObjects {
                txs_per_counter: default_cont_level_for_shared_obj(),
            },
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
        }
    }

    /// Create a new benchmark configuration for contention tests using Ethereum workload.
    pub fn new_for_ethereum_tests() -> Self {
        BenchmarkParameters {
            load: 100,
            duration: Duration::from_secs(5),
            workload: WorkloadType::EthereumTransfers,
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
        }
    }

    /// Create a new benchmark configuration for fake txn tests.
    pub fn new_for_fake_tests() -> Self {
        BenchmarkParameters {
            load: 10,
            duration: Duration::from_secs(1),
            workload: WorkloadType::FakeZipfian {
                execution_duration: default_fake_execution_duration(),
                alpha: 0.0,
                number_of_inputs: 2,
            },
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
        }
    }

    /// Create a new benchmark configuration for fake txn tests.
    pub fn new_for_fake_contention_tests() -> Self {
        BenchmarkParameters {
            load: 10,
            duration: Duration::from_secs(1),
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            workload: WorkloadType::FakeZipfian {
                execution_duration: default_fake_execution_duration(),
                alpha: 0.5,
                number_of_inputs: 2,
            },
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
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
            load: default_benchmark_config::default_load(),
            duration: default_benchmark_config::default_duration(),
            workload: default_benchmark_config::default_workload(),
            verification_duration: default_benchmark_config::default_fake_verification_duration(),
            expected_stateful_duration:
                default_benchmark_config::default_fake_verification_duration(),
        }
    }
}

impl ImportExport for BenchmarkParameters {}
