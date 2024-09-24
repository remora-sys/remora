// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    fs, io,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::primary::mock_consensus::{models::FixedDelay, MockConsensusParameters};

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

/// Configuration to collocate pre-executors.
#[derive(Serialize, Deserialize, Clone)]
pub struct CollocatedPreExecutors {
    /// The number of pre-executors running on the same machine as the primary.
    pub primary: usize,
    /// The number of pre-executors running on each proxy machine.
    pub proxy: usize,
}

/// The parameters for the validator.
#[derive(Serialize, Deserialize, Clone)]
pub struct ValidatorParameters {
    /// The number of collocated pre-executors to use. That is, the number of pre-executor running on
    /// the same machine as the primary and on the same machine as each proxy.
    #[serde(default = "default_validator_config::default_collocated_pre_executors")]
    pub collocated_pre_executors: CollocatedPreExecutors,
    /// The consensus delay model.
    #[serde(default = "default_validator_config::default_consensus_delay_model")]
    pub consensus_delay_model: FixedDelay,
    /// The consensus parameters.
    #[serde(default = "default_validator_config::default_consensus_parameters")]
    pub consensus_parameters: MockConsensusParameters,
}

impl ValidatorParameters {
    /// Create a new validator parameters for tests.
    pub fn new_for_tests() -> Self {
        Self::default()
    }
}

mod default_validator_config {
    use super::CollocatedPreExecutors;
    use crate::primary::mock_consensus::{models::FixedDelay, MockConsensusParameters};

    pub fn default_collocated_pre_executors() -> CollocatedPreExecutors {
        CollocatedPreExecutors {
            primary: 1,
            proxy: 0,
        }
    }

    pub fn default_consensus_delay_model() -> FixedDelay {
        FixedDelay::default()
    }

    pub fn default_consensus_parameters() -> MockConsensusParameters {
        MockConsensusParameters::default()
    }
}

impl Default for ValidatorParameters {
    fn default() -> Self {
        ValidatorParameters {
            collocated_pre_executors: default_validator_config::default_collocated_pre_executors(),
            consensus_delay_model: default_validator_config::default_consensus_delay_model(),
            consensus_parameters: default_validator_config::default_consensus_parameters(),
        }
    }
}

impl ImportExport for ValidatorParameters {}

/// The configuration for the validator, containing network addresses. This structure
/// is designed to be used by the orchestrator to configure the validator.
#[derive(Serialize, Deserialize)]
pub struct ValidatorConfig {
    /// The address of the primary server where the proxies connect.
    pub proxy_server_address: SocketAddr,
    /// The address of the primary server where the clients connect.
    pub client_server_address: SocketAddr,
    /// The address of the primary server where validator exposes metrics.
    pub metrics_address: SocketAddr,
    /// The parameters for the validator.
    pub validator_parameters: ValidatorParameters,
    /// The execution mode of proxy.
    pub parallel_proxy: bool,
}

impl ValidatorConfig {
    /// Create a new validator configuration for tests.
    pub fn new_for_tests() -> Self {
        ValidatorConfig {
            proxy_server_address: get_test_address(),
            client_server_address: get_test_address(),
            metrics_address: get_test_address(),
            validator_parameters: ValidatorParameters::new_for_tests(),
            parallel_proxy: true,
        }
    }
}

impl ImportExport for ValidatorConfig {}

/// The workload type to generate.
#[derive(Serialize, Deserialize, Clone)]
pub enum WorkloadType {
    Transfers,
    SharedObjects,
}

impl Debug for WorkloadType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadType::Transfers => write!(f, "transfers"),
            WorkloadType::SharedObjects => write!(f, "shared objects"),
        }
    }
}

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
}

impl BenchmarkParameters {
    /// Create a new benchmark configuration for tests.
    pub fn new_for_tests() -> Self {
        BenchmarkParameters {
            load: 10,
            duration: Duration::from_secs(1),
            workload: WorkloadType::Transfers,
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
}

impl Default for BenchmarkParameters {
    fn default() -> Self {
        BenchmarkParameters {
            load: default_benchmark_config::default_load(),
            duration: default_benchmark_config::default_duration(),
            workload: default_benchmark_config::default_workload(),
        }
    }
}

impl ImportExport for BenchmarkParameters {}
