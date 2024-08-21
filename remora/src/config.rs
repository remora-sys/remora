// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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

/// The default port for the primary.
pub fn default_primary_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18500)
}
/// The default port for the metrics server.
pub fn default_metrics_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18501)
}

/// Configuration to collocate pre-executors.
#[derive(Serialize, Deserialize, Clone)]
pub struct CollocatedPreExecutors {
    /// The number of pre-executors running on the same machine as the primary.
    pub primary: usize,
    /// The number of pre-executors running on each proxy machine.
    pub proxy: usize,
}

/// The configuration for the validator.
#[derive(Serialize, Deserialize, Clone)]
pub struct ValidatorConfig {
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

impl ValidatorConfig {
    /// Create a new validator configuration for tests.
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

impl Default for ValidatorConfig {
    fn default() -> Self {
        ValidatorConfig {
            collocated_pre_executors: default_validator_config::default_collocated_pre_executors(),
            consensus_delay_model: default_validator_config::default_consensus_delay_model(),
            consensus_parameters: default_validator_config::default_consensus_parameters(),
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
        }
    }
}

/// The configuration for the benchmark.
#[derive(Serialize, Deserialize, Clone)]
pub struct BenchmarkConfig {
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

impl BenchmarkConfig {
    /// Create a new benchmark configuration for tests.
    pub fn new_for_tests() -> Self {
        BenchmarkConfig {
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

impl Default for BenchmarkConfig {
    fn default() -> Self {
        BenchmarkConfig {
            load: default_benchmark_config::default_load(),
            duration: default_benchmark_config::default_duration(),
            workload: default_benchmark_config::default_workload(),
        }
    }
}

impl ImportExport for BenchmarkConfig {}
