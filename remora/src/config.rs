// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    io,
    net::{SocketAddr, TcpListener},
    path::Path,
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::mock_consensus::{models::FixedDelay, MockConsensusParameters};

/// Return a socket address on the local machine with a random port.
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
        let content =
            serde_yaml::to_string(self).expect("Failed to serialize object to YAML string");
        fs::write(&path, content)
    }
}

/// The configuration for the validator.
#[derive(Serialize, Deserialize, Clone)]
pub struct ValidatorConfig {
    /// The address to bind the validator to.
    #[serde(default = "default_validator_config::default_validator_address")]
    pub validator_address: SocketAddr,
    /// The address to expose metrics on.
    #[serde(default = "default_validator_config::default_metrics_address")]
    pub metrics_address: SocketAddr,
    /// The number of proxies to use.
    #[serde(default = "default_validator_config::default_num_proxies")]
    pub num_proxies: usize,
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
        ValidatorConfig {
            validator_address: get_test_address(),
            metrics_address: get_test_address(),
            ..Default::default()
        }
    }
}

mod default_validator_config {
    use std::net::SocketAddr;

    use crate::mock_consensus::{models::FixedDelay, MockConsensusParameters};

    pub fn default_validator_address() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 18500))
    }

    pub fn default_metrics_address() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 18501))
    }

    pub fn default_num_proxies() -> usize {
        1
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
            validator_address: default_validator_config::default_validator_address(),
            metrics_address: default_validator_config::default_metrics_address(),
            num_proxies: default_validator_config::default_num_proxies(),
            consensus_delay_model: default_validator_config::default_consensus_delay_model(),
            consensus_parameters: default_validator_config::default_consensus_parameters(),
        }
    }
}

impl ImportExport for ValidatorConfig {}

/// The workload type to generate.
#[derive(Serialize, Deserialize)]
pub enum WorkloadType {
    Transfers,
}

/// The configuration for the benchmark.
#[derive(Serialize, Deserialize)]
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
