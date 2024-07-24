// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, io, net::SocketAddr, path::Path, time::Duration};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::mock_consensus::{models::FixedDelay, MockConsensusParameters};

pub trait ImportExport: Serialize + DeserializeOwned {
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let content = fs::read_to_string(&path)?;
        let object =
            serde_yaml::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(object)
    }

    fn print<P: AsRef<Path>>(&self, path: P) -> Result<(), io::Error> {
        let content =
            serde_yaml::to_string(self).expect("Failed to serialize object to YAML string");
        fs::write(&path, content)
    }
}

#[derive(Serialize, Deserialize)]
pub enum WorkloadType {
    Transfers,
}

#[derive(Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub load: u64,
    pub duration: Duration,
    pub workload: WorkloadType,
}

impl BenchmarkConfig {
    pub fn new_for_tests() -> Self {
        BenchmarkConfig {
            load: 10,
            duration: Duration::from_secs(1),
            workload: WorkloadType::Transfers,
        }
    }
}

impl ImportExport for BenchmarkConfig {}

#[derive(Serialize, Deserialize, Clone)]
pub struct ValidatorConfig {
    pub address: SocketAddr,
    pub metrics_address: SocketAddr,
    pub num_proxies: usize,
    pub consensus_delay_model: FixedDelay,
    pub consensus_parameters: MockConsensusParameters,
}

impl ImportExport for ValidatorConfig {}
