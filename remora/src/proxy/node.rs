// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, marker::PhantomData, sync::Arc};
use serde::{
    de::DeserializeOwned,
    Serialize,
};
use tokio::{sync::mpsc, task::JoinHandle};

use super::core::{ProxyCore, ProxyId, ProxyMode};
use crate::{
    config::{ValidatorConfig, DEFAULT_CHANNEL_SIZE}, error::NodeResult, executor::api::Executor, metrics::Metrics,
    networking::client::NetworkClient,
};

pub struct ProxyNode<E: Executor> {
    pub phantom_data: PhantomData<E>,
    /// The handles for the core components.
    core_handles: Vec<std::thread::JoinHandle<NodeResult<()>>>,
    /// The handle for the network client.
    _network_handles: Vec<JoinHandle<io::Result<()>>>,
    /// The  metrics for the proxy
    _metrics: Arc<Metrics>,
}

impl<E: Executor + Send + Sync + 'static> ProxyNode<E> {
    pub async fn start(
        proxy_id: ProxyId,
        executor: E,
        config: &ValidatorConfig,
        metrics: Arc<Metrics>,
    ) -> Self where <E as Executor>::Store: Sync + Send,
        <E as Executor>::Transaction: Send + Sync + 'static,
        <E as Executor>::ExecutionContext: Send + Sync,
        <E as Executor>::ExecutionResults: Send + Sync + Serialize + DeserializeOwned,
    {
        let mut core_handles = Vec::new();
        let mut network_handles = Vec::new();
        let mode = match config.parallel_proxy {
            false => ProxyMode::SingleThreaded,
            true => ProxyMode::MultiThreaded,
        };

        for i in 0..config.validator_parameters.collocated_pre_executors.proxy {
            let id = format!("{proxy_id}-{i}");
            let (tx_transactions, rx_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

            let store = Arc::new(executor.init_store());
            let core_handle = ProxyCore::new(
                id,
                executor.clone(),
                mode,
                store,
                rx_transactions,
                tx_proxy_results,
                metrics.clone(),
            )
            .spawn_with_threads();
            core_handles.push(core_handle);

            // communication for distributed transactions from the load balancer in the primary
            let network_handle = NetworkClient::new(
                config.proxy_server_address,
                tx_transactions,
                rx_proxy_results,
            )
            .spawn();
            network_handles.push(network_handle);
        }

        Self {
            phantom_data: PhantomData,
            core_handles,
            _network_handles: network_handles,
            _metrics: metrics,
        }
    }

    /// Collect the results from the validator.
    pub fn await_completion(self) {
        for handle in self.core_handles {
            match handle.join() {
                Ok(_) => println!("Thread completed successfully!"),
                Err(e) => println!("Thread panicked: {:?}", e),
            }
        }
    }
}
