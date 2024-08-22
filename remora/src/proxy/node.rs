// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use futures::future::join_all;
use tokio::{sync::mpsc, task::JoinHandle};

use super::core::{ProxyCore, ProxyId};
use crate::{
    config::ValidatorConfig,
    executor::sui::SuiExecutor,
    metrics::Metrics,
    networking::client::NetworkClient,
};

/// Default channel size for communication between components.
const DEFAULT_CHANNEL_SIZE: usize = 1000;

pub struct ProxyNode {
    /// The handles for all components.
    handles: Vec<JoinHandle<()>>,
    /// The  metrics for the proxy
    _metrics: Arc<Metrics>,
}

impl ProxyNode {
    pub fn start(
        proxy_id: ProxyId,
        executor: SuiExecutor,
        config: &ValidatorConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        let mut handles = Vec::new();
        for i in 0..config.validator_parameters.collocated_pre_executors.proxy {
            let id = format!("{proxy_id}-{i}");
            let (tx_transactions, rx_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

            let store = executor.create_in_memory_store();
            let proxy_handle = ProxyCore::new(
                id,
                executor.clone(),
                store,
                rx_transactions,
                tx_proxy_results,
                metrics.clone(),
            )
            .spawn();
            handles.push(proxy_handle);

            // TODO: Add the handle of the network client to the handles list.
            let _handle = NetworkClient::new(
                config.proxy_server_address,
                tx_transactions,
                rx_proxy_results,
            )
            .spawn();
        }

        Self {
            handles,
            _metrics: metrics,
        }
    }

    /// Collect the results from the validator.
    pub async fn await_completion(self) {
        join_all(self.handles).await;
    }
}
