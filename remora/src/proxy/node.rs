// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

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
    proxy_handle: JoinHandle<()>,
    /// The  metrics for the proxy
    _metrics: Arc<Metrics>,
}

impl ProxyNode {
    pub async fn start(
        proxy_id: ProxyId,
        executor: SuiExecutor,
        config: &ValidatorConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        // Boot the local proxies. Additional proxies can still remotely connect.
        let (tx_transactions, rx_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let store = executor.create_in_memory_store();
        let proxy_handle = ProxyCore::new(
            proxy_id,
            executor.clone(),
            store,
            rx_transactions,
            tx_proxy_results,
            metrics.clone(),
        )
        .spawn();

        let address = config.validator_address;
        let _handle = NetworkClient::new(address, tx_transactions, rx_proxy_results).spawn();

        Self {
            proxy_handle,
            _metrics: metrics,
        }
    }

    /// Collect the results from the validator.
    pub async fn await_completion(self) {
        self.proxy_handle.await.unwrap();
    }
}
