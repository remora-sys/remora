// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use futures::{stream, StreamExt};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    config::ProxyMode,
    executor::api::{Executor, PrimaryToProxyMessage, RemoraTransaction},
    proxy::core::ProxyId,
};

/// Processor for transactions that only involve owned objects.
/// Used only for load balancing policy selection.
pub(crate) struct OwnedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) index: usize,
    pub(crate) proxy_mode: ProxyMode,
}

impl<E> OwnedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) async fn process_owned_txns(
        &mut self,
        mut owned_txn_receiver: Receiver<(u64, Vec<RemoraTransaction<E>>)>,
    ) {
        while let Some((consensus_index, owned_txns)) = owned_txn_receiver.recv().await {
            self.forward_owned_txns_in_parallel(consensus_index, owned_txns)
                .await;
        }
    }

    /// Forward owned-object transactions in parallel with true concurrency
    pub(crate) async fn forward_owned_txns_in_parallel(
        &mut self,
        consensus_index: u64,
        transactions: Vec<RemoraTransaction<E>>,
    ) {
        let proxy_count = self.proxy_connections.len();
        if proxy_count == 0 {
            tracing::warn!("No proxies available for transactions");
            return;
        }

        let start = self.index;
        let proxy_mode = self.proxy_mode.clone();

        self.index = (start + transactions.len()) % proxy_count;

        // prepare a set of futures
        let mut tasks = stream::FuturesUnordered::new();

        for (i, tx) in transactions.into_iter().enumerate() {
            let idx = (start + i) % proxy_count;
            let tx = Arc::new(tx);
            let proxy_connections = self.proxy_connections.clone();
            let fut = async move {
                if let Some(proxy_conn) = proxy_connections.get(&idx) {
                    if proxy_mode == ProxyMode::Separation {
                        let msg1 = PrimaryToProxyMessage::StatelessTxn(consensus_index, tx.clone());
                        let msg2 = PrimaryToProxyMessage::Txn(
                            consensus_index,
                            tx.clone(),
                            idx,
                            BTreeMap::new(),
                        );

                        if proxy_conn.send(msg1).await.is_err() {
                            tracing::warn!("Failed to send stateless txn to proxy {}", idx);
                        }
                        if proxy_conn.send(msg2).await.is_err() {
                            tracing::warn!("Failed to send stateful txn to proxy {}", idx);
                        }
                    } else {
                        let msg = PrimaryToProxyMessage::CombinedTxn(
                            consensus_index,
                            tx.clone(),
                            idx,
                            BTreeMap::new(),
                        );
                        if proxy_conn.send(msg).await.is_err() {
                            tracing::warn!("Failed to send combined txn to proxy {}", idx);
                        }
                    }
                }
            };

            // push it into our unordered set
            tasks.push(fut);
        }

        // drive all of them to completion, in parallel
        while (tasks.next().await).is_some() {}
    }

    #[cfg(feature = "benchmark")]
    pub async fn benchmark_parallel_forwarding(&mut self, transactions: Vec<RemoraTransaction<E>>) {
        self.forward_owned_txns_in_parallel(transactions.clone())
            .await;
    }

    #[cfg(feature = "benchmark")]
    pub async fn create_benchmark_transactions(&self, count: usize) -> Vec<RemoraTransaction<E>> {
        use crate::config::{BenchmarkParameters, WorkloadType};
        use std::time::Duration;
        let config = BenchmarkParameters {
            load: count as u64,
            duration: Duration::from_secs(1),
            workload: WorkloadType::Transfers,
            verification_duration: Duration::from_secs(0),
        };
        let transactions = E::generate_transactions(&config, None).await;
        transactions
            .into_iter()
            .take(count)
            .map(|tx| RemoraTransaction::<E>::new_for_tests(tx))
            .collect()
    }
}
