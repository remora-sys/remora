// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use futures::{stream, StreamExt};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    checkpoint::EpochId,
    config::ProxyMode,
    executor::api::{Executor, PrimaryToProxyMessage, RemoraTransaction},
    primary::pause_barrier::PauseBarrier,
    proxy::core::ProxyId,
};

/// Processor for transactions that only involve owned objects.
/// Used only for load balancing policy selection.
pub(crate) struct OwnedObjTxnForwarder<E>
where
    E: Executor,
    E::Transaction: Send + Sync + 'static,
{
    pub(crate) active_nodes: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    pub(crate) index: usize,
    pub(crate) proxy_mode: ProxyMode,
    /// Proxies that are in retirement; routing is gated by retirement epoch.
    /// The value is the retirement epoch used to decide per-epoch eligibility.
    pub(crate) retiring_proxies: Arc<DashMap<ProxyId, EpochId>>,
    /// Barrier to pause this worker during recovery.
    pub(crate) pause_barrier: Arc<PauseBarrier>,
}

impl<E> OwnedObjTxnForwarder<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    E::Transaction: Send + Sync + 'static,
{
    #[inline]
    fn effective_proxy_ids(&self, epoch_id: EpochId) -> Vec<ProxyId> {
        let mut keys: Vec<ProxyId> = self.proxy_connections.iter().map(|e| *e.key()).collect();
        keys.sort_unstable();
        let eligible: Vec<ProxyId> = keys
            .into_iter()
            .filter(|id| {
                self.retiring_proxies
                    .get(id)
                    .map(|retire_epoch| retire_epoch.0 >= epoch_id.0)
                    .unwrap_or(true)
            })
            .collect();

        // Enforce active_nodes limit
        let active_count = self.active_nodes.load(std::sync::atomic::Ordering::Relaxed);
        if active_count >= eligible.len() {
            return eligible;
        }

        let mut active: Vec<ProxyId> = eligible.iter().take(active_count).copied().collect();

        // Allow routing to a retiring proxy through its retirement epoch,
        // even though active_nodes has already been decremented.
        for id in eligible.iter().skip(active_count) {
            if self
                .retiring_proxies
                .get(id)
                .map(|retire_epoch| retire_epoch.0 >= epoch_id.0)
                .unwrap_or(false)
                && !active.contains(id)
            {
                active.push(*id);
            }
        }

        active.sort_unstable();
        active
    }

    pub(crate) async fn process_owned_txns(
        &mut self,
        mut owned_txn_receiver: Receiver<(EpochId, Vec<RemoraTransaction<E>>)>,
    ) {
        while let Some((epoch_id, owned_txns)) = owned_txn_receiver.recv().await {
            // Enter the barrier, pausing if recovery is in progress.
            let _ticket = self.pause_barrier.enter().await;
            self.forward_owned_txns_in_parallel(epoch_id, owned_txns)
                .await;
        }
    }

    /// Forward owned-object transactions in parallel with true concurrency
    pub(crate) async fn forward_owned_txns_in_parallel(
        &mut self,
        epoch_id: EpochId,
        transactions: Vec<RemoraTransaction<E>>,
    ) {
        let effective_proxies = self.effective_proxy_ids(epoch_id);
        let proxy_count = effective_proxies.len();
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
            let proxy_id = effective_proxies[idx];
            let tx = Arc::new(tx);
            let proxy_connections = self.proxy_connections.clone();
            let fut = async move {
                if let Some(proxy_conn) = proxy_connections.get(&proxy_id) {
                    if proxy_mode == ProxyMode::Separation {
                        let msg1 = PrimaryToProxyMessage::StatelessTxn(epoch_id, tx.clone());
                        let msg2 = PrimaryToProxyMessage::Txn(
                            epoch_id,
                            tx.clone(),
                            proxy_id,
                            BTreeMap::new(),
                        );

                        if proxy_conn.send(msg1).await.is_err() {
                            tracing::warn!("Failed to send stateless txn to proxy {}", proxy_id);
                        }
                        if proxy_conn.send(msg2).await.is_err() {
                            tracing::warn!("Failed to send stateful txn to proxy {}", proxy_id);
                        }
                    } else {
                        let msg = PrimaryToProxyMessage::CombinedTxn(
                            epoch_id,
                            tx.clone(),
                            proxy_id,
                            BTreeMap::new(),
                        );
                        if proxy_conn.send(msg).await.is_err() {
                            tracing::warn!("Failed to send combined txn to proxy {}", proxy_id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProxyMode;
    use crate::executor::fake::FakeExecutor;
    use crate::primary::pause_barrier::PauseBarrier;
    use rand::Rng;
    use std::sync::Arc;
    use tokio::sync::mpsc::channel;

    const NUM_ITERATIONS: usize = 100;

    #[test]
    fn prop_effective_proxy_ids_gates_retiring_by_epoch() {
        let mut rng = rand::thread_rng();
        for _ in 0..NUM_ITERATIONS {
            let proxy_count = rng.gen_range(2..12);
            let retiring_proxy = proxy_count - 1;
            let proxy_connections = Arc::new(DashMap::new());
            for id in 0..proxy_count {
                let (tx, _rx) = channel(1);
                proxy_connections.insert(id, tx);
            }
            let retiring_proxies = Arc::new(DashMap::new());
            let retirement_epoch = EpochId(5);
            retiring_proxies.insert(retiring_proxy, retirement_epoch);

            let forwarder = OwnedObjTxnForwarder::<FakeExecutor> {
                active_nodes: Arc::new(std::sync::atomic::AtomicUsize::new(proxy_count - 1)),
                proxy_connections,
                index: 0,
                proxy_mode: ProxyMode::Separation,
                retiring_proxies,
                pause_barrier: PauseBarrier::new(),
            };

            let effective_at_retire = forwarder.effective_proxy_ids(retirement_epoch);
            assert!(!effective_at_retire.is_empty());
            assert!(
                effective_at_retire.contains(&retiring_proxy),
                "expected retiring proxy for retirement epoch"
            );

            let effective_after = forwarder.effective_proxy_ids(EpochId(retirement_epoch.0 + 1));
            assert!(
                !effective_after.contains(&retiring_proxy),
                "effective_proxy_ids returned retiring proxy after retirement epoch"
            );
        }
    }
}
