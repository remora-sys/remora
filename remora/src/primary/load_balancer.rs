// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, ops::Deref, sync::Arc};

use rustc_hash::FxHashMap;
use sui_types::{base_types::ObjectID, transaction::InputObjectKind};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::api::{
        ExecutableTransaction, ExecutionResults, Executor, ExecutorIndex, NewStates,
        PrimaryToProxyMessage, RemoraTransaction,
    },
    metrics::Metrics,
};

/// A load balancer is responsible for distributing transactions to proxies.
pub struct LoadBalancer<E: Executor> {
    /// The executor is only used to assigned shared object versions.
    executor: E,
    /// Receive handles to forward transactions to proxies. When a new client connects,
    /// this channel receives a sender from the network layer which is used to forward
    /// transactions to the proxies.
    rx_proxy_connections: Receiver<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
    /// Holds senders to forward transactions to proxies.
    proxy_connections: Vec<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: ExecutorIndex,
    /// The sender to a local executor if no pre-executor is available.
    tx_executor_local: Sender<RemoraTransaction<E>>,
    /// The receiver of new effects from local executor and needs to forward to proxies.
    rx_states_sync: Receiver<ExecutionResults<E>>,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

const FIB_CONSTANT: u64 = 11400714819323198485; // Golden ratio * 2^64

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        executor: E,
        rx_proxy_connections: Receiver<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
        tx_executor_local: Sender<RemoraTransaction<E>>,
        rx_states_sync: Receiver<ExecutionResults<E>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            executor,
            rx_proxy_connections,
            proxy_connections: Vec::new(),
            rx_committed_txns,
            index: 0,
            tx_executor_local,
            rx_states_sync,
            metrics,
        }
    }

    /// Helper to get all shared object IDs from a transaction.
    fn get_shared_object_ids(&self, transaction: &E::Transaction) -> Vec<ObjectID> {
        transaction
            .input_objects()
            .iter()
            .filter_map(|input_object| {
                if let InputObjectKind::SharedMoveObject { id, .. } = input_object {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Fibonacci Hashing for ObjectID → Proxy Index Mapping (Fast & Even Distribution)
    fn fast_fibonacci_hash(&self, object_id: &ObjectID) -> ExecutorIndex {
        let mut hash = 0u64;
        for chunk in object_id.chunks(8) {
            let mut chunk_array = [0u8; 8];
            chunk_array[..chunk.len()].copy_from_slice(chunk);
            let num = u64::from_ne_bytes(chunk_array);
            hash ^= num; // XOR to spread entropy
        }

        // Apply Fibonacci hashing for fast and even distribution
        let proxy_count = self.proxy_connections.len().max(1); // Avoid div by zero
        ((hash.wrapping_mul(FIB_CONSTANT)) >> (64 - proxy_count.ilog2())) as usize % proxy_count
    }

    /// Get assigned proxies for shared objects in a transaction.
    fn get_proxies_for_shared_objects(
        &self,
        shared_object_ids: &[ObjectID],
    ) -> HashSet<ExecutorIndex> {
        shared_object_ids
            .iter()
            .map(|id| self.fast_fibonacci_hash(id))
            .collect()
    }

    /// Prepare state updates based on sharding
    fn prepare_state_updates(
        &mut self,
        execution_result: ExecutionResults<E>,
    ) -> FxHashMap<ExecutorIndex, NewStates> {
        // HashMap to hold the updates for each executor
        let mut updates_by_executor: FxHashMap<ExecutorIndex, NewStates> = FxHashMap::default();

        for (object_id, object) in execution_result.new_state.unwrap() {
            let executor_id = self.fast_fibonacci_hash(&object_id);
            let entry = updates_by_executor.entry(executor_id).or_default();
            entry.insert(object_id, object);
        }

        updates_by_executor
    }

    /// Determines the correct forwarding target for a transaction.
    async fn forward_txn_to_proxy(&mut self, transaction: RemoraTransaction<E>) {
        // If no proxies exist, send to the local executor.
        if self.proxy_connections.is_empty() {
            if self.tx_executor_local.send(transaction).await.is_err() {
                tracing::warn!("Failed to send transaction to the local executor");
            }
            return;
        }

        let shared_object_ids = self.get_shared_object_ids(transaction.deref());

        if shared_object_ids.is_empty() {
            // No shared objects, use round-robin for proxy selection.
            let proxy_index = self.index % self.proxy_connections.len();
            self.index += 1;

            if self.proxy_connections[proxy_index]
                .send(PrimaryToProxyMessage::Txn(transaction))
                .await
                .is_ok()
            {
                tracing::debug!("Sent transaction to proxy {}", proxy_index);
            } else {
                tracing::warn!(
                    "Failed to send transaction to proxy {}, trying other proxies",
                    proxy_index
                );
                self.proxy_connections.swap_remove(proxy_index);
            }
            return;
        }

        let assigned_proxies = self.get_proxies_for_shared_objects(&shared_object_ids);

        match assigned_proxies.len() {
            1 => {
                let proxy_index = *assigned_proxies.iter().next().unwrap();
                self.proxy_connections[proxy_index]
                    .send(PrimaryToProxyMessage::Txn(transaction))
                    .await
                    .ok();
            }
            _ => {
                if self.tx_executor_local.send(transaction).await.is_err() {
                    tracing::warn!("Failed to send transaction to local executor");
                }
            }
        }
    }

    /// Run the load balancer.
    pub async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Load balancer started");
        let mut txn_cnt = 0;
        loop {
            tokio::select! {
                Some(connection) = self.rx_proxy_connections.recv() => {
                    self.proxy_connections.push(connection);
                    tracing::info!("Added a new proxy connection");
                }

                Some(transactions) = self.rx_committed_txns.recv() => {

                    // Assign shared objects version.
                    self.executor
                        .assign_shared_object_versions(
                            &transactions.iter().map(|tx| tx.deref().clone()).collect::<Vec<_>>()
                        )
                        .await;


                    txn_cnt += 1;
                    if txn_cnt == 1 {
                        self.metrics.register_start_time();
                    }

                    for transaction in transactions {
                        self.forward_txn_to_proxy(transaction).await;
                    }
                }

                Some(result) = self.rx_states_sync.recv() => {
                    // send states updates to the proxy
                    if self.proxy_connections.is_empty() {
                        tracing::debug!("Skip states updating given no available other executors");
                        continue;
                    }

                    let states_updates = self.prepare_state_updates(result);
                    for (proxy_index, update) in states_updates {
                        match self.proxy_connections[proxy_index].send(PrimaryToProxyMessage::States(update)).await {
                            Ok(()) => {
                                tracing::debug!("Sent updates to proxy {}", proxy_index);
                            }
                            Err(_) => {
                                tracing::warn!("Failed to send states to proxy {}", proxy_index);
                                self.proxy_connections.swap_remove(proxy_index);
                            }
                        }
                    }
                }

                else => Err(NodeError::ShuttingDown)?,
            }
        }
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send,
        <E as Executor>::Transaction: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}
