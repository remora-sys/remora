// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet},
    ops::Deref,
    sync::Arc,
};

use rustc_hash::FxHashMap;
use sui_types::{base_types::ObjectID, digests::TransactionDigest, transaction::InputObjectKind};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::{
        api::{
            ExecutableTransaction, ExecutionResults, Executor, ExecutorIndex, MissingStates,
            NewStates, PrimaryToProxyMessage, RemoraTransaction,
        },
        sui::get_object_ids_for_dependency_tracking,
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
    /// The receiver of new effects from local executor and needs to forward to proxies.
    rx_states_sync: Receiver<ExecutionResults<E>>,
    /// The mapping of the states and the proxy index.
    states_to_proxy: FxHashMap<ObjectID, ExecutorIndex>,
    /// The routing information of stateless.
    stateless_routing: FxHashMap<TransactionDigest, ExecutorIndex>,
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
        rx_states_sync: Receiver<ExecutionResults<E>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            executor,
            rx_proxy_connections,
            proxy_connections: Vec::new(),
            rx_committed_txns,
            index: 0,
            rx_states_sync,
            states_to_proxy: FxHashMap::default(),
            stateless_routing: FxHashMap::default(),
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

    #[deprecated]
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
            tracing::error!("No proxies available");
            return;
        }

        let shared_object_ids = self.get_shared_object_ids(transaction.deref());

        if shared_object_ids.is_empty() {
            self.forward_owned_object_only_txn(transaction).await;
        } else {
            self.forward_shared_object_txn(transaction, shared_object_ids)
                .await;
        }
    }

    /// Helper method to determine missing states for a transaction
    /// and update the states ownership map
    fn get_missing_states_for_transaction(
        &mut self,
        transaction: &RemoraTransaction<E>,
        proxy_index: ExecutorIndex,
    ) -> MissingStates {
        let mut missing_states = BTreeMap::new();

        let object_ids = get_object_ids_for_dependency_tracking::<E>(transaction.clone());

        for object_id in object_ids {
            // If this object is not in our states_to_proxy map, it's missing
            let previous_owner = self.states_to_proxy.get(&object_id).unwrap();
            if *previous_owner != proxy_index {
                missing_states.insert(object_id, *previous_owner);
                self.states_to_proxy.insert(object_id, proxy_index);
            }
        }

        missing_states
    }

    /// Sends a transaction to a specific proxy and handles connection failures.
    async fn send_transaction_to_proxy(
        &mut self,
        proxy_index: usize,
        transaction: RemoraTransaction<E>,
        is_stateful: bool,
    ) -> bool {
        let message = if is_stateful {
            let stateless_proxy_id = self.stateless_routing.remove(transaction.digest()).unwrap();
            let missing_states = self.get_missing_states_for_transaction(&transaction, proxy_index);
            PrimaryToProxyMessage::Txn(transaction, stateless_proxy_id, missing_states)
        } else {
            self.stateless_routing
                .insert(*transaction.digest(), proxy_index);
            PrimaryToProxyMessage::StatelessTxn(transaction)
        };

        if self.proxy_connections[proxy_index]
            .send(message)
            .await
            .is_ok()
        {
            tracing::debug!("Sent transaction to proxy {}", proxy_index);
            true
        } else {
            tracing::warn!(
                "Failed to send transaction to proxy {}, removing connection",
                proxy_index
            );
            self.proxy_connections.swap_remove(proxy_index);
            false
        }
    }

    /// Forwards transactions with owned-object only using round-robin.
    async fn forward_owned_object_only_txn(&mut self, transaction: RemoraTransaction<E>) {
        let proxy_index = self.index % self.proxy_connections.len();
        self.index += 1;

        self.send_transaction_to_proxy(proxy_index, transaction.clone(), false)
            .await;
        self.send_transaction_to_proxy(proxy_index, transaction, true)
            .await;
    }

    /// Forwards transactions with shared objects to the appropriate proxy.
    async fn forward_shared_object_txn(
        &mut self,
        transaction: RemoraTransaction<E>,
        shared_object_ids: Vec<ObjectID>,
    ) {
        let assigned_proxies = self.get_proxies_for_shared_objects(&shared_object_ids);

        match assigned_proxies.len() {
            1 => {
                let proxy_index = *assigned_proxies.iter().next().unwrap();
                self.send_transaction_to_proxy(proxy_index, transaction.clone(), false)
                    .await;
                self.send_transaction_to_proxy(proxy_index, transaction, true)
                    .await;
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

                /*Some(result) = self.rx_states_sync.recv() => {
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
                }*/

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
