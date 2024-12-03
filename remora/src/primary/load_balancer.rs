// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

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
        PrimaryToProxyMessage, Transaction,
    },
    metrics::Metrics,
    primary::core::PendingTransactions,
};

/// A load balancer is responsible for distributing transactions to the consensus and proxies.
pub struct LoadBalancer<E: Executor> {
    /// The receiver for transactions.
    rx_transactions: Receiver<Transaction<E>>,
    /// The sender to forward transactions to the consensus.
    tx_consensus: Sender<Transaction<E>>,
    /// Receive handles to forward transactions to proxies. When a new client connects,
    /// this channel receives a sender from the network layer which is used to forward
    /// transactions to the proxies.
    rx_proxy_connections: Receiver<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
    /// Holds senders to forward transactions to proxies.
    proxy_connections: Vec<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Transaction<E>>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: ExecutorIndex,
    /// Keeps track of shared-objects and its shards (proxy)
    shared_object_shards: FxHashMap<ObjectID, ExecutorIndex>,
    /// The transactions sent out to proxies
    pending_txns: PendingTransactions<E>,
    /// The sender to a local executor if no pre-executor is available.
    tx_executor_local: Sender<Transaction<E>>,
    /// The receiver of new effects from local executor and needs to forward to proxies.
    rx_states_sync: Receiver<ExecutionResults<E>>,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        rx_transactions: Receiver<Transaction<E>>,
        tx_consensus: Sender<Transaction<E>>,
        rx_proxy_connections: Receiver<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        rx_committed_txns: Receiver<Transaction<E>>,
        pending_txns: PendingTransactions<E>,
        tx_executor_local: Sender<Transaction<E>>,
        rx_states_sync: Receiver<ExecutionResults<E>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            rx_transactions,
            tx_consensus,
            rx_proxy_connections,
            proxy_connections: Vec::new(),
            rx_committed_txns,
            index: 0,
            shared_object_shards: FxHashMap::default(),
            pending_txns,
            tx_executor_local,
            rx_states_sync,
            metrics,
        }
    }

    /// Forward a transaction to the consensus.
    async fn send_to_consensus(&mut self, transaction: Transaction<E>) -> NodeResult<()> {
        if self.index == 0 {
            self.metrics.register_start_time();
        }

        // Send the transaction to the consensus.
        self.tx_consensus
            .send(transaction.clone())
            .await
            .map_err(|_| NodeError::ShuttingDown)?;

        Ok(())
    }

    // TODO: a bit verbose and duplicated
    /// Helper to check if tx contains shared-objects
    fn check_shared_objects(&self, transaction: &E::Transaction) -> bool {
        transaction.input_objects().iter().any(|input_object| {
            matches!(
                input_object,
                InputObjectKind::SharedMoveObject {
                    id: _,
                    initial_shared_version: _,
                    mutable: _
                }
            )
        })
    }

    /// Helper function to get proxy for a shared object (if it exists).
    fn get_proxy_for_shared_object(&self, transaction: &E::Transaction) -> Option<&ExecutorIndex> {
        transaction.input_objects().iter().find_map(|input_object| {
            if let InputObjectKind::SharedMoveObject { id, .. } = input_object {
                self.shared_object_shards.get(id)
            } else {
                None
            }
        })
    }

    /// Helper function to assign a shared object to a proxy and update the map.
    fn assign_shared_object_to_proxy(
        &mut self,
        transaction: &E::Transaction,
        proxy_index: ExecutorIndex,
    ) {
        if let Some(shared_object_id) =
            transaction.input_objects().iter().find_map(|input_object| {
                if let InputObjectKind::SharedMoveObject { id, .. } = input_object {
                    Some(*id)
                } else {
                    None
                }
            })
        {
            self.shared_object_shards
                .insert(shared_object_id, proxy_index);
        }
    }

    /// Prepare state updates based on sharding
    fn prepare_state_updates(
        &mut self,
        execution_result: ExecutionResults<E>,
    ) -> FxHashMap<ExecutorIndex, NewStates> {
        // HashMap to hold the updates for each executor
        let mut updates_by_executor: FxHashMap<ExecutorIndex, NewStates> = FxHashMap::default();

        for (object_id, object) in execution_result.new_state {
            // Determine the target executor for this object
            if let Some(&executor_id) = self.shared_object_shards.get(&object_id) {
                // Get or create the BTreeMap for this executor
                let entry = updates_by_executor
                    .entry(executor_id)
                    .or_insert_with(BTreeMap::new);
                entry.insert(object_id, object);
            } else {
                eprintln!("Warning: No executor found for ObjectID {}", object_id);
            }
        }

        updates_by_executor
    }

    /// Run the load balancer.
    pub async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Load balancer started");
        loop {
            tokio::select! {
                Some(connection) = self.rx_proxy_connections.recv() => {
                    self.proxy_connections.push(connection);
                    tracing::info!("Added a new proxy connection");
                }

                Some(transaction) = self.rx_transactions.recv() =>
                    self
                    .send_to_consensus(transaction)
                    .await
                    .map_err(|_| NodeError::ShuttingDown)?,

                Some(transaction) = self.rx_committed_txns.recv() => {
                    // forward to proxies if exist else to the local executor
                    if !self.proxy_connections.is_empty() {
                        // Determine proxy_index based on shared object or round-robin.
                        let proxy_index = if self.check_shared_objects(transaction.deref()) {
                            // If a shared object is found, check the hashmap for proxy assignment
                            if let Some(&proxy_index) = self.get_proxy_for_shared_object(&transaction) {
                                proxy_index
                            } else {
                                // If no proxy is assigned to this shared object, assign one in a round-robin fashion
                                let proxy_index = self.index % self.proxy_connections.len();
                                self.assign_shared_object_to_proxy(&transaction, proxy_index);
                                proxy_index
                            }
                        } else {
                            // No shared object, use round-robin to select proxy
                            self.index % self.proxy_connections.len()
                        };

                        self.pending_txns.insert(*transaction.digest(), (proxy_index, transaction.clone()));
                        // Send the transaction to the selected proxy
                        match self.proxy_connections[proxy_index]
                            .send(PrimaryToProxyMessage::Txn(transaction.clone()))
                            .await
                        {
                            Ok(()) => {
                                tracing::debug!("Sent transaction to proxy {}", proxy_index);
                                self.index += 1;
                            }
                            Err(_) => {
                                tracing::warn!(
                                    "Failed to send transaction to proxy {}, trying other proxies",
                                    proxy_index
                                );
                                self.proxy_connections.swap_remove(proxy_index);
                            }
                        }
                    } else {
                        // send back to primary local executor
                        if self.tx_executor_local.send(transaction.clone()).await.is_err() {
                            tracing::warn!("Failed to send transaction to the local executor");
                        }
                    }
                }

                Some(result) = self.rx_states_sync.recv() => {
                    // send states updates to the proxy
                    if self.proxy_connections.len() == 0 {
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
        Transaction<E>: Send + Sync,
        ExecutionResults<E>: Send,
    {
        tokio::spawn(async move { self.run().await })
    }
}
