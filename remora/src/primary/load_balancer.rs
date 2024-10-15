// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{ops::Deref, sync::Arc};

use rustc_hash::FxHashMap;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::api::{ExecutableTransaction, Executor, Transaction},
    metrics::Metrics,
};

use sui_types::{base_types::ObjectID, transaction::InputObjectKind};

/// A load balancer is responsible for distributing transactions to the consensus and proxies.
pub struct LoadBalancer<E: Executor> {
    /// The receiver for transactions.
    rx_transactions: Receiver<Transaction<E>>,
    /// The sender to forward transactions to the consensus.
    tx_consensus: Sender<Transaction<E>>,
    /// Receive handles to forward transactions to proxies. When a new client connects,
    /// this channel receives a sender from the network layer which is used to forward
    /// transactions to the proxies.
    rx_proxy_connections: Receiver<Sender<Transaction<E>>>,
    /// Holds senders to forward transactions to proxies.
    proxy_connections: Vec<Sender<Transaction<E>>>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: usize,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
    /// Keeps track of shared-objects and its shards (proxy)
    shared_object_shards: FxHashMap<ObjectID, usize>,
}

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        rx_transactions: Receiver<Transaction<E>>,
        tx_consensus: Sender<Transaction<E>>,
        rx_proxy_connections: Receiver<Sender<Transaction<E>>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            rx_transactions,
            tx_consensus,
            rx_proxy_connections,
            proxy_connections: Vec::new(),
            index: 0,
            metrics,
            shared_object_shards: FxHashMap::default(),
        }
    }

    /// Forward a transaction to the consensus and proxies.
    async fn forward_transaction(&mut self, transaction: Transaction<E>) -> NodeResult<()> {
        if self.index == 0 {
            self.metrics.register_start_time();
        }

        // Send the transaction to the consensus.
        self.tx_consensus
            .send(transaction.clone())
            .await
            .map_err(|_| NodeError::ShuttingDown)?;

        // Determine proxy_index based on shared object or round-robin.
        while !self.proxy_connections.is_empty() {
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

            // Send the transaction to the selected proxy
            match self.proxy_connections[proxy_index]
                .send(transaction.clone())
                .await
            {
                Ok(()) => {
                    tracing::debug!("Sent transaction to proxy {}", proxy_index);
                    self.index += 1;
                    break;
                }
                Err(_) => {
                    tracing::warn!(
                        "Failed to send transaction to proxy {}, trying other proxies",
                        proxy_index
                    );
                    self.proxy_connections.swap_remove(proxy_index);
                }
            }
        }
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
    fn get_proxy_for_shared_object(&self, transaction: &E::Transaction) -> Option<&usize> {
        transaction.input_objects().iter().find_map(|input_object| {
            if let InputObjectKind::SharedMoveObject { id, .. } = input_object {
                self.shared_object_shards.get(id)
            } else {
                None
            }
        })
    }

    /// Helper function to assign a shared object to a proxy and update the map.
    fn assign_shared_object_to_proxy(&mut self, transaction: &E::Transaction, proxy_index: usize) {
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

    /// Run the load balancer.
    pub async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Load balancer started");
        loop {
            tokio::select! {
                Some(transaction) = self.rx_transactions.recv() => self
                    .forward_transaction(transaction)
                    .await
                    .map_err(|_| NodeError::ShuttingDown)?,
                Some(connection) = self.rx_proxy_connections.recv() => {
                    self.proxy_connections.push(connection);
                    tracing::info!("Added a new proxy connection");
                }
                else => Err(NodeError::ShuttingDown)?
            }
        }
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Transaction<E>: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}
