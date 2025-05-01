// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use rustc_hash::FxHashMap;
use std::{ops::Deref, sync::Arc};
use sui_types::{
    base_types::ObjectID,
    transaction::InputObjectKind,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    config::{LoadBalancingPolicy, DEFAULT_CHANNEL_SIZE},
    error::{NodeError, NodeResult},
    executor::api::{
        ExecutableTransaction, ExecutionResults, Executor, PrimaryToProxyMessage,
        RemoraTransaction, Store,
    },
    metrics::Metrics,
    primary::{owned_processors::OwnedTxnProcessor, shared_processor::SharedTxnProcessor},
    proxy::core::ProxyId,
};

/// A load balancer is responsible for distributing transactions to proxies.
pub struct LoadBalancer<E: Executor> {
    /// The executor is only used to assigned shared object versions.
    executor: E,
    /// The proxy connections.
    proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    /// The load balancing policy.
    policy: LoadBalancingPolicy,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

impl<E: Executor + Send + Sync + 'static> LoadBalancer<E>
where
    <E as Executor>::Transaction: Send + Sync + 'static,
{
    /// Create a new load balancer.
    pub fn new(
        executor: E,
        proxy_connections: Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
        policy: LoadBalancingPolicy,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            executor,
            proxy_connections,
            rx_committed_txns,
            metrics,
            policy,
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

    /// Initialize transaction processors and return the senders
    fn initialize_processors(
        &self,
    ) -> (
        Sender<Vec<RemoraTransaction<E>>>, // owned_txn_sender
        Sender<Vec<RemoraTransaction<E>>>, // shared_txn_sender
    ) {
        // Create channels for transactions
        let (owned_txn_sender, owned_txn_receiver) =
            tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (shared_txn_sender, shared_txn_receiver) =
            tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE);

        // Initialize the OwnedTxnProcessor
        let mut owned_txn_processor = OwnedTxnProcessor::<E> {
            proxy_connections: self.proxy_connections.clone(),
            policy: self.policy.clone(),
            index: 0,
        };

        // Initialize the SharedTxnProcessor
        let mut shared_txn_processor = SharedTxnProcessor::<E> {
            executor: Arc::new(self.executor.clone()),
            proxy_connections: self.proxy_connections.clone(),
            policy: self.policy.clone(),
            index: 0,
            states_to_proxy: FxHashMap::default(),
        };

        // Spawn a task to process owned transactions
        tokio::spawn(async move {
            owned_txn_processor
                .process_owned_txns(owned_txn_receiver)
                .await;
        });

        // Spawn a task to process shared transactions
        tokio::spawn(async move {
            shared_txn_processor
                .process_shared_txns(shared_txn_receiver)
                .await;
        });

        // Return the senders so they can be used in the run loop
        (owned_txn_sender, shared_txn_sender)
    }

    /// Run the load balancer.
    pub async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Load balancer started");

        // Initialize processors and get the transaction senders
        let (owned_txn_sender, shared_txn_sender) = self.initialize_processors();

        let mut txn_cnt = 0;
        loop {
            tokio::select! {
                Some(transactions) = self.rx_committed_txns.recv() => {
                    txn_cnt += 1;
                    if txn_cnt == 1 {
                        self.metrics.register_start_time();
                    }

                    // Separate transactions into owned-only and shared-object transactions
                    let mut owned_txns = Vec::new();
                    let mut shared_txns = Vec::new();

                    for transaction in transactions {
                        let shared_object_ids = self.get_shared_object_ids(transaction.deref());
                        if shared_object_ids.is_empty() {
                            owned_txns.push(transaction);
                        } else {
                            shared_txns.push(transaction);
                        }
                    }

                    // Send owned-only transactions to the dedicated task
                    if !owned_txns.is_empty() {
                        if let Err(e) = owned_txn_sender.send(owned_txns).await {
                            tracing::error!("Failed to send owned transactions: {:?}", e);
                        }
                    }

                    // Send shared-object transactions to the dedicated task
                    if !shared_txns.is_empty() {
                        if let Err(e) = shared_txn_sender.send(shared_txns).await {
                            tracing::error!("Failed to send shared transactions: {:?}", e);
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
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send,
        <E as Executor>::Transaction: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}