// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::{marker::PhantomData, sync::Arc, thread, time::Duration};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    config::{LoadBalancingPolicy, DEFAULT_CHANNEL_SIZE},
    error::{NodeError, NodeResult},
    executor::{
        api::{ExecutionResults, Executor, PrimaryToProxyMessage, RemoraTransaction, Store},
        versioned_dependency_controller::VersionedDependencyController,
    },
    metrics::Metrics,
    primary::{
        owned_obj_txn_forwarder::OwnedObjTxnForwarder,
        shared_obj_txn_forwarder::{
            PreConsensusSchedTask, PreConsensusSchedulingPolicy, SharedObjTxnForwarder,
            StatelessTxnForwarder, VersionAssignmentTask,
        },
    },
    proxy::core::ProxyId,
};

use sui_types::digests::TransactionDigest;

/// A load balancer is responsible for distributing transactions to proxies.
pub struct LoadBalancer<E: Executor> {
    /// The executor trait
    _phantom: PhantomData<E>,
    /// The proxy connections.
    proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    /// The load balancing policy.
    policy: PreConsensusSchedulingPolicy,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

impl<E: Executor + Send + Sync + 'static> LoadBalancer<E>
where
    <E as Executor>::Transaction: Send + Sync + 'static,
{
    /// Create a new load balancer.
    pub fn new(
        proxy_connections: Arc<
            DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
        policy: PreConsensusSchedulingPolicy,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            _phantom: PhantomData,
            proxy_connections,
            rx_committed_txns,
            metrics,
            policy,
        }
    }

    /// Initialize transaction processors and return the senders
    fn initialize_processors(
        &self,
        rx_pre_consensus_txns: Receiver<Vec<RemoraTransaction<E>>>,
        rx_stateless_txns: Receiver<(TransactionDigest, Duration)>,
    ) -> (
        Sender<Vec<RemoraTransaction<E>>>, // owned_txn_sender
        Sender<Vec<RemoraTransaction<E>>>, // shared_txn_sender
    ) {
        // Create channels for transactions
        let (owned_txn_sender, owned_txn_receiver) =
            tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (shared_txn_sender, shared_txn_receiver) =
            tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (version_assignment_sender, version_assignment_receiver) =
            tokio::sync::mpsc::channel(DEFAULT_CHANNEL_SIZE);

        // Initialize the OwnedTxnProcessor
        let mut owned_txn_processor = OwnedObjTxnForwarder::<E> {
            proxy_connections: self.proxy_connections.clone(),
            policy: LoadBalancingPolicy::RoundRobin,
            index: 0,
        };

        let mut version_assignment_processor = VersionAssignmentTask::<E> {
            shared_object_versions: rustc_hash::FxHashMap::default(),
            _phantom: PhantomData,
        };
        version_assignment_processor
            .shared_object_versions
            .reserve(10000000);

        let proxy_loads = Arc::new(DashMap::new());
        let stateless_forwarding_table = Arc::new(DashMap::new());
        let pre_consensus_routing_plan = Arc::new(DashMap::new());

        // Initialize the SharedTxnProcessor
        let mut shared_txn_processor = SharedObjTxnForwarder::<E> {
            proxy_connections: self.proxy_connections.clone(),
            states_to_proxy: Arc::new(DashMap::with_capacity(10000000)),
            dependency_controller: Arc::new(VersionedDependencyController::new()),
            metrics: self.metrics.clone(),
            pre_consensus_routing_plan: pre_consensus_routing_plan.clone(),
            stateless_forwarding_table: stateless_forwarding_table.clone(),
        };
        let mut pre_consensus_sched_processor = PreConsensusSchedTask::<E> {
            proxy_connections: self.proxy_connections.clone(),
            pre_consensus_routing_plan,
            _phantom: PhantomData,
            proxy_loads: proxy_loads.clone(),
            last_hot_set: Default::default(),
            last_hot_set_proxy: None,
            policy: self.policy.clone(),
        };

        let mut stateless_txn_processor = StatelessTxnForwarder::<E> {
            proxy_connections: self.proxy_connections.clone(),
            proxy_loads: proxy_loads.clone(),
            stateless_forwarding_table: stateless_forwarding_table.clone(),
            rx_stateless_txns,
        };

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                owned_txn_processor
                    .process_owned_txns(owned_txn_receiver)
                    .await;
            });
        });

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                version_assignment_processor
                    .process_version_assignments(shared_txn_receiver, version_assignment_sender)
                    .await;
            });
        });

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_cpus::get() / 2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                shared_txn_processor
                    .process_shared_txns(version_assignment_receiver)
                    .await;
            });
        });

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                pre_consensus_sched_processor
                    .process_pre_consensus_txns(rx_pre_consensus_txns)
                    .await;
            });
        });

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(8)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                stateless_txn_processor.forward_stateless_txn().await;
            });
        });

        // Return the senders so they can be used in the run loop
        (owned_txn_sender, shared_txn_sender)
    }

    /// Run the load balancer.
    pub async fn run(
        &mut self,
        rx_pre_consensus_txns: Receiver<Vec<RemoraTransaction<E>>>,
        rx_stateless_txns: Receiver<(TransactionDigest, Duration)>,
    ) -> NodeResult<()> {
        tracing::info!("Load balancer started");

        // Initialize processors and get the transaction senders
        let (owned_txn_sender, shared_txn_sender) =
            self.initialize_processors(rx_pre_consensus_txns, rx_stateless_txns);

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
                        let shared_object_ids = transaction.shared_objects();
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
    pub fn spawn(
        mut self,
        rx_pre_consensus_txns: Receiver<Vec<RemoraTransaction<E>>>,
        rx_stateless_txns: Receiver<(TransactionDigest, Duration)>,
    ) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send,
        <E as Executor>::Transaction: Send + Sync,
    {
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move { self.run(rx_pre_consensus_txns, rx_stateless_txns).await })
        })
    }
}
