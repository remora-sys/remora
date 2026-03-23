// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::{marker::PhantomData, sync::Arc, thread};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    config::{LoadBalancingPolicy, SeparationMode, DEFAULT_CHANNEL_SIZE},
    error::{NodeError, NodeResult},
    executor::{
        api::{
            ExecutionResults, Executor, PrimaryToProxyMessage, RemoraTransaction,
            StatelessVerificationRequest, Store,
        },
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

/// A load balancer is responsible for distributing transactions to proxies.
pub struct LoadBalancer<E: Executor> {
    /// The executor used to build stateless verification requests.
    executor: E,
    /// The proxy connections.
    proxy_connections:
        Arc<DashMap<ProxyId, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    /// The load balancing policy.
    policy: PreConsensusSchedulingPolicy,
    /// The proxy mode.
    separation_mode: SeparationMode,
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
        policy: PreConsensusSchedulingPolicy,
        separation_mode: SeparationMode,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            executor,
            proxy_connections,
            rx_committed_txns,
            policy,
            separation_mode,
            metrics,
        }
    }

    /// Initialize transaction processors and return the senders
    fn initialize_processors(
        &self,
        rx_pre_consensus_txns: Receiver<Vec<RemoraTransaction<E>>>,
        rx_stateless_txns: Receiver<StatelessVerificationRequest>,
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
            executor: self.executor.clone(),
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
        let mut shared_txn_processor = SharedObjTxnForwarder::<E>::new(
            self.executor.clone(),
            Arc::new(VersionedDependencyController::new()),
            self.proxy_connections.clone(),
            pre_consensus_routing_plan.clone(),
            self.metrics.clone(),
            Arc::new(DashMap::with_capacity(10000000)),
            stateless_forwarding_table.clone(),
            self.separation_mode,
            self.policy.clone(),
            proxy_loads.clone(),
        );
        let mut pre_consensus_sched_processor = PreConsensusSchedTask::<E>::new(
            self.proxy_connections.clone(),
            pre_consensus_routing_plan.clone(),
            proxy_loads.clone(),
        );

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

        // TODO: use more worker threads when not in primary separation mode
        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_cpus::get())
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
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                pre_consensus_sched_processor
                    .process_pre_consensus_txns(rx_pre_consensus_txns)
                    .await;
            });
        });

        if self.separation_mode == SeparationMode::PrimaryPreSeparation {
            let mut stateless_txn_processor = StatelessTxnForwarder::<E>::new(
                self.proxy_connections.clone(),
                proxy_loads.clone(),
                stateless_forwarding_table.clone(),
            );
            thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(8)
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    stateless_txn_processor
                        .forward_stateless_txn(rx_stateless_txns)
                        .await;
                });
            });
        }

        // Return the senders so they can be used in the run loop
        (owned_txn_sender, shared_txn_sender)
    }

    /// Run the load balancer.
    pub async fn run(
        &mut self,
        rx_pre_consensus_txns: Receiver<Vec<RemoraTransaction<E>>>,
        rx_stateless_txns: Receiver<StatelessVerificationRequest>,
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
                        self.metrics.update_metrics(transaction.timestamp(), "lb-ingress");
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
        rx_stateless_txns: Receiver<StatelessVerificationRequest>,
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
