// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::{collections::BTreeMap, ops::Deref, sync::Arc, time::Duration};
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
    object::Object,
    transaction::InputObjectKind,
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        oneshot, Notify,
    },
    task::JoinHandle,
};

use crate::{
    config::SeparationMode,
    error::{NodeError, NodeResult},
    executor::{
        api::{
            ExecutableTransaction, ExecutionResults, Executor, ExecutorIndex, InterProxyReply,
            InterProxyRequest, PrimaryToProxyMessage, ProxyToProxyMessage, RemoraTransaction,
            RequiredStates, StateStore, Store,
        },
        oneshot_dependency_controller::OneshotDependencyController,
        versioned_dependency_controller::VersionedDependencyController,
    },
    metrics::Metrics,
};

pub type ProxyId = ExecutorIndex;

struct PrimaryMessageProcessor<E: Executor> {
    id: ProxyId,
    executor: E,
    store: Store<E>,
    tx_results: Sender<ExecutionResults<E>>,
    tx_inter_proxy_replies: Arc<DashMap<ProxyId, Sender<ProxyToProxyMessage>>>,
    stateful_controller: Arc<VersionedDependencyController>,
    stateless_controller: Arc<OneshotDependencyController>,
    mode: SeparationMode,
    metrics: Arc<Metrics>,
}

impl<E: Executor + Send + Sync + 'static> PrimaryMessageProcessor<E>
where
    E: Send + 'static,
    Store<E>: Send + Sync,
    RemoraTransaction<E>: Send + Sync,
    ExecutionResults<E>: Send + Sync,
    <E as Executor>::ExecutionContext: Send + Sync,
    <E as Executor>::Transaction: Send,
{
    async fn run(
        &mut self,
        mut rx_transactions: Receiver<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
    ) where
        <E as Executor>::Transaction: Send + Sync,
    {
        let mut task_id = 0;
        while let Some(message) = rx_transactions.recv().await {
            if task_id == 0 {
                self.metrics.register_start_time();
            }
            task_id += 1;

            self.handle_primary_message(message).await;
        }
        tracing::info!(
            "Primary message handler for proxy {} shutting down",
            self.id
        );
    }

    async fn handle_primary_message(
        &mut self,
        message: PrimaryToProxyMessage<<E as Executor>::Transaction>,
    ) {
        if self.mode == SeparationMode::PrimaryPreSeparation
            || self.mode == SeparationMode::PrimaryPostSeparation
        {
            match message {
                PrimaryToProxyMessage::StatelessTxn(transaction, verification_duration) => {
                    tracing::debug!(
                        "Proxy {} received stateless transaction {:?}",
                        self.id,
                        transaction
                    );
                    self.process_stateless_transaction(transaction, verification_duration)
                        .await
                }

                PrimaryToProxyMessage::Txn(transaction, stateless_res_proxy_id, missing_states) => {
                    tracing::debug!(
                        "Proxy {} received stateful transaction {:?}, stateless proxy: {}",
                        self.id,
                        transaction.digest(),
                        stateless_res_proxy_id
                    );
                    self.process_stateful_transaction(
                        transaction,
                        stateless_res_proxy_id,
                        missing_states,
                    )
                    .await;
                }
                _ => {
                    panic!("Proxy {} received unexpected message", self.id);
                }
            }
        } else {
            match message {
                PrimaryToProxyMessage::CombinedTxn(
                    transaction,
                    stateless_res_proxy_id,
                    missing_states,
                ) => {
                    tracing::debug!(
                        "Proxy {} received combined transaction {:?}, stateless proxy: {}",
                        self.id,
                        transaction.digest(),
                        stateless_res_proxy_id
                    );

                    if self.mode.is_proxy_separation() {
                        self.process_stateless_transaction(
                            *transaction.digest(),
                            transaction.verification_duration(),
                        )
                        .await;
                    }
                    self.process_stateful_transaction(
                        transaction,
                        stateless_res_proxy_id,
                        missing_states,
                    )
                    .await;
                }
                _ => {
                    panic!("Proxy {} received unexpected message", self.id);
                }
            }
        }
    }

    async fn process_stateful_transaction(
        &mut self,
        transaction: Arc<RemoraTransaction<E>>,
        stateless_res_proxy_id: ProxyId,
        required_states: RequiredStates,
    ) {
        // If the stateless result is from the same proxy, look up the handle
        let rx = if self.mode.is_proxy_separation() {
            if stateless_res_proxy_id == self.id {
                self.stateless_controller
                    .get_dependency(transaction.digest())
            } else {
                // Otherwise, set a remote dependency
                // Send stateless request to the appropriate proxy
                let request = InterProxyRequest::Stateless(self.id, *transaction.digest());
                let tx = self
                    .tx_inter_proxy_replies
                    .get(&stateless_res_proxy_id)
                    .unwrap();
                if let Err(e) = tx.send(ProxyToProxyMessage::Request(request)).await {
                    tracing::error!("Failed to send stateless request: {}", e);
                }
                Some(
                    self.stateless_controller
                        .set_remote_dependency(*transaction.digest()),
                )
            }
        } else {
            None
        };

        self.schedule_stateful_transaction(transaction, required_states, rx)
            .await;
    }

    async fn process_stateless_transaction(
        &self,
        digest: TransactionDigest,
        verification_duration: Duration,
    ) {
        tracing::debug!(
            "Proxy {} processing stateless transaction {:?}",
            self.id,
            digest
        );

        let tx = self.stateless_controller.set_local_dependency(digest);

        let context = self.executor.context().clone();
        let id = self.id;
        tokio::spawn(async move {
            let res = E::verify_transaction(context, digest, verification_duration).await;
            tracing::debug!(
                "Proxy {} completed stateless verification for {:?}, result: {}",
                id,
                digest,
                res
            );
            tx.send(res).expect("Failed to send result");
        });
    }

    /// Schedule a stateful transaction in multi-threaded mode.
    /// including sending requests for missing states to other proxies.
    async fn schedule_stateful_transaction(
        &mut self,
        transaction: Arc<RemoraTransaction<E>>,
        required_states: RequiredStates,
        rx: Option<oneshot::Receiver<bool>>,
    ) {
        tracing::debug!(
            "Proxy {} scheduling stateful transaction {:?} with {} required states",
            self.id,
            transaction.digest(),
            required_states.len()
        );

        for (states, proxy_id) in &required_states {
            if let Some(proxy_id) = proxy_id {
                assert_ne!(*proxy_id, self.id);
                tracing::debug!(
                    "Proxy {} requesting {} missing states from proxy {}: {:?}",
                    self.id,
                    1,
                    proxy_id,
                    states
                );
                let request = InterProxyRequest::Stateful(self.id, vec![*states]);
                ProxyMessageProcessor::<E>::send_msg_to_proxy(
                    self.tx_inter_proxy_replies.clone(),
                    *proxy_id,
                    ProxyToProxyMessage::Request(request),
                )
                .await;
            }
        }

        self.metrics.increase_proxy_load(self.id);

        self.spawn_stateful_txn(transaction, rx, required_states)
            .await
            .expect("Failed to schedule transaction");
    }

    pub fn get_dependencies(
        required_states: RequiredStates,
        stateful_controller: Arc<VersionedDependencyController>,
    ) -> (
        Vec<(ObjectID, SequenceNumber)>,
        Vec<Arc<Notify>>,
        Vec<Arc<Notify>>,
    ) {
        let obj_ids: Vec<(ObjectID, SequenceNumber)> = required_states
            .keys()
            .map(|state| (state.0, state.1))
            .collect();

        // If there are no object dependencies, return empty vectors for handles
        if obj_ids.is_empty() {
            return (obj_ids, Vec::new(), Vec::new());
        }

        let (prior_handles, current_handles) =
            stateful_controller.get_prior_dependency_and_update(0, obj_ids.clone(), false, false);

        (obj_ids, prior_handles, current_handles)
    }

    pub async fn spawn_stateful_txn(
        &mut self,
        transaction: Arc<RemoraTransaction<E>>,
        stateless_handle: Option<oneshot::Receiver<bool>>,
        required_states: RequiredStates,
    ) -> NodeResult<()> {
        tracing::debug!(
            "Proxy {} spawning stateful transaction {:?}",
            self.id,
            transaction.digest()
        );

        let store = self.store.clone();
        let id = self.id;
        let tx_results = self.tx_results.clone();
        let ctx = self.executor.context().clone();
        let metrics = self.metrics.clone();
        let stateful_controller = self.stateful_controller.clone();
        let stateless_controller = self.stateless_controller.clone();
        let executor = self.executor.clone();
        let mode = self.mode.clone();

        tokio::spawn(async move {
            // Assign shared objects version.
            if !required_states.is_empty() {
                let required_versions: Vec<(ObjectID, SequenceNumber)> = required_states
                    .keys()
                    .map(|state| (state.0, state.1))
                    .collect();
                executor
                    .assign_shared_object_versions_with_required_versions(
                        &[transaction.deref().deref().clone()],
                        &required_versions,
                    )
                    .await;
            }

            // Get dependencies and schedule the transaction
            let (obj_ids, prior_handles, current_handles) =
                Self::get_dependencies(required_states.clone(), stateful_controller.clone());
            tracing::debug!(
                "Proxy {} got dependencies for transaction {:?}: objects {:?}",
                id,
                transaction.digest(),
                obj_ids.clone()
            );

            // Wait for the stateless dependency to be resolved
            if let Some(rx) = stateless_handle {
                rx.await.unwrap();
                stateless_controller.remove_dependency(transaction.digest());
                tracing::debug!(
                    "stateless dependency satisfied for transaction {:?}",
                    transaction.digest()
                );
            }

            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }
            stateful_controller.remove_dependency(obj_ids.clone());
            tracing::debug!(
                "stateful dependency satisfied for transaction {:?}",
                transaction.digest()
            );

            // check the version ID for shared objects
            // skip if versions don't match
            let ready_to_execute =
                !transaction.input_objects().iter().any(|input_object| {
                    matches!(
                        input_object,
                        InputObjectKind::SharedMoveObject {
                            id: _,
                            initial_shared_version: _,
                            mutable: _,
                        }
                    )
                }) || E::pre_execute_check(ctx.clone(), store.clone(), &transaction);

            let execution_result = if ready_to_execute {
                tracing::debug!(
                    "Proxy {} executing transaction {:?}",
                    id,
                    transaction.digest()
                );
                // If the proxy is not in proxy separation mode, need to run stateless verification
                if !mode.is_proxy_separation() {
                    E::verify_transaction(
                        ctx.clone(),
                        *transaction.digest(),
                        transaction.verification_duration(),
                    )
                    .await;
                }
                E::execute(ctx, store, transaction.deref().clone()).await
            } else {
                tracing::warn!(
                    "Proxy {} skipped execution for transaction {:?}",
                    id,
                    transaction.digest()
                );
                ExecutionResults::<E>::new(transaction.deref().clone(), None, None)
            };

            tracing::debug!(
                "Proxy {} completed execution for transaction {:?}, success: {}",
                id,
                transaction.digest(),
                execution_result.success()
            );

            for notify in current_handles {
                notify.notify_one();
            }

            tx_results
                .send(execution_result)
                .await
                .map_err(|_| NodeError::ShuttingDown)?;

            metrics.decrease_proxy_load(&id);
            //metrics.update_metrics(transaction.timestamp());
            Ok::<_, NodeError>(())
        });
        Ok(())
    }
}

struct ProxyMessageProcessor<E: Executor> {
    id: ProxyId,
    store: Store<E>,
    tx_inter_proxy_replies: Arc<DashMap<ProxyId, Sender<ProxyToProxyMessage>>>,
    stateful_controller: Arc<VersionedDependencyController>,
    stateless_controller: Arc<OneshotDependencyController>,
    mode: SeparationMode,
}

impl<E: Executor + Send + Sync + 'static> ProxyMessageProcessor<E>
where
    Store<E>: Send + Sync,
{
    async fn run(&mut self, mut rx_inter_proxy_requests: Receiver<ProxyToProxyMessage>) {
        while let Some(message) = rx_inter_proxy_requests.recv().await {
            self.handle_proxy_message(message).await;
        }
        tracing::info!(
            "Inter-proxy message handler for proxy {} shutting down",
            self.id
        );
    }

    async fn handle_proxy_message(&mut self, message: ProxyToProxyMessage) {
        match message {
            ProxyToProxyMessage::Request(request) => match request {
                InterProxyRequest::Stateful(proxy_id, requested_states) => {
                    tracing::debug!(
                        "Proxy {} received stateful request from proxy {}, states: {:?}",
                        self.id,
                        proxy_id,
                        requested_states
                    );
                    self.handle_stateful_request(proxy_id, requested_states)
                        .await;
                }
                InterProxyRequest::Stateless(proxy_id, txn_digest) => {
                    if self.mode == SeparationMode::NoSeparation {
                        tracing::error!(
                            "Proxy {} received stateless request from proxy {} for transaction {:?}",
                            self.id,
                            proxy_id,
                            txn_digest
                        );
                        return;
                    }
                    tracing::debug!(
                        "Proxy {} received stateless request from proxy {} for transaction {:?}",
                        self.id,
                        proxy_id,
                        txn_digest
                    );
                    self.handle_stateless_request(proxy_id, &txn_digest).await;
                }
            },
            ProxyToProxyMessage::Reply(reply) => match reply {
                InterProxyReply::Stateful(objects) => {
                    tracing::debug!(
                        "Proxy {} received stateful reply with {} objects",
                        self.id,
                        objects.len()
                    );
                    self.handle_stateful_reply(objects).await;
                }
                InterProxyReply::Stateless(digest, result) => {
                    tracing::debug!(
                        "Proxy {} received stateless reply for transaction {:?}, result: {}",
                        self.id,
                        digest,
                        result
                    );
                    self.handle_stateless_reply(digest, result).await;
                }
            },
        }
    }

    async fn handle_stateful_reply(&mut self, objects: BTreeMap<ObjectID, Object>) {
        tracing::debug!(
            "Proxy {} handling stateful reply with {} objects",
            self.id,
            objects.len()
        );

        // Mock the states update (oid, v) as a txn from (oid, v - 1) to (oid, v)
        let objs: Vec<_> = objects
            .iter()
            .map(|(oid, o)| (*oid, o.compute_object_reference().1.one_before().unwrap()))
            .collect();
        let (_, current_handles) = self
            .stateful_controller
            .get_prior_dependency_and_update(0, objs, true, false);

        let store = self.store.clone();
        tokio::spawn(async move {
            // Newly migrated states can be committed directly
            // without waiting for the prior dependencies
            store.commit_new_objects(objects);
            for notify in current_handles {
                notify.notify_one();
            }
        });
    }

    async fn handle_stateless_reply(&mut self, digest: TransactionDigest, result: bool) {
        tracing::debug!(
            "Proxy {} handling stateless reply for transaction {:?}, result: {}",
            self.id,
            digest,
            result
        );

        let tx = self.stateless_controller.take_signal(&digest);
        if let Some(tx) = tx {
            tx.send(result).expect("Failed to send result");
        }
        self.stateless_controller.remove_dependency(&digest);
    }

    async fn handle_stateful_request(
        &mut self,
        proxy_id: ProxyId,
        requested_states: Vec<(ObjectID, SequenceNumber)>,
    ) {
        tracing::debug!(
            "Proxy {} handling stateful request from proxy {} for states: {:?}",
            self.id,
            proxy_id,
            requested_states
        );

        // Need to ensure that the latest transaction accessing the object is already committed
        let (prior_handles, _) = self.stateful_controller.get_prior_dependency_and_update(
            0,
            requested_states.clone(),
            false,
            true,
        );

        let tx_inter_proxy_replies = self.tx_inter_proxy_replies.clone();
        let store = self.store.clone();
        let stateful_controller = self.stateful_controller.clone();
        tokio::spawn(async move {
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }
            stateful_controller.remove_dependency(requested_states.clone());
            tracing::debug!("Ready to get objects for stateful request");
            let mut objects = BTreeMap::new();
            for state in requested_states {
                let object = match store.read_object(&state.0) {
                    Ok(obj) => obj,
                    Err(e) => {
                        tracing::warn!("Failed to read object: {:?}", e);
                        None
                    }
                };

                // Check that the object version matches the requested version
                if let Some(obj) = object {
                    let obj_version = obj.version();
                    if obj_version == state.1 {
                        objects.insert(state.0, obj);
                    } else {
                        tracing::warn!(
                            "Version mismatch for object {:?}: requested version {}, found version {}",
                            state.0,
                            state.1,
                            obj_version
                        );
                    }
                }
            }

            let reply = InterProxyReply::Stateful(objects);
            Self::send_msg_to_proxy(
                tx_inter_proxy_replies,
                proxy_id,
                ProxyToProxyMessage::Reply(reply),
            )
            .await;
        });
    }

    async fn handle_stateless_request(
        &mut self,
        proxy_id: ProxyId,
        txn_digest: &TransactionDigest,
    ) {
        tracing::debug!(
            "Proxy {} handling stateless request from proxy {} for transaction {:?}",
            self.id,
            proxy_id,
            txn_digest
        );

        let rx = self
            .stateless_controller
            .get_dependency(txn_digest)
            .unwrap();

        let stateless_controller = self.stateless_controller.clone();
        let txn_digest = *txn_digest;
        let tx_inter_proxy_replies = self.tx_inter_proxy_replies.clone();
        tokio::spawn(async move {
            let verification_result = rx.await.unwrap();
            stateless_controller.remove_dependency(&txn_digest);

            let reply = InterProxyReply::Stateless(txn_digest, verification_result);
            Self::send_msg_to_proxy(
                tx_inter_proxy_replies.clone(),
                proxy_id,
                ProxyToProxyMessage::Reply(reply),
            )
            .await;
        });
    }

    async fn send_msg_to_proxy(
        tx_inter_proxy_replies: Arc<DashMap<ProxyId, Sender<ProxyToProxyMessage>>>,
        proxy_id: ProxyId,
        message: ProxyToProxyMessage,
    ) {
        let message_type = match &message {
            ProxyToProxyMessage::Request(req) => match req {
                InterProxyRequest::Stateful(_, _) => "Stateful request",
                InterProxyRequest::Stateless(_, _) => "Stateless request",
            },
            ProxyToProxyMessage::Reply(reply) => match reply {
                InterProxyReply::Stateful(_) => "Stateful reply",
                InterProxyReply::Stateless(_, _) => "Stateless reply",
            },
        };

        tracing::debug!("Sending {} to proxy {}", message_type, proxy_id);

        if let Some(tx) = tx_inter_proxy_replies.get(&proxy_id) {
            if tx.send(message).await.is_err() {
                tracing::warn!(
                    "Failed to send reply to proxy {}, connection may be lost",
                    proxy_id
                );
            }
        } else {
            tracing::warn!("No connection found for proxy {}", proxy_id);
        }
    }
}

/// A proxy is responsible for pre-executing transactions.
pub struct ProxyCore<E: Executor> {
    /// The ID of the proxy.
    id: ProxyId,
    /// The executor for the transactions.
    executor: E,
    /// The object store.
    store: Store<E>,
    /// The receiver for transactions.
    rx_transactions: Receiver<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
    /// The sender for transactions with results.
    tx_results: Sender<ExecutionResults<E>>,
    /// The receiver for inter-proxy requests.
    rx_inter_proxy_requests: Receiver<ProxyToProxyMessage>,
    /// The sender for inter-proxy replies.
    tx_inter_proxy_replies: Arc<DashMap<ProxyId, Sender<ProxyToProxyMessage>>>,
    /// The dependency controller for multi-core tx execution.
    stateful_controller: Arc<VersionedDependencyController>,
    /// The dependency controller for stateless transactions.
    stateless_controller: Arc<OneshotDependencyController>,
    /// The proxy mode (separation or no separation)
    mode: SeparationMode,
    /// The  metrics for the proxy
    metrics: Arc<Metrics>,
}

impl<E: Executor + Send + Sync + 'static> ProxyCore<E>
where
    E: Send + 'static,
    Store<E>: Send + Sync,
    RemoraTransaction<E>: Send + Sync,
    ExecutionResults<E>: Send + Sync,
    <E as Executor>::ExecutionContext: Send + Sync,
    <E as Executor>::Transaction: Send,
{
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: E,
        store: Store<E>,
        rx_transactions: Receiver<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
        tx_results: Sender<ExecutionResults<E>>,
        rx_inter_proxy_requests: Receiver<ProxyToProxyMessage>,
        tx_inter_proxy_replies: Arc<DashMap<ProxyId, Sender<ProxyToProxyMessage>>>,
        mode: SeparationMode,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            id,
            executor,
            store,
            rx_transactions,
            tx_results,
            rx_inter_proxy_requests,
            tx_inter_proxy_replies,
            stateful_controller: Arc::new(VersionedDependencyController::new()),
            stateless_controller: Arc::new(OneshotDependencyController::new()),
            mode,
            metrics,
        }
    }

    /// Sptransaction_awn the proxy in a new task.
    pub fn spawn(self) -> Vec<JoinHandle<NodeResult<()>>>
    where
        <E as Executor>::Transaction: Send + Sync,
    {
        let mut primary_processor = PrimaryMessageProcessor {
            id: self.id,
            executor: self.executor.clone(),
            store: self.store.clone(),
            tx_results: self.tx_results,
            tx_inter_proxy_replies: self.tx_inter_proxy_replies.clone(),
            stateful_controller: self.stateful_controller.clone(),
            stateless_controller: self.stateless_controller.clone(),
            mode: self.mode.clone(),
            metrics: self.metrics.clone(),
        };

        let mut proxy_processor = ProxyMessageProcessor::<E> {
            id: self.id,
            store: self.store,
            tx_inter_proxy_replies: self.tx_inter_proxy_replies,
            stateful_controller: self.stateful_controller,
            stateless_controller: self.stateless_controller,
            mode: self.mode,
        };

        let primary_handle = tokio::spawn(async move {
            primary_processor.run(self.rx_transactions).await;
            Ok(())
        });

        let proxy_handle = tokio::spawn(async move {
            proxy_processor.run(self.rx_inter_proxy_requests).await;
            Ok(())
        });

        vec![primary_handle, proxy_handle]
    }
}

#[cfg(test)]
mod tests {
    use dashmap::DashMap;
    use std::{collections::BTreeMap, sync::Arc};
    use sui_types::base_types::{ObjectID, SequenceNumber};
    use tokio::sync::mpsc;

    use crate::{
        config::{BenchmarkParameters, SeparationMode},
        executor::{
            api::{
                ExecutableTransaction, ExecutionResults, Executor, PrimaryToProxyMessage,
                ProxyToProxyMessage, RemoraTransaction,
            },
            fake::FakeExecutor,
            sui::SuiExecutor,
        },
        metrics::Metrics,
        primary::shared_obj_txn_forwarder::VersionAssignmentTask,
        proxy::core::ProxyCore,
    };

    async fn setup_proxy<E: Executor + Send + Sync + 'static>(
        executor: E,
        proxy_id: usize,
    ) -> (
        ProxyCore<E>,
        mpsc::Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
        mpsc::Receiver<ExecutionResults<E>>,
        mpsc::Sender<ProxyToProxyMessage>,
        Arc<DashMap<usize, mpsc::Sender<ProxyToProxyMessage>>>,
    )
    where
        <E as Executor>::ExecutionResults: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
        <E as Executor>::Store: Send + Sync,
    {
        // Create channels
        let (tx_to_proxy, rx_transactions) = mpsc::channel(100);
        let (tx_results, rx_results) = mpsc::channel(100);
        let (tx_inter_proxy_requests, rx_inter_proxy_requests) = mpsc::channel(100);
        let tx_inter_proxy_replies = Arc::new(DashMap::new());

        // Initialize store
        let store = executor.init_store();
        let metrics = Arc::new(Metrics::new_for_tests());

        // Create proxy
        let proxy = ProxyCore::<E>::new(
            proxy_id,
            executor,
            store.into(),
            rx_transactions,
            tx_results,
            rx_inter_proxy_requests,
            tx_inter_proxy_replies.clone(),
            SeparationMode::NoSeparation,
            metrics,
        );

        (
            proxy,
            tx_to_proxy,
            rx_results,
            tx_inter_proxy_requests,
            tx_inter_proxy_replies,
        )
    }

    async fn process_transaction<E: Executor + Send + Sync + 'static>(
        executor: E,
        config: &BenchmarkParameters,
    ) -> bool
    where
        <E as Executor>::ExecutionResults: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
        <E as Executor>::Store: Send + Sync,
    {
        // Setup proxy
        let (proxy, tx_to_proxy, mut rx_results, _, _) = setup_proxy(executor.clone(), 0).await;

        // Spawn the proxy
        let proxy_handles = proxy.spawn();

        // Generate transactions
        let transactions = E::generate_transactions(config, None).await;

        // Set up version assignment
        let mut version_assignment_processor = VersionAssignmentTask::<E> {
            shared_object_versions: rustc_hash::FxHashMap::default(),
            _phantom: std::marker::PhantomData,
        };

        // Send all transactions to proxy
        for tx in transactions {
            let transaction = RemoraTransaction::<E>::new_for_tests(tx.clone());

            // First send the stateless transaction
            let stateless_message = PrimaryToProxyMessage::StatelessTxn(
                *transaction.digest(),
                transaction.verification_duration(),
            );
            tx_to_proxy.send(stateless_message).await.unwrap();

            use crate::executor::api::TransactionWithTimestamp;
            use std::time::Duration;
            let timestamp = Metrics::now().as_secs_f64();
            let mut transaction_with_timestamp = TransactionWithTimestamp::new(
                tx.clone(),
                timestamp,
                tx.shared_object_ids(),
                Duration::from_secs(0),
                Duration::from_secs(0),
            );
            let required_versions = version_assignment_processor
                .assign_shared_object_versions(&mut transaction_with_timestamp);

            // Build required_states as a BTreeMap<(ObjectID, SequenceNumber), Option<usize>>
            // If required_versions is empty, this will be an empty BTreeMap
            let required_states: BTreeMap<(ObjectID, SequenceNumber), Option<usize>> =
                if !required_versions.is_empty() {
                    required_versions
                        .into_iter()
                        .map(|(obj_id, seq_num)| ((obj_id, seq_num), None))
                        .collect()
                } else {
                    BTreeMap::new()
                };

            // Then send the stateful transaction with required_states (empty or not)
            let stateful_message = PrimaryToProxyMessage::Txn(
                Arc::new(RemoraTransaction::<E>::new_for_tests(tx)),
                0,
                required_states,
            );
            tx_to_proxy.send(stateful_message).await.unwrap();
        }

        // Receive and check results - wait for at least one successful result
        let mut success = false;
        while let Ok(Some(execution_result)) =
            tokio::time::timeout(std::time::Duration::from_secs(5), rx_results.recv()).await
        {
            if execution_result.success() {
                success = true;
                break;
            }
        }

        // Clean up
        drop(tx_to_proxy);
        for handle in proxy_handles {
            handle.await.unwrap().unwrap();
        }

        success
    }

    #[tokio::test]
    // #[ignore = "currently fake txns are not supported"]
    async fn test_proxy_processes_fake_transaction() {
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(success, "Transaction should be processed successfully");
    }

    #[tokio::test]
    async fn test_proxy_stateless_transaction() {
        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;

        // Setup proxy
        let (proxy, tx_to_proxy, _, _, _) = setup_proxy(executor.clone(), 0).await;

        // Spawn the proxy
        let _proxy_handles = proxy.spawn();

        // Generate a transaction
        let transactions = SuiExecutor::generate_transactions(&config, None).await;
        let transaction = RemoraTransaction::<SuiExecutor>::new_for_tests(transactions[0].clone());

        // Send stateless transaction to proxy
        let message = PrimaryToProxyMessage::StatelessTxn(
            *transaction.digest(),
            transaction.verification_duration(),
        );
        tx_to_proxy.send(message).await.unwrap();

        // Allow time for processing
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Now send the stateful part
        let message = PrimaryToProxyMessage::Txn(Arc::new(transaction.clone()), 0, BTreeMap::new());
        tx_to_proxy.send(message).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_inter_proxy_communication_via_stateless_result() {
        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;

        // Setup two proxies
        let (proxy1, tx_to_proxy1, _, tx_inter_proxy_requests1, tx_inter_proxy_replies1) =
            setup_proxy(executor.clone(), 0).await;
        let (proxy2, tx_to_proxy2, _, tx_inter_proxy_requests2, tx_inter_proxy_replies2) =
            setup_proxy(executor.clone(), 1).await;

        // Connect the proxies
        tx_inter_proxy_replies1.insert(1, tx_inter_proxy_requests2.clone());
        tx_inter_proxy_replies2.insert(0, tx_inter_proxy_requests1.clone());

        // Spawn the proxies
        let _proxy1_handles = proxy1.spawn();
        let _proxy2_handles = proxy2.spawn();

        // Generate a transaction
        let transactions = SuiExecutor::generate_transactions(&config, None).await;
        let transaction = RemoraTransaction::<SuiExecutor>::new_for_tests(transactions[0].clone());

        // Send stateless transaction to proxy2
        let message = PrimaryToProxyMessage::StatelessTxn(
            *transaction.digest(),
            transaction.verification_duration(),
        );
        tx_to_proxy2.send(message).await.unwrap();

        // Send transaction to proxy1, but indicate stateless result is on proxy2
        let required_states: BTreeMap<(ObjectID, SequenceNumber), Option<usize>> = BTreeMap::new();
        let message = PrimaryToProxyMessage::Txn(Arc::new(transaction.clone()), 1, required_states);
        tx_to_proxy1.send(message).await.unwrap();
    }

    #[tokio::test]
    // #[ignore = "currently fake txns are not supported"]
    async fn test_proxy_fake_transactions() {
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(success, "Fake transaction should be processed successfully");
    }

    #[tokio::test]
    // #[ignore = "currently fake txns are not supported"]
    async fn test_proxy_fake_transactions_contention() {
        let config = BenchmarkParameters::new_for_fake_contention_tests();
        let executor = FakeExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(
            success,
            "Contention transaction should be processed successfully"
        );
    }

    #[tokio::test]
    async fn test_proxy_sui_transactions() {
        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(success, "Sui transaction should be processed successfully");
    }

    #[tokio::test]
    async fn test_proxy_sui_transactions_contention() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();

        let config = BenchmarkParameters::new_for_contention_tests();
        let executor = SuiExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(success, "Sui transaction should be processed successfully");
    }

    #[tokio::test]
    async fn test_proxy_ethereum_transactions() {
        let config = BenchmarkParameters::new_for_ethereum_tests();
        let executor = SuiExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(success, "Sui transaction should be processed successfully");
    }
}
