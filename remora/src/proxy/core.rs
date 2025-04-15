// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
    object::Object,
    transaction::InputObjectKind,
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Notify,
    },
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::{
        api::{
            ExecutableTransaction, ExecutionResults, Executor, ExecutorIndex, InterProxyReply,
            InterProxyRequest, MissingStates, PrimaryToProxyMessage, ProxyToProxyMessage,
            RemoraTransaction, StateStore, Store,
        },
        oneshot_dependency_controller::OneshotDependencyController,
        versioned_dependency_controller::VersionedDependencyController,
    },
    metrics::Metrics,
};

pub type ProxyId = ExecutorIndex;

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
    /// The buffer of pending stateful transactions which are waiting for the stateless results.
    pending_stateful_txns: DashMap<TransactionDigest, (RemoraTransaction<E>, MissingStates)>,
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
            pending_stateful_txns: DashMap::new(),
            metrics,
        }
    }

    /// Run the proxy.
    async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Proxy {} started", self.id);
        let mut task_id = 0;
        loop {
            tokio::select! {
                // handle transactions from the primary
                Some(message) = self.rx_transactions.recv() => {
                    if task_id == 0 {
                        self.metrics.register_start_time();
                    }
                    task_id += 1;

                    self.handle_primary_message(message).await;
                }

                // handle inter-proxy messages (request or reply)
                Some(message) = self.rx_inter_proxy_requests.recv() => {
                    self.handle_proxy_message(message).await;
                }

                // Both channels are closed, exit the loop
                else => {
                    tracing::info!("Proxy {} shutting down: all channels closed", self.id);
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_primary_message(
        &mut self,
        message: PrimaryToProxyMessage<<E as Executor>::Transaction>,
    ) {
        match message {
            PrimaryToProxyMessage::StatelessTxn(transaction) => {
                tracing::debug!(
                    "Proxy {} received stateless transaction {:?}",
                    self.id,
                    transaction.digest()
                );
                self.process_stateless_transaction(transaction).await
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
        }
    }

    async fn process_stateful_transaction(
        &mut self,
        transaction: RemoraTransaction<E>,
        stateless_res_proxy_id: ProxyId,
        missing_states: MissingStates,
    ) {
        if stateless_res_proxy_id == self.id {
            let rx = self
                .stateless_controller
                .get_dependencies(transaction.digest())
                .unwrap();

            // Check the stateless verification result before scheduling
            match rx.await {
                Ok(true) => {
                    self.schedule_stateful_transaction(transaction, missing_states)
                        .await;
                    return;
                }
                Ok(false) => {
                    tracing::debug!(
                        "Proxy {} skipping transaction {:?} due to failed stateless verification",
                        self.id,
                        transaction.digest()
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        "Proxy {} failed to get stateless result for {:?}: {:?}",
                        self.id,
                        transaction.digest(),
                        e
                    );
                    return;
                }
            }
        }

        // Send stateless request to the appropriate proxy
        let request = InterProxyRequest::Stateless(self.id, *transaction.digest());
        let tx = self
            .tx_inter_proxy_replies
            .get(&stateless_res_proxy_id)
            .unwrap();
        if let Err(e) = tx.send(ProxyToProxyMessage::Request(request)).await {
            tracing::error!("Failed to send stateless request: {}", e);
        }

        self.pending_stateful_txns
            .insert(*transaction.digest(), (transaction, missing_states));
    }

    async fn process_stateless_transaction(&self, transaction: RemoraTransaction<E>) {
        tracing::debug!(
            "Proxy {} processing stateless transaction {:?}",
            self.id,
            transaction.digest()
        );

        let tx = self
            .stateless_controller
            .set_dependency(*transaction.digest());

        let context = self.executor.context().clone();
        let id = self.id;
        tokio::spawn(async move {
            let res = E::verify_transaction(context, &transaction).await;
            tracing::debug!(
                "Proxy {} completed stateless verification for {:?}, result: {}",
                id,
                transaction.digest(),
                res
            );
            tx.send(res).expect("Failed to send result");
        });
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

        // Process the transaction if verification passed, otherwise just remove it
        if let Some((_, (transaction, missing_states))) = self.pending_stateful_txns.remove(&digest)
        {
            if result {
                tracing::debug!(
                    "Proxy {} scheduling stateful transaction {:?} after stateless verification",
                    self.id,
                    digest
                );
                self.schedule_stateful_transaction(transaction, missing_states)
                    .await;
            } else {
                tracing::debug!(
                    "Proxy {} discarding transaction {:?} due to failed stateless verification",
                    self.id,
                    digest
                );
            }
        }
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
        tokio::spawn(async move {
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }
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
            .get_dependencies(&txn_digest)
            .unwrap();
        let verification_result = rx.await.unwrap();

        let reply = InterProxyReply::Stateless(*txn_digest, verification_result);
        Self::send_msg_to_proxy(
            self.tx_inter_proxy_replies.clone(),
            proxy_id,
            ProxyToProxyMessage::Reply(reply),
        )
        .await;
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

    /// Schedule a stateful transaction in multi-threaded mode.
    /// including sending requests for missing states to other proxies.
    async fn schedule_stateful_transaction(
        &mut self,
        transaction: RemoraTransaction<E>,
        missing_states: MissingStates,
        // task_id: u64,
    ) {
        tracing::debug!(
            "Proxy {} scheduling stateful transaction {:?} with {} missing states",
            self.id,
            transaction.digest(),
            missing_states.len()
        );

        // Add tracing for required versions using the executor API
        let required_versions = self
            .executor
            .get_required_shared_object_versions(&transaction.digest())
            .await;

        tracing::debug!(
            "Transaction {:?} required versions: {:?}",
            transaction.digest(),
            required_versions
        );

        // Send requests for missing states to other proxies
        if required_versions.is_some() {
            let tx_inter_proxy_replies = self.tx_inter_proxy_replies.clone();
            let mut proxy_requests: BTreeMap<ProxyId, Vec<(ObjectID, SequenceNumber)>> =
                BTreeMap::new();

            for (obj_id, seq_num) in required_versions.unwrap() {
                if let Some(&proxy_id) = missing_states.get(&obj_id) {
                    proxy_requests
                        .entry(proxy_id)
                        .or_default()
                        .push((obj_id, seq_num));
                }
            }

            for (proxy_id, states) in proxy_requests {
                let request = InterProxyRequest::Stateful(self.id, states);
                Self::send_msg_to_proxy(
                    tx_inter_proxy_replies.clone(),
                    proxy_id,
                    ProxyToProxyMessage::Request(request),
                )
                .await;
            }
        }

        // TODO: check if assigning before all the states are received making sense
        // Assign shared objects version.
        self.executor
            .assign_shared_object_versions(&[transaction.deref().clone()])
            .await;

        self.metrics.increase_proxy_load(self.id);

        // Check and prepare objects
        if !E::pre_execute_check_objects(self.store.clone(), &transaction) {
            tracing::debug!(
                "Proxy {} optimistically pre-generating objects for transaction {:?}",
                self.id,
                transaction.digest()
            );
            E::optimistically_pre_generate_objects(self.store.clone(), &transaction);
        }

        // Get dependencies and schedule the transaction
        let (obj_ids, prior_handles, current_handles) = self.get_dependencies(transaction.clone());
        tracing::debug!(
            "Proxy {} got dependencies for transaction {:?}: {} objects",
            self.id,
            transaction.digest(),
            obj_ids.len()
        );

        self.spawn_stateful_txn(transaction, obj_ids, prior_handles, current_handles)
            .await
            .expect("Failed to schedule transaction");
    }

    pub fn get_dependencies(
        &mut self,
        transaction: RemoraTransaction<E>,
    ) -> (
        Vec<(ObjectID, SequenceNumber)>,
        Vec<Arc<Notify>>,
        Vec<Arc<Notify>>,
    ) {
        let obj_ids = E::get_objects_for_dependency_tracking(
            self.executor.context().clone(),
            self.store.clone(),
            transaction,
        );

        // If there are no object dependencies, return empty vectors for handles
        if obj_ids.is_empty() {
            return (obj_ids, Vec::new(), Vec::new());
        }

        let (prior_handles, current_handles) = self
            .stateful_controller
            .get_prior_dependency_and_update(0, obj_ids.clone(), false, false);

        (obj_ids, prior_handles, current_handles)
    }

    pub async fn spawn_stateful_txn(
        &mut self,
        transaction: RemoraTransaction<E>,
        obj_ids: Vec<(ObjectID, SequenceNumber)>,
        prior_handles: Vec<Arc<Notify>>,
        current_handles: Vec<Arc<Notify>>,
    ) -> NodeResult<()> {
        tracing::debug!(
            "Proxy {} spawning stateful transaction {:?}",
            self.id,
            transaction.digest()
        );

        let store = self.store.clone();
        let id = self.id.clone();
        let tx_results = self.tx_results.clone();
        let ctx = self.executor.context().clone();
        let metrics = self.metrics.clone();
        let stateful_controller = self.stateful_controller.clone();
        tokio::spawn(async move {
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }
            stateful_controller.remove_dependency(obj_ids.clone());

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
                E::execute(ctx, store, transaction.clone()).await
            } else {
                tracing::warn!(
                    "Proxy {} skipped execution for transaction {:?}",
                    id,
                    transaction.digest()
                );
                ExecutionResults::<E>::new(transaction.clone(), None, None)
            };

            tracing::debug!(
                "Proxy {} completed execution for transaction {:?}, success: {}",
                id,
                transaction.digest(),
                execution_result.success()
            );

            tx_results
                .send(execution_result)
                .await
                .map_err(|_| NodeError::ShuttingDown)?;

            for notify in current_handles {
                notify.notify_one();
            }

            metrics.decrease_proxy_load(&id);
            metrics.update_metrics(transaction.timestamp());
            Ok::<_, NodeError>(())
        });
        Ok(())
    }

    /// Sptransaction_awn the proxy in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        <E as Executor>::Transaction: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}

#[cfg(test)]
mod tests {
    use dashmap::DashMap;
    use std::{collections::BTreeMap, sync::Arc};
    use sui_types::base_types::ObjectID;
    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::{
            api::{
                ExecutionResults, Executor, PrimaryToProxyMessage, ProxyToProxyMessage,
                RemoraTransaction,
            },
            fake::FakeExecutor,
            sui::SuiExecutor,
        },
        metrics::Metrics,
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
        let proxy_handle = proxy.spawn();

        // Generate transactions
        let transactions = E::generate_transactions(config, None).await;

        // Send all transactions to proxy
        for tx in transactions {
            let transaction = RemoraTransaction::<E>::new_for_tests(tx.clone());

            // First send the stateless transaction
            let stateless_message = PrimaryToProxyMessage::StatelessTxn(transaction.clone());
            tx_to_proxy.send(stateless_message).await.unwrap();

            // Then send the stateful transaction
            let stateful_message = PrimaryToProxyMessage::Txn(
                RemoraTransaction::<E>::new_for_tests(tx),
                0,
                BTreeMap::new(),
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
        let _ = proxy_handle.await;

        success
    }

    #[tokio::test]
    #[ignore = "currently fake txns are not supported"]
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
        let _proxy_handle = proxy.spawn();

        // Generate a transaction
        let transactions = SuiExecutor::generate_transactions(&config, None).await;
        let transaction = RemoraTransaction::<SuiExecutor>::new_for_tests(transactions[0].clone());

        // Send stateless transaction to proxy
        let message = PrimaryToProxyMessage::StatelessTxn(transaction.clone());
        tx_to_proxy.send(message).await.unwrap();

        // Allow time for processing
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Now send the stateful part
        let message = PrimaryToProxyMessage::Txn(transaction, 0, BTreeMap::new());
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
        let _proxy1_handle = proxy1.spawn();
        let _proxy2_handle = proxy2.spawn();

        // Generate a transaction
        let transactions = SuiExecutor::generate_transactions(&config, None).await;
        let transaction = RemoraTransaction::<SuiExecutor>::new_for_tests(transactions[0].clone());

        // Send stateless transaction to proxy2
        let message = PrimaryToProxyMessage::StatelessTxn(transaction.clone());
        tx_to_proxy2.send(message).await.unwrap();

        // Send transaction to proxy1, but indicate stateless result is on proxy2
        let missing_states: BTreeMap<ObjectID, usize> = BTreeMap::new();
        let message = PrimaryToProxyMessage::Txn(transaction, 1, missing_states);
        tx_to_proxy1.send(message).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "currently fake txns are not supported"]
    async fn test_proxy_fake_transactions() {
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(success, "Fake transaction should be processed successfully");
    }

    #[tokio::test]
    #[ignore = "currently fake txns are not supported"]
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
        let config = BenchmarkParameters::new_for_contention_tests();
        let executor = SuiExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(success, "Sui transaction should be processed successfully");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_proxy_ethereum_transactions() {
        let config = BenchmarkParameters::new_for_ethereum_tests();
        let executor = SuiExecutor::new(&config).await;

        let success = process_transaction(executor, &config).await;
        assert!(success, "Sui transaction should be processed successfully");
    }
}
