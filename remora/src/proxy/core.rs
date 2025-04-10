// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
    sync::Arc,
};

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
            InterProxyRequest, PrimaryToProxyMessage, ProxyToProxyMessage, RemoraTransaction,
            StateStore, Store,
        },
        dependency_controller::DependencyController,
        sui::get_object_ids_for_dependency_tracking,
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
    tx_inter_proxy_replies: HashMap<ProxyId, Sender<ProxyToProxyMessage>>,
    /// The dependency controller for multi-core tx execution.
    dependency_controller: DependencyController,
    /// The buffer of stateless transactions.
    stateless_txn_results: DashMap<TransactionDigest, bool>,
    /// The buffer of pending stateful transactions which are waiting for the stateless results.
    pending_stateful_txns: DashMap<TransactionDigest, RemoraTransaction<E>>,
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
        tx_inter_proxy_replies: HashMap<ProxyId, Sender<ProxyToProxyMessage>>,
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
            dependency_controller: DependencyController::new(),
            stateless_txn_results: DashMap::new(),
            pending_stateful_txns: DashMap::new(),
            metrics,
        }
    }

    /// Run the proxy.
    async fn run(&mut self) -> NodeResult<()> {
        tracing::info!("Proxy {} started", self.id);
        loop {
            tokio::select! {
                // handle transactions from the primary
                Some(message) = self.rx_transactions.recv() => {
                    self.handle_primary_message(message).await;
                }

                // handle inter-proxy messages (request or reply)
                Some(message) = self.rx_inter_proxy_requests.recv() => {
                    self.handle_proxy_message(message).await;
                }
            }
        }
    }

    async fn handle_primary_message(
        &mut self,
        message: PrimaryToProxyMessage<<E as Executor>::Transaction>,
    ) {
        match message {
            PrimaryToProxyMessage::Txn(transaction, stateless_res_proxy_id) => {
                if stateless_res_proxy_id == self.id {
                    return self.process_stateful_transaction(transaction).await;
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
                    .insert(*transaction.digest(), transaction);
            }

            PrimaryToProxyMessage::StatelessTxn(transaction) => {
                self.process_stateless_transaction(transaction).await
            }

            _ => {
                tracing::warn!("Received unexpected message");
            }
        }
    }

    async fn process_stateless_transaction(&self, transaction: RemoraTransaction<E>) {
        let res = E::verify_transaction(self.executor.context().clone(), &transaction).await;
        self.stateless_txn_results
            .insert(*transaction.digest(), res);
    }

    async fn handle_proxy_message(&mut self, message: ProxyToProxyMessage) {
        match message {
            ProxyToProxyMessage::Request(request) => match request {
                InterProxyRequest::Stateful(proxy_id, requested_states) => {
                    self.handle_stateful_request(proxy_id, requested_states)
                        .await;
                }
                InterProxyRequest::Stateless(proxy_id, txn_digest) => {
                    self.handle_stateless_request(proxy_id, txn_digest).await;
                }
            },
            ProxyToProxyMessage::Reply(reply) => match reply {
                InterProxyReply::Stateful(objects) => {
                    self.handle_stateful_reply(objects).await;
                }
                InterProxyReply::Stateless(digest, result) => {
                    self.handle_stateless_reply(digest, result).await;
                }
            },
        }
    }

    async fn handle_stateful_reply(&mut self, objects: BTreeMap<ObjectID, Object>) {
        // TODO: schedule commit and notify
        // Update local store with received objects
        self.store.commit_new_objects(objects);
    }

    async fn handle_stateless_reply(&mut self, digest: TransactionDigest, result: bool) {
        // Process the transaction if verification passed, otherwise just remove it
        if let Some((_, transaction)) = self.pending_stateful_txns.remove(&digest) {
            if result {
                self.process_stateful_transaction(transaction).await;
            }
        }
    }

    async fn handle_stateful_request(
        &mut self,
        proxy_id: ProxyId,
        requested_states: Vec<(ObjectID, SequenceNumber)>,
    ) {
        let mut objects = BTreeMap::new();
        for state in requested_states {
            let object = match self.store.read_object(&state.0) {
                Ok(obj) => obj,
                Err(e) => {
                    tracing::warn!("Failed to read object: {:?}", e);
                    None
                }
            };
            // TODO: check version
            if let Some(obj) = object {
                objects.insert(state.0, obj);
            }
        }

        let reply = InterProxyReply::Stateful(objects);
        self.send_msg_to_proxy(proxy_id, ProxyToProxyMessage::Reply(reply))
            .await;
    }

    async fn handle_stateless_request(&mut self, proxy_id: ProxyId, txn_digest: TransactionDigest) {
        let verification_result = self
            .stateless_txn_results
            .remove(&txn_digest)
            .unwrap_or((txn_digest, false));

        let reply = InterProxyReply::Stateless(verification_result.0, verification_result.1);
        self.send_msg_to_proxy(proxy_id, ProxyToProxyMessage::Reply(reply))
            .await;
    }

    async fn send_msg_to_proxy(&self, proxy_id: ProxyId, message: ProxyToProxyMessage) {
        if let Some(tx) = self.tx_inter_proxy_replies.get(&proxy_id) {
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

    #[deprecated]
    /// Process a single transaction in single-threaded mode.
    async fn process_transaction_single_threaded(
        &mut self,
        transaction: RemoraTransaction<E>,
    ) -> bool {
        // Assign shared objects version.
        self.executor
            .assign_shared_object_versions(&[transaction.deref().clone()])
            .await;

        self.metrics.increase_proxy_load(self.id);

        let ctx = self.executor.context().clone();
        let store = self.store.clone();

        // Check and prepare objects
        if !E::pre_execute_check_objects(store.clone(), &transaction) {
            E::optimistically_pre_generate_objects(store.clone(), &transaction);
        }

        // Execute the transaction
        let execution_result = E::execute(ctx, store.clone(), transaction).await;

        self.metrics.decrease_proxy_load(&self.id);

        // Send the result back
        if self.tx_results.send(execution_result).await.is_err() {
            tracing::warn!(
                "Failed to send execution result, stopping proxy {}",
                self.id
            );
            return false;
        }
        true
    }

    /// Process a single transaction in multi-threaded mode.
    async fn process_stateful_transaction(
        &mut self,
        transaction: RemoraTransaction<E>,
        // task_id: u64,
    ) {
        // Assign shared objects version.
        self.executor
            .assign_shared_object_versions(&[transaction.deref().clone()])
            .await;

        // let new_task_id = if task_id == 0 {
        //     self.metrics.register_start_time();
        //     1
        // } else {
        //     task_id + 1
        // };

        self.metrics.increase_proxy_load(self.id);

        // Check and prepare objects
        if !E::pre_execute_check_objects(self.store.clone(), &transaction) {
            E::optimistically_pre_generate_objects(self.store.clone(), &transaction);
        }

        // Get dependencies and schedule the transaction
        let (prior_handles, current_handles) = self.get_dependencies(transaction.clone(), 0);
        self.schedule_txn_parallel(transaction, prior_handles, current_handles)
            .await
            .expect("Failed to schedule transaction");
    }

    pub fn get_dependencies(
        &mut self,
        transaction: RemoraTransaction<E>,
        task_id: u64,
    ) -> (Vec<Arc<Notify>>, Vec<Arc<Notify>>) {
        let obj_ids = get_object_ids_for_dependency_tracking::<E>(transaction);

        self.dependency_controller
            .get_dependencies(task_id, obj_ids)
    }

    pub async fn schedule_txn_parallel(
        &mut self,
        transaction: RemoraTransaction<E>,
        prior_handles: Vec<Arc<Notify>>,
        current_handles: Vec<Arc<Notify>>,
    ) -> NodeResult<()> {
        let store = self.store.clone();
        let id = self.id.clone();
        let tx_results = self.tx_results.clone();
        let ctx = self.executor.context().clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }

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
                E::execute(ctx, store, transaction.clone()).await
            } else {
                tracing::warn!("Proxy skipped execution");
                ExecutionResults::<E>::new(transaction.clone(), None, None)
            };

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
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>> {
        tokio::spawn(async move { self.run().await })
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::{
            api::{Executor, PrimaryToProxyMessage, RemoraTransaction},
            fake::FakeExecutor,
            sui::SuiExecutor,
        },
        metrics::Metrics,
        proxy::core::ProxyCore,
    };

    async fn pre_execute<E: Executor + Send + Sync + 'static>(
        executor: E,
        config: BenchmarkParameters,
    ) where
        <E as Executor>::ExecutionResults: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
        <E as Executor>::Store: Send + Sync,
    {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);
        let (tx_inter_proxy_requests, rx_inter_proxy_requests) = mpsc::channel(100);
        let tx_inter_proxy_replies = HashMap::new();

        let store = executor.init_store();
        let metrics = Arc::new(Metrics::new_for_tests());
        let proxy_id = 0;
        let proxy = ProxyCore::<E>::new(
            proxy_id,
            executor,
            store.into(),
            rx_proxy,
            tx_results,
            rx_inter_proxy_requests,
            tx_inter_proxy_replies,
            metrics,
        );

        // Send transactions to the proxy.
        let transactions = E::generate_transactions(&config, None).await;
        for tx in transactions {
            let transaction = RemoraTransaction::<E>::new_for_tests(tx);
            let message = PrimaryToProxyMessage::Txn(transaction);
            tx_proxy.send(message).await.unwrap();
        }

        // Spawn the proxy.
        proxy.spawn();

        // Receive the results.
        let results = rx_results.recv().await.unwrap();
        assert!(results.success());
    }

    #[tokio::test]
    async fn test_proxy() {
        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        pre_execute::<SuiExecutor>(executor, config).await;
    }

    #[tokio::test]
    async fn test_proxy_fake_transactions() {
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;
        pre_execute::<FakeExecutor>(executor, config).await;
    }

    #[tokio::test]
    async fn test_proxy_fake_transactions_contention() {
        let config = BenchmarkParameters::new_for_fake_contention_tests();
        let executor = FakeExecutor::new(&config).await;
        pre_execute::<FakeExecutor>(executor, config).await;
    }
}
