// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{marker::PhantomData, num::NonZeroUsize, sync::Arc};

use crate::config::SeparationMode;
use crate::executor::api::{Executor, RemoraTransaction, StatelessVerificationRequest};
use crate::primary::batch_breakdown::BatchBreakdownCollector;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};

/// Represents a consensus commit.
pub type ConsensusCommit<T> = Vec<T>;

/// The parameters of the mock consensus engine.
#[derive(Serialize, Deserialize, Clone)]
pub struct MockConsensusParameters {
    /// The preferred batch size (in number of transactions).
    batch_size: NonZeroUsize,
    /// The maximum delay after which to seal the batch.
    max_batch_delay: Duration,
    /// The maximum number of batches that can be in-flight at the same time.
    max_inflight_batches: NonZeroUsize,
}

impl Default for MockConsensusParameters {
    fn default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(1000).unwrap(),
            max_batch_delay: Duration::from_millis(100),
            max_inflight_batches: NonZeroUsize::new(10_000).unwrap(),
        }
    }
}

/// A trait for consensus delay models.
pub trait DelayModel<T> {
    /// Wait for the consensus to commit a batch of transactions.
    fn consensus_delay(
        &self,
        batch: ConsensusCommit<T>,
    ) -> impl Future<Output = ConsensusCommit<T>> + Send;
}

/// Mock consensus engine. It assembles transactions into batches of a preset size and sends them
/// to the primary executor after a specific delay (emulating the consensus latency).
// TODO: Replace the `Receiver` and `Sender` with their bounded counter parts
// to apply back pressure on the network.
pub struct MockConsensus<M, E: Executor + Send + 'static> {
    /// The executor used to build stateless verification requests.
    executor: E,
    /// The consensus delay model.
    model: M,
    /// The parameters of the mock consensus engine.
    parameters: MockConsensusParameters,
    /// Channel to receive transactions from the network.
    rx_load_balancer: Receiver<RemoraTransaction<E>>,
    /// Output channel to deliver mocked consensus commits to the primary executor.
    tx_primary_executor: Sender<ConsensusCommit<RemoraTransaction<E>>>,
    /// Channel to send stateless transactions to the load balancer.
    tx_stateless_txns: Sender<StatelessVerificationRequest>,
    /// Channel to pre-consensus scheduling of stateful txns.
    tx_pre_consensus_scheduling: Sender<Vec<RemoraTransaction<E>>>,
    /// Holds the current batch.
    current_batch: ConsensusCommit<RemoraTransaction<E>>,
    /// The number of batches currently in-flight.
    current_inflight_batches: usize,
    /// The proxy mode.
    separation_mode: SeparationMode,
    /// Batch-level latency breakdown collector.
    batch_breakdown: Arc<BatchBreakdownCollector>,
    /// The phantom data for the executor.
    _phantom: PhantomData<E>,
}

impl<M, E: Executor + Send + 'static> MockConsensus<M, E>
where
    <E as Executor>::Transaction: Send + Sync + 'static,
{
    /// Create a new mock consensus engine.
    pub(crate) fn new(
        executor: E,
        model: M,
        parameters: MockConsensusParameters,
        rx_load_balancer: Receiver<RemoraTransaction<E>>,
        tx_primary_executor: Sender<ConsensusCommit<RemoraTransaction<E>>>,
        tx_stateless_txns: Sender<StatelessVerificationRequest>,
        tx_pre_consensus_scheduling: Sender<ConsensusCommit<RemoraTransaction<E>>>,
        separation_mode: SeparationMode,
        batch_breakdown: Arc<BatchBreakdownCollector>,
    ) -> Self {
        let batch_size = parameters.batch_size.get();
        Self {
            executor,
            model,
            parameters,
            rx_load_balancer,
            tx_primary_executor,
            tx_stateless_txns,
            tx_pre_consensus_scheduling,
            current_batch: Vec::with_capacity(batch_size),
            current_inflight_batches: 0,
            separation_mode,
            batch_breakdown,
            _phantom: PhantomData,
        }
    }
}

impl<M: DelayModel<RemoraTransaction<E>>, E: Executor + Send + 'static> MockConsensus<M, E>
where
    <E as Executor>::Transaction: Send + Sync + 'static,
{
    /// Run the mock consensus engine.
    pub async fn run(&mut self) {
        let timer = sleep(self.parameters.max_batch_delay);
        tokio::pin!(timer);

        // Holds the futures of the in-flight batches waiting to be committed.
        let mut waiter = FuturesUnordered::new();

        let max_inflight_batches = self.parameters.max_inflight_batches.get();
        let batch_size = self.parameters.batch_size.get();
        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size. If there are too many
                // in-flight batches, wait for some to complete before accepting new transactions.
                Some(transaction) = self.rx_load_balancer.recv(),
                    if self.current_inflight_batches < max_inflight_batches => {
                    if self.separation_mode == SeparationMode::PrimaryPreSeparation {
                        self.tx_stateless_txns
                            .send(self.executor.make_verification_request(&transaction))
                            .await
                            .unwrap();
                    }

                    self.current_batch.push(transaction);
                    if self.current_batch.len() >= batch_size {
                        self.current_inflight_batches += 1;
                        let batch: Vec<_> = self.current_batch.drain(..).collect();
                        self.batch_breakdown.register_shared_batch(&batch);
                        tracing::debug!("Sealed batch with {} transactions", batch.len());
                        waiter.push(self.model.consensus_delay(batch.clone()));
                        self.tx_pre_consensus_scheduling.send(batch).await.unwrap();
                        timer.as_mut().reset(Instant::now() + self.parameters.max_batch_delay);
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.current_inflight_batches += 1;
                        let batch: Vec<_> = self.current_batch.drain(..).collect();
                        self.batch_breakdown.register_shared_batch(&batch);
                        tracing::debug!("Sealed batch with {} transactions", batch.len());
                        waiter.push(self.model.consensus_delay(batch.clone()));
                        self.tx_pre_consensus_scheduling.send(batch).await.unwrap();
                    } else if self.tx_primary_executor.is_closed() {
                        tracing::warn!("Terminating consensus task: primary executor dropped the channel");
                        break
                    }
                    timer.as_mut().reset(Instant::now() + self.parameters.max_batch_delay);
                }

                // Deliver the consensus commit to the primary executor.
                Some(commit) = waiter.next() => {
                    self.current_inflight_batches -= 1;
                    if self.tx_primary_executor.send(commit).await.is_err() {
                        tracing::warn!("Terminating consensus task: primary executor dropped the channel");
                        break
                    }
                    tracing::debug!("Delivered batch to primary executor");
                }
            }
        }
    }

    /// Spawn the mock consensus engine in a separate task.
    pub fn spawn(mut self) -> JoinHandle<()>
    where
        M: Send + 'static,
        <E as Executor>::Transaction: Send + 'static,
    {
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                self.run().await;
            })
        })
    }
}

/// Models for consensus delay.
pub mod models {
    use std::time::Duration;

    use rand::{thread_rng, Rng};
    use serde::{Deserialize, Serialize};
    use tokio::time::sleep;

    use super::{ConsensusCommit, DelayModel};

    /// A fixed delay model that applies a constant delay to each batch.
    #[derive(Serialize, Deserialize, Clone)]
    pub struct FixedDelay {
        /// The delay to apply to each batch.
        pub delay: Duration,
    }

    impl<T: Send> DelayModel<T> for FixedDelay {
        async fn consensus_delay(&self, batch: ConsensusCommit<T>) -> ConsensusCommit<T> {
            sleep(self.delay).await;
            batch
        }
    }

    impl Default for FixedDelay {
        fn default() -> Self {
            Self {
                delay: Duration::from_millis(300),
            }
        }
    }

    /// A uniform delay model that applies a random delay within a given range to each batch.
    #[derive(Serialize, Deserialize)]
    #[cfg_attr(test, derive(Clone))]
    pub struct UniformDelay {
        /// The minimum delay to apply to each batch.
        pub min_delay: Duration,
        /// The maximum delay to apply to each batch.
        pub max_delay: Duration,
    }

    impl<T: Send> DelayModel<T> for UniformDelay {
        async fn consensus_delay(&self, batch: ConsensusCommit<T>) -> ConsensusCommit<T> {
            let delay = thread_rng().gen_range(self.min_delay..self.max_delay);
            sleep(delay).await;
            batch
        }
    }

    impl Default for UniformDelay {
        fn default() -> Self {
            Self {
                min_delay: Duration::from_millis(100),
                max_delay: Duration::from_millis(500),
            }
        }
    }
}

// TODO: fix the tests (need to feed into correctly generated txns (easy))
/*#[cfg(test)]
mod test {
    use std::{num::NonZeroUsize, time::Duration};

    use tokio::{sync::mpsc, time::Instant};

    use crate::{
        executor::api::{RemoraTransaction, TransactionWithTimestamp},
        primary::mock_consensus::{
        models::{FixedDelay, UniformDelay},
        MockConsensus, MockConsensusParameters,
    }};

    #[tokio::test(start_paused = true)]
    async fn fixed_delay() {
        let model = FixedDelay::default();
        let parameters = MockConsensusParameters {
            batch_size: NonZeroUsize::new(3).unwrap(),
            max_inflight_batches: NonZeroUsize::new(10).unwrap(), // Ensure it is never hit.
            ..MockConsensusParameters::default()
        };

        let (tx_load_balancer, rx_load_balancer) = mpsc::channel(100);
        let (tx_primary_executor, mut rx_primary_executor) = mpsc::channel(100);
        let (tx_stateless_txns, _rx_stateless_txns) = mpsc::channel(100);

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
            tx_stateless_txns,
        )
        .spawn();

        // Send enough transactions to fill two batches.
        let start = Instant::now();
        for i in 0..parameters.batch_size.get() * 2 {
            tx_load_balancer.send(RemoraTransaction::new_for_tests(FakeTransaction::new(i))).await.unwrap();
        }

        // Wait for the consensus to commit the batches.
        let commit_1 = rx_primary_executor.recv().await.unwrap();
        assert_eq!(commit_1, vec![RemoraTransaction::new_for_tests(0), RemoraTransaction::new_for_tests(1), RemoraTransaction::new_for_tests(2)]);
        assert_eq!(start.elapsed(), model.delay);

        let commit_2 = rx_primary_executor.recv().await.unwrap();
        assert_eq!(commit_2, vec![RemoraTransaction::new_for_tests(3), RemoraTransaction::new_for_tests(4), RemoraTransaction::new_for_tests(5)]);
        assert_eq!(start.elapsed(), model.delay);
    }

    #[tokio::test(start_paused = true)]
    async fn uniform_delay() {
        let model = UniformDelay::default();
        let parameters = MockConsensusParameters {
            batch_size: NonZeroUsize::new(3).unwrap(),
            max_inflight_batches: NonZeroUsize::new(10).unwrap(), // Ensure it is never hit.
            ..MockConsensusParameters::default()
        };

        let (tx_load_balancer, rx_load_balancer) = mpsc::channel(100);
        let (tx_primary_executor, mut rx_primary_executor) = mpsc::channel(100);
        let (tx_stateless_txns, _rx_stateless_txns) = mpsc::channel(100);

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
            tx_stateless_txns,
        )
        .spawn();

        // Send enough transactions to fill two batches.
        let start = Instant::now();
        for i in 0..parameters.batch_size.get() * 2 {
            tx_load_balancer.send(i).await.unwrap();
        }

        // Wait for the consensus to commit the batches. Remember that the delay is random and
        // that consecutive batches may be committed in any order.
        let commit_1 = rx_primary_executor.recv().await.unwrap();
        let commit_2 = rx_primary_executor.recv().await.unwrap();
        let end = start.elapsed();

        assert!(end >= model.min_delay);
        assert!(end <= model.max_delay);
        assert!((0..parameters.batch_size.get() * 2)
            .all(|x| commit_1.contains(&x) || commit_2.contains(&x)));
    }

    #[tokio::test(start_paused = true)]
    async fn early_batch_seal() {
        let model = FixedDelay::default();
        let parameters = MockConsensusParameters {
            batch_size: NonZeroUsize::new(3).unwrap(),
            max_batch_delay: Duration::from_millis(100),
            ..MockConsensusParameters::default()
        };

        let (tx_load_balancer, rx_load_balancer) = mpsc::channel(100);
        let (tx_primary_executor, mut rx_primary_executor) = mpsc::channel(100);
        let (tx_stateless_txns, _rx_stateless_txns) = mpsc::channel(100);

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
            tx_stateless_txns,
        )
        .spawn();

        // Do not send enough transactions to seal a batch
        let start = Instant::now();
        tx_load_balancer.send(0).await.unwrap();

        // Wait for the consensus to commit the batches.
        let commit = rx_primary_executor.recv().await.unwrap();
        assert_eq!(commit, vec![0]);
        assert_eq!(start.elapsed(), model.delay + parameters.max_batch_delay);
    }

    #[tokio::test(start_paused = true)]
    async fn hit_max_inflight_batches() {
        let model = FixedDelay::default();
        let parameters = MockConsensusParameters {
            batch_size: NonZeroUsize::new(3).unwrap(),
            max_batch_delay: Duration::from_secs(100), // Ensure it is never hit.
            max_inflight_batches: NonZeroUsize::new(1).unwrap(),
            ..MockConsensusParameters::default()
        };

        let (tx_load_balancer, rx_load_balancer) = mpsc::channel(100);
        let (tx_primary_executor, mut rx_primary_executor) = mpsc::channel(100);
        let (tx_stateless_txns, _rx_stateless_txns) = mpsc::channel(100);

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
            tx_stateless_txns,
        )
        .spawn();

        // Send enough transactions to fill two batches.
        let start = Instant::now();
        for i in 0..parameters.batch_size.get() * 2 {
            tx_load_balancer.send(i).await.unwrap();
        }

        // Wait for the consensus to first commit.
        let commit_1 = rx_primary_executor.recv().await.unwrap();
        assert_eq!(commit_1, vec![0, 1, 2]);
        assert_eq!(start.elapsed(), model.delay);

        // The second commit should only happen after the first one completes.
        let commit_2 = rx_primary_executor.recv().await.unwrap();
        assert_eq!(commit_2, vec![3, 4, 5]);
        assert_eq!(start.elapsed(), model.delay * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn terminate_consensus() {
        let model = FixedDelay::default();
        let parameters = MockConsensusParameters::default();

        let (tx_load_balancer, rx_load_balancer) = mpsc::channel(100);
        let (tx_primary_executor, rx_primary_executor) = mpsc::channel(100);
        let (tx_stateless_txns, _rx_stateless_txns) = mpsc::channel(100);

        let consensus_handle = MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
            tx_stateless_txns,
        )
        .spawn();

        // Close the mock consensus engine.
        drop(rx_primary_executor);
        tx_load_balancer.send(0).await.unwrap();
        consensus_handle.await.unwrap();
    }

    #[tokio::test]
    async fn smoke_test() {
        let model = FixedDelay {
            delay: Duration::from_millis(1), // Ensure the test doesn't last too long.
        };
        let parameters = MockConsensusParameters::default();

        let (tx_load_balancer, rx_load_balancer) = mpsc::channel(100);
        let (tx_primary_executor, mut rx_primary_executor) = mpsc::channel(100);
        let (tx_stateless_txns, mut rx_stateless_txns) = mpsc::channel(100);

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
            tx_stateless_txns,
        )
        .spawn();

        // Send many transactions to the mock consensus engine.
        let total_batches = 100;
        let expected_total_stateless_txns = parameters.batch_size.get() * total_batches;

        let txn = TransactionWithTimestamp::
        tokio::spawn(async move {
            for i in 0..parameters.batch_size.get() * total_batches {
                tx_load_balancer.send(RemoraTransaction::new_for_tests()).await.unwrap();
            }
        });

        let mut batches_received_count = 0;
        let mut stateless_txns_received_count = 0;

        // Loop until all expected items are received from both channels.
        while batches_received_count < total_batches
            || stateless_txns_received_count < expected_total_stateless_txns
        {
            tokio::select! {
                // Only try to receive from rx_primary_executor if we still expect batches.
                maybe_batch = rx_primary_executor.recv(), if batches_received_count < total_batches => {
                    match maybe_batch {
                        Some(_batch) => {
                            batches_received_count += 1;
                        }
                        None => {
                            panic!("Primary executor channel closed before all batches were received. Expected {}, got {}", total_batches, batches_received_count);
                        }
                    }
                },
                // Only try to receive from rx_stateless_txns if we still expect stateless transactions.
                maybe_txn = rx_stateless_txns.recv(), if stateless_txns_received_count < expected_total_stateless_txns => {
                    match maybe_txn {
                        Some(_txn) => {
                            stateless_txns_received_count += 1;
                        }
                        None => {
                            panic!("Stateless transactions channel closed before all transactions were received. Expected {}, got {}", expected_total_stateless_txns, stateless_txns_received_count);
                        }
                    }
                },
            }
        }

        // Assert that we received the expected number of items.
        assert_eq!(
            batches_received_count, total_batches,
            "Did not receive all batches."
        );
        assert_eq!(
            stateless_txns_received_count, expected_total_stateless_txns,
            "Did not receive all stateless transactions."
        );
    }
}
*/
