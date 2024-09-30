// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

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
pub struct MockConsensus<M, T> {
    /// The consensus delay model.
    model: M,
    /// The parameters of the mock consensus engine.
    parameters: MockConsensusParameters,
    /// Channel to receive transactions from the network.
    rx_load_balancer: Receiver<T>,
    /// Output channel to deliver mocked consensus commits to the primary executor.
    tx_primary_executor: Sender<ConsensusCommit<T>>,
    /// Holds the current batch.
    current_batch: ConsensusCommit<T>,
    /// The number of batches currently in-flight.
    current_inflight_batches: usize,
}

impl<M, T> MockConsensus<M, T> {
    /// Create a new mock consensus engine.
    pub fn new(
        model: M,
        parameters: MockConsensusParameters,
        rx_load_balancer: Receiver<T>,
        tx_primary_executor: Sender<ConsensusCommit<T>>,
    ) -> Self {
        let batch_size = parameters.batch_size.get();
        Self {
            model,
            parameters,
            rx_load_balancer,
            tx_primary_executor,
            current_batch: Vec::with_capacity(batch_size),
            current_inflight_batches: 0,
        }
    }
}

impl<M: DelayModel<T>, T> MockConsensus<M, T> {
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

                    self.current_batch.push(transaction);
                    if self.current_batch.len() >= batch_size {
                        self.current_inflight_batches += 1;
                        let batch: Vec<_> = self.current_batch.drain(..).collect();
                        tracing::debug!("Sealed batch with {} transactions", batch.len());
                        waiter.push(self.model.consensus_delay(batch));
                        timer.as_mut().reset(Instant::now() + self.parameters.max_batch_delay);
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.current_inflight_batches += 1;
                        let batch: Vec<_> = self.current_batch.drain(..).collect();
                        tracing::debug!("Sealed batch with {} transactions", batch.len());
                        waiter.push(self.model.consensus_delay(batch));
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

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Spawn the mock consensus engine in a separate task.
    pub fn spawn(mut self) -> JoinHandle<()>
    where
        M: Send + 'static,
        T: Send + 'static,
    {
        tokio::spawn(async move { self.run().await })
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

#[cfg(test)]
mod test {
    use std::{num::NonZeroUsize, time::Duration};

    use tokio::{sync::mpsc, time::Instant};

    use crate::primary::mock_consensus::{
        models::{FixedDelay, UniformDelay},
        MockConsensus, MockConsensusParameters,
    };

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

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
        )
        .spawn();

        // Send enough transactions to fill two batches.
        let start = Instant::now();
        for i in 0..parameters.batch_size.get() * 2 {
            tx_load_balancer.send(i).await.unwrap();
        }

        // Wait for the consensus to commit the batches.
        let commit_1 = rx_primary_executor.recv().await.unwrap();
        assert_eq!(commit_1, vec![0, 1, 2]);
        assert_eq!(start.elapsed(), model.delay);

        let commit_2 = rx_primary_executor.recv().await.unwrap();
        assert_eq!(commit_2, vec![3, 4, 5]);
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

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
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

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
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

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
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

        let consensus_handle = MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
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

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
        )
        .spawn();

        // Send many transactions to the mock consensus engine.
        let total_batches = 100;
        tokio::spawn(async move {
            for i in 0..parameters.batch_size.get() * total_batches {
                tx_load_balancer.send(i).await.unwrap();
            }
        });

        // Wait for the consensus to commit the batches.
        for _ in 0..total_batches {
            let _ = rx_primary_executor.recv().await.unwrap();
        }
    }
}
