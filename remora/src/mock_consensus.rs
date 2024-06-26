// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::{stream::FuturesUnordered, Future, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc::{error::SendError, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};

/// Represents a consensus commit.
pub type ConsensusCommit<T> = Vec<T>;

/// The parameters of the mock consensus engine.
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Clone))]
pub struct MockConsensusParameters {
    /// The preferred batch size (in number of transactions).
    batch_size: usize,
    /// The maximum delay after which to seal the batch.
    max_batch_delay: Duration,
    /// The maximum number of batches that can be in-flight at the same time.
    max_inflight_batches: usize,
}

impl Default for MockConsensusParameters {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            max_batch_delay: Duration::from_millis(100),
            max_inflight_batches: 10_000,
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
// TODO: Replace the `UnboundedReceiver` and `UnboundedSender` with their bounded counter parts
// to apply back pressure on the network.
pub struct MockConsensus<M, T> {
    /// The consensus delay model.
    model: M,
    /// The parameters of the mock consensus engine.
    parameters: MockConsensusParameters,
    /// Channel to receive transactions from the network.
    rx_load_balancer: UnboundedReceiver<T>,
    /// Output channel to deliver mocked consensus commits to the primary executor.
    tx_primary_executor: UnboundedSender<ConsensusCommit<T>>,
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
        rx_load_balancer: UnboundedReceiver<T>,
        tx_primary_executor: UnboundedSender<ConsensusCommit<T>>,
    ) -> Self {
        let batch_size = parameters.batch_size;
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

impl<M: DelayModel<T> + Send + 'static, T: Send + 'static> MockConsensus<M, T> {
    /// Spawn the mock consensus engine in a separate task.
    pub fn spawn(mut self) -> JoinHandle<Result<(), SendError<ConsensusCommit<T>>>> {
        tokio::spawn(async move { self.run().await })
    }

    /// Run the mock consensus engine.
    pub async fn run(&mut self) -> Result<(), SendError<ConsensusCommit<T>>> {
        let timer = sleep(self.parameters.max_batch_delay);
        tokio::pin!(timer);

        let mut waiter = FuturesUnordered::new();

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size. If there are too many
                // in-flight batches, wait for some to complete before accepting new transactions.
                Some(transaction) = self.rx_load_balancer.recv(),
                    if self.current_inflight_batches < self.parameters.max_inflight_batches => {

                    self.current_batch.push(transaction);
                    if self.current_batch.len() >= self.parameters.batch_size {
                        self.current_inflight_batches += 1;
                        let batch = self.current_batch.drain(..).collect();
                        waiter.push(self.model.consensus_delay(batch));
                        timer.as_mut().reset(Instant::now() + self.parameters.max_batch_delay);
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.current_inflight_batches += 1;
                        let batch = self.current_batch.drain(..).collect();
                        waiter.push(self.model.consensus_delay(batch));
                    }
                    timer.as_mut().reset(Instant::now() + self.parameters.max_batch_delay);
                }

                // Deliver the consensus commit to the primary executor.
                Some(commit) = waiter.next() => {
                    self.current_inflight_batches -= 1;
                    self.tx_primary_executor.send(commit)?
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
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
    #[derive(Serialize, Deserialize)]
    #[cfg_attr(test, derive(Clone))]
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
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::mock_consensus::{
        models::{FixedDelay, UniformDelay},
        MockConsensus,
        MockConsensusParameters,
    };

    #[tokio::test(start_paused = true)]
    async fn fixed_delay() {
        let model = FixedDelay::default();
        let parameters = MockConsensusParameters {
            batch_size: 3,
            max_batch_delay: std::time::Duration::from_millis(100),
            max_inflight_batches: 10,
        };

        let (tx_load_balancer, rx_load_balancer) = tokio::sync::mpsc::unbounded_channel();
        let (tx_primary_executor, mut rx_primary_executor) = tokio::sync::mpsc::unbounded_channel();

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
        )
        .spawn();

        // Send enough transactions to fill two batches.
        let start = Instant::now();
        for i in 0..parameters.batch_size * 2 {
            tx_load_balancer.send(i).unwrap();
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
            batch_size: 3,
            max_batch_delay: Duration::from_millis(100),
            max_inflight_batches: 10,
        };

        let (tx_load_balancer, rx_load_balancer) = tokio::sync::mpsc::unbounded_channel();
        let (tx_primary_executor, mut rx_primary_executor) = tokio::sync::mpsc::unbounded_channel();

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
        )
        .spawn();

        // Send enough transactions to fill two batches.
        let start = Instant::now();
        for i in 0..parameters.batch_size * 2 {
            tx_load_balancer.send(i).unwrap();
        }

        // Wait for the consensus to commit the batches. Remember that the delay is random and
        // that consecutive batches may be committed in any order.
        let commit_1 = rx_primary_executor.recv().await.unwrap();
        let commit_2 = rx_primary_executor.recv().await.unwrap();
        let end = start.elapsed();

        assert!(end >= model.min_delay);
        assert!(end < model.max_delay);
        assert!(
            (0..parameters.batch_size * 2).all(|x| commit_1.contains(&x) || commit_2.contains(&x))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn early_batch_seal() {
        let model = FixedDelay::default();
        let parameters = MockConsensusParameters {
            batch_size: 3,
            max_batch_delay: Duration::from_millis(100),
            max_inflight_batches: 10,
        };

        let (tx_load_balancer, rx_load_balancer) = tokio::sync::mpsc::unbounded_channel();
        let (tx_primary_executor, mut rx_primary_executor) = tokio::sync::mpsc::unbounded_channel();

        MockConsensus::new(
            model.clone(),
            parameters.clone(),
            rx_load_balancer,
            tx_primary_executor,
        )
        .spawn();

        // Do not send enough transactions to seal a batch
        let start = Instant::now();
        tx_load_balancer.send(0).unwrap();

        // Wait for the consensus to commit the batches.
        let commit = rx_primary_executor.recv().await.unwrap();
        assert_eq!(commit, vec![0]);
        assert_eq!(start.elapsed(), model.delay + parameters.max_batch_delay);
    }
}
