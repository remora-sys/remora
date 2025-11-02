// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

/// A barrier that can be used to pause and resume a group of tasks.
#[derive(Debug)]
pub struct PauseBarrier {
    paused: AtomicBool,
    active: AtomicUsize,
    notify: Notify,
}

/// A ticket that represents an active task. When dropped, the active count is decremented.
pub struct PauseTicket(Arc<PauseBarrier>);

impl Drop for PauseTicket {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A guard that keeps the barrier paused. When dropped, the barrier is resumed.
pub struct PauseGuard<'a>(&'a PauseBarrier);

impl<'a> Drop for PauseGuard<'a> {
    fn drop(&mut self) {
        self.0.resume();
    }
}

impl PauseBarrier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            paused: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            notify: Notify::new(),
        })
    }

    /// Enter the barrier. If the barrier is paused, this function will wait until it is resumed.
    /// Returns a ticket that must be held for the duration of the task.
    pub async fn enter(self: &Arc<Self>) -> PauseTicket {
        loop {
            while self.paused.load(Ordering::SeqCst) {
                self.notify.notified().await;
            }

            self.active.fetch_add(1, Ordering::SeqCst);

            if !self.paused.load(Ordering::SeqCst) {
                return PauseTicket(self.clone());
            }

            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Pause the barrier and wait for all active tasks to complete.
    /// Returns a guard that will resume the barrier when dropped.
    pub async fn pause_and_wait(&self) -> PauseGuard<'_> {
        self.paused.store(true, Ordering::SeqCst);
        while self.active.load(Ordering::SeqCst) > 0 {
            // A short sleep to prevent a tight spin loop.
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        PauseGuard(self)
    }

    /// Resume the barrier and notify any waiting tasks.
    fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test]
    async fn test_pause_waits_for_active_tasks() {
        let barrier = PauseBarrier::new();
        let completion_counter = Arc::new(AtomicUsize::new(0));

        // Spawn a task that enters the barrier and simulates work.
        let barrier_clone = barrier.clone();
        let counter_clone = completion_counter.clone();
        tokio::spawn(async move {
            let _ticket = barrier_clone.enter().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Give the task time to enter the barrier.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let start = Instant::now();
        let _guard = barrier.pause_and_wait().await;
        let duration = start.elapsed();

        // Check that pause_and_wait waited for the task to finish.
        assert!(duration >= Duration::from_millis(150));
        assert_eq!(completion_counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_tasks_are_paused_and_resumed() {
        let barrier = PauseBarrier::new();
        let completion_counter = Arc::new(AtomicUsize::new(0));

        // Immediately pause the barrier.
        let guard = barrier.pause_and_wait().await;

        // Spawn tasks that will try to enter the barrier.
        for _ in 0..10 {
            let barrier_clone = barrier.clone();
            let counter_clone = completion_counter.clone();
            tokio::spawn(async move {
                let _ticket = barrier_clone.enter().await;
                counter_clone.fetch_add(1, Ordering::SeqCst);
            });
        }

        // Wait a bit to ensure tasks are blocked.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            completion_counter.load(Ordering::SeqCst),
            0,
            "Tasks should be paused and not have completed."
        );

        // Drop the guard to resume the barrier.
        drop(guard);

        // Wait for tasks to complete.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            completion_counter.load(Ordering::SeqCst),
            10,
            "Tasks should resume and complete after the guard is dropped."
        );
    }
}
