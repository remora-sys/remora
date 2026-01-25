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
        tracing::debug!("Resuming barrier");
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
    async fn test_pause_waits_for_yielded_task() {
        // This test confirms that `pause_and_wait` correctly waits for a task
        // that has acquired a ticket and then yields execution (e.g. via .await).
        // The ticket's lifetime spans the entire async task, so the barrier
        // should wait until the task is fully complete.
        let barrier = PauseBarrier::new();
        let state = Arc::new(AtomicUsize::new(0)); // 0: initial, 1: work done, 2: commit done

        // Worker task, analogous to `process_snapshot`
        let barrier_clone = barrier.clone();
        let state_clone = state.clone();
        let worker = tokio::spawn(async move {
            let _ticket = barrier_clone.enter().await;
            // This task is now "active". It does some work, then yields.
            state_clone.store(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            // It does more work after resuming.
            state_clone.store(2, Ordering::SeqCst);
        });

        // Pauser task, analogous to `begin_recovery`
        let pauser = tokio::spawn(async move {
            // Yield to give the worker a chance to start and get its ticket.
            tokio::task::yield_now().await;
            let _guard = barrier.pause_and_wait().await;
            // The snapshot is taken here. We read the state.
            state.load(Ordering::SeqCst)
        });

        let (worker_res, pauser_res) = tokio::join!(worker, pauser);
        worker_res.unwrap();
        let state_at_pause = pauser_res.unwrap();

        // `pause_and_wait` should only complete AFTER the worker task has finished
        // and dropped its ticket. Therefore, the final state should be 2.
        // If it were 1, it would mean `pause_and_wait` did not wait for the
        // yielded task to complete, which would be a bug.
        assert_eq!(state_at_pause, 2, "Snapshot was taken mid-task.");
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_enter_pause_race_condition() {
        // This is a probabilistic test to expose a race condition in `enter()`.
        // The race occurs if `pause_and_wait` sets `paused = true` *after* a
        // worker task does `active.fetch_add(1)` but *before* it checks
        // `paused.load()`. In this case, the worker will see the paused flag,
        // decrement `active` back to 0, and loop. `pause_and_wait` will then
        // see `active == 0` and return, even though the worker task is still
        // "in-flight" inside the `enter` method, about to be blocked.
        const NUM_ITERATIONS: usize = 100;
        const NUM_WORKERS: usize = 4;

        for i in 0..NUM_ITERATIONS {
            let barrier = PauseBarrier::new();
            let tasks_completed = Arc::new(AtomicUsize::new(0));
            let pause_completed = Arc::new(Notify::new());

            // Spawn a task that will immediately try to pause the barrier.
            let barrier_clone = barrier.clone();
            let tasks_completed_clone = tasks_completed.clone();
            let pause_completed_clone = pause_completed.clone();
            let pauser = tokio::spawn(async move {
                let guard = barrier_clone.pause_and_wait().await;

                // At this point, `pause_and_wait` has returned. It believes no tasks are active.
                let active_count = barrier_clone.active.load(Ordering::SeqCst);
                let completed_count = tasks_completed_clone.load(Ordering::SeqCst);

                // The invariant that `pause_and_wait` should guarantee is that no tasks
                // are running when it returns. Any task that successfully got a ticket
                // should have been waited for. Any task that was trying to enter should
                // now be blocked.
                assert_eq!(
                    active_count, 0,
                    "Iteration {}: Active count should be 0 after pause",
                    i
                );
                assert_eq!(
                    completed_count, 0,
                    "Iteration {}: No tasks should have completed before pause returns",
                    i
                );

                // Signal to the main thread that assertions have passed and the guard is held.
                pause_completed_clone.notify_one();

                // Now, resume the barrier by dropping the guard.
                drop(guard);
            });

            // Spawn worker tasks that will contend to enter the barrier.
            let mut worker_handles = Vec::new();
            for _ in 0..NUM_WORKERS {
                let barrier_clone = barrier.clone();
                let tasks_completed_clone = tasks_completed.clone();
                worker_handles.push(tokio::spawn(async move {
                    let _ticket = barrier_clone.enter().await;
                    tasks_completed_clone.fetch_add(1, Ordering::SeqCst);
                }));
            }

            // Wait for the pauser to acquire the lock and run its assertions.
            pause_completed.notified().await;

            // All workers should now be able to complete as the pauser task will drop the guard.
            for handle in worker_handles {
                handle.await.unwrap();
            }
            pauser.await.unwrap(); // Make sure pauser task completes.

            // Check that all workers eventually completed.
            assert_eq!(
                tasks_completed.load(Ordering::SeqCst),
                NUM_WORKERS,
                "Iteration {}: All workers should complete after resume",
                i
            );
        }
    }
}
