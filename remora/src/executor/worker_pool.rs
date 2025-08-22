use std::thread;
use tokio::sync::mpsc::Sender;

use crate::config::DEFAULT_CHANNEL_SIZE;

/// Generic worker pool trait for processing tasks
pub trait WorkerTask: Send + 'static {
    type Context: Clone + Send + Sync + 'static;

    /// Process the task with the given context
    fn process(self, context: &Self::Context) -> impl futures::Future<Output = ()> + Send;
}

/// Generic worker pool configuration
pub struct WorkerPoolConfig {
    pub num_workers: Option<usize>,
    pub buffer_size: usize,
    pub enable_core_affinity: bool,
    pub core_offset: Option<usize>,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            num_workers: None,
            buffer_size: DEFAULT_CHANNEL_SIZE,
            enable_core_affinity: true,
            core_offset: None,
        }
    }
}

/// Global counter for automatic core offset allocation
static NEXT_CORE_OFFSET: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Generic worker pool that can handle any task type
pub struct WorkerPool<T: WorkerTask> {
    worker_txs: Vec<Sender<T>>,
    worker_handles: Vec<thread::JoinHandle<()>>,
    next_worker: usize,
}

impl<T: WorkerTask> WorkerPool<T> {
    /// Create a generic worker pool
    pub fn new(context: T::Context, config: WorkerPoolConfig) -> Self {
        let num_workers = config.num_workers.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        });

        // Calculate core offset to prevent conflicts between worker pools
        let core_offset = if config.enable_core_affinity && config.core_offset.is_none() {
            // Auto-calculate offset to prevent conflicts
            NEXT_CORE_OFFSET.fetch_add(num_workers, std::sync::atomic::Ordering::SeqCst)
        } else {
            config.core_offset.unwrap_or(0)
        };

        Self::new_with_core_offset(context, config, num_workers, core_offset)
    }

    /// Create a generic worker pool with explicit core offset
    fn new_with_core_offset(
        context: T::Context,
        config: WorkerPoolConfig,
        num_workers: usize,
        core_offset: usize,
    ) -> Self {
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut worker_rxs = Vec::with_capacity(num_workers);
        let mut worker_handles = Vec::with_capacity(num_workers);

        // Create channels for each worker
        for _ in 0..num_workers {
            let (tx, rx) = tokio::sync::mpsc::channel::<T>(config.buffer_size);
            worker_txs.push(tx);
            worker_rxs.push(rx);
        }

        // Spawn worker threads with core affinity
        for (i, rx) in worker_rxs.into_iter().enumerate() {
            let context = context.clone();
            let enable_core_affinity = config.enable_core_affinity;

            let handle = thread::spawn(move || {
                // Set core affinity if enabled
                if enable_core_affinity {
                    if let Some(core_ids) = core_affinity::get_core_ids() {
                        if !core_ids.is_empty() {
                            let core_index = (i + core_offset) % core_ids.len();
                            let core_id = core_ids[core_index];
                            core_affinity::set_for_current(core_id);
                            tracing::debug!(
                                "Worker {} pinned to core {} (offset: {}, index: {})",
                                i,
                                core_id.id,
                                core_offset,
                                core_index
                            );
                        } else {
                            tracing::warn!("Worker {}: no cores available for pinning", i);
                        }
                    } else {
                        tracing::warn!("Worker {}: could not get core IDs", i);
                    }
                }

                // Worker main loop
                let future = Self::worker_loop(rx, context);
                futures::executor::block_on(future);
            });

            worker_handles.push(handle);
        }

        Self {
            worker_txs,
            worker_handles,
            next_worker: 0,
        }
    }

    /// Generic worker loop
    async fn worker_loop(mut rx: tokio::sync::mpsc::Receiver<T>, context: T::Context) {
        while let Some(task) = rx.recv().await {
            task.process(&context).await;
        }
    }

    /// Send task using round-robin distribution (automatically selects next worker)
    pub async fn send_task(
        &mut self,
        task: T,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        let worker_index = self.next_worker;
        self.next_worker = (self.next_worker + 1) % self.worker_txs.len();
        self.worker_txs[worker_index].send(task).await
    }

    /// Get number of workers
    pub fn worker_count(&self) -> usize {
        self.worker_txs.len()
    }
}

impl<T: WorkerTask> Drop for WorkerPool<T> {
    fn drop(&mut self) {
        // Wait for all worker threads to finish
        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }
    }
}
