use crate::checkpoint::{EpochId, EpochObjectVersions};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};

use crate::executor::api::ProxyToPrimaryMessage;

// FIXME: the snapshot process only happens when all updates are received.

/// Collects state snapshots from all proxies and manages atomic state updates
pub struct StateCollector {
    /// Expected number of proxies
    expected_proxies: usize,
    /// Current epoch being collected
    current_epoch: Option<EpochId>,
    /// Snapshots received for current epoch (proxy_id -> snapshot)
    pending_snapshots: BTreeMap<crate::proxy::core::ProxyId, EpochObjectVersions>,
    /// Sender to notify when epoch is complete
    tx_epoch_complete: Option<Sender<EpochId>>,
}

impl StateCollector {
    pub fn new(expected_proxies: usize) -> Self {
        Self {
            expected_proxies,
            current_epoch: None,
            pending_snapshots: BTreeMap::new(),
            tx_epoch_complete: None,
        }
    }

    /// Start collecting snapshots for a new epoch
    pub fn start_epoch(&mut self, epoch: EpochId, tx_complete: Sender<EpochId>) {
        debug!("Starting epoch collection for epoch {}", epoch.0);
        self.current_epoch = Some(epoch);
        self.pending_snapshots.clear();
        self.tx_epoch_complete = Some(tx_complete);
    }

    /// Process a state snapshot from a proxy
    pub fn process_snapshot(
        &mut self,
        proxy_id: crate::proxy::core::ProxyId,
        epoch: EpochId,
        snapshot: EpochObjectVersions,
    ) {
        if let Some(current_epoch) = self.current_epoch {
            if current_epoch != epoch {
                warn!(
                    "Received snapshot for epoch {} but current epoch is {}",
                    epoch.0, current_epoch.0
                );
                return;
            }

            debug!(
                "Received snapshot from proxy {} for epoch {}: {} objects",
                proxy_id,
                epoch.0,
                snapshot.len()
            );

            self.pending_snapshots.insert(proxy_id, snapshot);

            // Check if we have all snapshots
            if self.pending_snapshots.len() >= self.expected_proxies {
                self.complete_epoch(epoch);
            }
        } else {
            warn!("Received snapshot but no epoch is being collected");
        }
    }

    /// Complete the current epoch and merge all snapshots
    fn complete_epoch(&mut self, epoch: EpochId) {
        info!(
            "Completing epoch {} with {} snapshots",
            epoch.0,
            self.pending_snapshots.len()
        );

        // Merge all snapshots into a single state update
        let mut merged_state = EpochObjectVersions::new();
        for (proxy_id, snapshot) in &self.pending_snapshots {
            debug!("Merging {} objects from proxy {}", snapshot.len(), proxy_id);
            for (obj_id, version) in snapshot {
                // Keep the latest version for each object
                merged_state.insert(*obj_id, *version);
            }
        }

        info!(
            "Epoch {} complete: {} unique objects in merged state",
            epoch.0,
            merged_state.len()
        );

        // Reset for next epoch
        self.current_epoch = None;
        self.pending_snapshots.clear();

        // Notify completion
        if let Some(tx) = self.tx_epoch_complete.take() {
            let _ = tx.try_send(epoch);
        }
    }

    /// Run the state collector, processing messages from proxies
    pub async fn run(&mut self, mut rx_snapshots: Receiver<ProxyToPrimaryMessage>) {
        while let Some(message) = rx_snapshots.recv().await {
            match message {
                ProxyToPrimaryMessage::StateSnapshot(proxy_id, epoch, snapshot) => {
                    self.process_snapshot(proxy_id, epoch, snapshot);
                }
            }
        }
        info!("State collector shutting down");
    }
}
