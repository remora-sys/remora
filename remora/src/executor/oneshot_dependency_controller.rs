// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use sui_types::base_types::TransactionDigest;
use tokio::sync::oneshot;

// This struct is for managing the dependency for stateless transactions.

/// A task handle is a tuple containing a oneshot channel receiver that is used to notify the task
/// that it has been completed, and an optional sender for signaling.
/// The bool is used to indicate whether the stateless transaction succeeds.
pub type TaskHandle = (
    Option<oneshot::Receiver<bool>>,
    Option<oneshot::Sender<bool>>,
);
pub type TaskEntry = Option<TaskHandle>;
pub type ObjectTaskMap = DashMap<TransactionDigest, TaskEntry>;

/// The dependency controller is responsible for dynamically maintaining
/// inter-task dependency graph due to overlapped resource accesses.
pub struct OneshotDependencyController {
    /// This map contains the tail task of all priors ones
    /// which access the given object.
    obj_task_map: ObjectTaskMap,
}

impl Default for OneshotDependencyController {
    fn default() -> Self {
        Self::new()
    }
}

impl OneshotDependencyController {
    pub fn new() -> Self {
        let obj_task_map: ObjectTaskMap = DashMap::default();

        Self { obj_task_map }
    }

    /// Checks if a given `TransactionDigest` has an associated task.
    pub fn has_entry_for_txn(&self, txn_id: &TransactionDigest) -> bool {
        self.obj_task_map.contains_key(txn_id)
    }

    /// Set the remote dependency for a given `TransactionDigest`.
    pub fn set_remote_dependency(&self, txn_id: TransactionDigest) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel::<bool>();
        self.obj_task_map.insert(txn_id, Some((None, Some(tx))));
        rx
    }

    /// Sets a local dependency for a transaction.
    ///
    /// This method creates or updates an entry for the given transaction ID in the dependency map.
    /// It stores only the receiver part of the oneshot channel in the map and returns the sender.
    ///
    /// If an entry already exists but has no sender, a new channel is created.
    /// If an entry exists with a sender, that sender is taken and returned.
    ///
    /// # Arguments
    /// * `txn_id` - The transaction digest to set the dependency for
    ///
    /// # Returns
    /// A oneshot sender that can be used to signal completion of the transaction
    pub fn set_local_dependency(&self, txn_id: TransactionDigest) -> oneshot::Sender<bool> {
        use dashmap::mapref::entry::Entry;

        match self.obj_task_map.entry(txn_id) {
            Entry::Occupied(mut occ) => {
                if let Some((_, tx_opt)) = occ.get_mut() {
                    if let Some(tx) = tx_opt.take() {
                        return tx;
                    }
                }
                // If no sender exists, create a new one
                let (tx, rx) = oneshot::channel::<bool>();
                *occ.get_mut() = Some((Some(rx), None));
                tx
            }
            Entry::Vacant(vac) => {
                // Create new entry if it doesn't exist
                let (tx, rx) = oneshot::channel::<bool>();
                vac.insert(Some((Some(rx), None)));
                tx
            }
        }
    }

    /// Retrieves the dependency receiver for a transaction.
    ///
    /// This method looks up the entry for the given transaction ID and returns the receiver
    /// part of the oneshot channel if it exists. The receiver is removed from the entry,
    /// but the entry remains if a sender still exists.
    ///
    /// If no entry exists, a new channel is created, the sender is stored, and the receiver
    /// is returned.
    ///
    /// If both receiver and sender are gone after taking the receiver, the entry is removed
    /// entirely from the map.
    ///
    /// # Arguments
    /// * `txn_id` - The transaction digest to get the dependency for
    ///
    /// # Returns
    /// An optional oneshot receiver that can be awaited to determine transaction completion
    pub fn get_dependency(&self, txn_id: &TransactionDigest) -> Option<oneshot::Receiver<bool>> {
        use dashmap::mapref::entry::Entry;

        match self.obj_task_map.entry(txn_id.clone()) {
            Entry::Occupied(mut occ) => {
                if let Some((rx_opt, tx_opt)) = occ.get_mut().take() {
                    // If sender still exists, leave entry with sender only
                    if tx_opt.is_some() {
                        *occ.get_mut() = Some((None, tx_opt));
                    } else {
                        // If sender is also None, remove entry
                        occ.remove();
                    }
                    rx_opt
                } else {
                    // Entry exists but is None
                    None
                }
            }
            Entry::Vacant(vac) => {
                // Create new entry if it doesn't exist
                let (tx, rx) = oneshot::channel::<bool>();
                vac.insert(Some((None, Some(tx))));
                Some(rx)
            }
        }
    }

    /// Remove the dependency for a given `TransactionDigest` without returning the receiver.
    pub fn remove_dependency(&self, txn_id: &TransactionDigest) {
        self.obj_task_map.remove(txn_id);
    }

    /// Get the signal sender for a given `TransactionDigest` if it exists.
    /// This removes the sender from the entry, but leaves the entry if the receiver exists.
    /// If both receiver and sender are gone, removes the entry entirely.
    pub fn take_signal(&self, txn_id: &TransactionDigest) -> Option<oneshot::Sender<bool>> {
        use dashmap::mapref::entry::Entry;

        match self.obj_task_map.entry(txn_id.clone()) {
            Entry::Occupied(mut occ) => {
                if let Some((rx_opt, tx_opt)) = occ.get_mut().take() {
                    // If receiver still exists, leave entry with receiver only
                    if rx_opt.is_some() {
                        *occ.get_mut() = Some((rx_opt, None));
                    } else {
                        // If receiver is also None, remove entry
                        occ.remove();
                    }
                    tx_opt
                } else {
                    // Entry exists but is None
                    None
                }
            }
            Entry::Vacant(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::base_types::TransactionDigest;

    #[test]
    fn test_set_and_get_dependency() {
        let dependency_controller = OneshotDependencyController::new();
        let txn_id = TransactionDigest::random();

        // Initially there should be no entry
        assert!(!dependency_controller.has_entry_for_txn(&txn_id));

        // Set a dependency
        let sender = dependency_controller.set_local_dependency(txn_id);

        // Now there should be an entry
        assert!(dependency_controller.has_entry_for_txn(&txn_id));

        // Get the dependency
        let handle = dependency_controller
            .get_dependency(&txn_id)
            .expect("Should have dependency");

        // Verify we can use the sender to signal completion
        sender.send(true).expect("Failed to send completion signal");

        // We should be able to receive the signal through the handle
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { handle.await.expect("Failed to receive completion signal") });

        assert!(result, "Expected successful completion signal");
    }

    #[test]
    fn test_has_entry_for_txn() {
        let dependency_controller = OneshotDependencyController::new();
        let txn_id = TransactionDigest::random();

        // Initially there should be no entry
        assert!(!dependency_controller.has_entry_for_txn(&txn_id));

        // Set a dependency
        dependency_controller.set_remote_dependency(txn_id);

        // Now there should be an entry
        assert!(dependency_controller.has_entry_for_txn(&txn_id));
    }

    #[test]
    fn test_get_dependencies_existent() {
        let dependency_controller = OneshotDependencyController::new();
        let txn_id = TransactionDigest::random();

        // This should return None because the transaction doesn't exist
        assert!(dependency_controller.get_dependency(&txn_id).is_some());
    }

    #[test]
    fn test_multiple_transactions() {
        let dependency_controller = OneshotDependencyController::new();
        let txn_id1 = TransactionDigest::random();
        let txn_id2 = TransactionDigest::random();

        // Set dependencies for both transactions
        let sender1 = dependency_controller.set_local_dependency(txn_id1);
        let sender2 = dependency_controller.set_local_dependency(txn_id2);

        // Both should have entries
        assert!(dependency_controller.has_entry_for_txn(&txn_id1));
        assert!(dependency_controller.has_entry_for_txn(&txn_id2));

        // Complete the first transaction with success
        sender1
            .send(true)
            .expect("Failed to send completion signal");

        // Complete the second transaction with failure
        sender2
            .send(false)
            .expect("Failed to send completion signal");

        // Get the handles
        let handle1 = dependency_controller
            .get_dependency(&txn_id1)
            .expect("Should have dependency for txn1");
        let handle2 = dependency_controller
            .get_dependency(&txn_id2)
            .expect("Should have dependency for txn2");

        // Verify the results
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let result1 =
            runtime.block_on(async { handle1.await.expect("Failed to receive completion signal") });
        assert!(result1, "Expected successful completion for transaction 1");

        let result2 =
            runtime.block_on(async { handle2.await.expect("Failed to receive completion signal") });
        assert!(!result2, "Expected failure completion for transaction 2");
    }

    #[test]
    fn test_remove_dependency() {
        let dependency_controller = OneshotDependencyController::new();
        let txn_id = TransactionDigest::random();

        // Set a dependency
        dependency_controller.set_remote_dependency(txn_id);

        // Ensure the entry exists
        assert!(dependency_controller.has_entry_for_txn(&txn_id));

        // Remove the dependency
        dependency_controller.remove_dependency(&txn_id);

        // Ensure the entry no longer exists
        assert!(!dependency_controller.has_entry_for_txn(&txn_id));
    }

    #[test]
    fn test_take_signal() {
        let dependency_controller = OneshotDependencyController::new();
        let txn_id = TransactionDigest::random();

        // Set a remote dependency
        let _ = dependency_controller.set_remote_dependency(txn_id);

        // Ensure the entry exists
        assert!(dependency_controller.has_entry_for_txn(&txn_id));

        // Take the signal (sender)
        let taken_sender = dependency_controller.take_signal(&txn_id);

        // Ensure the sender is the same
        assert!(taken_sender.is_some());

        // Try to take the signal again, should be None
        let taken_sender_again = dependency_controller.take_signal(&txn_id);
        assert!(taken_sender_again.is_none());
    }
}
