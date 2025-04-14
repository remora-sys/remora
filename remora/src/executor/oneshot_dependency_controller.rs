// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use rustc_hash::FxHashMap;
use sui_types::base_types::TransactionDigest;
use tokio::sync::oneshot;

// This struct is for managing the dependency for stateless transactions.

/// A task handle is a oneshot channel that is used to notify the task that it has been completed.
/// The bool is used to indicate whether the stateless transaction succeeds.
pub type TaskHandle = oneshot::Receiver<bool>;
pub type TaskEntry = Option<TaskHandle>;
pub type ObjectTaskMap = FxHashMap<TransactionDigest, TaskEntry>;

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
        let obj_task_map: ObjectTaskMap = FxHashMap::default();

        Self { obj_task_map }
    }

    /// Checks if a given `ObjectID` has an associated task.
    pub fn has_entry_for_txn(&self, txn_id: &TransactionDigest) -> bool {
        self.obj_task_map.contains_key(txn_id)
    }

    /// Set the dependency for a given `TransactionDigest`.
    pub fn set_dependency(&mut self, txn_id: TransactionDigest) -> oneshot::Sender<bool> {
        let (tx, rx) = oneshot::channel::<bool>();
        self.obj_task_map.insert(txn_id, Some(rx.into()));
        tx
    }

    /// Get and remove the dependency for a given `TransactionDigest`.
    /// Returns None if the transaction doesn't exist.
    pub fn get_dependencies(&mut self, txn_id: TransactionDigest) -> Option<TaskHandle> {
        self.obj_task_map.remove(&txn_id).and_then(|entry| entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::base_types::TransactionDigest;

    #[test]
    fn test_set_and_get_dependency() {
        let mut dependency_controller = OneshotDependencyController::new();
        let txn_id = TransactionDigest::random();
        
        // Initially there should be no entry
        assert!(!dependency_controller.has_entry_for_txn(&txn_id));
        
        // Set a dependency
        let sender = dependency_controller.set_dependency(txn_id);
        
        // Now there should be an entry
        assert!(dependency_controller.has_entry_for_txn(&txn_id));
        
        // Get the dependency
        let handle = dependency_controller.get_dependencies(txn_id).expect("Should have dependency");
        
        // Verify we can use the sender to signal completion
        sender.send(true).expect("Failed to send completion signal");
        
        // We should be able to receive the signal through the handle
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { 
                handle.await.expect("Failed to receive completion signal") 
            });
        
        assert!(result, "Expected successful completion signal");
    }

    #[test]
    fn test_has_entry_for_txn() {
        let mut dependency_controller = OneshotDependencyController::new();
        let txn_id = TransactionDigest::random();
        
        // Initially there should be no entry
        assert!(!dependency_controller.has_entry_for_txn(&txn_id));
        
        // Set a dependency
        dependency_controller.set_dependency(txn_id);
        
        // Now there should be an entry
        assert!(dependency_controller.has_entry_for_txn(&txn_id));
    }

    #[test]
    fn test_get_dependencies_nonexistent() {
        let mut dependency_controller = OneshotDependencyController::new();
        let txn_id = TransactionDigest::random();
        
        // This should return None because the transaction doesn't exist
        assert!(dependency_controller.get_dependencies(txn_id).is_none());
    }

    #[test]
    fn test_multiple_transactions() {
        let mut dependency_controller = OneshotDependencyController::new();
        let txn_id1 = TransactionDigest::random();
        let txn_id2 = TransactionDigest::random();
        
        // Set dependencies for both transactions
        let sender1 = dependency_controller.set_dependency(txn_id1);
        let sender2 = dependency_controller.set_dependency(txn_id2);
        
        // Both should have entries
        assert!(dependency_controller.has_entry_for_txn(&txn_id1));
        assert!(dependency_controller.has_entry_for_txn(&txn_id2));
        
        // Complete the first transaction with success
        sender1.send(true).expect("Failed to send completion signal");
        
        // Complete the second transaction with failure
        sender2.send(false).expect("Failed to send completion signal");
        
        // Get the handles
        let handle1 = dependency_controller.get_dependencies(txn_id1).expect("Should have dependency for txn1");
        let handle2 = dependency_controller.get_dependencies(txn_id2).expect("Should have dependency for txn2");
        
        // Verify the results
        let runtime = tokio::runtime::Runtime::new().unwrap();
        
        let result1 = runtime.block_on(async { 
            handle1.await.expect("Failed to receive completion signal") 
        });
        assert!(result1, "Expected successful completion for transaction 1");
        
        let result2 = runtime.block_on(async { 
            handle2.await.expect("Failed to receive completion signal") 
        });
        assert!(!result2, "Expected failure completion for transaction 2");
    }
}
