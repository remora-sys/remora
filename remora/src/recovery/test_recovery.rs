// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use crate::checkpoint::EpochId;
    use crate::executor::api::{ExecutableTransaction, TransactionWithTimestamp};
    use crate::recovery::{EpochLogger, LogRecord, RecoveryCoordinator};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use sui_types::base_types::ObjectID;
    use sui_types::digests::TransactionDigest;
    use sui_types::transaction::InputObjectKind;

    // Simple test transaction type
    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    struct TestTransaction {
        id: u64,
    }

    impl ExecutableTransaction for TestTransaction {
        fn digest(&self) -> &TransactionDigest {
            // This is a test implementation - in real code this would be stored
            static DIGEST: std::sync::OnceLock<TransactionDigest> = std::sync::OnceLock::new();
            DIGEST.get_or_init(|| TransactionDigest::random())
        }

        fn input_objects(&self) -> Vec<InputObjectKind> {
            vec![]
        }

        fn shared_object_ids(&self) -> Vec<ObjectID> {
            vec![]
        }
    }

    fn create_test_transaction(id: u64) -> Arc<TransactionWithTimestamp<TestTransaction>> {
        Arc::new(TransactionWithTimestamp::new(
            TestTransaction { id },
            0.0,
            vec![],
            std::time::Duration::from_millis(0),
            std::time::Duration::from_millis(0),
            Some(0),
        ))
    }

    #[test]
    fn test_epoch_logger_basic_operations() {
        let logger = EpochLogger::<TestTransaction>::new();
        let epoch = EpochId(1);

        // Test appending records
        let record = LogRecord {
            consensus_index: Some(100),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch,
        };

        logger.append(epoch, record.clone());

        // Test retrieving records
        let retrieved = logger.get_epoch(epoch).unwrap();
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].consensus_index, Some(100));

        // Test pruning
        logger.prune_epoch(epoch);
        assert!(logger.get_epoch(epoch).is_none());
    }

    #[test]
    fn test_recovery_coordinator_basic_operations() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger);

        // Test begin recovery
        let replacement = coordinator.begin_recovery(0, 1);
        assert_eq!(replacement, 1);

        // Test persist index (starts at 0)
        let persist_index = coordinator.get_persist_index();
        assert_eq!(persist_index, 0);

        // Test update persist index
        coordinator.update_persist_index(100);
        let updated_persist_index = coordinator.get_persist_index();
        assert_eq!(updated_persist_index, 100);
    }

    #[test]
    fn test_recovery_coordinator_collect_replay_set() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let epoch = EpochId(1);

        // Add some test records
        let record1 = LogRecord {
            consensus_index: Some(100),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch,
        };

        let record2 = LogRecord {
            consensus_index: Some(150),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 1, // Different proxy
            required_states: BTreeMap::new(),
            epoch,
        };

        let record3 = LogRecord {
            consensus_index: Some(200),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch,
        };

        logger.append(epoch, record1);
        logger.append(epoch, record2);
        logger.append(epoch, record3);

        // Test collecting replay set for proxy 0 from index 120
        let replay_set = coordinator.collect_replay_set(epoch, 120, 0);
        assert_eq!(replay_set.len(), 1); // Only record3 should match
        assert_eq!(replay_set[0].consensus_index, Some(200));

        // Test collecting from index 50 (should get both records for proxy 0)
        let replay_set_all = coordinator.collect_replay_set(epoch, 50, 0);
        assert_eq!(replay_set_all.len(), 2);
    }

    #[test]
    fn test_recovery_coordinator_drain_dirty_queue() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        // Set persist index to 120
        coordinator.update_persist_index(120);

        // Add records to different epochs
        let epoch1 = EpochId(1);
        let epoch2 = EpochId(2);

        let record1 = LogRecord {
            consensus_index: Some(100), // Below persist index
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch1,
        };

        let record2 = LogRecord {
            consensus_index: Some(150), // Above persist index
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch1,
        };

        let record3 = LogRecord {
            consensus_index: Some(200), // Above persist index
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 1, // Different proxy
            required_states: BTreeMap::new(),
            epoch: epoch2,
        };

        logger.append(epoch1, record1);
        logger.append(epoch1, record2);
        logger.append(epoch2, record3);

        // Test draining dirty queue for proxy 0
        let dirty_queue = coordinator.drain_dirty_queue(0);
        assert_eq!(dirty_queue.len(), 1); // Only record2 should match (proxy 0, index >= 120)
        assert_eq!(dirty_queue[0].consensus_index, Some(150));

        // Test draining for proxy 1
        let dirty_queue_1 = coordinator.drain_dirty_queue(1);
        assert_eq!(dirty_queue_1.len(), 1); // Only record3 should match (proxy 1, index >= 120)
        assert_eq!(dirty_queue_1[0].consensus_index, Some(200));
    }

    #[test]
    fn test_epoch_logger_multiple_epochs() {
        let logger = EpochLogger::<TestTransaction>::new();

        // Add records to different epochs
        for epoch_num in 1..=3 {
            let epoch = EpochId(epoch_num);
            let record = LogRecord {
                consensus_index: Some(epoch_num * 100),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(epoch_num),
                destination_proxy: 0,
                required_states: BTreeMap::new(),
                epoch,
            };
            logger.append(epoch, record);
        }

        // Verify all epochs have records
        for epoch_num in 1..=3 {
            let epoch = EpochId(epoch_num);
            let records = logger.get_epoch(epoch).unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].consensus_index, Some(epoch_num * 100));
        }

        // Prune epoch 2
        logger.prune_epoch(EpochId(2));

        // Verify epoch 2 is gone but others remain
        assert!(logger.get_epoch(EpochId(2)).is_none());
        assert!(logger.get_epoch(EpochId(1)).is_some());
        assert!(logger.get_epoch(EpochId(3)).is_some());
    }
}
