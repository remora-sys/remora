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

    #[test]
    fn test_recovery_coordinator_batch_processing() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new_with_batch_size(logger.clone(), 3);
        let epoch = EpochId(1);

        // Add more records than batch size
        for i in 1..=7 {
            let record = LogRecord {
                consensus_index: Some(100 + i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: 0,
                required_states: BTreeMap::new(),
                epoch,
            };
            logger.append(epoch, record);
        }

        // Set persist index to 100
        coordinator.update_persist_index(100);

        // Test that get_next_replay_batch returns batches
        let batch1 = coordinator.get_next_replay_batch(0);
        assert!(batch1.is_some());
        assert!(batch1.unwrap().len() <= 3); // Should be limited by batch_size

        let batch2 = coordinator.get_next_replay_batch(0);
        assert!(batch2.is_some());
        assert!(batch2.unwrap().len() <= 3); // Should be limited by batch_size

        let batch3 = coordinator.get_next_replay_batch(0);
        assert!(batch3.is_some());
        assert!(batch3.unwrap().len() <= 3); // Should be limited by batch_size

        let batch4 = coordinator.get_next_replay_batch(0);
        assert!(batch4.is_some());
        assert!(batch4.unwrap().len() <= 3); // Should be limited by batch_size
    }

    #[test]
    fn test_recovery_coordinator_empty_dirty_queue() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        // Set persist index high
        coordinator.update_persist_index(1000);

        // Add records below persist index
        let epoch = EpochId(1);
        let record = LogRecord {
            consensus_index: Some(100), // Below persist index
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch,
        };
        logger.append(epoch, record);

        // Should return empty dirty queue
        let dirty_queue = coordinator.drain_dirty_queue(0);
        assert!(dirty_queue.is_empty());
    }

    #[test]
    fn test_recovery_coordinator_mixed_proxy_epochs() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        // Set persist index
        coordinator.update_persist_index(150);

        // Add records to different epochs and proxies
        let epoch1 = EpochId(1);
        let epoch2 = EpochId(2);

        // Epoch 1: proxy 0 (above persist index)
        let record1 = LogRecord {
            consensus_index: Some(200),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch1,
        };

        // Epoch 1: proxy 1 (above persist index)
        let record2 = LogRecord {
            consensus_index: Some(250),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 1,
            required_states: BTreeMap::new(),
            epoch: epoch1,
        };

        // Epoch 2: proxy 0 (above persist index)
        let record3 = LogRecord {
            consensus_index: Some(300),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch2,
        };

        logger.append(epoch1, record1);
        logger.append(epoch1, record2);
        logger.append(epoch2, record3);

        // Test draining for proxy 0 (should get records from both epochs)
        let dirty_queue_0 = coordinator.drain_dirty_queue(0);
        assert_eq!(dirty_queue_0.len(), 2);
        // Order may vary, so check both possible orders
        let indices: Vec<Option<u64>> = dirty_queue_0.iter().map(|r| r.consensus_index).collect();
        assert!(indices.contains(&Some(200)) && indices.contains(&Some(300)));

        // Test draining for proxy 1 (should get record from epoch 1 only)
        let dirty_queue_1 = coordinator.drain_dirty_queue(1);
        assert_eq!(dirty_queue_1.len(), 1);
        assert_eq!(dirty_queue_1[0].consensus_index, Some(250));
    }

    #[test]
    fn test_recovery_coordinator_consensus_index_filtering() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let epoch = EpochId(1);

        // Add records with different consensus indices
        let records = vec![
            (50, 0),  // Below persist index, proxy 0
            (100, 0), // At persist index, proxy 0
            (150, 0), // Above persist index, proxy 0
            (200, 1), // Above persist index, proxy 1
        ];

        for (consensus_index, proxy) in records {
            let record = LogRecord {
                consensus_index: Some(consensus_index),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(consensus_index),
                destination_proxy: proxy,
                required_states: BTreeMap::new(),
                epoch,
            };
            logger.append(epoch, record);
        }

        // Set persist index to 100
        coordinator.update_persist_index(100);

        // Test collect_replay_set with different from_index values
        let replay_set_50 = coordinator.collect_replay_set(epoch, 50, 0);
        assert!(replay_set_50.len() >= 2); // indices 100 and 150

        let replay_set_120 = coordinator.collect_replay_set(epoch, 120, 0);
        assert!(replay_set_120.len() >= 1); // index 150 only

        let replay_set_200 = coordinator.collect_replay_set(epoch, 200, 0);
        assert_eq!(replay_set_200.len(), 0); // no records

        // Test drain_dirty_queue for proxy 0
        let dirty_queue_0 = coordinator.drain_dirty_queue(0);
        assert_eq!(dirty_queue_0.len(), 2); // indices 100 and 150
                                            // Verify the indices are correct (order may vary)
        let indices_0: Vec<Option<u64>> = dirty_queue_0.iter().map(|r| r.consensus_index).collect();
        assert!(indices_0.contains(&Some(100)) && indices_0.contains(&Some(150)));

        // Test drain_dirty_queue for proxy 1
        let dirty_queue_1 = coordinator.drain_dirty_queue(1);
        assert_eq!(dirty_queue_1.len(), 1); // index 200
    }

    #[test]
    fn test_recovery_coordinator_none_consensus_index() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let epoch = EpochId(1);

        // Add records with None consensus_index
        let record = LogRecord {
            consensus_index: None,
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch,
        };
        logger.append(epoch, record);

        // Should not appear in replay sets or dirty queue
        let replay_set = coordinator.collect_replay_set(epoch, 0, 0);
        assert!(replay_set.is_empty());

        let dirty_queue = coordinator.drain_dirty_queue(0);
        assert!(dirty_queue.is_empty());
    }

    #[test]
    fn test_epoch_logger_concurrent_access() {
        let logger = EpochLogger::<TestTransaction>::new();
        let epoch = EpochId(1);

        // Test concurrent appends (simulated with multiple calls)
        for i in 1..=10 {
            let record = LogRecord {
                consensus_index: Some(i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: 0,
                required_states: BTreeMap::new(),
                epoch,
            };
            logger.append(epoch, record);
        }

        // Verify all records are present
        let records = logger.get_epoch(epoch).unwrap();
        assert_eq!(records.len(), 10);

        // Verify order is maintained
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.consensus_index, Some((i + 1) as u64));
        }
    }

    #[test]
    fn test_recovery_coordinator_persist_index_updates() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        // Initial persist index should be 0
        assert_eq!(coordinator.get_persist_index(), 0);

        // Update persist index
        coordinator.update_persist_index(500);
        assert_eq!(coordinator.get_persist_index(), 500);

        // Update again
        coordinator.update_persist_index(1000);
        assert_eq!(coordinator.get_persist_index(), 1000);
    }

    #[test]
    fn test_recovery_coordinator_begin_recovery() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger);

        // Test begin_recovery returns the standby proxy
        let failed_proxy = 0;
        let standby_proxy = 1;
        let replacement = coordinator.begin_recovery(failed_proxy, standby_proxy);
        assert_eq!(replacement, standby_proxy);
    }

    #[test]
    fn test_epoch_logger_empty_epoch() {
        let logger = EpochLogger::<TestTransaction>::new();
        let epoch = EpochId(1);

        // Test getting non-existent epoch
        assert!(logger.get_epoch(epoch).is_none());

        // Test pruning non-existent epoch (should not panic)
        logger.prune_epoch(epoch);
        assert!(logger.get_epoch(epoch).is_none());
    }

    #[test]
    fn test_recovery_coordinator_custom_batch_size() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new_with_batch_size(logger.clone(), 5);
        let epoch = EpochId(1);

        // Add 12 records
        for i in 1..=12 {
            let record = LogRecord {
                consensus_index: Some(100 + i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: 0,
                required_states: BTreeMap::new(),
                epoch,
            };
            logger.append(epoch, record);
        }

        coordinator.update_persist_index(100);

        // Test that get_next_replay_batch returns batches
        let batch1 = coordinator.get_next_replay_batch(0);
        assert!(batch1.is_some());
        assert!(batch1.unwrap().len() <= 5); // Should be limited by batch_size

        let batch2 = coordinator.get_next_replay_batch(0);
        assert!(batch2.is_some());
        assert!(batch2.unwrap().len() <= 5); // Should be limited by batch_size

        let batch3 = coordinator.get_next_replay_batch(0);
        assert!(batch3.is_some());
        assert!(batch3.unwrap().len() <= 5); // Should be limited by batch_size

        let batch4 = coordinator.get_next_replay_batch(0);
        assert!(batch4.is_some());
        assert!(batch4.unwrap().len() <= 5); // Should be limited by batch_size
    }
}
