// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use crate::checkpoint::state_collector::StateCollector;
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
        let state_collector = StateCollector::new(3);

        // Test begin recovery
        let replacement = coordinator.begin_recovery(0, 1);
        assert_eq!(replacement, 1);

        // Test persist index (starts at 0 when no proxies have reported)
        let persist_index = coordinator.get_persist_index(&state_collector);
        assert_eq!(persist_index, 0);

        // Simulate proxy 0 reporting epoch 100 via process_snapshot
        use std::collections::BTreeMap;
        let snapshot = BTreeMap::new();
        state_collector.process_snapshot::<TestTransaction>(0, 100, snapshot, 3, None);

        let updated_persist_index = coordinator.get_persist_index(&state_collector);
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
        let state_collector = StateCollector::new(3);

        // Simulate all 3 proxies reporting epoch 1 (persist_index = 1)
        use std::collections::BTreeMap;
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 1, snapshot, 3, None);
        }

        // Add records to different epochs
        let epoch1 = EpochId(1); // At persist_index - should be excluded
        let epoch2 = EpochId(2); // Above persist_index - should be included

        let record1 = LogRecord {
            consensus_index: Some(100),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch1, // Epoch 1, should be excluded
        };

        let record2 = LogRecord {
            consensus_index: Some(150),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch2, // Epoch 2, should be included
        };

        let record3 = LogRecord {
            consensus_index: Some(200),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 1, // Different proxy
            required_states: BTreeMap::new(),
            epoch: epoch2, // Epoch 2, should be included
        };

        logger.append(epoch1, record1);
        logger.append(epoch2, record2);
        logger.append(epoch2, record3);

        // Test draining dirty queue for proxy 0
        let persist_index = state_collector.get_persist_index();
        let dirty_queue = coordinator.drain_dirty_queue(0, persist_index);
        assert_eq!(dirty_queue.len(), 1); // Only record2 should match (proxy 0, epoch > 1)
        assert_eq!(dirty_queue[0].consensus_index, Some(150));

        // Test draining for proxy 1
        let dirty_queue_1 = coordinator.drain_dirty_queue(1, persist_index);
        assert_eq!(dirty_queue_1.len(), 1); // Only record3 should match (proxy 1, epoch > 1)
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
        let state_collector = StateCollector::new(3);
        let epoch = EpochId(101); // Epoch > persist_index

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

        // Simulate all proxies reporting epoch 100 (persist_index = 100)
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 100, snapshot, 3, None);
        }

        // Test that get_next_replay_batch returns all dirty transactions
        // Note: drain_dirty_queue doesn't actually remove items, so each call returns the same items
        // The batch_size parameter is deprecated and ignored - all transactions are returned
        let persist_index = state_collector.get_persist_index();
        let batch1 = coordinator.get_next_replay_batch(0, persist_index);
        assert!(batch1.is_some());
        assert_eq!(batch1.unwrap().len(), 7); // Returns all 7 items (batch_size is ignored)

        // Subsequent calls return the same batch (items aren't removed)
        let batch2 = coordinator.get_next_replay_batch(0, persist_index);
        assert!(batch2.is_some());
        assert_eq!(batch2.unwrap().len(), 7);
    }

    #[test]
    fn test_recovery_coordinator_empty_dirty_queue() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);

        // Simulate all proxies reporting high epoch 1000
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 1000, snapshot, 3, None);
        }

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
        let persist_index = state_collector.get_persist_index();
        let dirty_queue = coordinator.drain_dirty_queue(0, persist_index);
        assert!(dirty_queue.is_empty());
    }

    #[test]
    fn test_recovery_coordinator_mixed_proxy_epochs() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);

        // Simulate all proxies reporting epoch 1 (persist_index = 1)
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 1, snapshot, 3, None);
        }

        // Add records to different epochs and proxies
        let epoch2 = EpochId(2); // > persist_index
        let epoch3 = EpochId(3); // > persist_index

        // Epoch 2: proxy 0
        let record1 = LogRecord {
            consensus_index: Some(200),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch2,
        };

        // Epoch 2: proxy 1
        let record2 = LogRecord {
            consensus_index: Some(250),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 1,
            required_states: BTreeMap::new(),
            epoch: epoch2,
        };

        // Epoch 3: proxy 0
        let record3 = LogRecord {
            consensus_index: Some(300),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch3,
        };

        logger.append(epoch2, record1);
        logger.append(epoch2, record2);
        logger.append(epoch3, record3);

        // Test draining for proxy 0 (should get records from both epochs)
        let persist_index = state_collector.get_persist_index();
        let dirty_queue_0 = coordinator.drain_dirty_queue(0, persist_index);
        assert_eq!(dirty_queue_0.len(), 2);
        // Order may vary, so check both possible orders
        let indices: Vec<Option<u64>> = dirty_queue_0.iter().map(|r| r.consensus_index).collect();
        assert!(indices.contains(&Some(200)) && indices.contains(&Some(300)));

        // Test draining for proxy 1 (should get record from epoch 2 only)
        let dirty_queue_1 = coordinator.drain_dirty_queue(1, persist_index);
        assert_eq!(dirty_queue_1.len(), 1);
        assert_eq!(dirty_queue_1[0].consensus_index, Some(250));
    }

    #[test]
    fn test_recovery_coordinator_consensus_index_filtering() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);
        let epoch = EpochId(101); // > persist_index of 100

        // Add records with different consensus indices
        let records = vec![
            (50, 0), // Various consensus indices, proxy 0
            (100, 0),
            (150, 0),
            (200, 1), // proxy 1
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

        // Simulate all proxies reporting epoch 100 (persist_index = 100)
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 100, snapshot, 3, None);
        }

        // Test collect_replay_set with different from_index values (uses consensus_index filtering)
        let replay_set_50 = coordinator.collect_replay_set(epoch, 50, 0);
        assert_eq!(replay_set_50.len(), 3); // indices 50, 100, and 150

        let replay_set_120 = coordinator.collect_replay_set(epoch, 120, 0);
        assert_eq!(replay_set_120.len(), 1); // index 150 only

        let replay_set_200 = coordinator.collect_replay_set(epoch, 200, 0);
        assert_eq!(replay_set_200.len(), 0); // no records

        // Test drain_dirty_queue for proxy 0 (uses epoch filtering)
        // Should get ALL records for proxy 0 since epoch 101 > persist_index 100
        let persist_index = state_collector.get_persist_index();
        let dirty_queue_0 = coordinator.drain_dirty_queue(0, persist_index);
        assert_eq!(dirty_queue_0.len(), 3); // All 3 records for proxy 0

        // Test drain_dirty_queue for proxy 1
        let dirty_queue_1 = coordinator.drain_dirty_queue(1, persist_index);
        assert_eq!(dirty_queue_1.len(), 1); // 1 record for proxy 1
    }

    #[test]
    fn test_recovery_coordinator_none_consensus_index() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);

        // Simulate all proxies reporting epoch 1
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 1, snapshot, 3, None);
        }

        let epoch = EpochId(2); // > persist_index

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

        // Should not appear in collect_replay_set (which filters by consensus_index)
        let replay_set = coordinator.collect_replay_set(epoch, 0, 0);
        assert!(replay_set.is_empty());

        // WILL appear in drain_dirty_queue (which only filters by epoch)
        let persist_index = state_collector.get_persist_index();
        let dirty_queue = coordinator.drain_dirty_queue(0, persist_index);
        assert_eq!(dirty_queue.len(), 1); // Record appears because epoch 2 > persist_index 1
        assert!(dirty_queue[0].consensus_index.is_none());
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
        let state_collector = StateCollector::new(3);

        // Initial persist index should be 0
        assert_eq!(coordinator.get_persist_index(&state_collector), 0);

        // Simulate all proxies reporting epoch 500
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 500, snapshot, 3, None);
        }
        assert_eq!(coordinator.get_persist_index(&state_collector), 500);

        // Simulate all proxies reporting epoch 1000
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 1000, snapshot, 3, None);
        }
        assert_eq!(coordinator.get_persist_index(&state_collector), 1000);
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
        let state_collector = StateCollector::new(3);
        let epoch = EpochId(101); // > persist_index

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

        // Simulate all proxies reporting epoch 100 (persist_index = 100)
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 100, snapshot, 3, None);
        }

        // Test that get_next_replay_batch returns all dirty transactions
        // Note: drain_dirty_queue doesn't actually remove items, so each call returns the same items
        // The batch_size parameter is deprecated and ignored - all transactions are returned
        let persist_index = state_collector.get_persist_index();
        let batch1 = coordinator.get_next_replay_batch(0, persist_index);
        assert!(batch1.is_some());
        assert_eq!(batch1.unwrap().len(), 12); // Returns all 12 items (batch_size is ignored)

        // Subsequent calls return the same batch (items aren't removed)
        let batch2 = coordinator.get_next_replay_batch(0, persist_index);
        assert!(batch2.is_some());
        assert_eq!(batch2.unwrap().len(), 12);
    }

    /// This test validates that dirty transactions can be retrieved after a failure.
    /// It simulates a real failure scenario where:
    /// 1. Transactions are logged with high consensus indices but low epoch numbers
    /// 2. Proxies report completion up to a certain epoch (setting persist_index)
    /// 3. Recovery coordinator should find transactions in epochs AFTER persist_index
    #[test]
    fn test_recovery_coordinator_failure_scenario_with_epochs() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);

        // Simulate scenario:
        // - All proxies have completed and reported up to epoch 10
        // - Then new transactions arrive in epoch 11 and 12 for proxy 0
        // - Proxy 0 fails before completing those epochs
        // - We need to recover those transactions

        // Step 1: All proxies report completion of epoch 10
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 10, snapshot, 3, None);
        }

        // Verify persist_index is now 10
        let persist_index = state_collector.get_persist_index();
        assert_eq!(persist_index, 10, "Persist index should be epoch 10");

        // Step 2: Add transactions for proxy 0 in epochs 11 and 12
        // These transactions have high consensus indices (e.g., 1000+)
        // but are in epochs 11 and 12
        let epoch11 = EpochId(11);
        let epoch12 = EpochId(12);

        let record1 = LogRecord {
            consensus_index: Some(1000), // High consensus index
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch11,
        };

        let record2 = LogRecord {
            consensus_index: Some(1001),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch11,
        };

        let record3 = LogRecord {
            consensus_index: Some(1002),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch12,
        };

        logger.append(epoch11, record1);
        logger.append(epoch11, record2);
        logger.append(epoch12, record3);

        // Step 3: Proxy 0 fails - try to get dirty transactions
        // These should be found because they're in epochs 11 and 12 (> persist_index of 10)
        let dirty_queue = coordinator.drain_dirty_queue(0, persist_index);

        // This assertion will FAIL with the current bug because the code compares
        // consensus_index (1000+) with persist_index (10), which makes it seem like
        // all transactions are much newer than the persist point
        assert_eq!(
            dirty_queue.len(),
            3,
            "Should find 3 dirty transactions for proxy 0 in epochs 11 and 12"
        );

        // Verify the transactions can be retrieved via get_next_replay_batch
        let batch = coordinator.get_next_replay_batch(0, persist_index);
        assert!(
            batch.is_some(),
            "Should be able to get replay batch for failed proxy"
        );
        let batch = batch.unwrap();
        assert!(
            !batch.is_empty(),
            "Replay batch should contain transactions"
        );
    }

    /// Test that verifies transactions in completed epochs are NOT included in dirty queue
    #[test]
    fn test_recovery_coordinator_filters_completed_epochs() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);

        // All proxies report completion of epoch 20
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 20, snapshot, 3, None);
        }

        let persist_index = state_collector.get_persist_index();
        assert_eq!(persist_index, 20);

        // Add transactions in various epochs
        let epoch15 = EpochId(15); // Before persist_index
        let epoch20 = EpochId(20); // At persist_index - should be excluded
        let epoch25 = EpochId(25); // After persist_index - should be included

        let record_old = LogRecord {
            consensus_index: Some(500),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch15,
        };

        let record_at_persist = LogRecord {
            consensus_index: Some(600),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch20,
        };

        let record_new = LogRecord {
            consensus_index: Some(700),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch25,
        };

        logger.append(epoch15, record_old);
        logger.append(epoch20, record_at_persist);
        logger.append(epoch25, record_new);

        // Drain dirty queue
        let dirty_queue = coordinator.drain_dirty_queue(0, persist_index);

        // Should only include record_new (epoch 25 > persist_index 20)
        // record_at_persist should be excluded even though epoch == persist_index
        assert_eq!(
            dirty_queue.len(),
            1,
            "Should only find transactions in epochs > persist_index"
        );
        assert_eq!(
            dirty_queue[0].epoch.0, 25,
            "Dirty transaction should be from epoch 25"
        );
    }

    /// Test recovery with mixed consensus indices and epochs
    #[test]
    fn test_recovery_coordinator_mixed_consensus_and_epochs() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);

        // Simulate persist_index = 100 (all proxies completed epoch 100)
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 100, snapshot, 3, None);
        }

        let persist_index = state_collector.get_persist_index();
        assert_eq!(persist_index, 100);

        // Add transactions with LOW consensus indices but HIGH epoch numbers
        // This is the key test case that exposes the bug
        let epoch101 = EpochId(101);
        let epoch102 = EpochId(102);

        let record1 = LogRecord {
            consensus_index: Some(5), // Very low consensus index
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch101, // But high epoch number
        };

        let record2 = LogRecord {
            consensus_index: Some(10), // Low consensus index
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch102,
        };

        // Also add a transaction with HIGH consensus index in OLD epoch (should be filtered)
        let epoch50 = EpochId(50);
        let record_old = LogRecord {
            consensus_index: Some(9999), // Very high consensus index
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch50, // But old epoch
        };

        logger.append(epoch101, record1);
        logger.append(epoch102, record2);
        logger.append(epoch50, record_old);

        // Drain dirty queue
        let dirty_queue = coordinator.drain_dirty_queue(0, persist_index);

        // Should find records in epochs 101 and 102, but NOT epoch 50
        // This will FAIL with the bug because:
        // - Bug compares consensus_index (5, 10) < persist_index (100), so excludes them
        // - Bug compares consensus_index (9999) >= persist_index (100), so includes old epoch
        assert_eq!(
            dirty_queue.len(),
            2,
            "Should find 2 dirty transactions in epochs 101 and 102"
        );

        // Verify the correct epochs are included
        let epochs: Vec<u64> = dirty_queue.iter().map(|r| r.epoch.0).collect();
        assert!(epochs.contains(&101), "Should include epoch 101");
        assert!(epochs.contains(&102), "Should include epoch 102");
        assert!(!epochs.contains(&50), "Should NOT include epoch 50");
    }

    /// Test per-proxy persist_index scenario:
    /// When a proxy fails, we should use its own persist_index, not the global minimum
    #[test]
    fn test_recovery_with_per_proxy_persist_index() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);

        // Simulate a realistic scenario:
        // - Proxy 0 reports epoch 10
        // - Proxy 1 reports epoch 10
        // - Proxy 2 reports epoch 10
        // Then more transactions arrive in epoch 11 for all proxies
        // - Proxy 0 FAILS before reporting epoch 11
        // - Proxy 1 reports epoch 11
        // - Proxy 2 reports epoch 11

        // All proxies report epoch 10
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 10, snapshot, 3, None);
        }

        // Add transactions in epoch 11 for all proxies
        let epoch11 = EpochId(11);
        for i in 0..5 {
            let record = LogRecord {
                consensus_index: Some(1000 + i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: 0, // All for proxy 0
                required_states: BTreeMap::new(),
                epoch: epoch11,
            };
            logger.append(epoch11, record);
        }

        // Proxy 1 and 2 report epoch 11, but proxy 0 fails
        state_collector.process_snapshot::<TestTransaction>(1, 11, BTreeMap::new(), 3, None);
        state_collector.process_snapshot::<TestTransaction>(2, 11, BTreeMap::new(), 3, None);

        // At this point:
        // - Proxy 0 persist_index = 10 (failed, didn't report epoch 11)
        // - Proxy 1 persist_index = 11
        // - Proxy 2 persist_index = 11
        // - Global persist_index (minimum) = 10

        // Recovery should use proxy 0's own persist_index (10), not the global minimum
        let proxy0_persist_index = state_collector.get_proxy_persist_index(0);
        assert_eq!(proxy0_persist_index, 10);

        let dirty_queue = coordinator.drain_dirty_queue(0, proxy0_persist_index);
        assert_eq!(
            dirty_queue.len(),
            5,
            "Should find 5 dirty transactions in epoch 11 (> proxy 0's persist_index of 10)"
        );

        // Verify we can get a replay batch
        let batch = coordinator.get_next_replay_batch(0, proxy0_persist_index);
        assert!(batch.is_some(), "Should get a replay batch for proxy 0");
        assert!(!batch.unwrap().is_empty());
    }

    /// Test that demonstrates the actual bug with realistic values
    #[test]
    fn test_recovery_coordinator_realistic_failure_scenario() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new_with_batch_size(logger.clone(), 10);
        let state_collector = StateCollector::new(3);

        // Realistic scenario:
        // - System has been running, all proxies completed epoch 500
        // - Consensus indices are much higher (e.g., 50000+) from accumulated transactions
        // - New transactions arrive in epoch 501 with consensus indices 50000-50010
        // - Proxy 0 fails

        // All proxies report epoch 500
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 500, snapshot, 3, None);
        }

        let persist_index = state_collector.get_persist_index();
        assert_eq!(persist_index, 500);

        // Add 5 transactions in epoch 501 with high consensus indices
        let epoch501 = EpochId(501);
        for i in 0..5 {
            let record = LogRecord {
                consensus_index: Some(50000 + i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: 0,
                required_states: BTreeMap::new(),
                epoch: epoch501,
            };
            logger.append(epoch501, record);
        }

        // Proxy 0 fails - try to recover
        let batch = coordinator.get_next_replay_batch(0, persist_index);

        assert!(
            batch.is_some(),
            "Should be able to get replay batch after failure"
        );

        let batch = batch.unwrap();
        assert_eq!(
            batch.len(),
            5,
            "Should get all 5 transactions from epoch 501"
        );

        // Verify all transactions are from the correct epoch
        for record in batch {
            assert_eq!(
                record.epoch.0, 501,
                "All replay transactions should be from epoch 501"
            );
        }
    }

    #[test]
    fn test_collect_uncommitted_transactions_all_proxies() {
        // Test that collect_uncommitted_transactions includes transactions to ALL proxies
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        let epoch1 = EpochId(1);
        let epoch2 = EpochId(2);

        // Add transactions to different proxies with various consensus_indices
        // Proxy 0: consensus_index 1, 2, 3
        for i in 1..=3 {
            let record = LogRecord {
                consensus_index: Some(i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: 0,
                required_states: BTreeMap::new(),
                epoch: epoch1,
            };
            logger.append(epoch1, record);
        }

        // Proxy 1: consensus_index 4, 5, 6
        for i in 4..=6 {
            let record = LogRecord {
                consensus_index: Some(i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: 1,
                required_states: BTreeMap::new(),
                epoch: epoch1,
            };
            logger.append(epoch1, record);
        }

        // Proxy 2: consensus_index 7, 8, 9
        for i in 7..=9 {
            let record = LogRecord {
                consensus_index: Some(i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: 2,
                required_states: BTreeMap::new(),
                epoch: epoch2,
            };
            logger.append(epoch2, record);
        }

        // Set watermark at batch 3
        let completed_up_to = 3;

        // Collect uncommitted transactions
        let uncommitted = coordinator.collect_uncommitted_transactions(completed_up_to);

        // Should include transactions from ALL proxies with consensus_index > 3
        assert_eq!(
            uncommitted.len(),
            6,
            "Should collect 6 transactions (indices 4-9) from all proxies"
        );

        // Verify all have consensus_index > 3
        for record in &uncommitted {
            assert!(
                record.consensus_index.unwrap() > completed_up_to,
                "All transactions should have consensus_index > {}",
                completed_up_to
            );
        }

        // Verify transactions from all proxies are included
        let proxy_0_count = uncommitted
            .iter()
            .filter(|r| r.destination_proxy == 0)
            .count();
        let proxy_1_count = uncommitted
            .iter()
            .filter(|r| r.destination_proxy == 1)
            .count();
        let proxy_2_count = uncommitted
            .iter()
            .filter(|r| r.destination_proxy == 2)
            .count();

        assert_eq!(proxy_0_count, 0, "Proxy 0 has no txns with index > 3");
        assert_eq!(proxy_1_count, 3, "Proxy 1 should have 3 transactions");
        assert_eq!(proxy_2_count, 3, "Proxy 2 should have 3 transactions");

        // Verify sorted by consensus_index
        for i in 0..uncommitted.len() - 1 {
            assert!(
                uncommitted[i].consensus_index <= uncommitted[i + 1].consensus_index,
                "Transactions should be sorted by consensus_index"
            );
        }
    }

    #[test]
    fn test_collect_uncommitted_transactions_watermark_boundary() {
        // Test exact boundary: transactions at watermark are excluded, above are included
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        let epoch = EpochId(10);

        // Add transactions with consensus_indices around watermark
        for i in 3..=7 {
            let record = LogRecord {
                consensus_index: Some(i),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                destination_proxy: i as usize % 3, // Distribute across proxies
                required_states: BTreeMap::new(),
                epoch,
            };
            logger.append(epoch, record);
        }

        // Test watermark at 5
        let completed_up_to = 5;
        let uncommitted = coordinator.collect_uncommitted_transactions(completed_up_to);

        // Should include only 6 and 7
        assert_eq!(uncommitted.len(), 2, "Should only include indices 6 and 7");
        assert_eq!(uncommitted[0].consensus_index, Some(6));
        assert_eq!(uncommitted[1].consensus_index, Some(7));

        // Test watermark at 3
        let completed_up_to = 3;
        let uncommitted = coordinator.collect_uncommitted_transactions(completed_up_to);

        // Should include 4, 5, 6, 7
        assert_eq!(uncommitted.len(), 4, "Should include indices 4, 5, 6, 7");

        // Test watermark at 7 (all complete)
        let completed_up_to = 7;
        let uncommitted = coordinator.collect_uncommitted_transactions(completed_up_to);

        // Should be empty
        assert_eq!(
            uncommitted.len(),
            0,
            "Should be empty when all batches complete"
        );
    }

    #[test]
    fn test_collect_uncommitted_transactions_empty() {
        // Test with no transactions
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        let uncommitted = coordinator.collect_uncommitted_transactions(0);
        assert_eq!(uncommitted.len(), 0, "Should be empty with no transactions");

        // Test with transactions but all complete
        let epoch = EpochId(5);
        for i in 1..=5 {
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

        let uncommitted = coordinator.collect_uncommitted_transactions(10);
        assert_eq!(
            uncommitted.len(),
            0,
            "Should be empty when watermark exceeds all transactions"
        );
    }

    #[test]
    fn test_collect_uncommitted_transactions_consensus_order() {
        // Test that transactions are returned in consensus order
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        // Add transactions in non-sequential order across epochs
        let records_data = vec![
            (EpochId(3), 15),
            (EpochId(1), 3),
            (EpochId(2), 8),
            (EpochId(1), 1),
            (EpochId(3), 12),
            (EpochId(2), 5),
        ];

        for (epoch, consensus_index) in records_data {
            let record = LogRecord {
                consensus_index: Some(consensus_index),
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(consensus_index),
                destination_proxy: (consensus_index % 3) as usize,
                required_states: BTreeMap::new(),
                epoch,
            };
            logger.append(epoch, record);
        }

        // Collect with watermark at 2
        let uncommitted = coordinator.collect_uncommitted_transactions(2);

        // Should get: 3, 5, 8, 12, 15 (in order)
        assert_eq!(uncommitted.len(), 5);

        let indices: Vec<u64> = uncommitted
            .iter()
            .map(|r| r.consensus_index.unwrap())
            .collect();
        assert_eq!(
            indices,
            vec![3, 5, 8, 12, 15],
            "Should be in consensus order"
        );
    }

    #[test]
    fn test_collect_uncommitted_transactions_handles_none_consensus_index() {
        // Test behavior with transactions that have None consensus_index
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        let epoch = EpochId(5);

        // Add transactions with consensus_index
        for i in 1..=5 {
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

        // Add a transaction with None consensus_index
        let record_none = LogRecord {
            consensus_index: None,
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(999),
            destination_proxy: 1,
            required_states: BTreeMap::new(),
            epoch,
        };
        logger.append(epoch, record_none);

        // Collect with watermark at 2
        let uncommitted = coordinator.collect_uncommitted_transactions(2);

        // Should include indices 3, 4, 5
        // Note: transactions with None are treated as 0, so excluded (0 <= 2)
        assert_eq!(
            uncommitted.len(),
            3,
            "Should not include None consensus_index"
        );

        for record in uncommitted {
            assert!(
                record.consensus_index.is_some(),
                "Should only have Some values"
            );
            assert!(record.consensus_index.unwrap() > 2);
        }
    }

    #[test]
    fn test_collect_uncommitted_vs_drain_dirty_queue() {
        // Compare old approach (drain_dirty_queue) vs new approach (collect_uncommitted_transactions)
        // Note: drain_dirty_queue uses epoch-based filtering (epoch.0 > persist_index)
        //       collect_uncommitted_transactions uses batch watermark (consensus_index > completed_up_to)
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        let epoch1 = EpochId(1);
        let epoch2 = EpochId(2);
        let epoch3 = EpochId(3);

        // Add transactions to different proxies with consensus_indices
        // Proxy 0 (will "fail"): consensus_index 1, 3, 5 in epochs 1, 2, 3
        let record1 = LogRecord {
            consensus_index: Some(1),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(1),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch1,
        };
        logger.append(epoch1, record1);

        let record3 = LogRecord {
            consensus_index: Some(3),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(3),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch2,
        };
        logger.append(epoch2, record3);

        let record5 = LogRecord {
            consensus_index: Some(5),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(5),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
            epoch: epoch3,
        };
        logger.append(epoch3, record5);

        // Proxy 1: consensus_index 2, 4, 6 in epochs 1, 2, 3
        let record2 = LogRecord {
            consensus_index: Some(2),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(2),
            destination_proxy: 1,
            required_states: BTreeMap::new(),
            epoch: epoch1,
        };
        logger.append(epoch1, record2);

        let record4 = LogRecord {
            consensus_index: Some(4),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(4),
            destination_proxy: 1,
            required_states: BTreeMap::new(),
            epoch: epoch2,
        };
        logger.append(epoch2, record4);

        let record6 = LogRecord {
            consensus_index: Some(6),
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(6),
            destination_proxy: 1,
            required_states: BTreeMap::new(),
            epoch: epoch3,
        };
        logger.append(epoch3, record6);

        // Test with watermark/persist_index = 2
        // OLD APPROACH: drain_dirty_queue filters by epoch.0 > 2, so gets epochs 3+
        let dirty_only = coordinator.drain_dirty_queue(0, 2);

        // NEW APPROACH: collect_uncommitted_transactions filters by consensus_index > 2
        let all_uncommitted = coordinator.collect_uncommitted_transactions(2);

        // Old approach: only proxy 0's transactions in epochs > 2 (index 5 in epoch 3)
        assert_eq!(
            dirty_only.len(),
            1,
            "drain_dirty_queue: epoch-based, only proxy 0"
        );
        assert_eq!(dirty_only[0].consensus_index, Some(5));

        // New approach: all proxies' transactions with consensus_index > 2 (indices 3, 4, 5, 6)
        assert_eq!(
            all_uncommitted.len(),
            4,
            "collect_uncommitted_transactions: watermark-based, all proxies"
        );

        // Verify new approach includes healthy proxy transactions
        let has_proxy_1_txns = all_uncommitted.iter().any(|r| r.destination_proxy == 1);
        assert!(
            has_proxy_1_txns,
            "New approach should include healthy proxy transactions"
        );

        // Verify old approach does NOT include healthy proxy transactions
        let has_proxy_1_in_dirty = dirty_only.iter().any(|r| r.destination_proxy == 1);
        assert!(
            !has_proxy_1_in_dirty,
            "Old approach should NOT include healthy proxy transactions"
        );

        // Verify consensus order in new approach
        let indices: Vec<u64> = all_uncommitted
            .iter()
            .map(|r| r.consensus_index.unwrap())
            .collect();
        assert_eq!(indices, vec![3, 4, 5, 6], "Should be in consensus order");
    }
}
