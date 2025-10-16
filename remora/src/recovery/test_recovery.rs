// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::EpochId;
    use crate::recovery::{EpochLogger, LogRecord, RecoveryCoordinator};
    use std::collections::BTreeMap;
    use sui_types::base_types::ObjectID;
    use sui_types::digests::TransactionDigest;

    #[test]
    fn test_epoch_logger_basic_operations() {
        let logger = EpochLogger::new();
        let epoch = EpochId(1);

        // Test appending records
        let record = LogRecord {
            consensus_index: Some(100),
            txn_digest: TransactionDigest::random(),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
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
        let logger = EpochLogger::new();
        let coordinator = RecoveryCoordinator::new(logger);

        // Test begin recovery
        let replacement = coordinator.begin_recovery(0, 1);
        assert_eq!(replacement, 1);

        // Test locate cut
        let cut = coordinator.locate_cut(0, 50);
        assert_eq!(cut, 50); // Should return default when no last applied

        // Test update last applied
        coordinator.update_last_applied(0, 100);
        let cut_after_update = coordinator.locate_cut(0, 50);
        assert_eq!(cut_after_update, 100);
    }

    #[test]
    fn test_recovery_coordinator_collect_replay_set() {
        let logger = EpochLogger::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let epoch = EpochId(1);

        // Add some test records
        let record1 = LogRecord {
            consensus_index: Some(100),
            txn_digest: TransactionDigest::random(),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
        };

        let record2 = LogRecord {
            consensus_index: Some(150),
            txn_digest: TransactionDigest::random(),
            destination_proxy: 1, // Different proxy
            required_states: BTreeMap::new(),
        };

        let record3 = LogRecord {
            consensus_index: Some(200),
            txn_digest: TransactionDigest::random(),
            destination_proxy: 0,
            required_states: BTreeMap::new(),
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
    fn test_epoch_logger_multiple_epochs() {
        let logger = EpochLogger::new();

        // Add records to different epochs
        for epoch_num in 1..=3 {
            let epoch = EpochId(epoch_num);
            let record = LogRecord {
                consensus_index: Some(epoch_num * 100),
                txn_digest: TransactionDigest::random(),
                destination_proxy: 0,
                required_states: BTreeMap::new(),
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
