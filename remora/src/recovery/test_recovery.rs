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
    fn test_recovery_coordinator_persist_index_updates() {
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let state_collector = StateCollector::new(3);

        // Initial persist epoch should be 0
        assert_eq!(coordinator.get_persist_epoch(&state_collector), EpochId(0));

        // Simulate all proxies reporting epoch 500
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 500, snapshot, 3, None);
        }
        assert_eq!(
            coordinator.get_persist_epoch(&state_collector),
            EpochId(500)
        );

        // Simulate all proxies reporting epoch 1000
        for proxy_id in 0..3 {
            let snapshot = BTreeMap::new();
            state_collector.process_snapshot::<TestTransaction>(proxy_id, 1000, snapshot, 3, None);
        }
        assert_eq!(
            coordinator.get_persist_epoch(&state_collector),
            EpochId(1000)
        );
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
    fn test_collect_uncommitted_transactions_by_epoch() {
        // Test behavior with transactions across epochs
        let logger = EpochLogger::<TestTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());

        // Add transactions in epoch 5
        let epoch5 = EpochId(5);
        for i in 1..=5 {
            let record = LogRecord {
                txn_digest: TransactionDigest::random(),
                transaction: create_test_transaction(i),
                required_states: BTreeMap::new(),
                epoch: epoch5,
            };
            logger.append(epoch5, record);
        }

        // Add transactions in epoch 6
        let epoch6 = EpochId(6);
        let record_epoch6 = LogRecord {
            txn_digest: TransactionDigest::random(),
            transaction: create_test_transaction(999),
            required_states: BTreeMap::new(),
            epoch: epoch6,
        };
        logger.append(epoch6, record_epoch6);

        // Collect with watermark at epoch 4 (should include epochs 5 and 6)
        let uncommitted = coordinator.collect_uncommitted_transactions(EpochId(4));

        // Should include all transactions from epochs 5 and 6
        assert_eq!(
            uncommitted.len(),
            6,
            "Should include all transactions from epochs > 4"
        );

        // Verify epochs are correct
        for record in uncommitted {
            assert!(record.epoch > EpochId(4));
        }
    }
}
