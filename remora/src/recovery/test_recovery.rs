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
}
