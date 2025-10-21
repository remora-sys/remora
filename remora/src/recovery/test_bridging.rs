// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Comprehensive tests for bridging transaction identification logic.
//!
//! These tests verify that the recovery system correctly identifies which transactions
//! from healthy proxies need to be replayed to regenerate missing intermediate state versions.

#[cfg(test)]
mod tests {
    use crate::checkpoint::state_collector::StateCollector;
    use crate::checkpoint::EpochId;
    use crate::executor::fake::FakeTransaction;
    use crate::recovery::{EpochLogger, LogRecord, RecoveryCoordinator};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use sui_types::base_types::{ObjectID, SequenceNumber};
    use sui_types::digests::TransactionDigest;
    use sui_types::object::Object;

    /// Helper to create a test LogRecord
    fn create_log_record(
        epoch: u64,
        consensus_index: u64,
        destination_proxy: usize,
        txn_digest: TransactionDigest,
        required_states: Vec<(ObjectID, SequenceNumber, Option<usize>)>,
    ) -> LogRecord<FakeTransaction> {
        let required_states_map: BTreeMap<(ObjectID, SequenceNumber), Option<usize>> =
            required_states
                .into_iter()
                .map(|(oid, ver, proxy)| ((oid, ver), proxy))
                .collect();

        LogRecord {
            consensus_index: Some(consensus_index),
            txn_digest,
            transaction: Arc::new(
                crate::executor::api::TransactionWithTimestamp::new_for_tests(
                    FakeTransaction::new(vec![]),
                ),
            ),
            destination_proxy,
            required_states: required_states_map,
            epoch: EpochId(epoch),
        }
    }

    // Removed: unused helper function - we construct collectors inline in tests for clarity

    #[test]
    fn test_case1_version_available_in_snapshot() {
        // Case 1: required_version == persisted_version
        // No bridging needed - version is available in snapshot

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot has Object A at version 5
        let obj_a_v5 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v5);

        // Dirty transaction on failed proxy (P0) requires A:v5
        let dirty_txn = create_log_record(
            11, // epoch
            100,
            0, // failed proxy
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(5), None)],
        );

        let dirty_txns = vec![dirty_txn];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: No missing versions - exact match
        assert_eq!(missing.len(), 0, "No bridging needed when version matches");
    }

    #[test]
    fn test_case2_version_gap_healthy_advanced() {
        // Case 2: required_version < persisted_version
        // Bridging needed - healthy proxy advanced past required version

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot has Object A at version 7 (healthy proxy advanced it)
        let obj_a_v7 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v7);

        // Log a transaction on healthy proxy P1 that produced v5
        // (requires v4, produces v5)
        let healthy_txn_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                100,
                1, // healthy proxy
                healthy_txn_digest,
                vec![(obj_a, SequenceNumber::from_u64(4), None)],
            ),
        );

        // Dirty transaction on failed proxy P0 requires A:v5
        let dirty_txn = create_log_record(
            11,
            101,
            0, // failed proxy
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(5), None)],
        );

        let dirty_txns = vec![dirty_txn];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: v5 is missing (gap between v5 and v7)
        assert_eq!(missing.len(), 1, "Should identify version gap");
        assert!(
            missing.contains(&(obj_a, SequenceNumber::from_u64(5))),
            "Should identify v5 as missing"
        );

        // Test: Collect bridging transactions
        let bridging = coordinator.collect_bridging_transactions(&missing, 10, 0);

        // Assert: Should include the healthy proxy transaction
        assert_eq!(bridging.len(), 1, "Should find bridging transaction");
        assert_eq!(bridging[0].txn_digest, healthy_txn_digest);
        assert_eq!(bridging[0].destination_proxy, 1);
    }

    #[test]
    fn test_case3a_version_produced_by_dirty_txn() {
        // Case 3a: required_version > persisted_version
        // But the version will be produced by another dirty transaction
        // No bridging needed

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot has Object A at version 4
        let obj_a_v4 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v4);

        // Dirty transaction 1 on failed proxy: requires v4, produces v5
        let dirty_txn1 = create_log_record(
            11,
            100,
            0, // failed proxy
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(4), None)],
        );

        // Dirty transaction 2 on failed proxy: requires v5, produces v6
        let dirty_txn2 = create_log_record(
            11,
            101,
            0, // failed proxy
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(5), None)],
        );

        let dirty_txns = vec![dirty_txn1, dirty_txn2];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: No missing versions - v5 will be produced by dirty_txn1
        assert_eq!(
            missing.len(),
            0,
            "No bridging needed when dirty txns produce required versions"
        );
    }

    #[test]
    fn test_case3b_version_produced_by_healthy_proxy_not_snapshotted() {
        // Case 3b: required_version > persisted_version
        // And the version was produced by a healthy proxy (not yet snapshotted)
        // Bridging needed

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot has Object A at version 4
        let obj_a_v4 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v4);

        // Log a transaction on healthy proxy P1 that produced v5
        // (requires v4, produces v5) - but not yet snapshotted
        let healthy_txn_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                100,
                1, // healthy proxy
                healthy_txn_digest,
                vec![(obj_a, SequenceNumber::from_u64(4), None)],
            ),
        );

        // Dirty transaction on failed proxy P0 requires A:v5
        let dirty_txn = create_log_record(
            11,
            101,
            0, // failed proxy
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(5), None)],
        );

        let dirty_txns = vec![dirty_txn];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: v5 is missing (produced by healthy proxy)
        assert_eq!(
            missing.len(),
            1,
            "Should identify missing version from healthy proxy"
        );
        assert!(missing.contains(&(obj_a, SequenceNumber::from_u64(5))));

        // Test: Collect bridging transactions
        let bridging = coordinator.collect_bridging_transactions(&missing, 10, 0);

        // Assert: Should include the healthy proxy transaction
        assert_eq!(bridging.len(), 1, "Should find bridging transaction");
        assert_eq!(bridging[0].txn_digest, healthy_txn_digest);
    }

    #[test]
    fn test_case4_new_object_created_by_healthy_proxy() {
        // Case 4: Object doesn't exist in snapshot at all (None case)
        // Dirty transaction requires a version from a newly created object
        // And that object was created/modified by a healthy proxy
        // Bridging needed

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Object A doesn't exist in snapshot (collector is empty for obj_a)
        // No objects in collector at all

        // Log a transaction on healthy proxy P1 that creates/modifies A from v1 to v2
        let healthy_txn_creates_v2 = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                99,
                1, // healthy proxy
                healthy_txn_creates_v2,
                vec![(obj_a, SequenceNumber::from_u64(1), None)], // requires v1, produces v2
            ),
        );

        // Dirty transaction on failed proxy P0 requires A:v2
        // But collector doesn't have obj_a at all (None case)
        let dirty_txn_needs_v2 = create_log_record(
            11,
            102,
            0, // failed proxy
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(2), None)],
        );

        let dirty_txns = vec![dirty_txn_needs_v2];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: v2 is missing (object not in collector, but produced by healthy proxy)
        assert_eq!(
            missing.len(),
            1,
            "v2 is missing (object created by healthy proxy)"
        );
        assert!(missing.contains(&(obj_a, SequenceNumber::from_u64(2))));

        // Test: Collect bridging transactions
        let bridging = coordinator.collect_bridging_transactions(&missing, 10, 0);

        // Assert: Should find the transaction that produces v2
        assert_eq!(bridging.len(), 1, "Should find txn that produces v2");
        assert_eq!(bridging[0].txn_digest, healthy_txn_creates_v2);
        assert_eq!(bridging[0].destination_proxy, 1);
    }

    #[test]
    fn test_multiple_bridging_transactions_ordered() {
        // Test that multiple bridging transactions are returned in consensus order

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot has Object A at version 4
        let obj_a_v4 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v4);

        // Log transactions on healthy proxies that produced v5, v6, v7
        // Add them out of order to test sorting

        let txn2_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                102,
                1,
                txn2_digest,
                vec![(obj_a, SequenceNumber::from_u64(5), None)], // produces v6
            ),
        );

        let txn1_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                101,
                1,
                txn1_digest,
                vec![(obj_a, SequenceNumber::from_u64(4), None)], // produces v5
            ),
        );

        let txn3_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                103,
                2,
                txn3_digest,
                vec![(obj_a, SequenceNumber::from_u64(6), None)], // produces v7
            ),
        );

        // Dirty transaction needs v7
        let dirty_txn = create_log_record(
            11,
            104,
            0,
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(7), None)],
        );

        let dirty_txns = vec![dirty_txn];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: v7 is missing
        assert!(missing.contains(&(obj_a, SequenceNumber::from_u64(7))));

        // Test: Collect bridging transactions
        let bridging = coordinator.collect_bridging_transactions(&missing, 10, 0);

        // Assert: Should return transactions in consensus order
        assert_eq!(
            bridging.len(),
            1,
            "Should find the transaction producing v7"
        );
        assert_eq!(bridging[0].txn_digest, txn3_digest);
        assert_eq!(
            bridging[0].consensus_index,
            Some(103),
            "Should maintain consensus order"
        );
    }

    #[test]
    fn test_multiple_objects_with_gaps() {
        // Test handling multiple objects with different gap patterns

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();
        let obj_b = ObjectID::random();
        let obj_c = ObjectID::random();

        // Setup collector:
        // - Object A at v10 (advanced)
        // - Object B at v3 (matches)
        // - Object C at v2 (behind)

        let obj_a_v10 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v10);

        let obj_b_v3 = Object::immutable_with_id_for_testing(obj_b);
        collector.merged_state.insert(obj_b, obj_b_v3);

        let obj_c_v2 = Object::immutable_with_id_for_testing(obj_c);
        collector.merged_state.insert(obj_c, obj_c_v2);

        // Log healthy proxy transactions
        let txn_a5_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                100,
                1,
                txn_a5_digest,
                vec![(obj_a, SequenceNumber::from_u64(4), None)], // produces A:v5
            ),
        );

        let txn_c4_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                101,
                2,
                txn_c4_digest,
                vec![(obj_c, SequenceNumber::from_u64(3), None)], // produces C:v4
            ),
        );

        // Dirty transactions need:
        // - A:v5 (gap: collector has v10)
        // - B:v3 (match: collector has v3)
        // - C:v4 (ahead: collector has v2, but produced by healthy proxy)

        let dirty_txn = create_log_record(
            11,
            105,
            0,
            TransactionDigest::random(),
            vec![
                (obj_a, SequenceNumber::from_u64(5), None),
                (obj_b, SequenceNumber::from_u64(3), None),
                (obj_c, SequenceNumber::from_u64(4), None),
            ],
        );

        let dirty_txns = vec![dirty_txn];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: A:v5 and C:v4 are missing
        assert_eq!(missing.len(), 2, "Should identify gaps for A and C");
        assert!(missing.contains(&(obj_a, SequenceNumber::from_u64(5))));
        assert!(missing.contains(&(obj_c, SequenceNumber::from_u64(4))));
        assert!(!missing.contains(&(obj_b, SequenceNumber::from_u64(3))));

        // Test: Collect bridging transactions
        let bridging = coordinator.collect_bridging_transactions(&missing, 10, 0);

        // Assert: Should include both bridging transactions
        assert_eq!(bridging.len(), 2, "Should find both bridging transactions");

        let digests: Vec<_> = bridging.iter().map(|r| r.txn_digest).collect();
        assert!(digests.contains(&txn_a5_digest));
        assert!(digests.contains(&txn_c4_digest));
    }

    #[test]
    fn test_complex_scenario_with_chain() {
        // Test a complex dependency chain:
        // Snapshot: A=v4
        // Healthy P1: txn1(A:v4→v5)
        // Healthy P2: txn2(A:v5→v6)
        // Failed P0: txn3(A:v6→v7) [dirty]
        // Healthy P1: txn4(A:v7→v8)
        // Collector now has A=v8
        // Failed P0: txn5 needs A:v6 [dirty]
        //
        // Should identify v6 as missing and include txn2 as bridging

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot at v4, collector now at v8
        let obj_a_v8 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v8);

        // Log the transaction chain
        let txn1_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                100,
                1,
                txn1_digest,
                vec![(obj_a, SequenceNumber::from_u64(4), None)],
            ),
        );

        let txn2_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                101,
                2,
                txn2_digest,
                vec![(obj_a, SequenceNumber::from_u64(5), None)],
            ),
        );

        let txn3_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                102,
                0,
                txn3_digest,
                vec![(obj_a, SequenceNumber::from_u64(6), None)],
            ),
        );

        let txn4_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                103,
                1,
                txn4_digest,
                vec![(obj_a, SequenceNumber::from_u64(7), None)],
            ),
        );

        // Dirty transaction needs v6
        let dirty_txn = create_log_record(
            11,
            104,
            0,
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(6), None)],
        );

        let dirty_txns = vec![dirty_txn];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: v6 is missing (gap between snapshot v4 and current v8)
        assert_eq!(missing.len(), 1);
        assert!(missing.contains(&(obj_a, SequenceNumber::from_u64(6))));

        // Test: Collect bridging transactions
        let bridging = coordinator.collect_bridging_transactions(&missing, 10, 0);

        // Assert: Should include txn2 (produces v6), not txn3 (from failed proxy)
        assert_eq!(bridging.len(), 1, "Should find txn2 as bridging");
        assert_eq!(bridging[0].txn_digest, txn2_digest);
        assert_eq!(bridging[0].destination_proxy, 2);
    }

    #[test]
    fn test_begin_recovery_with_bridging_integration() {
        // Integration test for the complete workflow

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot at v4, collector at v10
        let obj_a_v10 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v10);

        // Healthy proxy produces v5
        let bridging_txn_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                100,
                1,
                bridging_txn_digest,
                vec![(obj_a, SequenceNumber::from_u64(4), None)],
            ),
        );

        // Failed proxy transaction needs v5
        let dirty_txn_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                101,
                0,
                dirty_txn_digest,
                vec![(obj_a, SequenceNumber::from_u64(5), None)],
            ),
        );

        // Set persist_index for failed proxy
        collector
            .per_proxy_persist_index
            .insert(0, std::sync::atomic::AtomicU64::new(10));

        // Test: Complete recovery workflow
        let (bridging_txns, dirty_txns) =
            coordinator.begin_recovery_with_bridging(0, 10, &collector);

        // Assert: Should return both sets correctly
        assert_eq!(
            bridging_txns.len(),
            1,
            "Should have one bridging transaction"
        );
        assert_eq!(dirty_txns.len(), 1, "Should have one dirty transaction");

        assert_eq!(bridging_txns[0].txn_digest, bridging_txn_digest);
        assert_eq!(bridging_txns[0].destination_proxy, 1);

        assert_eq!(dirty_txns[0].txn_digest, dirty_txn_digest);
        assert_eq!(dirty_txns[0].destination_proxy, 0);
    }

    #[test]
    fn test_no_bridging_needed_all_versions_available() {
        // Test case where all required versions are available in snapshot
        // No bridging transactions should be needed

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot has A at v5
        let obj_a_v5 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v5);

        // Failed proxy transaction needs v5 (exact match)
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                100,
                0,
                TransactionDigest::random(),
                vec![(obj_a, SequenceNumber::from_u64(5), None)],
            ),
        );

        collector
            .per_proxy_persist_index
            .insert(0, std::sync::atomic::AtomicU64::new(10));

        // Test: Complete recovery workflow
        let (bridging_txns, dirty_txns) =
            coordinator.begin_recovery_with_bridging(0, 10, &collector);

        // Assert: No bridging needed
        assert_eq!(bridging_txns.len(), 0, "No bridging transactions needed");
        assert_eq!(dirty_txns.len(), 1, "Should have dirty transaction");
    }

    #[test]
    fn test_chain_of_dependencies_in_dirty_txns() {
        // Test that we correctly identify version chains within dirty transactions
        // Dirty txn1: v4→v5
        // Dirty txn2: v5→v6
        // Dirty txn3 needs v6
        // v6 should NOT be in missing (produced by dirty txn chain)

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot has v4
        let obj_a_v4 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v4);

        // Dirty txn1: requires v4, produces v5
        let dirty_txn1 = create_log_record(
            11,
            100,
            0,
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(4), None)],
        );

        // Dirty txn2: requires v5, produces v6
        let dirty_txn2 = create_log_record(
            11,
            101,
            0,
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(5), None)],
        );

        // Dirty txn3: requires v6, produces v7
        let dirty_txn3 = create_log_record(
            11,
            102,
            0,
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(6), None)],
        );

        let dirty_txns = vec![dirty_txn1, dirty_txn2, dirty_txn3];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: No missing versions - the chain produces everything
        assert_eq!(
            missing.len(),
            0,
            "Chain of dirty txns produces all required versions"
        );
    }

    #[test]
    fn test_chain_of_dependencies_with_bridging() {
        // Test mixed chain: some from dirty txns, some from healthy proxy
        // Snapshot: v4
        // Healthy: v4→v5
        // Dirty: v5→v6
        // Dirty: needs v6
        // v5 should be in bridging (from healthy), v6 should not (from dirty chain)

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Snapshot has v4
        let obj_a_v4 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v4);

        // Healthy txn: requires v4, produces v5
        let healthy_txn = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                99,
                1, // healthy proxy
                healthy_txn,
                vec![(obj_a, SequenceNumber::from_u64(4), None)],
            ),
        );

        // Dirty txn1: requires v5, produces v6
        let dirty_txn1 = create_log_record(
            11,
            100,
            0,
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(5), None)],
        );

        // Dirty txn2: requires v6, produces v7
        let dirty_txn2 = create_log_record(
            11,
            101,
            0,
            TransactionDigest::random(),
            vec![(obj_a, SequenceNumber::from_u64(6), None)],
        );

        let dirty_txns = vec![dirty_txn1, dirty_txn2];

        // Test: Identify missing versions
        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Assert: Only v5 is missing (produced by healthy proxy)
        // v6 is NOT missing because dirty chain can produce it from v5
        assert_eq!(missing.len(), 1, "Only v5 needs bridging");
        assert!(missing.contains(&(obj_a, SequenceNumber::from_u64(5))));

        // Test: Collect bridging transactions
        let bridging = coordinator.collect_bridging_transactions(&missing, 10, 0);

        // Assert: Only the healthy txn producing v5
        assert_eq!(bridging.len(), 1);
        assert_eq!(bridging[0].txn_digest, healthy_txn);
    }

    #[test]
    fn test_ignore_failed_proxy_transactions_in_bridging() {
        // Verify that transactions from the failed proxy are NOT included in bridging set
        // Even if they produce needed versions

        let logger = EpochLogger::<FakeTransaction>::new();
        let coordinator = RecoveryCoordinator::new(logger.clone());
        let collector = StateCollector::new(3);

        let obj_a = ObjectID::random();

        // Setup: Collector at v10
        let obj_a_v10 = Object::immutable_with_id_for_testing(obj_a);
        collector.merged_state.insert(obj_a, obj_a_v10);

        // Failed proxy transaction that produces v5 (should NOT be in bridging)
        let failed_txn_digest = TransactionDigest::random();
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                100,
                0, // failed proxy
                failed_txn_digest,
                vec![(obj_a, SequenceNumber::from_u64(4), None)],
            ),
        );

        // Another failed proxy transaction needs v5
        logger.append(
            EpochId(11),
            create_log_record(
                11,
                101,
                0,
                TransactionDigest::random(),
                vec![(obj_a, SequenceNumber::from_u64(5), None)],
            ),
        );

        let dirty_txns = coordinator.drain_dirty_queue(0, 10);

        let missing = coordinator.identify_missing_versions(&dirty_txns, &collector, 10);

        // Since v5 is produced by the first dirty txn, it shouldn't be in missing
        assert_eq!(
            missing.len(),
            0,
            "v5 produced by dirty txn, not a missing version"
        );
    }
}
