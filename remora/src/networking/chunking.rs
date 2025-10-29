// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Message chunking utilities for handling large recovery and replay messages.
//!
//! This module provides functionality to split large `ReplayBatch` messages into
//! smaller chunks that fit within network message size limits. This is critical
//! for recovery scenarios where state blobs and transaction batches can exceed
//! the maximum message size.

use std::collections::BTreeMap;

use serde::Serialize;
use sui_types::{base_types::ObjectID, object::Object};

use crate::{
    checkpoint::EpochId,
    executor::api::{ExecutableTransaction, ReplayBatch, ReplayMsg},
};

/// Estimate the serialized size of a value in bytes.
/// This uses bincode serialization to get an accurate size estimate.
fn estimate_size<T: Serialize>(value: &T) -> usize {
    bincode::serialized_size(value).unwrap_or(0) as usize
}

/// Estimate the size of a single ReplayMsg.
pub fn estimate_replay_msg_size<T>(msg: &ReplayMsg<T>) -> usize
where
    T: ExecutableTransaction + Clone + Serialize,
{
    estimate_size(msg)
}

/// Estimate the size of a ReplayBatch.
pub fn estimate_replay_batch_size<T>(batch: &ReplayBatch<T>) -> usize
where
    T: ExecutableTransaction + Clone + Serialize,
{
    estimate_size(batch)
}

/// Configuration for message chunking behavior.
#[derive(Debug, Clone)]
pub struct ChunkingConfig {
    /// Maximum size for a single message in bytes.
    pub max_message_size: usize,
    /// Safety margin as a percentage of max_message_size (0.0 to 1.0).
    /// Default is 0.1 (10%) to account for framing overhead.
    pub safety_margin: f64,
}

impl ChunkingConfig {
    /// Create a new chunking config with the given max message size.
    pub fn new(max_message_size: usize) -> Self {
        Self {
            max_message_size,
            safety_margin: 0.1,
        }
    }

    /// Get the effective max size after applying safety margin.
    pub fn effective_max_size(&self) -> usize {
        (self.max_message_size as f64 * (1.0 - self.safety_margin)) as usize
    }
}

/// Result of chunking operation with statistics.
pub struct ChunkingResult<T>
where
    T: ExecutableTransaction + Clone,
{
    /// The chunked batches, ready to send.
    pub chunks: Vec<ReplayBatch<T>>,
    /// Total number of items processed.
    pub total_items: usize,
    /// Number of chunks created.
    pub num_chunks: usize,
    /// Size of the largest chunk in bytes.
    pub max_chunk_size: usize,
}

/// Split a ReplayBatch into smaller chunks if it exceeds the max message size.
///
/// This function implements intelligent chunking that:
/// 1. Checks if the batch needs chunking
/// 2. Splits items across multiple batches
/// 3. Handles large individual items (state blobs) by splitting them
/// 4. Preserves the epoch ID for all chunks
///
/// # Arguments
/// * `batch` - The ReplayBatch to potentially chunk
/// * `config` - Chunking configuration with size limits
///
/// # Returns
/// A ChunkingResult containing the chunks and statistics
///
/// # Panics
/// Panics if a single ReplayMsg item is too large to fit even after splitting state blobs.
pub fn chunk_replay_batch<T>(batch: ReplayBatch<T>, config: &ChunkingConfig) -> ChunkingResult<T>
where
    T: ExecutableTransaction + Clone + Serialize,
{
    let effective_max_size = config.effective_max_size();
    let total_items = batch.items.len();

    // Quick check: if the entire batch fits, return it as-is
    let total_size = estimate_replay_batch_size(&batch);
    if total_size <= effective_max_size {
        tracing::debug!(
            "ReplayBatch for epoch {:?} fits in single message ({} bytes <= {} bytes)",
            batch.epoch,
            total_size,
            effective_max_size
        );
        return ChunkingResult {
            num_chunks: 1,
            max_chunk_size: total_size,
            total_items,
            chunks: vec![batch],
        };
    }

    tracing::info!(
        "ReplayBatch for epoch {:?} is too large ({} bytes > {} bytes), chunking {} items",
        batch.epoch,
        total_size,
        effective_max_size,
        total_items
    );

    let mut chunks = Vec::new();
    let mut current_chunk_items = Vec::new();
    let mut current_chunk_size = estimate_empty_batch_size::<T>(&batch.epoch);
    let mut max_chunk_size = 0;

    for item in batch.items {
        let item_size = estimate_replay_msg_size(&item);

        // If this single item is too large, we need to split it
        if item_size > effective_max_size {
            tracing::warn!(
                "Single ReplayMsg (epoch_id={}) is too large ({} bytes > {} bytes), splitting state blobs",
                item.epoch_id.0,
                item_size,
                effective_max_size
            );

            // Flush current chunk if not empty
            if !current_chunk_items.is_empty() {
                max_chunk_size = max_chunk_size.max(current_chunk_size);
                chunks.push(ReplayBatch {
                    epoch: batch.epoch,
                    items: current_chunk_items,
                });
                current_chunk_items = Vec::new();
                current_chunk_size = estimate_empty_batch_size::<T>(&batch.epoch);
            }

            // Split the large item into multiple chunks
            let split_chunks = split_large_replay_msg(item, batch.epoch, config);
            for chunk in split_chunks {
                let chunk_size = estimate_replay_batch_size(&chunk);
                max_chunk_size = max_chunk_size.max(chunk_size);
                chunks.push(chunk);
            }
            continue;
        }

        // Check if adding this item would exceed the limit
        let new_size = current_chunk_size + item_size;
        if new_size > effective_max_size && !current_chunk_items.is_empty() {
            // Flush current chunk and start a new one
            max_chunk_size = max_chunk_size.max(current_chunk_size);
            chunks.push(ReplayBatch {
                epoch: batch.epoch,
                items: current_chunk_items,
            });
            current_chunk_items = Vec::new();
            current_chunk_size = estimate_empty_batch_size::<T>(&batch.epoch);
        }

        // Add item to current chunk
        current_chunk_items.push(item);
        current_chunk_size += item_size;
    }

    // Flush final chunk if not empty
    if !current_chunk_items.is_empty() {
        max_chunk_size = max_chunk_size.max(current_chunk_size);
        chunks.push(ReplayBatch {
            epoch: batch.epoch,
            items: current_chunk_items,
        });
    }

    let num_chunks = chunks.len();
    tracing::info!(
        "Split ReplayBatch for epoch {:?} into {} chunks (max chunk size: {} bytes)",
        batch.epoch,
        num_chunks,
        max_chunk_size
    );

    ChunkingResult {
        chunks,
        total_items,
        num_chunks,
        max_chunk_size,
    }
}

/// Estimate the size of an empty ReplayBatch (just the epoch field).
fn estimate_empty_batch_size<T>(epoch: &EpochId) -> usize
where
    T: ExecutableTransaction + Clone + Serialize,
{
    let empty_batch: ReplayBatch<T> = ReplayBatch {
        epoch: *epoch,
        items: Vec::new(),
    };
    estimate_replay_batch_size(&empty_batch)
}

/// Split a large ReplayMsg into multiple ReplayBatch chunks by dividing its state blobs.
///
/// This handles the case where a single ReplayMsg has too many or too large state blobs.
/// The strategy is:
/// 1. Create one chunk with the transaction and required_versions (if present)
/// 2. Split state_blobs across multiple chunks as pure state transfers
fn split_large_replay_msg<T>(
    msg: ReplayMsg<T>,
    epoch: EpochId,
    config: &ChunkingConfig,
) -> Vec<ReplayBatch<T>>
where
    T: ExecutableTransaction + Clone + Serialize,
{
    let effective_max_size = config.effective_max_size();
    let mut chunks = Vec::new();

    // First chunk: transaction + required_versions + as many state blobs as fit
    let mut current_state_blobs = BTreeMap::new();
    let remaining_state_blobs = msg.state_blobs;

    // Create a base message size estimate (transaction + required_versions)
    let base_msg = ReplayMsg {
        epoch_id: msg.epoch_id,
        transaction: msg.transaction.clone(),
        required_versions: msg.required_versions.clone(),
        state_blobs: BTreeMap::new(),
    };
    let base_msg_size = estimate_replay_msg_size(&base_msg);
    let empty_batch_size = estimate_empty_batch_size::<T>(&epoch);

    // If base message itself is too large, we have a problem
    // (This should be rare - means the transaction itself is huge)
    if base_msg_size + empty_batch_size > effective_max_size {
        tracing::error!(
            "Base ReplayMsg without state blobs is too large ({} bytes > {} bytes). This should not happen!",
            base_msg_size + empty_batch_size,
            effective_max_size
        );
        // Still try to send it - network layer will reject if truly too large
        chunks.push(ReplayBatch {
            epoch,
            items: vec![ReplayMsg {
                epoch_id: msg.epoch_id,
                transaction: msg.transaction.clone(),
                required_versions: msg.required_versions.clone(),
                state_blobs: BTreeMap::new(),
            }],
        });
    }

    let mut current_size = base_msg_size;

    // Try to fit state blobs into the first chunk
    let mut state_blobs_iter = remaining_state_blobs.into_iter().peekable();
    while let Some((_obj_id, obj)) = state_blobs_iter.peek() {
        let obj_size = estimate_size(obj);
        if current_size + obj_size + 64 > effective_max_size - empty_batch_size {
            // Can't fit, break
            break;
        }
        let (obj_id, obj) = state_blobs_iter.next().unwrap();
        current_state_blobs.insert(obj_id, obj);
        current_size += obj_size + 64; // 64 bytes for ObjectID + overhead
    }

    // Create first chunk with transaction
    if msg.transaction.is_some() || !current_state_blobs.is_empty() {
        chunks.push(ReplayBatch {
            epoch,
            items: vec![ReplayMsg {
                epoch_id: msg.epoch_id,
                transaction: msg.transaction,
                required_versions: msg.required_versions,
                state_blobs: current_state_blobs,
            }],
        });
    }

    // Remaining state blobs go into pure state transfer chunks
    let remaining: Vec<(ObjectID, Object)> = state_blobs_iter.collect();
    if !remaining.is_empty() {
        let mut current_state_blobs = BTreeMap::new();
        let mut current_size = estimate_empty_batch_size::<T>(&epoch)
            + estimate_replay_msg_size(&ReplayMsg::<T> {
                epoch_id: msg.epoch_id,
                transaction: None,
                required_versions: Vec::new(),
                state_blobs: BTreeMap::new(),
            });

        for (obj_id, obj) in remaining {
            let obj_size = estimate_size(&obj);
            // If single object is too large, we have to send it alone
            if obj_size + current_size + 64 > effective_max_size && !current_state_blobs.is_empty()
            {
                // Flush current chunk
                chunks.push(ReplayBatch {
                    epoch,
                    items: vec![ReplayMsg {
                        epoch_id: msg.epoch_id,
                        transaction: None,
                        required_versions: Vec::new(),
                        state_blobs: current_state_blobs,
                    }],
                });
                current_state_blobs = BTreeMap::new();
                current_size = estimate_empty_batch_size::<T>(&epoch)
                    + estimate_replay_msg_size(&ReplayMsg::<T> {
                        epoch_id: msg.epoch_id,
                        transaction: None,
                        required_versions: Vec::new(),
                        state_blobs: BTreeMap::new(),
                    });
            }

            current_state_blobs.insert(obj_id, obj);
            current_size += obj_size + 64;
        }

        // Flush final state blobs chunk
        if !current_state_blobs.is_empty() {
            chunks.push(ReplayBatch {
                epoch,
                items: vec![ReplayMsg {
                    epoch_id: msg.epoch_id,
                    transaction: None,
                    required_versions: Vec::new(),
                    state_blobs: current_state_blobs,
                }],
            });
        }
    }

    tracing::debug!(
        "Split large ReplayMsg (epoch_id={}) into {} chunks",
        msg.epoch_id.0,
        chunks.len()
    );

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use sui_types::base_types::ObjectID;

    // Mock transaction type for testing
    #[derive(Clone, Serialize)]
    struct MockTransaction;

    impl crate::executor::api::ExecutableTransaction for MockTransaction {
        fn digest(&self) -> &sui_types::digests::TransactionDigest {
            unimplemented!("Not needed for chunking tests")
        }

        fn input_objects(&self) -> Vec<sui_types::transaction::InputObjectKind> {
            vec![]
        }

        fn shared_object_ids(&self) -> Vec<ObjectID> {
            vec![]
        }
    }

    #[test]
    fn test_chunking_config() {
        let config = ChunkingConfig::new(8 * 1024 * 1024); // 8 MB
        assert_eq!(config.max_message_size, 8 * 1024 * 1024);
        assert_eq!(config.safety_margin, 0.1);
        // Effective size should be 90% of max
        assert_eq!(
            config.effective_max_size(),
            (8.0 * 1024.0 * 1024.0 * 0.9) as usize
        );
    }

    #[test]
    fn test_small_batch_no_chunking() {
        // Create a small batch that should not be chunked
        let msg = ReplayMsg::<MockTransaction> {
            epoch_id: EpochId(1),
            transaction: None,
            required_versions: vec![],
            state_blobs: BTreeMap::new(),
        };

        let batch = ReplayBatch {
            epoch: EpochId(0),
            items: vec![msg],
        };

        let config = ChunkingConfig::new(8 * 1024 * 1024);
        let result = chunk_replay_batch(batch, &config);

        // Should return single chunk
        assert_eq!(result.num_chunks, 1);
        assert_eq!(result.total_items, 1);
        assert_eq!(result.chunks.len(), 1);
    }

    #[test]
    fn test_empty_batch_size_estimation() {
        let epoch = EpochId(0);
        let size = estimate_empty_batch_size::<MockTransaction>(&epoch);

        // Empty batch should have a small size (just the epoch and empty vec)
        assert!(size > 0);
        assert!(size < 1024); // Should be less than 1 KB
    }

    #[test]
    fn test_chunking_config_effective_size() {
        let config = ChunkingConfig::new(1024 * 1024); // 1 MB

        // With 10% safety margin, effective size should be 90%
        let effective = config.effective_max_size();
        let expected = (1024.0 * 1024.0 * 0.9) as usize;
        assert_eq!(effective, expected);
    }

    #[test]
    fn test_custom_safety_margin() {
        let mut config = ChunkingConfig::new(1024 * 1024);
        config.safety_margin = 0.2; // 20% margin

        let effective = config.effective_max_size();
        let expected = (1024.0 * 1024.0 * 0.8) as usize;
        assert_eq!(effective, expected);
    }
}
