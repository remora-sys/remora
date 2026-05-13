// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::VecDeque,
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::FutureExt;
use serde::{de::DeserializeOwned, Serialize};
use sui_types::digests::TransactionDigest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    networking::stats::ConnectionStats,
    primary::batch_breakdown::{BatchBreakdownCollector, MeasuredMessage},
};

#[derive(Clone, Copy)]
struct IoTiming {
    start: Instant,
    elapsed: Duration,
}

struct ReadTimingChunk {
    start: Instant,
    elapsed: Duration,
    total_bytes: usize,
    consumed_bytes: usize,
}

impl ReadTimingChunk {
    fn new(start: Instant, elapsed: Duration, total_bytes: usize) -> Self {
        Self {
            start,
            elapsed,
            total_bytes,
            consumed_bytes: 0,
        }
    }

    fn remaining_bytes(&self) -> usize {
        self.total_bytes.saturating_sub(self.consumed_bytes)
    }
}

struct ReadFrame {
    payload: Vec<u8>,
    io_timings: Vec<IoTiming>,
}

struct WriteTimingTarget {
    digest: TransactionDigest,
    offset_bytes: usize,
    wire_bytes: usize,
}

fn timing_segment(
    start: Instant,
    elapsed: Duration,
    offset_bytes: usize,
    segment_bytes: usize,
    total_bytes: usize,
) -> IoTiming {
    let offset = scale_duration(elapsed, offset_bytes, total_bytes);
    IoTiming {
        start: start.checked_add(offset).unwrap_or(start),
        elapsed: scale_duration(elapsed, segment_bytes, total_bytes),
    }
}

fn drain_read_timings(
    read_timings: &mut VecDeque<ReadTimingChunk>,
    mut wire_bytes: usize,
) -> Vec<IoTiming> {
    let mut timings = Vec::new();

    while wire_bytes > 0 {
        let Some(chunk) = read_timings.front_mut() else {
            break;
        };

        let take = wire_bytes.min(chunk.remaining_bytes());
        timings.push(timing_segment(
            chunk.start,
            chunk.elapsed,
            chunk.consumed_bytes,
            take,
            chunk.total_bytes,
        ));

        chunk.consumed_bytes += take;
        wire_bytes -= take;

        if chunk.remaining_bytes() == 0 {
            read_timings.pop_front();
        }
    }

    timings
}

fn combine_io_timings(timings: &[IoTiming]) -> Option<IoTiming> {
    let start = timings.first()?.start;
    let total_nanos = timings
        .iter()
        .map(|timing| timing.elapsed.as_nanos())
        .fold(0u128, |total, nanos| total.saturating_add(nanos));

    Some(IoTiming {
        start,
        elapsed: Duration::from_nanos(total_nanos.min(u64::MAX as u128) as u64),
    })
}

fn scale_duration(duration: Duration, numerator: usize, denominator: usize) -> Duration {
    if numerator == 0 || denominator == 0 {
        return Duration::ZERO;
    }

    let nanos = duration.as_nanos().saturating_mul(numerator as u128) / denominator as u128;
    Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
}

/// A worker that handles a bidirectional connection with a peer.
pub struct ConnectionWorker<I, O> {
    /// The TCP stream.
    stream: TcpStream,
    /// The sender for messages received from the network.
    tx_incoming: Sender<I>,
    /// The receiver for messages to send to the network.
    rx_outgoing: Receiver<O>,
    /// Batch-level latency breakdown collector used by primary-side measurements.
    batch_breakdown: Option<Arc<BatchBreakdownCollector>>,
    /// Optional observer for actual socket traffic.
    connection_stats: Option<Arc<dyn ConnectionStats>>,
}

impl<I, O> ConnectionWorker<I, O>
where
    I: Send + DeserializeOwned + MeasuredMessage + 'static,
    O: Send + Serialize + MeasuredMessage,
{
    /// The maximum size of a network message.
    const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
    /// The maximum batch size for batched processing.
    const MAX_BATCH_SIZE: usize = 16;

    /// Create a new worker.
    pub fn new(
        stream: TcpStream,
        tx_incoming: Sender<I>,
        rx_outgoing: Receiver<O>,
        batch_breakdown: Option<Arc<BatchBreakdownCollector>>,
        connection_stats: Option<Arc<dyn ConnectionStats>>,
    ) -> Self {
        Self {
            stream,
            tx_incoming,
            rx_outgoing,
            batch_breakdown,
            connection_stats,
        }
    }

    /// Run the worker.
    pub async fn run(self) {
        let (reader, writer) = self.stream.into_split();
        let read_stream_handle = Self::handle_read_stream(
            reader,
            self.tx_incoming,
            self.batch_breakdown.clone(),
            self.connection_stats.clone(),
        )
        .boxed();
        let write_stream_handle = Self::handle_write_stream(
            writer,
            self.rx_outgoing,
            self.batch_breakdown,
            self.connection_stats,
        )
        .boxed();

        // Use join! instead of select! to keep the read stream going even if write stream stops
        let (read_result, write_result) = tokio::join!(read_stream_handle, write_stream_handle,);

        if let Err(e) = read_result {
            tracing::error!("Error in read stream: {:?}", e);
        }

        if let Err(e) = write_result {
            tracing::error!("Error in write stream: {:?}", e);
        }
    }

    /// Handle reading from the stream.
    async fn handle_read_stream(
        mut reader: OwnedReadHalf,
        tx_incoming: Sender<I>,
        batch_breakdown: Option<Arc<BatchBreakdownCollector>>,
        connection_stats: Option<Arc<dyn ConnectionStats>>,
    ) -> io::Result<()> {
        use byteorder::{BigEndian, ByteOrder};
        use bytes::{Buf, BytesMut};
        use futures::StreamExt;
        // buffer holds leftover bytes between reads
        let mut buf = BytesMut::with_capacity(Self::MAX_MESSAGE_SIZE * 2);
        let mut read_timings = VecDeque::new();

        loop {
            // 1) fill buffer in one syscall
            let read_start = Instant::now();
            let n = reader.read_buf(&mut buf).await?;
            let read_elapsed = read_start.elapsed();
            if n == 0 {
                tracing::warn!("Connection closed by peer (EOF)");
                break;
            }
            read_timings.push_back(ReadTimingChunk::new(read_start, read_elapsed, n));
            if let Some(connection_stats) = connection_stats.as_ref() {
                connection_stats.record_rx_bytes(n);
            }

            // 2) extract all complete frames and the read timing for their bytes
            let mut offset = 0;
            let mut frames = Vec::new();
            while buf.len() >= offset + 4 {
                let size = BigEndian::read_u32(&buf[offset..offset + 4]) as usize;
                if size > Self::MAX_MESSAGE_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Frame too large: {} > {}", size, Self::MAX_MESSAGE_SIZE),
                    ));
                }
                if buf.len() < offset + 4 + size {
                    break; // wait for more data
                }
                let start = offset + 4;
                let end = start + size;
                if let Some(connection_stats) = connection_stats.as_ref() {
                    connection_stats.record_rx_message(size);
                }
                frames.push(ReadFrame {
                    payload: buf[start..end].to_vec(),
                    io_timings: drain_read_timings(&mut read_timings, size + 4),
                });
                offset = end;
            }

            // drop consumed bytes
            if offset > 0 {
                buf.advance(offset);
            }

            // 3) deserialize & forward up to MAX_BATCH_SIZE at once
            futures::stream::iter(frames)
                .for_each_concurrent(Some(Self::MAX_BATCH_SIZE), |frame| {
                    let tx = tx_incoming.clone();
                    let batch_breakdown = batch_breakdown.clone();
                    async move {
                        let deserialize_start = Instant::now();
                        let item = bincode::deserialize::<I>(&frame.payload);
                        let deserialize_elapsed = deserialize_start.elapsed();
                        match item {
                            Ok(item) => {
                                if let (Some(batch_breakdown), Some(digest)) =
                                    (batch_breakdown.as_ref(), item.measurement_digest())
                                {
                                    if let Some(timing) = combine_io_timings(&frame.io_timings) {
                                        batch_breakdown.record_network_rx_read(
                                            digest,
                                            timing.start,
                                            timing.elapsed,
                                        );
                                    }
                                    batch_breakdown.record_network_rx_deser(
                                        digest,
                                        deserialize_start,
                                        deserialize_elapsed,
                                    );
                                }
                                if tx.send(item).await.is_err() {
                                    tracing::warn!("Incoming channel closed, stopping reader");
                                }
                            }
                            Err(e) => tracing::error!("Deserialize error: {:?}", e),
                        }
                    }
                })
                .await;
        }

        Ok(())
    }

    /// Handle writing from the stream.
    async fn handle_write_stream(
        mut writer: OwnedWriteHalf,
        mut rx_outgoing: Receiver<O>,
        batch_breakdown: Option<Arc<BatchBreakdownCollector>>,
        connection_stats: Option<Arc<dyn ConnectionStats>>,
    ) -> io::Result<()> {
        let mut buffer: Vec<O> = Vec::with_capacity(Self::MAX_BATCH_SIZE);
        let mut serialized_buffer: Vec<u8> = Vec::new();

        loop {
            let num_received = rx_outgoing
                .recv_many(&mut buffer, Self::MAX_BATCH_SIZE)
                .await;

            if num_received == 0 {
                tracing::warn!(
                    "Cannot receive transaction from application layer, stopping worker"
                );
                break;
            }

            // Batching writes
            let mut write_targets = Vec::new();
            for transaction in &buffer {
                let digest = transaction.measurement_digest();
                let start = Instant::now();
                let serialized = bincode::serialize(transaction).expect("Infallible serialization");
                if let (Some(batch_breakdown), Some(digest)) = (batch_breakdown.as_ref(), digest) {
                    batch_breakdown.record_network_tx_serialize(digest, start, start.elapsed());
                }
                if let Some(connection_stats) = connection_stats.as_ref() {
                    connection_stats.record_tx_message(serialized.len());
                }

                let size = serialized.len() as u32;
                let offset_bytes = serialized_buffer.len();
                serialized_buffer.extend_from_slice(&size.to_be_bytes());
                serialized_buffer.extend_from_slice(&serialized);
                if let Some(digest) = digest {
                    write_targets.push(WriteTimingTarget {
                        digest,
                        offset_bytes,
                        wire_bytes: serialized.len() + 4,
                    });
                }
            }

            let write_start = Instant::now();
            writer.write_all(&serialized_buffer).await?;
            let write_elapsed = write_start.elapsed();
            if let Some(batch_breakdown) = batch_breakdown.as_ref() {
                let total_bytes = serialized_buffer.len();
                for target in write_targets {
                    let timing = timing_segment(
                        write_start,
                        write_elapsed,
                        target.offset_bytes,
                        target.wire_bytes,
                        total_bytes,
                    );
                    batch_breakdown.record_network_tx_write(
                        target.digest,
                        timing.start,
                        timing.elapsed,
                    );
                }
            }
            if let Some(connection_stats) = connection_stats.as_ref() {
                connection_stats.record_tx_bytes(serialized_buffer.len());
            }

            serialized_buffer.clear();
            buffer.clear();
        }

        Ok(())
    }

    /// Spawn the worker in a new task.
    pub fn spawn(self) -> JoinHandle<()>
    where
        I: 'static,
        O: 'static,
    {
        tokio::spawn(async move {
            self.run().await;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timing_segment_splits_elapsed_by_wire_bytes() {
        let base = Instant::now();
        let timing = timing_segment(base, Duration::from_millis(10), 40, 20, 100);

        assert_eq!(timing.start.duration_since(base), Duration::from_millis(4));
        assert_eq!(timing.elapsed, Duration::from_millis(2));
    }

    #[test]
    fn drain_read_timings_spans_partial_reads() {
        let base = Instant::now();
        let mut chunks = VecDeque::new();
        chunks.push_back(ReadTimingChunk::new(base, Duration::from_millis(10), 10));
        chunks.push_back(ReadTimingChunk::new(
            base + Duration::from_millis(10),
            Duration::from_millis(20),
            20,
        ));

        let first_frame = drain_read_timings(&mut chunks, 15);
        assert_eq!(first_frame.len(), 2);
        assert_eq!(first_frame[0].elapsed, Duration::from_millis(10));
        assert_eq!(
            first_frame[1].start.duration_since(base),
            Duration::from_millis(10)
        );
        assert_eq!(first_frame[1].elapsed, Duration::from_millis(5));

        let second_frame = drain_read_timings(&mut chunks, 15);
        assert_eq!(second_frame.len(), 1);
        assert_eq!(
            second_frame[0].start.duration_since(base),
            Duration::from_millis(15)
        );
        assert_eq!(second_frame[0].elapsed, Duration::from_millis(15));
        assert!(chunks.is_empty());
    }
}
