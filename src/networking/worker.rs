// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io;

use futures::FutureExt;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

/// A worker that handles a bidirectional connection with a peer.
pub struct ConnectionWorker<I, O> {
    /// The TCP stream.
    stream: TcpStream,
    /// The sender for messages received from the network.
    tx_incoming: Sender<I>,
    /// The receiver for messages to send to the network.
    rx_outgoing: Receiver<O>,
}

impl<I, O> ConnectionWorker<I, O>
where
    I: Send + DeserializeOwned + 'static,
    O: Send + Serialize,
{
    /// The maximum size of a network message.
    const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
    /// The maximum batch size for batched processing.
    const MAX_BATCH_SIZE: usize = 16;

    /// Create a new worker.
    pub fn new(stream: TcpStream, tx_incoming: Sender<I>, rx_outgoing: Receiver<O>) -> Self {
        Self {
            stream,
            tx_incoming,
            rx_outgoing,
        }
    }

    /// Run the worker.
    pub async fn run(self) {
        let (reader, writer) = self.stream.into_split();
        let read_stream_handle = Self::handle_read_stream(reader, self.tx_incoming).boxed();
        let write_stream_handle = Self::handle_write_stream(writer, self.rx_outgoing).boxed();

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
    ) -> io::Result<()> {
        use byteorder::{BigEndian, ByteOrder};
        use bytes::{Buf, BytesMut};
        use futures::StreamExt;
        // buffer holds leftover bytes between reads
        let mut buf = BytesMut::with_capacity(Self::MAX_MESSAGE_SIZE * 2);

        loop {
            // 1) fill buffer in one syscall
            let n = reader.read_buf(&mut buf).await?;
            if n == 0 {
                tracing::warn!("Connection closed by peer (EOF)");
                break;
            }

            // 2) extract all complete frames into a Vec of Vec<u8>
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
                frames.push(buf[start..end].to_vec());
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
                    async move {
                        match bincode::deserialize::<I>(&frame) {
                            Ok(item) => {
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
            for transaction in &buffer {
                let serialized = bincode::serialize(transaction).expect("Infallible serialization");

                let size = serialized.len() as u32;
                serialized_buffer.extend_from_slice(&size.to_be_bytes());

                serialized_buffer.extend_from_slice(&serialized);
            }

            writer.write_all(&serialized_buffer).await?;

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
