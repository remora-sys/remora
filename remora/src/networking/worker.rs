// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::io;

use bytes::{Buf, BytesMut};
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
        tokio::select! {
            _ = read_stream_handle => (),
            _ = write_stream_handle => (),
        }
    }

    /// Handle reading from the stream.
    async fn handle_read_stream(
        mut reader: OwnedReadHalf,
        tx_incoming: tokio::sync::mpsc::Sender<I>,
    ) -> io::Result<()> {
        const MSG_LEN_BYTES: usize = std::mem::size_of::<u32>();
        let buffer_size: usize = MSG_LEN_BYTES + Self::MAX_MESSAGE_SIZE * 10;
        let mut buffer = BytesMut::with_capacity(buffer_size);

        loop {
            // Wait until we have more data in the buffer.
            if reader.read_buf(&mut buffer).await? == 0 {
                // EOF reached, breaking the loop.
                tracing::info!("EOF reached, stopping read stream");
                break;
            }

            // Process all available messages in the buffer.
            while buffer.len() >= MSG_LEN_BYTES {
                // big-endian is aligned to handle_write_stream which uses network byte order
                let size = u32::from_be_bytes(buffer[..MSG_LEN_BYTES].try_into().unwrap()) as usize;

                if buffer.len() < MSG_LEN_BYTES + size {
                    break;
                }

                // Extract the message and process it
                buffer.advance(MSG_LEN_BYTES);
                let data = buffer.split_to(size);

                let tx_incoming = tx_incoming.clone();
                let data = data.to_vec();

                // Offload deserialization and message sending to a separate task.
                tokio::spawn(async move {
                    match bincode::deserialize(&data) {
                        Ok(data) => {
                            if tx_incoming.send(data).await.is_err() {
                                tracing::warn!(
                                    "Cannot send message to application layer, stopping worker"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Cannot deserialize message (killing connection): {e:?}"
                            );
                        }
                    }
                });
            }

            // Shrink the buffer if it has grown too large, to free memory
            if buffer.capacity() > buffer_size {
                buffer = BytesMut::with_capacity(buffer_size);
            }
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
