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
        tokio::select! {
            _ = read_stream_handle => (),
            _ = write_stream_handle => (),
        }
    }

    /// Handle reading from the stream.
    async fn handle_read_stream(
        mut reader: OwnedReadHalf,
        tx_incoming: Sender<I>,
    ) -> io::Result<()> {
        use futures::{stream, StreamExt};

        let buffer_size = Self::MAX_MESSAGE_SIZE;

        // Create an stream using unfold.
        let message_stream = stream::unfold(&mut reader, move |reader| async move {
            match reader.read_u32().await {
                Ok(size) => {
                    let size = size as usize;

                    // Make sure that the size is not larger than the buffer size to prevent overflows.
                    if size > buffer_size {
                        tracing::error!(
                            "Message size exceeds maximum allowed: {} > {}",
                            size,
                            buffer_size
                        );
                        return None;
                    }

                    let mut message = vec![0u8; size];
                    match reader.read_exact(&mut message).await {
                        Ok(_) => Some((message, reader)),
                        Err(e) => {
                            tracing::error!("Error reading message content: {:?}", e);
                            None
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    tracing::warn!("Connection closed by peer (EOF)");
                    None
                }
                Err(e) => {
                    tracing::error!("Error reading message size: {:?}", e);
                    None
                }
            }
        });

        message_stream
            .for_each_concurrent(Some(Self::MAX_BATCH_SIZE), |message| {
                let tx_incoming = tx_incoming.clone();
                async move {
                    match bincode::deserialize::<I>(&message) {
                        Ok(data) => {
                            if tx_incoming.send(data).await.is_err() {
                                tracing::warn!(
                                    "Cannot send message to application layer, stopping worker"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Cannot deserialize message (killing connection): {:?}",
                                e
                            );
                        }
                    }
                }
            })
            .await;

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
