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
        let mut buffer = vec![0u8; Self::MAX_MESSAGE_SIZE as usize].into_boxed_slice();

        loop {
            // Deserialize the message.
            let size = reader.read_u32().await?;
            let message = &mut buffer[..size as usize];
            let bytes_read = reader.read_exact(message).await?;
            debug_assert_eq!(bytes_read, message.len());

            // Send the message to the application layer.
            match bincode::deserialize(message) {
                Ok(data) => {
                    if tx_incoming.send(data).await.is_err() {
                        tracing::warn!("Cannot send message to application layer, stopping worker");
                        break Ok(());
                    }
                }
                Err(e) => {
                    tracing::error!("Cannot deserialize message (killing connection): {e:?}");
                    break Ok(());
                }
            }
        }
    }

    /// Handle writing to the stream.
    async fn handle_write_stream(
        mut writer: OwnedWriteHalf,
        mut rx_outgoing: Receiver<O>,
    ) -> io::Result<()> {
        while let Some(transaction) = rx_outgoing.recv().await {
            // Serialize and send the message.
            let serialized = bincode::serialize(&transaction).expect("Infallible serialization");
            writer.write_u32(serialized.len() as u32).await?;
            writer.write_all(&serialized).await?;
        }
        tracing::warn!("Cannot receive transaction from application layer, stopping worker");
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