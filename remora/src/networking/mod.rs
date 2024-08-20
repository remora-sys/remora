// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod client;
pub mod server;
mod worker;

#[cfg(test)]
mod tests {

    use futures::join;
    use tokio::sync::mpsc;

    use crate::{
        config::ValidatorConfig,
        networking::{client::NetworkClient, server::NetworkServer},
    };

    #[tokio::test]
    async fn client_server() {
        let config = ValidatorConfig::default();
        let transaction = "transaction".to_string();
        let result = "transaction result".to_string();

        // Spawn the server, wait for a proxy connection, and send a transaction to the proxy.
        let cloned_config = config.clone();
        let cloned_transaction = transaction.clone();
        let cloned_result = result.clone();
        let server = async move {
            let (tx_proxy_connections, mut rx_proxy_connections) = mpsc::channel(1);
            let (tx_proxy_results, mut rx_proxy_results) = mpsc::channel(1);

            let server = NetworkServer::new(cloned_config, tx_proxy_connections, tx_proxy_results);
            let _server_handle = server.spawn();

            // Wait for a proxy connection and send a transaction.
            let connection = rx_proxy_connections.recv().await.unwrap();
            connection.send(cloned_transaction).await.unwrap();

            // Wait for the result.
            let r: String = rx_proxy_results.recv().await.unwrap();
            assert_eq!(r, cloned_result);
        };

        // Spawn the client, wait for a transaction from the primary, and send back a result.
        let client = async move {
            let config = ValidatorConfig::default();
            let (tx_transactions, mut rx_transactions) = mpsc::channel(1);
            let (tx_proxy_results, rx_proxy_results) = mpsc::channel(1);

            let client = NetworkClient::new(config, tx_transactions, rx_proxy_results);
            let _client_handle = client.spawn();

            let t: String = rx_transactions.recv().await.unwrap();
            assert_eq!(t, transaction);
            tx_proxy_results.send(result).await.unwrap();
        };

        // Ensure both the client and server completed successfully.
        join!(server, client);
    }
}
