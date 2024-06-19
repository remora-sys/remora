use core::panic;

use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};

use super::types::*;

/*****************************************************************************************
 *                                    MockConsensus Worker                               *
 *****************************************************************************************/
const CONSENSUS_DURATION: u64 = 300;
const TIMEOUT: u64 = 1000;
const BLOCK_SIZE: usize = 1000;

async fn send_transactions(
    tx_vec: Vec<TransactionWithEffects>,
    out_channel: mpsc::UnboundedSender<Vec<TransactionWithEffects>>,
) {
    println!("Consensus engine sending {} transactions", tx_vec.len());
    tokio::spawn(async move {
        sleep(Duration::from_millis(CONSENSUS_DURATION)).await;
        if let Err(e) = out_channel.send(tx_vec) {
            eprintln!("Failed to send transactions: {:?}", e);
        }
    });
}

pub async fn mock_consensus_worker_run(
    in_channel: &mut mpsc::UnboundedReceiver<RemoraMessage>,
    out_channel: &mpsc::UnboundedSender<Vec<TransactionWithEffects>>,
    _my_id: u16,
) {
    let mut timer = tokio::time::interval(Duration::from_millis(TIMEOUT));
    let mut tx_vec: Vec<TransactionWithEffects> = Vec::new();

    loop {
        tokio::select! {
            Some(msg) = in_channel.recv() => {
                println!("Consensus receive a txn");
                if let RemoraMessage::ProposeExec(full_tx) = msg {
                    tx_vec.push(full_tx);

                    // A transaction block is ready
                    if tx_vec.len() >= BLOCK_SIZE {
                        send_transactions(tx_vec.clone(), out_channel.clone()).await;
                        tx_vec.clear();
                    }
                } else {
                    eprintln!("PRI consensus received unexpected message from: {:?}", msg);
                    panic!("unexpected message");
                };
            },

            // Timer triggered
            _ = timer.tick() => {
                if !tx_vec.is_empty() {
                    send_transactions(tx_vec.clone(), out_channel.clone()).await;
                    tx_vec.clear();
                }
            }
        }
    }
}
