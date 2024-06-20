use core::panic;

use tokio::{
    sync::mpsc,
    time::{interval, sleep, Duration, Interval},
};

use super::types::*;

/*****************************************************************************************
 *                                    MockConsensus Worker                               *
 *****************************************************************************************/
const CONSENSUS_DURATION: u64 = 300;
const TIMEOUT: u64 = 10000;
const BLOCK_SIZE: usize = 100;

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

async fn send_and_reset(
    tx_vec: &mut Vec<TransactionWithEffects>,
    out_channel: &mpsc::UnboundedSender<Vec<TransactionWithEffects>>,
    timer: &mut Option<Interval>,
) {
    send_transactions(tx_vec.clone(), out_channel.clone()).await;
    tx_vec.clear();
    *timer = Some(interval(Duration::from_millis(TIMEOUT)));
}

async fn tick_if_some(timer: &mut Option<Interval>) {
    if let Some(ref mut t) = timer {
        t.tick().await;
    }
}

pub async fn mock_consensus_worker_run(
    in_channel: &mut mpsc::UnboundedReceiver<RemoraMessage>,
    out_channel: &mpsc::UnboundedSender<Vec<TransactionWithEffects>>,
    _my_id: u16,
) {
    // let mut timer = tokio::time::interval(Duration::from_millis(TIMEOUT));
    let mut timer: Option<Interval> = None;
    let mut tx_vec: Vec<TransactionWithEffects> = Vec::new();

    loop {
        tokio::select! {
            Some(msg) = in_channel.recv() => {
                // println!("Consensus receive a txn");
                if let RemoraMessage::ProposeExec(full_tx) = msg {
                    tx_vec.push(full_tx);

                    // Initialize the timer when the first transaction is received
                    if timer.is_none() {
                        timer = Some(interval(Duration::from_millis(TIMEOUT)));
                    }

                    // A transaction block is ready
                    if tx_vec.len() >= BLOCK_SIZE {
                        send_and_reset(&mut tx_vec, out_channel, &mut timer).await;
                    }
                } else {
                    eprintln!("PRI consensus received unexpected message from: {:?}", msg);
                    panic!("unexpected message");
                };
            },

            // Timer triggered
            _ = tick_if_some(&mut timer), if timer.is_some() => {
                if !tx_vec.is_empty() {
                    println!("timeout is triggered");
                    send_and_reset(&mut tx_vec, out_channel, &mut timer).await;
                }
            },
        }
    }
}
