use core::panic;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::metrics::Metrics;

use super::types::*;

/*****************************************************************************************
 *                              Input Traffic Manager in Primary                         *
 *****************************************************************************************/

pub async fn input_traffic_manager_run(
    in_channel: &mut mpsc::Receiver<NetworkMessage>,
    out_consensus: &mpsc::UnboundedSender<RemoraMessage>,
    out_executor: &mpsc::UnboundedSender<RemoraMessage>,
    my_id: u16,
    metrics: Arc<Metrics>,
) {
    let mut num_txn = 0;
    loop {
        tokio::select! {
            Some(msg) = in_channel.recv() => {
                let msg = msg.payload;
                if let RemoraMessage::ProposeExec(..) = msg {
                    num_txn += 1;
                    if num_txn == 1 {
                        metrics.register_start_time();
                    }
                    
                    if let Err(e) = out_consensus.send(msg) {
                        eprintln!("Failed to forward to consensus engine: {:?}", e);
                    };
                } else if let RemoraMessage::PreExecResult{..} = msg {
                    // println!("PRI receive a result from PRE");
                    if let Err(e) = out_executor.send(msg) {
                        eprintln!("Failed to forward to executor engine: {:?}", e);
                    };
                } else {
                    eprintln!("PRI {} received unexpected message from: {:?}", my_id, msg);
                    panic!("unexpected message");
                };
            },
        }
    }
}
