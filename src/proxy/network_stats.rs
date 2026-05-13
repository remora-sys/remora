// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io::{self, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::time::{interval, MissedTickBehavior};

use crate::{networking::stats::ConnectionStats, proxy::core::ProxyId};

const DEFAULT_REPORT_INTERVAL_SECS: u64 = 5;
const REPORT_INTERVAL_ENV: &str = "REMORA_PROXY_NETWORK_REPORT_INTERVAL_SECS";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProxyConnectionClass {
    Primary,
    InterProxy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransferDirection {
    Send,
    Receive,
}

#[derive(Default)]
struct TrafficCounters {
    rx_bytes: AtomicU64,
    tx_bytes: AtomicU64,
}

impl TrafficCounters {
    fn record(&self, direction: TransferDirection, bytes: u64) {
        match direction {
            TransferDirection::Send => {
                self.tx_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            TransferDirection::Receive => {
                self.rx_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> TrafficSnapshot {
        TrafficSnapshot {
            rx_bytes: self.rx_bytes.load(Ordering::Relaxed),
            tx_bytes: self.tx_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct TrafficSnapshot {
    rx_bytes: u64,
    tx_bytes: u64,
}

#[derive(Default)]
struct LeaseTransferCounters {
    transfer_count: AtomicU64,
    object_count: AtomicU64,
    payload_bytes: AtomicU64,
    max_payload_bytes: AtomicU64,
}

impl LeaseTransferCounters {
    fn record(&self, object_count: u64, payload_bytes: u64) {
        self.transfer_count.fetch_add(1, Ordering::Relaxed);
        self.object_count.fetch_add(object_count, Ordering::Relaxed);
        self.payload_bytes
            .fetch_add(payload_bytes, Ordering::Relaxed);
        self.max_payload_bytes
            .fetch_max(payload_bytes, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LeaseTransferSnapshot {
        LeaseTransferSnapshot {
            transfer_count: self.transfer_count.load(Ordering::Relaxed),
            object_count: self.object_count.load(Ordering::Relaxed),
            payload_bytes: self.payload_bytes.load(Ordering::Relaxed),
            max_payload_bytes: self.max_payload_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LeaseTransferSnapshot {
    transfer_count: u64,
    object_count: u64,
    payload_bytes: u64,
    max_payload_bytes: u64,
}

impl LeaseTransferSnapshot {
    fn avg_payload_bytes(self) -> f64 {
        if self.transfer_count == 0 {
            0.0
        } else {
            self.payload_bytes as f64 / self.transfer_count as f64
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ProxyNetworkSnapshot {
    elapsed: Duration,
    total: TrafficSnapshot,
    primary: TrafficSnapshot,
    inter_proxy: TrafficSnapshot,
    lease_send: LeaseTransferSnapshot,
    lease_receive: LeaseTransferSnapshot,
}

pub(crate) struct ProxyNetworkStats {
    proxy_id: ProxyId,
    started_at: Instant,
    total: TrafficCounters,
    primary: TrafficCounters,
    inter_proxy: TrafficCounters,
    lease_send: LeaseTransferCounters,
    lease_receive: LeaseTransferCounters,
}

impl ProxyNetworkStats {
    pub(crate) fn new(proxy_id: ProxyId) -> Self {
        Self {
            proxy_id,
            started_at: Instant::now(),
            total: TrafficCounters::default(),
            primary: TrafficCounters::default(),
            inter_proxy: TrafficCounters::default(),
            lease_send: LeaseTransferCounters::default(),
            lease_receive: LeaseTransferCounters::default(),
        }
    }

    pub(crate) fn connection_handle(
        self: &Arc<Self>,
        connection_class: ProxyConnectionClass,
    ) -> Arc<dyn ConnectionStats> {
        Arc::new(ProxyConnectionStatsHandle {
            stats: Arc::clone(self),
            connection_class,
        })
    }

    pub(crate) fn record_connection_bytes(
        &self,
        connection_class: ProxyConnectionClass,
        direction: TransferDirection,
        bytes: usize,
    ) {
        let bytes = bytes as u64;
        self.total.record(direction, bytes);
        match connection_class {
            ProxyConnectionClass::Primary => self.primary.record(direction, bytes),
            ProxyConnectionClass::InterProxy => self.inter_proxy.record(direction, bytes),
        }
    }

    pub(crate) fn record_lease_transfer(
        &self,
        direction: TransferDirection,
        object_count: usize,
        payload_bytes: u64,
    ) {
        match direction {
            TransferDirection::Send => self.lease_send.record(object_count as u64, payload_bytes),
            TransferDirection::Receive => self
                .lease_receive
                .record(object_count as u64, payload_bytes),
        }

        let mut stdout = io::stdout().lock();
        writeln!(
            stdout,
            "[proxy-lease-size] proxy_id={} direction={} object_count={} object_payload_bytes={} avg_object_payload_bytes={:.1}",
            self.proxy_id,
            direction.as_str(),
            object_count,
            payload_bytes,
            average_bytes_per_object(payload_bytes, object_count as u64),
        )
        .expect("proxy lease size write to stdout should succeed");
    }

    pub(crate) async fn report_periodically(self: Arc<Self>) {
        let report_interval = reporting_interval();
        let mut ticker = interval(report_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;

        let mut previous = self.snapshot();
        loop {
            ticker.tick().await;

            let current = self.snapshot();
            if current.total.rx_bytes == 0
                && current.total.tx_bytes == 0
                && current.lease_send.transfer_count == 0
                && current.lease_receive.transfer_count == 0
            {
                continue;
            }

            let summary = summarize_snapshots(self.proxy_id, previous, current);
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "{}", format_summary(&summary))
                .expect("proxy network summary write to stdout should succeed");

            previous = current;
        }
    }

    fn snapshot(&self) -> ProxyNetworkSnapshot {
        ProxyNetworkSnapshot {
            elapsed: self.started_at.elapsed(),
            total: self.total.snapshot(),
            primary: self.primary.snapshot(),
            inter_proxy: self.inter_proxy.snapshot(),
            lease_send: self.lease_send.snapshot(),
            lease_receive: self.lease_receive.snapshot(),
        }
    }
}

struct ProxyConnectionStatsHandle {
    stats: Arc<ProxyNetworkStats>,
    connection_class: ProxyConnectionClass,
}

impl ConnectionStats for ProxyConnectionStatsHandle {
    fn record_rx_bytes(&self, bytes: usize) {
        self.stats.record_connection_bytes(
            self.connection_class,
            TransferDirection::Receive,
            bytes,
        );
    }

    fn record_tx_bytes(&self, bytes: usize) {
        self.stats
            .record_connection_bytes(self.connection_class, TransferDirection::Send, bytes);
    }

    fn record_rx_message(&self, payload_bytes: usize) {
        self.stats.print_message_size(
            self.connection_class,
            TransferDirection::Receive,
            payload_bytes,
        );
    }

    fn record_tx_message(&self, payload_bytes: usize) {
        self.stats.print_message_size(
            self.connection_class,
            TransferDirection::Send,
            payload_bytes,
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ProxyNetworkSummary {
    proxy_id: ProxyId,
    elapsed_s: f64,
    interval_s: f64,
    interval_rx_bytes: u64,
    interval_tx_bytes: u64,
    interval_rx_mib_per_s: f64,
    interval_tx_mib_per_s: f64,
    total_rx_bytes: u64,
    total_tx_bytes: u64,
    avg_rx_mib_per_s: f64,
    avg_tx_mib_per_s: f64,
    primary_rx_bytes: u64,
    primary_tx_bytes: u64,
    inter_proxy_rx_bytes: u64,
    inter_proxy_tx_bytes: u64,
    interval_lease_send_count: u64,
    interval_lease_send_objects: u64,
    interval_lease_send_payload_bytes: u64,
    interval_lease_send_avg_payload_bytes: f64,
    interval_lease_receive_count: u64,
    interval_lease_receive_objects: u64,
    interval_lease_receive_payload_bytes: u64,
    interval_lease_receive_avg_payload_bytes: f64,
    lease_send_count: u64,
    lease_send_objects: u64,
    lease_send_payload_bytes: u64,
    lease_send_avg_payload_bytes: f64,
    lease_send_max_payload_bytes: u64,
    lease_receive_count: u64,
    lease_receive_objects: u64,
    lease_receive_payload_bytes: u64,
    lease_receive_avg_payload_bytes: f64,
    lease_receive_max_payload_bytes: u64,
}

fn summarize_snapshots(
    proxy_id: ProxyId,
    previous: ProxyNetworkSnapshot,
    current: ProxyNetworkSnapshot,
) -> ProxyNetworkSummary {
    let interval = current
        .elapsed
        .checked_sub(previous.elapsed)
        .unwrap_or_default();
    let interval_s = duration_secs(interval);
    let elapsed_s = duration_secs(current.elapsed);
    let interval_rx_bytes = current
        .total
        .rx_bytes
        .saturating_sub(previous.total.rx_bytes);
    let interval_tx_bytes = current
        .total
        .tx_bytes
        .saturating_sub(previous.total.tx_bytes);
    let interval_lease_send_count = current
        .lease_send
        .transfer_count
        .saturating_sub(previous.lease_send.transfer_count);
    let interval_lease_send_objects = current
        .lease_send
        .object_count
        .saturating_sub(previous.lease_send.object_count);
    let interval_lease_send_payload_bytes = current
        .lease_send
        .payload_bytes
        .saturating_sub(previous.lease_send.payload_bytes);
    let interval_lease_receive_count = current
        .lease_receive
        .transfer_count
        .saturating_sub(previous.lease_receive.transfer_count);
    let interval_lease_receive_objects = current
        .lease_receive
        .object_count
        .saturating_sub(previous.lease_receive.object_count);
    let interval_lease_receive_payload_bytes = current
        .lease_receive
        .payload_bytes
        .saturating_sub(previous.lease_receive.payload_bytes);

    ProxyNetworkSummary {
        proxy_id,
        elapsed_s,
        interval_s,
        interval_rx_bytes,
        interval_tx_bytes,
        interval_rx_mib_per_s: bytes_per_second_to_mib(interval_rx_bytes, interval_s),
        interval_tx_mib_per_s: bytes_per_second_to_mib(interval_tx_bytes, interval_s),
        total_rx_bytes: current.total.rx_bytes,
        total_tx_bytes: current.total.tx_bytes,
        avg_rx_mib_per_s: bytes_per_second_to_mib(current.total.rx_bytes, elapsed_s),
        avg_tx_mib_per_s: bytes_per_second_to_mib(current.total.tx_bytes, elapsed_s),
        primary_rx_bytes: current.primary.rx_bytes,
        primary_tx_bytes: current.primary.tx_bytes,
        inter_proxy_rx_bytes: current.inter_proxy.rx_bytes,
        inter_proxy_tx_bytes: current.inter_proxy.tx_bytes,
        interval_lease_send_count,
        interval_lease_send_objects,
        interval_lease_send_payload_bytes,
        interval_lease_send_avg_payload_bytes: average_bytes_per_object(
            interval_lease_send_payload_bytes,
            interval_lease_send_count,
        ),
        interval_lease_receive_count,
        interval_lease_receive_objects,
        interval_lease_receive_payload_bytes,
        interval_lease_receive_avg_payload_bytes: average_bytes_per_object(
            interval_lease_receive_payload_bytes,
            interval_lease_receive_count,
        ),
        lease_send_count: current.lease_send.transfer_count,
        lease_send_objects: current.lease_send.object_count,
        lease_send_payload_bytes: current.lease_send.payload_bytes,
        lease_send_avg_payload_bytes: current.lease_send.avg_payload_bytes(),
        lease_send_max_payload_bytes: current.lease_send.max_payload_bytes,
        lease_receive_count: current.lease_receive.transfer_count,
        lease_receive_objects: current.lease_receive.object_count,
        lease_receive_payload_bytes: current.lease_receive.payload_bytes,
        lease_receive_avg_payload_bytes: current.lease_receive.avg_payload_bytes(),
        lease_receive_max_payload_bytes: current.lease_receive.max_payload_bytes,
    }
}

fn format_summary(summary: &ProxyNetworkSummary) -> String {
    format!(
        "[proxy-network-bandwidth] proxy_id={} report_window_s={:.3} inbound_bandwidth_mib_per_s={:.3} outbound_bandwidth_mib_per_s={:.3} total_bandwidth_mib_per_s={:.3}",
        summary.proxy_id,
        summary.interval_s,
        summary.interval_rx_mib_per_s,
        summary.interval_tx_mib_per_s,
        summary.interval_rx_mib_per_s + summary.interval_tx_mib_per_s,
    )
}

fn reporting_interval() -> Duration {
    std::env::var(REPORT_INTERVAL_ENV)
        .ok()
        .and_then(|value| match value.parse::<u64>() {
            Ok(secs) if secs > 0 => Some(Duration::from_secs(secs)),
            Ok(_) => None,
            Err(_) => {
                tracing::warn!(
                    "Invalid {} value {:?}, falling back to {}s",
                    REPORT_INTERVAL_ENV,
                    value,
                    DEFAULT_REPORT_INTERVAL_SECS
                );
                None
            }
        })
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_REPORT_INTERVAL_SECS))
}

fn duration_secs(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

fn bytes_per_second_to_mib(bytes: u64, seconds: f64) -> f64 {
    if seconds <= f64::EPSILON {
        0.0
    } else {
        bytes as f64 / seconds / (1024.0 * 1024.0)
    }
}

fn average_bytes_per_object(payload_bytes: u64, object_count: u64) -> f64 {
    if object_count == 0 {
        0.0
    } else {
        payload_bytes as f64 / object_count as f64
    }
}

impl ProxyNetworkStats {
    fn print_message_size(
        &self,
        connection_class: ProxyConnectionClass,
        direction: TransferDirection,
        payload_bytes: usize,
    ) {
        let wire_bytes = payload_bytes + 4;
        let mut stdout = io::stdout().lock();
        writeln!(
            stdout,
            "[proxy-message-size] proxy_id={} connection={} direction={} message_payload_bytes={} message_wire_bytes={}",
            self.proxy_id,
            connection_class.as_str(),
            direction.as_str(),
            payload_bytes,
            wire_bytes,
        )
        .expect("proxy message size write to stdout should succeed");
    }
}

impl ProxyConnectionClass {
    fn as_str(self) -> &'static str {
        match self {
            ProxyConnectionClass::Primary => "primary",
            ProxyConnectionClass::InterProxy => "inter_proxy",
        }
    }
}

impl TransferDirection {
    fn as_str(self) -> &'static str {
        match self {
            TransferDirection::Send => "outbound",
            TransferDirection::Receive => "inbound",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_bytes_by_connection_class() {
        let stats = ProxyNetworkStats::new(2);
        stats.record_connection_bytes(
            ProxyConnectionClass::Primary,
            TransferDirection::Receive,
            64,
        );
        stats.record_connection_bytes(
            ProxyConnectionClass::InterProxy,
            TransferDirection::Send,
            96,
        );
        stats.record_lease_transfer(TransferDirection::Send, 3, 120);

        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot.total,
            TrafficSnapshot {
                rx_bytes: 64,
                tx_bytes: 96
            }
        );
        assert_eq!(
            snapshot.primary,
            TrafficSnapshot {
                rx_bytes: 64,
                tx_bytes: 0
            }
        );
        assert_eq!(
            snapshot.inter_proxy,
            TrafficSnapshot {
                rx_bytes: 0,
                tx_bytes: 96
            }
        );
        assert_eq!(
            snapshot.lease_send,
            LeaseTransferSnapshot {
                transfer_count: 1,
                object_count: 3,
                payload_bytes: 120,
                max_payload_bytes: 120
            }
        );
    }

    #[test]
    fn summary_format_is_bandwidth_only() {
        let previous = ProxyNetworkSnapshot {
            elapsed: Duration::from_secs(5),
            total: TrafficSnapshot {
                rx_bytes: 100,
                tx_bytes: 200,
            },
            primary: TrafficSnapshot {
                rx_bytes: 40,
                tx_bytes: 150,
            },
            inter_proxy: TrafficSnapshot {
                rx_bytes: 60,
                tx_bytes: 50,
            },
            lease_send: LeaseTransferSnapshot {
                transfer_count: 1,
                object_count: 2,
                payload_bytes: 80,
                max_payload_bytes: 80,
            },
            lease_receive: LeaseTransferSnapshot::default(),
        };
        let current = ProxyNetworkSnapshot {
            elapsed: Duration::from_secs(10),
            total: TrafficSnapshot {
                rx_bytes: 1_100,
                tx_bytes: 2_200,
            },
            primary: TrafficSnapshot {
                rx_bytes: 440,
                tx_bytes: 1_500,
            },
            inter_proxy: TrafficSnapshot {
                rx_bytes: 660,
                tx_bytes: 700,
            },
            lease_send: LeaseTransferSnapshot {
                transfer_count: 3,
                object_count: 8,
                payload_bytes: 1_280,
                max_payload_bytes: 700,
            },
            lease_receive: LeaseTransferSnapshot {
                transfer_count: 2,
                object_count: 5,
                payload_bytes: 640,
                max_payload_bytes: 400,
            },
        };

        let summary = summarize_snapshots(4, previous, current);
        assert_eq!(summary.proxy_id, 4);
        assert_eq!(summary.interval_rx_bytes, 1_000);
        assert_eq!(summary.interval_tx_bytes, 2_000);
        assert_eq!(summary.total_rx_bytes, 1_100);
        assert_eq!(summary.total_tx_bytes, 2_200);
        assert_eq!(summary.primary_rx_bytes, 440);
        assert_eq!(summary.inter_proxy_tx_bytes, 700);
        assert_eq!(summary.interval_lease_send_count, 2);
        assert_eq!(summary.interval_lease_send_objects, 6);
        assert_eq!(summary.interval_lease_send_payload_bytes, 1_200);
        assert_eq!(summary.interval_lease_receive_count, 2);
        assert_eq!(summary.interval_lease_receive_objects, 5);
        assert_eq!(summary.interval_lease_receive_payload_bytes, 640);
        assert_eq!(summary.lease_send_count, 3);
        assert_eq!(summary.lease_receive_count, 2);

        let formatted = format_summary(&summary);
        assert!(formatted.contains("[proxy-network-bandwidth]"));
        assert!(formatted.contains("report_window_s="));
        assert!(formatted.contains("inbound_bandwidth_mib_per_s="));
        assert!(formatted.contains("outbound_bandwidth_mib_per_s="));
        assert!(formatted.contains("total_bandwidth_mib_per_s="));
        assert!(!formatted.contains("avg_"));
        assert!(!formatted.contains("lease_"));
    }
}
