# Setup and Usage

## Prerequisites

- **Rust**: 1.70 or later (install via [rustup](https://rustup.rs/))
- **OS**: Linux (tested on Ubuntu 20.04+)
- **Cores**: At least 4 CPU cores recommended for multi-proxy experiments

## Building

Build the release version:

```bash
cargo build --release
```

The binaries will be in `target/release/`:
- `remora` - primary/proxy node
- `load_generator` - client load generator

Build for development (faster compilation, slower execution):

```bash
cargo build
```

## Configuration

Remora requires two configuration files: validator config (network/node setup) and benchmark config (workload parameters).

### Validator Configuration

See [`assets/example-validator.yml`](../assets/example-validator.yml):

```yaml
# Primary network addresses
proxy_server_address: "127.0.0.1:8080"    # primary listens for proxies
client_server_address: "127.0.0.1:8081"   # primary listens for clients
metrics_address: "127.0.0.1:9090"         # Prometheus metrics

# Proxy configurations
proxies:
  - proxy_id: 0
    listen_proxy_address: 127.0.0.1:19000      # Proxy-to-Proxy communication
    listen_primary_address: 127.0.0.1:19002    # Proxy-to-Primary communication
    metrics_address: 127.0.0.1:19090
  - proxy_id: 1
    listen_proxy_address: 127.0.0.1:19001
    listen_primary_address: 127.0.0.1:19003
    metrics_address: 127.0.0.1:19091

# System parameters
validator_parameters:
  load_balancing_policy: !LSDS    # Scheduling policy: LSDS, SDS, RSDS, Zeus
  separation_mode: !NoSeparation  # Stateless/stateful separation mode
```

**Scheduling Policies** (see [`src/primary/shared_obj_txn_forwarder.rs`](../src/primary/shared_obj_txn_forwarder.rs)):
- `LSDS`: Locality-aware Subgraph-first Scheduling (default, balances locality and load)
- `SDS`: Subgraph-first Scheduling (locality-only)
- `RSDS`: Random Subgraph Dispatch (load-only)
- `Zeus`: Send to proxy with most required objects

**Separation Modes**:
- `NoSeparation`: No stateless/stateful separation
- `ProxySeparation`: proxies separate execution stages
- `PrimaryPreSeparation`: primary separates before scheduling
- `PrimaryPostSeparation`: primary separates after scheduling

### Benchmark Configuration

See [`assets/example-benchmark.yml`](../assets/example-benchmark.yml):

```yaml
load: 10000                # Transactions per second
duration:
  secs: 10                 # Benchmark duration
  nanos: 0
workload: !FakeZipfian     # Workload type
  alpha: 0.5               # Zipfian skew (0.0 = uniform, higher = more skewed)
  number_of_inputs: 5      # Objects per transaction
```

**Workload Types**:

- **Fake workloads** (no actual execution): `FakeZipfian`, `FakeSolanaTransactions`, `FakeEthereumTransfers`
- **TPC-C** ([`assets/example-tpcc.yml`](../assets/example-tpcc.yml)):
  ```yaml
  workload: !Tpcc
    num_warehouses: 10
    payment_ratio: 0.5           # 0.0 = all NEW_ORDER, 1.0 = all PAYMENT
    num_nodes: 4                 # Partitions for locality experiments
    hotspot_percentage: 0.8      # Fraction of requests to hotspot
    rotation_interval_secs: 20   # Hotspot rotation interval
  ```
- **Sui transactions**: `Transfers`, `SharedObjects`, `UniswapNormal`

## Running

### Local Single-Machine Setup

**Terminal 1** - Start the primary:
```bash
RUST_LOG=info cargo run --release --bin remora -- \
  --validator-config assets/example-validator.yml \
  --benchmark-config assets/example-benchmark.yml \
  primary
```

**Terminal 2** - Start proxy 0:
```bash
RUST_LOG=info cargo run --release --bin remora -- \
  --validator-config assets/example-validator.yml \
  --benchmark-config assets/example-benchmark.yml \
  proxy --proxy-id 0
```

**Terminal 3** - Start proxy 1:
```bash
RUST_LOG=info cargo run --release --bin remora -- \
  --validator-config assets/example-validator.yml \
  --benchmark-config assets/example-benchmark.yml \
  proxy --proxy-id 1
```

**Terminal 4** - Run the load generator:
```bash
RUST_LOG=info cargo run --release --bin load_generator -- \
  --validator-config assets/example-validator.yml \
  --benchmark-config assets/example-benchmark.yml
```

The load generator will submit transactions for the configured duration and report throughput/latency metrics.

### Multi-Machine Setup

For distributed deployment:

1. Update network addresses in validator config to use actual IPs instead of `127.0.0.1`
2. Run the primary on one machine: `remora ... primary`
3. Run each proxy on a separate machine: `remora ... proxy --proxy-id N`
4. Run the load generator from a client machine

Use `--binding-address <IP>` flag to override the binding address:

```bash
cargo run --release --bin remora -- \
  --validator-config assets/example-validator.yml \
  --binding-address 0.0.0.0 \
  primary
```

## Testing

Run the test suite:

```bash
cargo test
```

Run a specific test:

```bash
cargo test remote_proxy_sui
```

**Integration tests** ([`tests/common_case.rs`](../tests/common_case.rs)) start a primary, multiple proxies, and a load generator to verify end-to-end functionality.

## Metrics and Monitoring

### Prometheus Metrics

Each node exposes metrics on its configured `metrics_address`:

- **primary**: `127.0.0.1:9090` (default)
- **proxy 0**: `127.0.0.1:19090`
- **proxy 1**: `127.0.0.1:19091`

View metrics:
```bash
curl http://127.0.0.1:9090/metrics
```

### Key Metrics

See [`src/metrics.rs`](../src/metrics.rs) for all metrics. Important ones:

- `remora_latency_seconds`: End-to-end transaction latency (histogram)
- `remora_throughput_tps`: Transactions per second (gauge)
- `remora_proxy_load`: Per-proxy load (gauge)
- `remora_object_migrations`: Object ownership transfers (counter)

### Grafana Dashboard

Import [`assets/grafana-dashboard.json`](../assets/grafana-dashboard.json) into Grafana to visualize:

- Throughput and latency over time
- Per-proxy utilization
- Object migration patterns
- Queue depths

The primary also prints periodic metric summaries to stdout when run with `RUST_LOG=info`.
