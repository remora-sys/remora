# Remora

Remora is a scale-out execution engine for blockchain validators. It uses an asymmetric architecture where a single primary (coordinator) node handles consensus and scheduling, while a pool of proxy nodes execute smart contracts in parallel with strict determinism guarantees.

## Overview

Remora addresses the performance bottleneck in modern blockchain systems where validators must execute transactions sequentially. The system provides:

- **Asymmetric architecture**: primary handles consensus/scheduling, proxies execute contracts
- **Strict determinism**: Version-based execution ordering without coordination overhead
- **Stateless/stateful separation**: Different scheduling policies for verification vs execution
- **Workload adaptiveness**: Subgraph-first scheduling optimizes for locality and load balance
- **Consensus window optimization**: Pre-consensus scheduling and stateless execution
- **Elasticity**: Dynamic proxy pool scaling without execution stalls

The implementation consists of ~13k lines of Rust code and supports multiple workload types including synthetic benchmarks (Fake), database benchmarks (TPC-C), and real blockchain transactions (Sui).

## Architecture

Remora implements the following key components:

### Primary (Coordinator) Node

**Code**: [`src/primary/`](src/primary/), [`src/bin/remora.rs`](src/bin/remora.rs)

The primary is the single point of coordination that interfaces with consensus. It contains:

- **Consensus module** ([`src/primary/mock_consensus.rs`](src/primary/mock_consensus.rs)): Receives ordered transaction batches
- **Scheduler** ([`src/primary/shared_obj_txn_forwarder.rs`](src/primary/shared_obj_txn_forwarder.rs)): Implements subgraph-first scheduling (SFS) and other policies (LSDS, Zeus, SDS, RSDS)
- **Load balancer** ([`src/primary/load_balancer.rs`](src/primary/load_balancer.rs)): Routes stateless work to proxies
- **Metadata manager**: Maintains object ownership and version information
- **Persistence layer**: Snapshots execution state at epoch boundaries

The primary assigns unique versions to each object access within a consensus batch, ensuring deterministic execution order across distributed proxies.

### Proxy Node

**Code**: [`src/proxy/`](src/proxy/), [`src/bin/remora.rs`](src/bin/remora.rs)

Proxy nodes execute transactions assigned by the primary. Each proxy:

- Maintains leased object state in memory
- Runs a deterministic parallel runtime using dependency DAGs
- Fetches remote objects from other proxies when needed
- Reports load metrics and state snapshots to the primary

The runtime ([`src/proxy/core.rs`](src/proxy/core.rs)) uses `tokio::sync::Notify` primitives to enforce version-based dependencies, allowing transactions to execute in parallel while respecting the consensus-established order.

### Executor Backends

**Code**: [`src/executor/`](src/executor/)

Remora supports three executor types:

- **FakeExecutor** ([`src/executor/fake.rs`](src/executor/fake.rs)): Simulates execution with configurable delays, no actual state changes
- **TpccExecutor** ([`src/executor/tpcc/`](src/executor/tpcc/)): Implements TPC-C NEW_ORDER and PAYMENT transactions
- **SuiExecutor** ([`src/executor/sui.rs`](src/executor/sui.rs)): Executes real Sui Move smart contracts

All executors implement the `Executor` trait ([`src/executor/api.rs`](src/executor/api.rs)) which defines transaction generation, execution, and verification interfaces.

### Load Generator (Client)

**Code**: [`src/client/`](src/client/), [`src/bin/load_generator.rs`](src/bin/load_generator.rs)

The load generator submits transactions to the primary at a configured rate and measures end-to-end latency.

## Directory Structure

```
remora/
├── src/
│   ├── bin/
│   │   ├── remora.rs              # Main binary (primary/proxies)
│   │   └── load_generator.rs      # Client load generator
│   ├── primary/                   # Primary implementation
│   │   ├── shared_obj_txn_forwarder.rs  # Scheduling policies
│   │   ├── load_balancer.rs       # Stateless routing
│   │   ├── mock_consensus.rs      # Consensus module
│   │   └── node.rs                # Primary node logic
│   ├── proxy/                     # Proxy implementation
│   │   ├── core.rs                # Proxy runtime and DAG execution
│   │   └── node.rs                # Proxy node logic
│   ├── executor/                  # Executor backends
│   │   ├── api.rs                 # Executor trait definition
│   │   ├── fake.rs                # Fake executor
│   │   ├── tpcc/                  # TPC-C executor
│   │   └── sui.rs                 # Sui executor
│   ├── client/                    # Load generator
│   ├── networking/                # Network communication
│   ├── config.rs                  # Configuration structures
│   └── metrics.rs                 # Prometheus metrics
├── assets/                        # Configuration examples
└── tests/                         # Integration tests
```

## Prerequisites

- **Rust**: 1.70 or later (install via [rustup](https://rustup.rs/))
- **OS**: Linux (tested on Ubuntu 20.04+)
- **Cores**: At least 4 CPU cores recommended for multi-proxy experiments

## Building

Build the release version:

cargo build --release
```

The binaries will be in `target/release/`:
- `remora` - primary/proxy node
- `load_generator` - Client

Build for development (faster compilation, slower execution):
cargo build
```

## Configuration

Remora requires two configuration files: validator config (network/node setup) and benchmark config (workload parameters).

### Validator Configuration

See [`assets/example-validator.yml`](assets/example-validator.yml):

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

**Scheduling Policies** (see [`src/primary/shared_obj_txn_forwarder.rs`](src/primary/shared_obj_txn_forwarder.rs)):
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

See [`assets/example-benchmark.yml`](assets/example-benchmark.yml):

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
- **TPC-C** ([`assets/example-tpcc.yml`](assets/example-tpcc.yml)):
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

**Integration tests** ([`tests/common_case.rs`](tests/common_case.rs)) start a primary, multiple proxies, and a load generator to verify end-to-end functionality.

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

See [`src/metrics.rs`](src/metrics.rs) for all metrics. Important ones:

- `remora_latency_seconds`: End-to-end transaction latency (histogram)
- `remora_throughput_tps`: Transactions per second (gauge)
- `remora_proxy_load`: Per-proxy load (gauge)
- `remora_object_migrations`: Object ownership transfers (counter)

### Grafana Dashboard

Import [`assets/grafana-dashboard.json`](assets/grafana-dashboard.json) into Grafana to visualize:

- Throughput and latency over time
- Per-proxy utilization
- Object migration patterns
- Queue depths

The primary also prints periodic metric summaries to stdout when run with `RUST_LOG=info`.

## Research Context

Key concepts and their code locations:

| Paper Concept | Code Location |
|---------------|---------------|
| Primary | [`src/primary/`](src/primary/) |
| Proxy | [`src/proxy/`](src/proxy/) |
| Version assignment | [`src/primary/shared_obj_txn_forwarder.rs`](src/primary/shared_obj_txn_forwarder.rs) (assign_versions) |
| Subgraph-first scheduling (SFS) | [`src/primary/shared_obj_txn_forwarder.rs`](src/primary/shared_obj_txn_forwarder.rs) (PreConsensusSchedulingPolicy::LSDS) |
| DAG-based execution | [`src/proxy/core.rs`](src/proxy/core.rs) (dependency tracking) |
| Object ownership tracking | [`src/primary/shared_obj_txn_forwarder.rs`](src/primary/shared_obj_txn_forwarder.rs) (metadata maps) |
| Stateless/stateful separation | [`src/executor/api.rs`](src/executor/api.rs) (PrimaryToProxyMessage enum) |
| Periodic snapshotting | [`src/proxy/core.rs`](src/proxy/core.rs) (epoch-based state reporting) |
| Elasticity (scale-out/scale-in) | [`src/primary/load_balancer.rs`](src/primary/load_balancer.rs) |

## License

Apache License 2.0. See source file headers for details.
