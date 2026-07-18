# Architecture

Remora uses an asymmetric architecture: a single primary (coordinator) node handles consensus and scheduling, while a pool of proxy nodes execute smart contracts in parallel with strict determinism guarantees.

## Primary (Coordinator) Node

**Code**: [`src/primary/`](../src/primary/), [`src/bin/remora.rs`](../src/bin/remora.rs)

The primary is the single point of coordination that interfaces with consensus. It contains:

- **Consensus module** ([`src/primary/mock_consensus.rs`](../src/primary/mock_consensus.rs)): Receives ordered transaction batches
- **Scheduler** ([`src/primary/shared_obj_txn_forwarder.rs`](../src/primary/shared_obj_txn_forwarder.rs)): Implements subgraph-first scheduling (SFS) and other policies (LSDS, Zeus, SDS, RSDS)
- **Load balancer** ([`src/primary/load_balancer.rs`](../src/primary/load_balancer.rs)): Routes stateless work to proxies
- **Metadata manager**: Maintains object ownership and version information
- **Persistence layer**: Snapshots execution state at epoch boundaries

The primary assigns unique versions to each object access within a consensus batch, ensuring deterministic execution order across distributed proxies.

## Proxy Node

**Code**: [`src/proxy/`](../src/proxy/), [`src/bin/remora.rs`](../src/bin/remora.rs)

Proxy nodes execute transactions assigned by the primary. Each proxy:

- Maintains leased object state in memory
- Runs a deterministic parallel runtime using dependency DAGs
- Fetches remote objects from other proxies when needed
- Reports load metrics and state snapshots to the primary

The runtime ([`src/proxy/core.rs`](../src/proxy/core.rs)) uses `tokio::sync::Notify` primitives to enforce version-based dependencies, allowing transactions to execute in parallel while respecting the consensus-established order.

## Executor Backends

**Code**: [`src/executor/`](../src/executor/)

Remora supports three executor types:

- **FakeExecutor** ([`src/executor/fake.rs`](../src/executor/fake.rs)): Simulates execution with configurable delays, no actual state changes
- **TpccExecutor** ([`src/executor/tpcc/`](../src/executor/tpcc/)): Implements TPC-C NEW_ORDER and PAYMENT transactions
- **SuiExecutor** ([`src/executor/sui.rs`](../src/executor/sui.rs)): Executes real Sui Move smart contracts

All executors implement the `Executor` trait ([`src/executor/api.rs`](../src/executor/api.rs)) which defines transaction generation, execution, and verification interfaces.

## Load Generator (Client)

**Code**: [`src/client/`](../src/client/), [`src/bin/load_generator.rs`](../src/bin/load_generator.rs)

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

## Paper-to-Code Map

Key concepts from the [paper](https://arxiv.org/abs/2607.02817) and their code locations:

| Paper Concept | Code Location |
|---------------|---------------|
| Primary | [`src/primary/`](../src/primary/) |
| Proxy | [`src/proxy/`](../src/proxy/) |
| Version assignment | [`src/primary/shared_obj_txn_forwarder.rs`](../src/primary/shared_obj_txn_forwarder.rs) (assign_versions) |
| Subgraph-first scheduling (SFS) | [`src/primary/shared_obj_txn_forwarder.rs`](../src/primary/shared_obj_txn_forwarder.rs) (PreConsensusSchedulingPolicy::LSDS) |
| DAG-based execution | [`src/proxy/core.rs`](../src/proxy/core.rs) (dependency tracking) |
| Object ownership tracking | [`src/primary/shared_obj_txn_forwarder.rs`](../src/primary/shared_obj_txn_forwarder.rs) (metadata maps) |
| Stateless/stateful separation | [`src/executor/api.rs`](../src/executor/api.rs) (PrimaryToProxyMessage enum) |
| Periodic snapshotting | [`src/proxy/core.rs`](../src/proxy/core.rs) (epoch-based state reporting) |
| Elasticity (scale-out/scale-in) | [`src/primary/load_balancer.rs`](../src/primary/load_balancer.rs) |
