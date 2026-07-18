# Remora

[![arXiv](https://img.shields.io/badge/arXiv-2607.02817-b31b1b.svg)](https://arxiv.org/abs/2607.02817)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](#license)
![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)

Remora is a scale-out execution engine for blockchain validators. It uses an asymmetric architecture where a single **primary** (coordinator) node handles consensus and scheduling, while a pool of **proxy** nodes execute smart contracts in parallel with strict determinism guarantees.

This repository contains the implementation accompanying the paper [*Remora: Scale-out Deterministic Execution for Smart Contracts*](https://arxiv.org/abs/2607.02817).

## Highlights

- **Asymmetric architecture**: primary handles consensus/scheduling, proxies execute contracts
- **Strict determinism**: version-based execution ordering without coordination overhead
- **Stateless/stateful separation**: different scheduling policies for verification vs execution
- **Workload adaptiveness**: subgraph-first scheduling optimizes for locality and load balance
- **Consensus window optimization**: pre-consensus scheduling and stateless execution
- **Elasticity**: dynamic proxy pool scaling without execution stalls

The implementation is ~13k lines of Rust and supports synthetic benchmarks (Fake), database benchmarks (TPC-C), and real blockchain transactions (Sui).

## Quick Start

```bash
cargo build --release

# Terminal 1: primary
RUST_LOG=info ./target/release/remora \
  --validator-config assets/example-validator.yml \
  --benchmark-config assets/example-benchmark.yml \
  primary

# Terminals 2 & 3: proxies (repeat with --proxy-id 1)
RUST_LOG=info ./target/release/remora \
  --validator-config assets/example-validator.yml \
  --benchmark-config assets/example-benchmark.yml \
  proxy --proxy-id 0

# Terminal 4: load generator
RUST_LOG=info ./target/release/load_generator \
  --validator-config assets/example-validator.yml \
  --benchmark-config assets/example-benchmark.yml
```

The load generator submits transactions for the configured duration and reports throughput and latency. See [docs/setup.md](docs/setup.md) for configuration details, multi-machine deployment, and monitoring.

## Documentation

| Document | Contents |
|----------|----------|
| [docs/setup.md](docs/setup.md) | Building, configuration, running (local and distributed), testing, metrics and Grafana |
| [docs/architecture.md](docs/architecture.md) | System components, directory structure, and a paper-to-code map |

## Reference

If you use Remora in your research, please cite:

```bibtex
@misc{liu2026remora,
  title         = {Remora: Scale-out Deterministic Execution for Smart Contracts},
  author        = {Liu, Zhengqing and Sonnino, Alberto and Zablotski, Igor and
                   Kokoris-Kogias, Eleftherios and Kogias, Marios},
  year          = {2026},
  eprint        = {2607.02817},
  archivePrefix = {arXiv},
  primaryClass  = {cs.DC},
  url           = {https://arxiv.org/abs/2607.02817}
}
```

## License

Apache License 2.0. See source file headers for details.
