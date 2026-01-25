# Remora

## Run

### Primary
```
RUST_LOG=info cargo run --release --bin remora -- \
--benchmark-config assets/example-benchmark.yml \
--validator-config assets/example-validator.yml primary
```

### Load generator
```
RUST_LOG=info cargo run --release --bin load_generator -- \
--benchmark-config assets/example-benchmark.yml \
--validator-config assets/example-validator.yml
```
