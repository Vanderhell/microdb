# Benchmarks (ESP32-S3 N16R8)

This page is the publication home for **measured** benchmark results from the verified ESP32-S3 N16R8 setup.

It is intentionally a template first: fill it only with real measured numbers from the existing local benchmark runs.

<!-- TODO(maintainer): Replace all TBD cells with real numbers from ESP32-S3 N16R8 measurements. -->

## Test platform

- Platform: ESP32-S3 N16R8 (16MB NOR flash, 8MB PSRAM)
- ESP-IDF / Arduino core:
  - <!-- TODO(maintainer): fill exact version (IDF or core version) -->
- CPU frequency:
  - <!-- TODO(maintainer): fill -->
- Flash mode / frequency:
  - <!-- TODO(maintainer): fill -->
- Storage backend used:
  - <!-- TODO(maintainer): e.g. RAM-backed flash-like HAL, real flash partition, SD/FATFS, etc. -->

## Methodology

- Iterations per measurement:
  - <!-- TODO(maintainer): fill -->
- Latency reporting:
  - p50 / p95 / p99 (microseconds)
- Outliers:
  - <!-- TODO(maintainer): describe if/what is discarded and why -->
- Warmup / cold vs steady:
  - <!-- TODO(maintainer): describe -->

## Results — KV engine

| Operation | p50 (us) | p95 (us) | p99 (us) | throughput (ops/s) | Notes |
|---|---:|---:|---:|---:|---|
| `kv_put` | TBD | TBD | TBD | TBD | <!-- TODO(maintainer) --> |
| `kv_get` | TBD | TBD | TBD | TBD | <!-- TODO(maintainer) --> |
| `kv_del` | TBD | TBD | TBD | TBD | <!-- TODO(maintainer) --> |

WAL impact (KV):
- <!-- TODO(maintainer): summarize delta with WAL enabled vs disabled, if measured -->

## Results — TS engine

| Stream type | insert rate (samples/s) | query p50 (us) | query p95 (us) | Notes |
|---|---:|---:|---:|---|
| `F32` | TBD | TBD | TBD | <!-- TODO(maintainer) --> |
| `I32` | TBD | TBD | TBD | <!-- TODO(maintainer) --> |
| `U32` | TBD | TBD | TBD | <!-- TODO(maintainer) --> |
| `RAW` | TBD | TBD | TBD | <!-- TODO(maintainer) --> |

## Results — REL engine

| Rows (N) | insert p50 (us) | find_by_index p50 (us) | scan p50 (us) | Notes |
|---:|---:|---:|---:|---|
| TBD | TBD | TBD | TBD | <!-- TODO(maintainer) --> |
| TBD | TBD | TBD | TBD | <!-- TODO(maintainer) --> |

## WAL sync modes comparison

| Mode | KV latency delta | TS latency delta | REL latency delta | Notes |
|---|---|---|---|---|
| `LOX_WAL_SYNC_ALWAYS` | TBD | TBD | TBD | <!-- TODO(maintainer) --> |
| `LOX_WAL_SYNC_FLUSH_ONLY` | TBD | TBD | TBD | <!-- TODO(maintainer) --> |

## Power-loss recovery

| Scenario | WAL fill level | recovery time (ms) | Notes |
|---|---:|---:|---|
| TBD | TBD | TBD | <!-- TODO(maintainer) --> |
| TBD | TBD | TBD | <!-- TODO(maintainer) --> |

## RAM profile sweep

| RAM budget | KV/TS/REL split | Key results summary |
|---:|---|---|
| 16 KB | TBD | <!-- TODO(maintainer) --> |
| 32 KB | TBD | <!-- TODO(maintainer) --> |
| 64 KB | TBD | <!-- TODO(maintainer) --> |
| 128 KB | TBD | <!-- TODO(maintainer) --> |

## Reproducibility

Benchmark runner(s) in this repository:

- `bench/loxdb_esp32_s3_bench_head/`
- `bench/loxdb_esp32_s3_bench_base/`

Steps to reproduce:

1. Build and flash the bench sketch for ESP32-S3 N16R8.
2. Run the terminal-driven commands described in the bench README.
3. Copy measured outputs into the tables above (only real numbers; no estimates).

