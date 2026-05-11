# Benchmarks (ESP32-S3 N16R8)

This page is the publication home for **measured** benchmark results from the verified ESP32-S3 N16R8 setup.

It is intentionally a template first: fill it only with real measured numbers from the existing local benchmark runs.

<!-- TODO(maintainer): Replace all TBD cells with real numbers from ESP32-S3 N16R8 measurements. -->

## Test platform

- Platform: ESP32-S3 N16R8 (16MB NOR flash, 8MB PSRAM)
- ESP-IDF / Arduino core:
  - Arduino-ESP32 core `3.3.8` (FQBN `esp32:esp32:esp32s3:...`)
- CPU frequency:
  - `240 MHz`
- Flash mode / frequency:
  - `QIO @ 80 MHz` (flash size `16MB`)
- Storage backend used:
  - In-RAM flash-like storage HAL (see `bench/loxdb_esp32_s3_bench_head/README.md`)

## Methodology

- Iterations per measurement:
  - <!-- TODO(maintainer): fill -->
- Latency reporting:
  - p50 / p95 / max (microseconds)
- Outliers:
  - <!-- TODO(maintainer): describe if/what is discarded and why -->
- Warmup / cold vs steady:
  - <!-- TODO(maintainer): describe -->

<!-- BENCHMARKS:BEGIN -->
## Results - KV engine (deterministic profile)

| Operation | p50 (us) | p95 (us) | max (us) | throughput (ops/s) | Notes |
|---|---:|---:|---:|---:|---|
| `kv_put` | 26 | 27 | 69 | 38117.9 | `esp32_deterministic_20260511_101754_1a1c569_com19.log` |
| `kv_get` | 9 | 9 | 24 | 113609.5 | `esp32_deterministic_20260511_101754_1a1c569_com19.log` |
| `kv_del` | 22 | 26 | 108 | 42077.6 | `esp32_deterministic_20260511_101754_1a1c569_com19.log` |

WAL impact (KV):
- `wal_kv_put` p50/p95/max: 32/33/42 us (`esp32_deterministic_20260511_101754_1a1c569_com19.log`)

## Results - TS engine (deterministic profile)

| Stream type | insert rate (samples/s) | query p50 (us) | query p95 (us) | Notes |
|---|---:|---:|---:|---|
| `F32` | 52538.0 | 337 | 337 | `esp32_deterministic_20260511_101754_1a1c569_com19.log` (retained=384) |
| `I32` | TBD | TBD | TBD | <!-- TODO(maintainer): add I32 run --> |
| `U32` | TBD | TBD | TBD | <!-- TODO(maintainer): add U32 run --> |
| `RAW` | TBD | TBD | TBD | <!-- TODO(maintainer): add RAW run --> |

## Results - REL engine (deterministic profile)

| Rows (N) | insert p50 (us) | find_by_index p50 (us) | Notes |
|---:|---:|---:|---|
| 240 | 25 | 10 | `esp32_deterministic_20260511_101754_1a1c569_com19.log` |

## WAL / maintenance (deterministic profile)

| Operation | total (ms) | Notes |
|---|---:|---|
| `compact` | 8.783 | `esp32_deterministic_20260511_101754_1a1c569_com19.log` |
| `reopen` | 12.346 | `esp32_deterministic_20260511_101754_1a1c569_com19.log` |

## Throughput reference - balanced profile

| Operation | throughput (ops/s) | Notes |
|---|---:|---|
| `kv_put` | 38025.1 | `esp32_balanced_20260511_101754_1a1c569_com19.log` |
| `kv_get` | 112471.7 | `esp32_balanced_20260511_101754_1a1c569_com19.log` |
| `kv_del` | 42524.0 | `esp32_balanced_20260511_101754_1a1c569_com19.log` |
| `ts_insert` | 32030.4 | `esp32_balanced_20260511_101754_1a1c569_com19.log` |
| `rel_insert` | 13264.4 | `esp32_balanced_20260511_101754_1a1c569_com19.log` |


## Stress profile reference

| Metric | Value | Notes |
|---|---:|---|
| `kv_put` throughput (ops/s) | 38007.7 | `esp32_stress_20260511_102425_1a1c569_com19.log` |
| `kv_get` throughput (ops/s) | 111762.1 | `esp32_stress_20260511_102425_1a1c569_com19.log` |
| `kv_del` throughput (ops/s) | 42958.6 | `esp32_stress_20260511_102425_1a1c569_com19.log` |
| `ts_insert` throughput (samples/s) | 29407.4 | `esp32_stress_20260511_102425_1a1c569_com19.log` (retained=1792) |
| `rel_insert` throughput (rows/s) | 4153.4 | `esp32_stress_20260511_102425_1a1c569_com19.log` (N=1200) |
| `wal_kv_put` throughput (ops/s) | 23837.9 | `esp32_stress_20260511_102425_1a1c569_com19.log` |
| `compact` total (ms) | 22.798 | `esp32_stress_20260511_102425_1a1c569_com19.log` |
| `reopen` total (ms) | 277.816 | `esp32_stress_20260511_102425_1a1c569_com19.log` |
<!-- BENCHMARKS:END -->

## Reproducibility

Benchmark runner(s) in this repository:

- `bench/loxdb_esp32_s3_bench_head/`
- `bench/loxdb_esp32_s3_bench_base/`

Steps to reproduce:

1. Build and flash the bench sketch for ESP32-S3 N16R8.
2. Run the terminal-driven commands described in the bench README.
3. Copy measured outputs into the tables above (only real numbers; no estimates).

Optional automation (logs + doc update):

- `./scripts/run_esp32_bench_and_update_docs.ps1 -Port COM19`

## Run notes

- Latest merge-prep verdict: `docs/results/bench_verdict_20260511.md`

## Related benches

- SD endurance / pressure stress test: `docs/SD_STRESS_BENCH.md`

