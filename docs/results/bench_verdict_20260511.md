# Bench verdict (ESP32-S3 N16R8) — 2026-05-11

This note summarizes the ESP32-S3 N16R8 benchmark runs executed on `COM19` on **2026-05-11** for merge readiness.

Repo state:

- repo commit: `1a1c569`
- Arduino-ESP32 core: `3.3.8`
- FQBN: `esp32:esp32:esp32s3:CDCOnBoot=cdc,FlashSize=16M,PSRAM=opi`
- CPU: `240 MHz`
- Flash: `QIO @ 80 MHz`

## Artifacts

HEAD bench (from `bench/loxdb_esp32_s3_bench_head/`):

- deterministic: `docs/results/esp32_deterministic_20260511_101754_1a1c569_com19.log`
- balanced: `docs/results/esp32_balanced_20260511_101754_1a1c569_com19.log`
- stress: `docs/results/esp32_stress_20260511_102425_1a1c569_com19.log`

BASE bench (from `bench/loxdb_esp32_s3_bench_base/`):

- deterministic: `docs/results/esp32_deterministic_20260511_104504_1a1c569_com19.log`
- balanced: `docs/results/esp32_balanced_20260511_104504_1a1c569_com19.log`
- stress: `docs/results/esp32_stress_20260511_104504_1a1c569_com19.log`

## Findings

- HEAD is substantially faster than BASE for write-heavy KV + WAL paths on this setup (deterministic + balanced).
- `kv_get` is roughly neutral between HEAD and BASE.
- Stress runs show tail spikes in both, but BASE stress shows much larger extremes in KV/WAL and much smaller `wal_total` in its effective config (`8160B` vs `32736B` in HEAD stress).

### Deterministic profile (p50 / ops/s)

| Metric | HEAD | BASE | Notes |
|---|---:|---:|---|
| `kv_put` p50 (us) | 26 | 64 | ~2.46× faster on HEAD |
| `kv_del` p50 (us) | 22 | 97 | ~4.41× faster on HEAD |
| `wal_kv_put` p50 (us) | 32 | 70 | ~2.19× faster on HEAD |
| `kv_get` p50 (us) | 9 | 8 | neutral |
| `kv_put` ops/s | 38117.9 | 15523.9 | ~2.46× higher on HEAD |
| `kv_del` ops/s | 42077.6 | 10114.3 | ~4.16× higher on HEAD |

### Balanced profile (throughput ops/s)

| Metric | HEAD | BASE | Notes |
|---|---:|---:|---|
| `kv_put` ops/s | 38025.1 | 15450.7 | ~2.46× higher on HEAD |
| `kv_del` ops/s | 42524.0 | 10082.2 | ~4.22× higher on HEAD |
| `ts_insert` ops/s | 32030.4 | 30923.9 | ~1.04× higher on HEAD |
| `rel_insert` ops/s | 13264.4 | 12395.3 | ~1.07× higher on HEAD |

### Stress profile notes

- HEAD stress: KV ops capped to capacity (`kv_capacity=248`) and TS drops samples once the TS arena fills (`retained=1792 dropped=608` in the run).
- BASE stress: KV shows large max spikes (`kv_put max=7439us`, `kv_del max=7737us`), and WAL shows a large max spike (`wal_kv_put max=15866us`); also `wal_total` effective size is much smaller (`8160B`).

