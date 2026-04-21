# microdb Bench: ESP32-S3 N16R8

This folder contains a terminal-driven benchmark runner for `ESP32-S3 N16R8`.

- file: `microdb_esp32_s3_bench.ino`
- goal: validate core API behavior and measure latency/throughput metrics
- mode: **terminal/manual trigger**; tests do not auto-run on boot

## What The Benchmark Covers

The terminal app can run:

1. `KV`: `microdb_kv_put`, `microdb_kv_get`, `microdb_kv_del`
2. `TS`: `microdb_ts_register`, `microdb_ts_insert`, `microdb_ts_query_buf`
3. `REL`: schema create + `microdb_rel_insert` + `microdb_rel_find`
4. `WAL`: WAL fill, `microdb_compact`, `microdb_inspect` validation
5. `Migration`: schema-version bump and `on_migrate` callback validation
6. `TXN`: `microdb_txn_begin`, `microdb_txn_commit`, `microdb_txn_rollback`

Each measured step prints:

- `total=<x.xxx> ms`
- `avg=<x.xxx> us/op`
- `min/p50/p95/max` latency (`us`)
- `ops/s` throughput
- `MB/s` data throughput (where applicable)
- `ops=<N>`
- `heap_d=<delta>` (free 8-bit heap delta)

## Backend Notes

- The sketch currently uses an **in-RAM storage HAL** (flash-like API over a RAM buffer).
- Benefit: reproducible runs without custom flash partition setup.
- WAL/compact/reopen/migration/txn paths are still exercised.
- On ESP32, storage buffer allocation prefers **PSRAM** (`MALLOC_CAP_SPIRAM`), with internal RAM fallback.

If you need real flash-latency benchmarking, switch to `port/esp32/microdb_port_esp32.*` with a partition label.

## Arduino Integration

This folder already includes local build helpers:

- `microdb.h` (local copy of public header)
- `src/*.c` + `src/*.h` (local copies of microdb sources)

This allows Arduino IDE builds without fragile `../../...` include paths.

If core files under `include/` or `src/` change in the repository, sync local copies in `bench/microdb_esp32_s3_bench/`.

For PlatformIO/ESP-IDF, you can instead add the repo as a component and move the benchmark to `main.cpp`.

## Benchmark Profiles

Runtime profiles:

- `quick`: fast sanity run
- `deterministic`: recommended for stable tail-latency behavior
- `balanced`: default profile (good runtime/depth tradeoff)
- `stress`: larger workload for regressions and limits

Profile knobs include:

- `ram_kb` and `kv/ts/rel` split
- `kv_ops`, `ts_ops`, `rel_rows`
- WAL workload (`wal_ops`, `wal_key_span`, `wal_val_bytes`)
- `wal_compact_threshold_pct`

## Golden Profiles

Current recommended settings:

- `deterministic`:
  - `kv=192`, `ts=384`, `rel=240`, `wal_ops=700`, `wal_key=140`, `wal_val=24`
  - `pace_every=1`, `pace_us=12`, `flush_every=0`
  - use via `run_det` (sets deterministic + paced OFF + fresh DB)
- `balanced`:
  - `kv=320`, `ts=640`, `rel=500`, `wal_ops=1200`, `wal_key=200`, `wal_val=32`
  - use for throughput comparison between builds
- `stress`:
  - `ram=320`, `kv=900`, `ts=2400`, `rel=1200`, `wal_ops=3200`, `wal_key=320`, `wal_val=64`
  - note: requires larger storage budget; if open fails (`-6`), reduce workload or increase storage

## Output Format

Expected serial output example:

```text
microdb ESP32-S3 terminal bench is ready.
Tests do NOT run automatically at power-on.
microdb-bench> run
=== microdb ESP32-S3 benchmark start (profile=balanced) ===
[BENCH] kv_put           total=... ms avg=... us p50=... p95=... min=... max=... max_op~... xmax/p50=... spk>1ms=...@... spk>5ms=...@... ops/s=... MB/s=... ops=... heap_d=...
[SLO] kv_put           OK/WARN (...)
[PHASE] kv_put           cold_ops=... cold_avg=... steady_ops=... steady_avg=...
...
=== microdb ESP32-S3 benchmark end ===
microdb-bench>
```

## Metric Interpretation

- `max_op~N`: approximate operation index where max latency occurred
- `spk>1ms=a@b`: `a` samples above 1ms, `b` first index above 1ms
- `spk>5ms=a@b`: key metric for deterministic tail behavior
- `xmax/p50`: extreme-to-median ratio; lower is more stable
- `[PHASE] cold/steady`: separates startup from steady-state behavior
- `[SLO]`: profile-aware tail checks (`OK` / `WARN`)

## POSIX vs ESP32 Interpretation

- Do not treat desktop POSIX latency numbers as direct predictors of ESP32 SPI-flash latency.
- Practical reference from recent runs:
  - ESP32-S3 (COM17, N16R8 profile): `kv_put` ~`70 us/op`
  - POSIX simulation (desktop): `kv_put` ~`37 us/op`
- The desktop path can amplify syscall effects (for example WAL scatter-write as multiple `write()` calls), while ESP32 cost is dominated by flash transaction latency.
- Use POSIX primarily for relative trend detection, and validate release-critical perf conclusions on target hardware.

## WAL Sync Mode Decision Table

Reference run (ESP32-S3 N16R8, COM17, deterministic profile, `2026-04-21`) shows:

| Metric | `SYNC_ALWAYS` | `SYNC_FLUSH_ONLY` | Gain |
|---|---:|---:|---:|
| `kv_put` avg | ~67.6 us | ~27.6 us | ~2.44x faster |
| `kv_del` avg | ~101.9 us | ~24.9 us | ~4.09x faster |
| `wal_kv_put` avg | ~74.2 us | ~32.9 us | ~2.26x faster |
| `ts_insert` avg | ~20.7 us | ~19.1 us | ~1.08x faster |
| `kv_get` avg | ~8.65 us | ~8.81 us | neutral |
| `compact` total | ~10.94 ms | ~10.55 ms | ~1.04x faster |
| `reopen` total | ~21.43 ms | ~15.30 ms | ~1.40x faster |

Choose mode by requirement:

| If you need... | Use mode | Durability model |
|---|---|---|
| Every write durable before API returns | `MICRODB_WAL_SYNC_ALWAYS` (default) | No acknowledged-write loss on power loss (assuming storage contract holds). |
| Lowest write latency / high-rate ingest | `MICRODB_WAL_SYNC_FLUSH_ONLY` | On power loss you may lose last `N` WAL entries since last `microdb_flush()`. |

Practical guidance:

- `SYNC_FLUSH_ONLY` is usually the better fit for sensor/log ingestion workloads with frequent writes.
- Call `microdb_flush()` at explicit durability boundaries (for example end of batch, periodic timer, before controlled shutdown/sleep).
- Treat latency numbers as directional and board-specific; re-run on your exact flash chip and partition layout (in practice, values can vary by about +/-20% across SPI flash vendors).

## Terminal Commands

- `help`: show command list
- `run`: execute full benchmark suite (fresh DB)
- `kv` / `ts` / `rel` / `wal` / `reopenchk` / `migrate` / `txn`: run one test group
- `stats`: print `microdb_inspect()` snapshot
- `metrics`: print latest benchmark metrics
- `config`: print active benchmark configuration
- `profiles`: list available profiles
- `profile`: show active profile
- `profile <name>`: switch profile + reopen DB with wipe
- `run_det`: deterministic profile + paced OFF + fresh DB + full run (recommended)
- `run_det_paced`: deterministic profile + paced ON + full run (experimental)
- `paced`: show paced mode state
- `paced on|off`: toggle paced mode
- `resetdb`: wipe storage + reopen DB
- `reopen`: reopen DB without wipe

`run_det` validates deterministic profile tail behavior only; it does not automatically validate all profiles/workloads.

## Recommended Workflow

1. Run `run_det`
2. Run `metrics`
3. Check:
   - `spk>5ms` for `kv_del`, `ts_insert`, `rel_insert`, `wal_kv_put`
   - `[SLO]` lines (`OK` / `WARN`)
4. For throughput comparison, run `profile balanced` then `run`

## Benchmark Log Template

Use this table to log runs:

| Date | FW commit | Board | CPU MHz | kv_put ms/op | kv_get ms/op | kv_del ms/op | ts_insert ms/op | rel_insert ms/op | compact total ms | Notes |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| YYYY-MM-DD | `<sha>` | ESP32-S3 N16R8 | 240 |  |  |  |  |  |  |  |

## Consistency Recommendations

- fix CPU frequency (for example 240 MHz)
- keep serial baudrate fixed (`115200`)
- run from a fresh boot
- disable unrelated background load where possible
- compare at least 3 runs and report median
