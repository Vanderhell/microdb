# SD stress bench (ESP32-S3 N16R8, SD_MMC)

This benchmark is a **long-running real-hardware stress test** that uses SD_MMC storage (persistent file) and continuously writes mixed `KV`/`TS`/`REL` workload while reporting live utilization.

It is complementary to `docs/BENCHMARKS.md`:

- `docs/BENCHMARKS.md` focuses on short, reproducible latency/throughput micro-bench runs.
- SD stress bench focuses on endurance, pressure behavior, compaction, and long-run stability on real SD media.

## Hardware / wiring

See `bench/loxdb_esp32_s3_sd_stress_bench/README.md`.

## How to run (automated logging)

1. Flash the sketch:
   - `bench/loxdb_esp32_s3_sd_stress_bench/loxdb_esp32_s3_sd_stress_bench.ino`
2. Run the logger:

   - `./scripts/run_sd_stress_bench.ps1 -Port COM19 -DurationSec 600 -Profile soak -Mode all -Verify on -ResetDb`

If you see a message like "Detected terminal bench firmware (loxdb-bench>)", it means the board is running the other bench sketch (HEAD/BASE terminal bench). Re-flash the SD stress sketch and retry.

Artifacts are written to `docs/results/`:

- raw serial log: `esp32_sd_stress_<timestamp>_<sha>_com19.log`
- pressure CSV: `esp32_sd_stress_<timestamp>_<sha>_com19.csv` (parsed from `[PRESSURE]` lines)
- short run note: `esp32_sd_stress_<timestamp>_<sha>_com19.md`

## What to look at

From the raw log:

- `[PRESSURE] kv/ts/rel/wal/risk ops=...` (pressure trend over time)
- `[STATS] kv_entries / ts_samples / rel_rows / wal_bytes` (capacity + growth)
- `[BENCH] ... compact=<N> last_compact_ms=<ms> ok/fail=<verify>` (compaction + verification health)

From the CSV:

- plot `ops` over time vs `risk_pct`, `wal_pct` to see sustained ingest and near-full behavior.
