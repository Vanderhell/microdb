# Hard Verdict Report (2026-04-19)

## Product Identity
loxdb is a deterministic durable storage core for MCU/embedded systems with power-fail-safe WAL recovery.

## Desktop (full matrix + soak)
Source:
- `docs/results/validation_summary_20260419_180500.md`
- `docs/results/worstcase_matrix_*_20260419_180500.csv`
- `docs/results/soak_*_20260419_180500.csv`

Verdict:
- deterministic: PASS
  - matrix_slo=PASS soak_slo=PASS fail_count=0
  - max_kv_put=34378us max_ts_insert=33623us max_rel_insert=36121us max_rel_delete=33536us
  - max_txn_commit=45342us max_compact=28058us max_reopen=63712us
- balanced: PASS
  - matrix_slo=PASS soak_slo=PASS fail_count=0
  - max_kv_put=34103us max_ts_insert=33704us max_rel_insert=33546us max_rel_delete=32054us
  - max_txn_commit=48991us max_compact=29681us max_reopen=63530us
- stress: PASS
  - matrix_slo=PASS soak_slo=PASS fail_count=0
  - max_kv_put=48603us max_ts_insert=46308us max_rel_insert=49320us max_rel_delete=44767us
  - max_txn_commit=47658us max_compact=42825us max_reopen=85009us

## ESP32-S3 (real HW)
Latest source:
- `docs/results/validation_summary_20260419_193234.md`
- `docs/results/esp32_deterministic_20260419_193234.log`
- `docs/results/esp32_balanced_20260419_193234.log`
- `docs/results/esp32_stress_20260419_193234.log`

Status:
- deterministic: PASS (2026-04-19)
- balanced: PASS (2026-04-19)
- stress: PASS (2026-04-19)

## Current Overall Verdict
- Core quality: strong and validated on latest desktop full matrix + soak across all three profiles.
- Release readiness: desktop + ESP gates are green for current firmware validation.
