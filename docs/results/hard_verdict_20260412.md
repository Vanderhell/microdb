# Hard Verdict Report (2026-04-12)

## Product Identity
loxdb is a deterministic durable storage core for MCU/embedded systems with power-fail-safe WAL recovery.

## Desktop (full matrix + soak)
Source:
- `docs/results/validation_summary_20260412_214613.md`
- `docs/results/worstcase_matrix_*_20260412_214613.csv`
- `docs/results/soak_*_20260412_214613.csv`

Verdict:
- deterministic: PASS
  - max_kv_put=30051us max_ts_insert=29784us max_rel_insert=29900us max_rel_delete=28620us
  - max_txn_commit=29809us max_compact=26018us max_reopen=68163us
  - spikes_gt_5ms(matrix sum)=192 fail_count=0
  - soak_slo=PASS
- balanced: PASS
  - max_kv_put=40118us max_ts_insert=38141us max_rel_insert=41492us max_rel_delete=33952us
  - max_txn_commit=42622us max_compact=33388us max_reopen=79201us
  - spikes_gt_5ms(matrix sum)=248 fail_count=0
  - soak_slo=PASS
- stress: PASS
  - max_kv_put=49567us max_ts_insert=50491us max_rel_insert=47908us max_rel_delete=44269us
  - max_txn_commit=47601us max_compact=44218us max_reopen=95913us
  - spikes_gt_5ms(matrix sum)=318 fail_count=0
  - soak_slo=PASS

## ESP32-S3 (real HW)
Sources:
- full ESP wrapper run: `docs/results/validation_summary_20260412_203442.md`
- stress retest after storage-layout fix: `docs/results/esp32_stress_20260412_205405.log`

Verdict:
- deterministic: PASS
- balanced: PASS
- stress: PASS

Hard maxima (latest stress run):
- kv_put max=9709us
- kv_del max=10028us
- ts_insert max=40us
- rel_insert max=14917us
- wal_kv_put max=18586us
- compact max=21613us
- reopen max=301204us

## Hard Note
- robust core != automatically sellable product
- sellable product still requires:
  - explicit profile guarantees/limits table
  - one sharp product positioning sentence
  - simple adoption path (quick start + profile guide + API contract)

## Current Overall Verdict
- Core quality: strong and validated on desktop + ESP32-S3.
- Product readiness: good technical base, but final value packaging is still documentation/adoption work.
