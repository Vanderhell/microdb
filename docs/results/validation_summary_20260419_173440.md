# Full Validation Summary (20260419_173440)

## Desktop Verdicts
- profile=deterministic verdict=PASS matrix_slo=PASS soak_slo=PASS max_kv_put=30584 max_ts_insert=29486 max_rel_insert=30361 max_rel_delete=28352 max_txn_commit=29082 max_compact=24772 max_reopen=67121 spikes_gt_5ms=198 fail_count=0
- profile=balanced verdict=PASS matrix_slo=PASS soak_slo=PASS max_kv_put=37288 max_ts_insert=37943 max_rel_insert=44301 max_rel_delete=35775 max_txn_commit=40835 max_compact=34467 max_reopen=76041 spikes_gt_5ms=504 fail_count=0
- profile=stress verdict=FAIL matrix_slo=FAIL soak_slo=PASS max_kv_put=48476 max_ts_insert=47890 max_rel_insert=46580 max_rel_delete=47324 max_txn_commit=182180 max_compact=42000 max_reopen=93496 spikes_gt_5ms=3507 fail_count=0

## ESP32 Runs
- profile=deterministic verdict=FAIL reason=Exception calling "Open" with "0" argument(s): "The port 'COM17' does not exist." log=C:\Users\vande\Desktop\github\loxdb\docs\results\esp32_deterministic_20260419_173440.log
- profile=balanced verdict=FAIL reason=Exception calling "Open" with "0" argument(s): "The port 'COM17' does not exist." log=C:\Users\vande\Desktop\github\loxdb\docs\results\esp32_balanced_20260419_173440.log
- profile=stress verdict=FAIL reason=Exception calling "Open" with "0" argument(s): "The port 'COM17' does not exist." log=C:\Users\vande\Desktop\github\loxdb\docs\results\esp32_stress_20260419_173440.log

