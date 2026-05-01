# Full Validation Summary (20260412_172006)

## Desktop Verdicts
- profile=deterministic verdict=FAIL matrix_slo=FAIL soak_slo=FAIL max_kv_put=30966 max_ts_insert=32335 max_rel_insert=30638 max_rel_delete=29935 max_txn_commit=30326 max_compact=24595 max_reopen=63547 spikes_gt_5ms=746 fail_count=0
- profile=balanced verdict=FAIL matrix_slo=FAIL soak_slo=FAIL max_kv_put=45822 max_ts_insert=32423 max_rel_insert=41688 max_rel_delete=34595 max_txn_commit=35939 max_compact=28818 max_reopen=69616 spikes_gt_5ms=958 fail_count=0
- profile=stress verdict=FAIL matrix_slo=FAIL soak_slo=FAIL max_kv_put=39594 max_ts_insert=42846 max_rel_insert=39779 max_rel_delete=37551 max_txn_commit=41061 max_compact=36152 max_reopen=79067 spikes_gt_5ms=1149 fail_count=0

## ESP32 Runs
- profile=deterministic verdict=FAIL reason=A positional parameter cannot be found that accepts argument 'System.Object[]'. log=<repo>\\docs\results\esp32_deterministic_20260412_172006.log
- profile=balanced verdict=FAIL reason=A positional parameter cannot be found that accepts argument 'System.Object[]'. log=<repo>\\docs\results\esp32_balanced_20260412_172006.log
- profile=stress verdict=FAIL reason=A positional parameter cannot be found that accepts argument 'System.Object[]'. log=<repo>\\docs\results\esp32_stress_20260412_172006.log

