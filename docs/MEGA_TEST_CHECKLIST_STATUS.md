# Mega Test Checklist Status

Source checklist:

- `bench/lox_esp32_s3_bench/MEGA_BENCH_TEST_PLAN.md`

This file is a reality check against tests currently present in `tests/` and CTest wiring in `CMakeLists.txt`.

## Current status summary

- Core API domains (KV/TS/REL/WAL/txn/migration/stats) have dedicated tests.
- Recovery and resilience paths have dedicated tests.
- Optional backend adapter matrix paths are covered by dedicated tests.
- Stress lanes exist for managed and fs/block matrix paths.

## Areas with explicit test files

- Boot/open/reopen, integration and durability:
  - `test_integration.c`, `test_wal.c`, `test_compact.c`, `test_durability_closure.c`, `test_resilience.c`
- KV:
  - `test_kv.c`, `test_kv_reject.c`
- TS:
  - `test_ts.c`, `test_ts_reject.c`, `test_ts_downsample.c`
- REL:
  - `test_rel.c`, `test_rel_corruption_replay.c`
- Transactions:
  - `test_txn.c`
- Contracts/validation:
  - `test_api_contract_matrix.c`, `test_fail_code_contract.c`, `test_error_strings.c`, `test_offline_verifier.c`
- Capacity/profile variants:
  - `test_limits.c`, `test_profile_matrix.c`, `test_storage_capacity_profiles.c`, `test_tiny_footprint.c`, `footprint_min_baseline.c`
- Thread safety/C++ wrapper:
  - `test_thread_safety.c`, `test_cpp_wrapper.cpp`
- Optional backend modules:
  - `test_backend_compat.c`, `test_backend_decision.c`, `test_backend_registry.c`, `test_backend_open.c`
  - `test_backend_aligned_adapter.c`, `test_backend_managed_adapter.c`, `test_backend_fs_adapter.c`
  - `test_backend_aligned_recovery.c`, `test_backend_managed_recovery.c`, `test_backend_fs_recovery.c`
  - `test_backend_managed_stress.c`, `test_backend_fs_matrix.c`

## Remaining practical gaps (still recommended)

- broader storage-driver fault injection matrices on open path
- wider corruption corpus breadth beyond current deterministic fixtures
- explicit CI latency guardrails for target hardware (outside generic host CI)

## Notes

- Historical numeric targets are intentionally omitted here.
- For exact active tests, use `ctest -N` in your configured build directory.
