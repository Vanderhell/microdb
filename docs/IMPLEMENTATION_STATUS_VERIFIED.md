# loxdb Implementation Status (Verified Snapshot)

Date: 2026-04-19

This file summarizes what is implemented in the current repository state.

## Implemented core scope

- KV engine (`src/lox_kv.c`)
- TS engine (`src/lox_ts.c`)
- REL engine (`src/lox_rel.c`)
- WAL/compact/recovery path (`src/lox_wal.c`, `src/loxdb.c`)
- public C API (`include/lox.h`)
- C++ wrapper (`include/lox_cpp.hpp`)

## Storage and ports

- RAM port (`port/ram/*`)
- POSIX port (`port/posix/*`)
- ESP32 port (`port/esp32/*`)

## Optional backend modules

Available modules under `src/backends/` and `include/lox_backend_*.h`:

- registry/compat/decision/open flow
- aligned adapter
- managed adapter
- fs adapter
- test-oriented backend stubs

## Current strict storage contract

- `erase_size > 0`
- `write_size == 1`

## Test coverage areas (by test files)

- Core API: `test_kv`, `test_ts`, `test_rel`, `test_txn`, `test_wal`, `test_compact`, `test_migration`
- Durability/recovery: `test_durability_closure`, `test_resilience`, `test_fail_code_contract`, `test_offline_verifier`
- Contracts and misuse: `test_api_contract_matrix`, `test_error_strings`
- Optional backend paths: `test_backend_*`
- Capacity/profile variants: `test_profile_matrix`, `test_limits`, `test_storage_capacity_profiles`, `test_tiny_footprint`
- REL corruption fixtures: `test_rel_corruption_replay`
- Thread safety/C++: `test_thread_safety`, `test_cpp_wrapper`

## Tooling/scripts present

- validation runner: `tools/run_full_validation.ps1`
- offline verifier: `tools/lox_verify.c`
- baseline threshold tooling under `scripts/` for managed and fs matrix lanes

## Notes

- `docs/results/` contains run artifacts and historical summaries.
- Artifacts are evidence snapshots, not the API contract source of truth.
