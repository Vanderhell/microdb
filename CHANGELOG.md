# Changelog

All notable changes to this project are documented in this file.

The format is inspired by Keep a Changelog and follows semantic versioning intent where possible.

## [Unreleased]

## [1.3.0] - 2026-04-21

### Added

- Real-data integration coverage on host (`tests/test_realdata_integration.c`) including KV/TS/REL flows, JSON wrapper, import/export, TXN, and reopen recovery assertions.
- ESP32-S3 `run_real` smoke command in `bench/microdb_esp32_s3_bench` for on-device validation of realistic end-to-end data paths.
- C++ wrapper coverage extension in `tests/test_cpp_wrapper.cpp` for practical KV/TS/REL wrapper usage.

### Changed

- ESP32 benchmark documentation expanded with:
  - WAL sync mode decision table (`SYNC_ALWAYS` vs `SYNC_FLUSH_ONLY`) with measured ESP32 values.
  - POSIX-vs-ESP32 interpretation guidance and follow-up notes for benchmark fidelity.
- Bench sources synchronized for ESP32 harness (`microdb`, JSON, and import/export modules) to keep local bench build behavior aligned with core sources.

### Fixed

- KV JSON import/export TTL sentinel handling:
  - non-expiring keys exported via KV iter sentinel (`UINT32_MAX`) are now normalized to `ttl=0` before JSON encoding.
  - prevents immediate expiry after import for persistent keys.
- KV admission performance:
  - `microdb_admit_kv_set` now uses O(1) `live_value_bytes` accounting instead of O(n) bucket scan for compact-availability calculation.
  - verified on ESP32-S3 hardware with reduced `admit_kv_set` latency in `run_real`.
- REL/TS mutation/admission semantics hardening:
  - dedicated `MICRODB_ERR_MODIFIED` behavior for TS mutation detection in query flows.
  - deterministic budget signaling consistency in REL admission.

### Repository Hygiene

- Removed tracked ESP32 build artifacts under `bench/microdb_esp32_s3_bench/build_sync_flush_only/` and added ignore rule to keep binary outputs out of git history.

## [1.2.0] - 2026-04-20

### Added

- Optional wrapper and backend adapter documentation in README.
- Automated GitHub Release workflow on tag push (`v*`).

### Fixed

- UBSAN misalignment issues in TS/REL and WAL-related paths.
- ASAN leak paths in multiple recovery/reopen test flows.

## [1.1.0] - 2026-04-12

### Added

- Read-only diagnostics and admission APIs.
- Managed stress baseline tooling and threshold recommendation scripts.

### Changed

- Preset-driven CI/release testing strategy (`ci-debug-*`, `release-*`).

## [1.0.0] - 2026-04-01

### Added

- Initial public release with KV, TS, and REL engines.
- WAL durability core and recovery flow.
- C and C++ wrapper surfaces.
