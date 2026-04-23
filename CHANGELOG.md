# Changelog

All notable changes to this project are documented in this file.

The format is inspired by Keep a Changelog and follows semantic versioning intent where possible.

## [Unreleased]

### Added

- Free-tier core additions (MIT):
  - `lox_selfcheck()` API + runtime structural checks for KV/TS/REL/WAL.
  - WCET package:
    - `include/lox_wcet.h` compile-time bound macros,
    - `docs/WCET_ANALYSIS.md` methodology and per-API formulas,
    - `tests/test_wcet_bounds.c` verification coverage.
  - TS logarithmic retention support:
    - new policy constant `LOX_TS_POLICY_LOG_RETAIN`,
    - extended registration API `lox_ts_register_ex(...)`,
    - stream-level log-retain config (`zones`/`zone_pct`),
    - dedicated tests in `tests/test_ts_log_retain.c`.
  - New self-check coverage in `tests/test_selfcheck.c`.

### Changed

- Build/test wiring:
  - registered `test_selfcheck`, `test_wcet_bounds`, and `test_ts_log_retain` in CMake.
  - added dedicated `lox_ts_log_retain` test library target so default TS policy behavior remains unchanged.

## [1.3.6] - 2026-04-22

### Added

- Offline verifier quality package:
  - deep decode passes for KV/TS/REL pages with warning surfacing.
  - WAL semantic summary counters (including orphaned TXN markers).
  - strict `--check` mode for CI gating.
- New coverage:
  - `tests/test_offline_verifier.c` extended with corruption/recovery/JSON/check-flag scenarios.
  - `tests/test_capacity_estimator_model.c` for capacity-model consistency checks.
  - `tests/test_safety_invariants.c` for safety-critical invariants (magic clear, WAL replay boundary, superblock switch, null-handle contract).
- Certification readiness artifacts:
  - `docs/SAFETY_READINESS.md`
  - `scripts/run_static_analysis.sh`

### Changed

- `tools/lox_capacity_estimator.html` rewritten as single-file real-time planner:
  - preset-driven inputs (FOOTPRINT_MIN, CORE_MIN, CORE_WAL, CORE_PERF, CORE_HIMEM, Custom),
  - RAM/storage/wear outputs,
  - CMake define snippet generation,
  - formula comments tied to source files.
- `docs/PROGRAMMER_MANUAL.md` expanded with Capacity Planning section.
- CI workflow enriched with:
  - verifier integration smoke step (Linux build lane),
  - non-blocking static-analysis job with artifact upload.

### Fixed

- Windows subprocess quoting and WAL entry construction in `tests/test_offline_verifier.c`:
  - stable verifier invocation on Windows (`cmd /c` quoting),
  - orphaned TXN WAL test now writes valid `data_len` in entry header.

## [1.3.5] - 2026-04-22

### Changed

- Release workflow/platform hardening:
  - publish job now explicitly depends on sanitizer gate and build jobs.
  - macOS release artifact label aligned to `macos-arm64`.
- Header-level KV iteration contract clarified as weakly-consistent under concurrent mutation.
- Programmer and thread-safety docs updated with explicit REL mutation semantics and lock/copy behavior notes.

### Fixed

- REL consistency and error semantics:
  - `lox_rel_find` now returns `LOX_ERR_MODIFIED` (not `LOX_ERR_INVALID`) when table mutation is detected after callback re-lock.
  - `lox_rel_iter` now captures `mutation_seq` snapshot and returns `LOX_ERR_MODIFIED` when concurrent mutation is detected.
  - Added regression coverage in `tests/test_rel.c` for both `rel_find` and `rel_iter` concurrent mutation detection.
- REL schema guard:
  - `lox_schema_seal` now rejects rows larger than `LOX_REL_ROW_SCRATCH_MAX` with `LOX_ERR_OVERFLOW`.
  - Added regression test `rel_schema_seal_rejects_oversized_row`.
- TS test-suite stabilization for sanitizer/release profiles:
  - fixed `test_ts` collector overflow for capacities above 256 samples.
  - made `test_ts_reject` and `test_ts_downsample` capacity-aware and RAM-only in setup to avoid storage-path false failures.
  - updated downsample tests to avoid hardcoded query bounds and fixed-size output buffers.
- macOS release build compatibility:
  - fixed footprint baseline linker map flag on Apple toolchain (`-Wl,-map,...` instead of GNU `-Map` form).
  - `tests/check_footprint_min_size.cmake` now falls back to parsing `size -m` output when `size -A` is unavailable on macOS.

## [1.3.0] - 2026-04-21

### Added

- Real-data integration coverage on host (`tests/test_realdata_integration.c`) including KV/TS/REL flows, JSON wrapper, import/export, TXN, and reopen recovery assertions.
- ESP32-S3 `run_real` smoke command in `bench/lox_esp32_s3_bench` for on-device validation of realistic end-to-end data paths.
- C++ wrapper coverage extension in `tests/test_cpp_wrapper.cpp` for practical KV/TS/REL wrapper usage.

### Changed

- ESP32 benchmark documentation expanded with:
  - WAL sync mode decision table (`SYNC_ALWAYS` vs `SYNC_FLUSH_ONLY`) with measured ESP32 values.
  - POSIX-vs-ESP32 interpretation guidance and follow-up notes for benchmark fidelity.
- Bench sources synchronized for ESP32 harness (`loxdb`, JSON, and import/export modules) to keep local bench build behavior aligned with core sources.
- Release hardening:
  - added macOS CI/release presets (`ci-debug-macos`, `release-macos`) and workflow matrix coverage.
  - release workflow now includes mandatory Linux ASan/UBSan sanitizer gate before packaging.
  - release page body now reads from `CHANGELOG.md` (`body_path`) instead of auto-generated commit-only notes.

### Fixed

- KV JSON import/export TTL sentinel handling:
  - non-expiring keys exported via KV iter sentinel (`UINT32_MAX`) are now normalized to `ttl=0` before JSON encoding.
  - prevents immediate expiry after import for persistent keys.
- KV admission performance:
  - `lox_admit_kv_set` now uses O(1) `live_value_bytes` accounting instead of O(n) bucket scan for compact-availability calculation.
  - verified on ESP32-S3 hardware with reduced `admit_kv_set` latency in `run_real`.
- REL/TS mutation/admission semantics hardening:
  - dedicated `LOX_ERR_MODIFIED` behavior for TS mutation detection in query flows.
  - deterministic budget signaling consistency in REL admission.

### Repository Hygiene

- Removed tracked ESP32 build artifacts under `bench/lox_esp32_s3_bench/build_sync_flush_only/` and added ignore rule to keep binary outputs out of git history.

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
