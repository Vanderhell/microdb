# Release Log

This file tracks release-level outcomes and notable delivery notes.
For detailed code-level change history, see [CHANGELOG.md](CHANGELOG.md).

## Unreleased

### Highlights

- Added free-tier runtime integrity API `lox_selfcheck()` with anomaly counters and first-anomaly string.
- Added WCET deliverables for integrators:
  - `include/lox_wcet.h` bounds,
  - `docs/WCET_ANALYSIS.md` formulas and measurement guide,
  - `tests/test_wcet_bounds.c` coverage.
- Added TS logarithmic retention feature set:
  - `LOX_TS_POLICY_LOG_RETAIN`,
  - `lox_ts_register_ex(...)`,
  - per-stream zone configuration and validation tests.

### Validation

- New suites are green in CI preset validation:
  - `test_selfcheck`
  - `test_wcet_bounds`
  - `test_ts_log_retain`
- Full Windows preset regression remains passing after integration.

## v1.3.5 - 2026-04-22

### Highlights

- REL mutation semantics are now consistent and explicit:
  - `lox_rel_find` and `lox_rel_iter` both return `LOX_ERR_MODIFIED` on concurrent mutation after callback re-lock.
  - regression coverage added in `tests/test_rel.c`.
- REL schema sealing now fails fast for oversized rows (`LOX_ERR_OVERFLOW`) instead of deferring failure to read paths.
- Release/CI robustness improved for current platform matrix:
  - release publish depends on sanitizer gate and build completion.
  - macOS artifact lane labeling aligned (`macos-arm64`).
  - macOS linker/map and footprint gate compatibility fixed (`-map` + `size -m` fallback parsing).

### Test/Validation Stability

- TS tests made capacity-safe for large profile capacities:
  - fixed callback buffer overflow in `test_ts`.
  - made `test_ts_reject` / `test_ts_downsample` setup and assertions resilient across sanitizer/release capacities.

### Documentation

- Added KV iteration weak-consistency contract in `include/lox.h`.
- Added known TS lifecycle limitation note (`no lox_ts_unregister`) and thread-safety notes for `lox_rel_find_by` lock-held copy behavior.

## v1.3.0 - 2026-04-21

### Added

- Real-data integration suite on host (`tests/test_realdata_integration.c`) covering KV/TS/REL, JSON wrapper, import/export, TXN, and recovery.
- ESP32-S3 `run_real` command in bench harness for on-device end-to-end validation.
- Extended C++ wrapper coverage in `tests/test_cpp_wrapper.cpp`.

### Fixed

- KV JSON import/export TTL sentinel bug:
  - non-expiring keys now export/import as `ttl=0` (no immediate post-import expiry).
- KV admission compact-budget computation is now O(1):
  - `lox_admit_kv_set` uses `core->kv.live_value_bytes` instead of O(n) bucket scan.
  - verified on ESP32-S3 hardware (`admit_kv_set` observed from ~498us to ~190us in `run_real` flow).

### Performance / Validation

- ESP32-S3 real-data flow validated with `[REAL_DATA] PASS` on COM17.
- Lifecycle cost measured explicitly in real flow:
  - `flush` ~7033us
  - `deinit` ~7198us
  - `reinit` ~9716us

### Repository Hygiene

- Removed tracked ESP32 build artifacts under `bench/loxdb_esp32_s3_bench_head/build_sync_flush_only/` and added ignore rule.

## v1.2.0 - 2026-04-20

### Added

- Optional wrappers and backend adapter modules documented in `README.md`.
- GitHub wiki synchronized in English and aligned with `docs/`.
- Automated GitHub release publishing on tag push (`v*`) via `.github/workflows/release.yml`.

### Fixed

- Multiple sanitizer and leak issues across WAL, TS, REL alignment and test reopen paths.
- CI stability improvements for ASAN/UBSAN validation lane.

### Process

- Strengthened repository hygiene with contribution/security/community templates.

## Release Process

1. Ensure `CHANGELOG.md` has finalized entries for the target version.
2. Tag the release with `vX.Y.Z`.
3. Push the tag (`git push origin vX.Y.Z`).
4. GitHub Actions `release.yml` builds artifacts and publishes GitHub Release.
5. Append final entry to this file with date and highlights.
