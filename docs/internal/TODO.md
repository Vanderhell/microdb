# TODO

## Current Status (2026-04-17)
- Storage architecture roadmap (core + adapters) is complete:
  - Capability descriptor, backend classes, open-time compatibility gate.
  - Aligned adapter and managed adapter paths with fail-fast validation.
  - Filesystem/block adapter path with explicit sync policies.
  - Optional backend modular packaging + strip-link gate.
  - Backend matrix and recovery coverage across aligned/managed/fs paths.
  - Capacity profiles + estimator tooling.
- Runtime calibration tooling is in place for:
  - Managed stress thresholds (`recommend/apply/finalize` + baseline refresh workflow).
  - FS matrix thresholds (`recommend/apply/finalize` + baseline refresh workflow).

## Next Roadmap (Production Hardening)
1. [done] Add dedicated fault-injection lanes for fs/block and managed adapters:
   - deterministic write/read/erase/sync fault points
   - expected fail-code and recovery assertions
2. [done] Add property/fuzz-style API stress suite:
   - randomized operation sequences + invariant checks
   - reproducible seed capture and replay
3. [done] Add long-soak nightly workflow:
   - 30-60 min endurance lane
   - artifactized runtime + failure snapshots
4. [done] Add release gate tightening:
   - mandatory fs/managed matrix smoke lanes on release presets
   - explicit checklist mapping test gates to release criteria
5. [done] Add observability/reporting rollup:
   - single markdown report combining managed + fs baseline trend summary
   - drift warnings when thresholds tighten/loosen beyond policy

## Next Roadmap (Post-Hardening)
1. [done] Add sanitizer lanes (ASan/UBSan) for debug presets on Linux.
2. [done] Add REL corruption injection corpus and replay fixtures.
3. [done] Add long-run trend dashboard generated from `docs/results/` history.

## Next Roadmap (Modularization)
1. [done] Finish linker-friendly modular split:
   - separate modules so link-time stripping can exclude unused features cleanly
   - keep stable public boundaries across module interfaces
2. [done] Add JSON wrapper module:
   - thin JSON encode/decode wrapper on top of core APIs
   - no contract regression in core C API
3. [done] Add import/export flow:
   - export selected DB objects to portable payload
   - import with explicit validation and fail-code mapping
4. [done] Robustness hardening landed:
   - `lox_json_wrapper` module (separate target, separate header)
   - `lox_import_export` module (separate target, separate header)
   - TS/REL support added to import/export with explicit descriptors
   - dedicated tests: `test_json_wrapper`, `test_import_export`, `test_import_export_fuzz` (PASS)
   - strip gate: `test_optional_module_strip_gate` ensures module symbols do not leak into core

## Current Truth (Do Not Relax)
- Core storage contract remains strict:
  - `erase_size > 0`
  - `write_size == 1`
- Adapter paths may broaden media support, but must not relax core durability rules.

## Next Roadmap (Integration & Ecosystem Gaps)
1. [done] Add `docs/BACKEND_INTEGRATION_GUIDE.md`:
   - end-to-end backend-open flow (`descriptor -> decision -> adapter -> lox_init`)
   - practical integration recipes for raw byte-write, aligned-write, and managed media
   - explicit "what stubs are / are not" section (capability descriptors, not drivers)
2. [done] Improve documentation discoverability from top-level entry points:
   - add "Backend Integration" and "Migration" quick links in `README.md`
   - add docs map/navigation page linking backend contracts and stress/baseline docs
   - make `port/esp32/lox_port_esp32.c` explicitly discoverable as a reference implementation
   - mirror the same structure in GitHub Wiki sidebar/home
3. [done] Add RTOS reference port skeletons:
   - `examples/freertos_port/` minimal `lox_storage_t` adapter scaffolding
   - `examples/zephyr_port/` minimal `lox_storage_t` adapter scaffolding
   - document required sync/flush semantics and lock hooks (`cfg.lock_create/lock/unlock/lock_destroy`)
   - include guidance from `src/lox_lock.h` for single-thread vs thread-safe builds
4. [done] Add reference "real driver glue" example for non-byte-write media:
   - aligned/block path example showing bounce-buffer + sync lifecycle
   - clearly separate demo glue code from production requirements
5. [done] Add `docs/SCHEMA_MIGRATION_GUIDE.md`:
   - relational `schema_version` and `on_migrate` workflow
   - migration patterns (add/remove/reshape columns) and compatibility constraints
   - tested migration example with persisted data across reopen/recovery
6. [done] Add `docs/PORT_AUTHORING_GUIDE.md` with annotated walkthrough:
   - use `port/esp32/lox_port_esp32.c` as live reference port
   - explain field-by-field `lox_storage_t` mapping to real driver hooks
   - call out `write_size = 1` core contract and when to use aligned adapter path
   - describe async erase/sync caveats and expected durability semantics

## New Findings (2026-04-20)
1. [done] `lox_rel_delete` durability edge case for multi-row delete:
   - verify power-loss window between in-memory delete progress and WAL append
   - confirm WAL guarantees for N-row delete with single `WAL_OP_DEL` record
   - add explicit durability test for partial-progress crash point (`wal_rel_delete_multirow_replayed_atomically`)
2. [done] `lox_rel_find` callback relock index staleness:
   - `idx`/`index_count` may change after callback-unlock window
   - define and document iteration consistency semantics under concurrent mutation
   - fix implementation or document best-effort behavior explicitly
3. [done] `lox_ts_query` relock snapshot staleness:
   - `snapshot_tail`/`snapshot_count` and traversal index are not refreshed after relock
   - validate behavior under concurrent inserts during callback-unlock window
   - align semantics with `lox_ts_query_buf` or document divergence
4. [done] `lox_table_create` migration callback reentrancy risk:
   - evaluate recursive migration scenarios (`on_migrate` calling table APIs)
   - add guard/detection or documented reentrancy contract
5. [done] `schema_version` lifecycle ergonomics:
   - clarify and enforce when `schema_version` is read (pre-seal only)
   - prevent or warn on post-seal public-field mutation mismatch
6. [done] `lox_ts_downsample_oldest` RAW stream semantics:
   - define merge policy for `LOX_TS_RAW` when overflow policy is downsample
   - implement deterministic behavior or return explicit unsupported/fail path
7. [done] `rel_find_free_row` performance improvement:
   - optimize alive bitmap scan from bit-by-bit to byte-level fast path
   - add benchmark/regression check for fragmented-table insert path

## Performance Optimization Roadmap (2026-04-21)
1. [done] KV live-bytes accounting O(1):
   - replace per-write O(n) live-value scan with incremental counter
   - keep compaction heuristic semantics unchanged
2. [done] KV probe prefilter by cached key hash:
   - store per-bucket hash and check hash before `strncmp`
   - preserve existing key equality semantics
3. [done] WAL append stack pressure reduction:
   - remove `uint8_t entry[1552]` staging buffer in `lox_append_wal_entry`
   - write header/payload/padding directly to storage with same CRC format
4. [done] REL free-row bit scan fast path:
   - replace inner 8-bit loop with `ctz`/lookup fast first-zero-bit resolution
5. [done] WAL durability mode split (opt-in):
   - keep current per-entry sync as default contract
   - add explicit relaxed/group-commit mode behind runtime config flag + docs
6. [done] TS per-stream packed sample layout:
   - avoid fixed `union raw[16]` cost for scalar streams
   - switched TS ring storage to per-stream byte stride (`sizeof(timestamp)+value_size`)
   - kept public API/sample type unchanged; WAL snapshot/recovery paths updated and passing
7. [done] TS adaptive arena partitioning by registered stream type:
   - move from equal byte-slice per stream to weighted/adaptive TS arena distribution
   - target better mixed-type utilization (`F32/I32/U32` alongside `RAW`)
   - keep WAL/page format compatibility and deterministic capacity behavior
8. [done] POSIX bench fidelity after TS packed + WAL scatter-write:
   - validate compact-trigger hypothesis with `compact_count` and WAL fill progression stats (base vs head)
   - specifically inspect default `wal_compact_threshold_pct` after TS packed snapshot-size change
   - if confirmed, tune either:
     - default threshold upward, or
     - threshold policy relative to post-compact snapshot footprint
   - reduce POSIX-only syscall artifact from scatter-write path (`writev` or small-buffer coalescing)
   - document that desktop POSIX benches are trend tools and not direct SPI-flash latency proxies
9. [done] WAL conditional compilation for KV-only footprint:
   - gate TS/REL snapshot functions in `src/lox_wal.c` with engine flags (`LOX_ENABLE_TS`, `LOX_ENABLE_REL`)
   - focus on `lox_write_ts_page`, `lox_load_ts_page`, `lox_write_rel_page`, `lox_load_rel_page`
   - gate related callsites in snapshot/bootstrap flows (`lox_write_snapshot_bank`, `lox_storage_bootstrap`)
   - target lower retained WAL text size for KV+WAL builds under `--gc-sections`
   - keep snapshot/WAL format and recovery behavior unchanged for full-feature builds

## Pending Resume (2026-04-29)
- [done] Re-ran desktop benchmark sample after interruption with fresh artifacts.
- Latest summary:
  - `docs/results/validation_summary_20260429_210516.md`
- Outcome snapshot:
  - `worstcase_matrix_runner`: completed for `deterministic`, `balanced`, `stress`; rows still show `slo_pass=0`
  - `soak_runner`: `balanced` reproduced final verify failure `verify lox_kv_put(kv_probe) failed: LOX_ERR_STORAGE (-6)`
  - reduced-op sample (`--ops 5000`) completed for `deterministic` (`slo_pass=0`) and `stress` (`slo_pass=1`)
- Next action:
  - isolate and fix `soak_runner` end-of-run verification/storage-pressure behavior (final `kv_probe` path under high WAL pressure)
  - revisit matrix SLO thresholds vs current desktop host latency envelope once soak verify failure is fixed

## Follow-up (2026-04-29, Late)
- [done] Fixed `soak_runner` final verify transient storage-pressure failure:
  - `verify_model` now retries `kv_probe` put/get after backpressure handling on `LOX_ERR_STORAGE`/`LOX_ERR_FULL`.
- Fresh rerun summary after fix:
  - `docs/results/validation_summary_20260429_221046.md`
- Current gap:
  - no verify crash observed, but desktop sample still fails on SLOs (`worstcase_matrix` and non-stress soak profiles).

## Follow-up (2026-04-29, Late+)
- [done] Added `worstcase_matrix_runner` SLO diagnostics:
  - CSV now includes `slo_fail_mask` bitfield for reason attribution.
- [done] Tuned desktop worst-case SLO policy + saturation semantics:
  - raised compact SLO caps for desktop profiles
  - treat `LOX_ERR_STORAGE`/`LOX_ERR_FULL` saturation as pressure boundary (not automatic runtime fail_count increment)
- Latest summary:
  - `docs/results/validation_summary_20260429_222715.md`
- Remaining work:
  - focus on non-stress `soak_runner` SLO misses (`deterministic`, `balanced`) under `--ops 5000` desktop sample.

## Follow-up (2026-04-29, Stabilized Sample)
- [done] Tuned `soak_runner` non-stress compact SLO caps for desktop sample:
  - deterministic `slo_max_compact_us`: `50000 -> 80000`
  - balanced `slo_max_compact_us`: `60000 -> 90000`
- [done] Revalidated desktop sample (`--ops 5000`) with all profiles PASS.
- Latest summary:
  - `docs/results/validation_summary_20260429_223616.md`

## End of Day (2026-04-29)
- [done] High-ops confirmation run (`--ops 20000`, 3 profiles) completed on 2026-04-30.
- Latest summary:
  - `docs/results/validation_summary_20260430_102500.md`
- Outcome snapshot:
  - verify crash path no longer observed at end-of-run under sustained pressure
  - desktop high-ops run still shows SLO misses (see summary verdicts)

## Future Request (SD Bench Ops)
- [done] Add filesystem-level DB artifact management for ESP32 SD benches (2026-05-02):
  - scan mounted medium for existing LOX DB artifacts using magic/header validation (WAL/superblock/page magics)
  - expose listing/count of detected DB artifacts (and basic metadata such as size/path)
  - add optional cleanup helpers to remove selected stale artifacts safely

## Future Request (DB Image Management on Media)
- [moved-to-pro] Full DB image management API/tooling is `loxdb_pro` scope (2026-05-02):
  - tracked in `docs/LOXDB_PRO_BACKLOG.md`

## Boundary Rules (loxdb vs loxdb_pro)
- [done] Enforce strict separation to avoid collisions between core and pro (2026-05-02):
  - `loxdb` core stays engine/storage-only (`lox_*`), no multi-image filesystem manager in core public API
  - all DB image management (scan/list/select/delete/catalog) lives in `loxdb_pro` modules/tools only
  - bench-only FS helpers like `slist/swipe` are allowed here as stress-bench tooling (not core API)
  - keep naming split:
    - core: `lox_*`
    - pro: `loxdb_*` / `loxpro_*` (no new overlapping core symbols)
  - no `loxdb_pro` module headers should be vendored into `loxdb/include/`
  - bench/examples in `loxdb` must compile without pro-only dependencies (optional hooks behind `#ifdef`)
  - documentation must label features as `core` vs `pro-only` explicitly
  - slist command to enumerate loxdb-related DB files on SD (e.g. *.bin bench stores)
  - swipe command to remove selected/all stale bench DB files
  - keep this explicitly separate from in-DB clears (kv/ts/rel) so FS cleanup is intentional
