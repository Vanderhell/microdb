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
   - `microdb_json_wrapper` module (separate target, separate header)
   - `microdb_import_export` module (separate target, separate header)
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
   - end-to-end backend-open flow (`descriptor -> decision -> adapter -> microdb_init`)
   - practical integration recipes for raw byte-write, aligned-write, and managed media
   - explicit "what stubs are / are not" section (capability descriptors, not drivers)
2. [done] Improve documentation discoverability from top-level entry points:
   - add "Backend Integration" and "Migration" quick links in `README.md`
   - add docs map/navigation page linking backend contracts and stress/baseline docs
   - make `port/esp32/microdb_port_esp32.c` explicitly discoverable as a reference implementation
   - mirror the same structure in GitHub Wiki sidebar/home
3. [done] Add RTOS reference port skeletons:
   - `examples/freertos_port/` minimal `microdb_storage_t` adapter scaffolding
   - `examples/zephyr_port/` minimal `microdb_storage_t` adapter scaffolding
   - document required sync/flush semantics and lock hooks (`cfg.lock_create/lock/unlock/lock_destroy`)
   - include guidance from `src/microdb_lock.h` for single-thread vs thread-safe builds
4. [ ] Add reference "real driver glue" example for non-byte-write media:
   - aligned/block path example showing bounce-buffer + sync lifecycle
   - clearly separate demo glue code from production requirements
5. [ ] Add `docs/SCHEMA_MIGRATION_GUIDE.md`:
   - relational `schema_version` and `on_migrate` workflow
   - migration patterns (add/remove/reshape columns) and compatibility constraints
   - tested migration example with persisted data across reopen/recovery
6. [done] Add `docs/PORT_AUTHORING_GUIDE.md` with annotated walkthrough:
   - use `port/esp32/microdb_port_esp32.c` as live reference port
   - explain field-by-field `microdb_storage_t` mapping to real driver hooks
   - call out `write_size = 1` core contract and when to use aligned adapter path
   - describe async erase/sync caveats and expected durability semantics
