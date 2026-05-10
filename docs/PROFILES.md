# Profiles (compile-time) and footprint-min notes

This document summarizes the supported compile-time “core profiles” and the smallest **durable** configuration (`LOX_PROFILE_FOOTPRINT_MIN`), without mixing in benchmark numbers.

Source of truth remains:
- public API and limits: `include/lox.h`
- build variants and test targets: `CMakeLists.txt`
- behavioral evidence: `tests/`

## Supported core profiles

Enable **at most one** of:

- `LOX_PROFILE_CORE_MIN`
- `LOX_PROFILE_CORE_WAL`
- `LOX_PROFILE_CORE_PERF`
- `LOX_PROFILE_CORE_HIMEM`
- `LOX_PROFILE_FOOTPRINT_MIN`

If none is set, `LOX_PROFILE_CORE_WAL` is selected by default.

## Engine availability (build-time)

- KV: available when `LOX_ENABLE_KV=1`
- TS: available when `LOX_ENABLE_TS=1`
- REL: available when `LOX_ENABLE_REL=1`
- WAL/recovery path: available when `LOX_ENABLE_WAL=1` and a storage backend is provided

## Durable storage contract (current releases)

Validated at `lox_init()` / open path:

- `erase_size > 0`
- `write_size == 1`

If violated, initialization fails with `LOX_ERR_INVALID`.

## `LOX_PROFILE_FOOTPRINT_MIN` (smallest durable profile)

Intended behavior:

- KV enabled
- TS disabled
- REL disabled
- WAL enabled (power-fail/recovery path remains active)

Important separation:

- `FOOTPRINT_MIN` is the smallest supported **durable** profile.
- `lox_tiny` is a separate “smallest size” variant (KV-only, WAL-off) and has weaker power-loss durability semantics than WAL-enabled profiles.

## Footprint-min baseline test intent

The canonical footprint sanity is `test_footprint_min_baseline` and focuses on:

1. `init/open` (persistent POSIX storage)
2. a minimal KV set/get
3. `close/deinit`
4. `reopen`
5. KV get (persistence/recovery)

No benchmark workload and no extra features.

## Size gates and linkage audit (CI)

The footprint-min size-gate tests are intended to fail CI if the minimal durable profile exceeds section budgets (Release) or links forbidden objects.

See:
- `CMakeLists.txt` (size-gate test definitions)
- `tests/` (baseline + gate helpers)

