![loxdb](docs/banner.svg)

# loxdb

> Embedded database for microcontrollers.
> Three engines. One malloc. Zero dependencies.
> Deterministic durable storage core for MCU/embedded systems.

[![CI](https://github.com/Vanderhell/loxdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Vanderhell/loxdb/actions/workflows/ci.yml)
[![Language: C99](https://img.shields.io/badge/language-C99-blue)](https://en.wikipedia.org/wiki/C99)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Platform: MCU | Linux | Windows | macOS](https://img.shields.io/badge/platform-MCU%20%7C%20Linux%20%7C%20Windows%20%7C%20macOS-informational)](https://github.com/Vanderhell/loxdb)
[![Tests](https://img.shields.io/badge/tests-ctest-brightgreen)](https://github.com/Vanderhell/loxdb/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Vanderhell/loxdb)](https://github.com/Vanderhell/loxdb/releases)
[![Wiki](https://img.shields.io/badge/docs-wiki-blue)](https://github.com/Vanderhell/loxdb/wiki)
[![Contributing](https://img.shields.io/badge/contributions-welcome-success)](CONTRIBUTING.md)
[![Security](https://img.shields.io/badge/security-policy-important)](SECURITY.md)

## What is loxdb?

loxdb is a compact embedded database written in C99 for firmware and small edge runtimes.
It combines three storage models behind one API surface:

- KV for configuration, caches, and TTL-backed state
- Time-series for sensor samples and rolling telemetry
- Relational for small indexed tables

The library allocates exactly once in `lox_init()`, runs without external dependencies,
and can operate either in RAM-only mode or with a storage HAL for persistence and WAL recovery.

## Recent additions (Unreleased)

- Runtime integrity API: `lox_selfcheck(...)`
- WCET package:
  - compile-time bounds: `include/lox_wcet.h`
  - analysis guide: `docs/WCET_ANALYSIS.md`
- TS logarithmic retention:
  - policy: `LOX_TS_POLICY_LOG_RETAIN`
  - extended registration: `lox_ts_register_ex(...)`

## Product Contract

- Positioning: see [PRODUCT_POSITIONING.md](docs/PRODUCT_POSITIONING.md)
- Product brief (1 page): see [PRODUCT_BRIEF.md](docs/PRODUCT_BRIEF.md)
- Profile guarantees and limits: see [PROFILE_GUARANTEES.md](docs/PROFILE_GUARANTEES.md)
- Fail-code contract: see [FAIL_CODE_CONTRACT.md](docs/FAIL_CODE_CONTRACT.md)
- Runtime error text helper: `lox_err_to_string(lox_err_t)`
- Offline verifier contract: see [OFFLINE_VERIFIER.md](docs/OFFLINE_VERIFIER.md)
- WCET analysis: see [WCET_ANALYSIS.md](docs/WCET_ANALYSIS.md)
- Safety readiness package: see [SAFETY_READINESS.md](docs/SAFETY_READINESS.md)
- Professional readiness checklist: see [PROFESSIONAL_READINESS.md](docs/PROFESSIONAL_READINESS.md)
- Footprint-min contract: see [FOOTPRINT_MIN_CONTRACT.md](docs/FOOTPRINT_MIN_CONTRACT.md)
- Latest hard verdict (currently 2026-04-19): see [hard_verdict_20260419.md](docs/results/hard_verdict_20260419.md)
- Full validation artifacts and trend dashboard: see [docs/results/](docs/results/) and [trend_dashboard.md](docs/results/trend_dashboard.md)
- Getting started (5 min): see [GETTING_STARTED_5_MIN.md](docs/GETTING_STARTED_5_MIN.md)
- Developer quickstart (10 min): see [GETTING_STARTED_DEV_10_MIN.md](docs/GETTING_STARTED_DEV_10_MIN.md)
- Limits and failures contract: see [LIMITS_AND_FAILURES.md](docs/LIMITS_AND_FAILURES.md)
- Startup decision flow: see [STARTUP_DECISION_FLOW.md](docs/STARTUP_DECISION_FLOW.md)
- Troubleshooting guide: see [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
- Golden hardware profiles: see [GOLDEN_PROFILES.md](docs/GOLDEN_PROFILES.md)
- Programmer manual: see [PROGRAMMER_MANUAL.md](docs/PROGRAMMER_MANUAL.md)
- Backend integration guide: see [BACKEND_INTEGRATION_GUIDE.md](docs/BACKEND_INTEGRATION_GUIDE.md)
- Port authoring guide (ESP32 reference): see [PORT_AUTHORING_GUIDE.md](docs/PORT_AUTHORING_GUIDE.md)
- Schema migration guide: see [SCHEMA_MIGRATION_GUIDE.md](docs/SCHEMA_MIGRATION_GUIDE.md)
- Full docs map: see [DOCS_MAP.md](docs/DOCS_MAP.md)
- Core/PRO docs sync plan: see [DOCS_SYNC_PLAN.md](docs/DOCS_SYNC_PLAN.md)
- Change cycle checklist: see [CHANGE_CYCLE_CHECKLIST.md](docs/CHANGE_CYCLE_CHECKLIST.md)
- Release checklist: see [RELEASE_CHECKLIST.md](docs/RELEASE_CHECKLIST.md)
- Release tag template: see [RELEASE_TAG_TEMPLATE.md](docs/RELEASE_TAG_TEMPLATE.md)

## Project Governance

- Contribution guide: [CONTRIBUTING.md](CONTRIBUTING.md)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Release log: [RELEASE_LOG.md](RELEASE_LOG.md)
- Security policy: [SECURITY.md](SECURITY.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- Support policy: [SUPPORT.md](SUPPORT.md)

## Why not SQLite?

SQLite is excellent, but it targets a different operating point.
loxdb is intentionally narrower:

- one malloc at init, no allocator churn during normal operation
- fixed RAM budgeting across engines
- tiny integration surface for MCUs and RTOS firmware
- simpler persistence model for flash partitions and file-backed simulation

If you need SQL, dynamic schemas, concurrent access, or large secondary indexes, use SQLite.
If you need predictable memory and embedded-first behavior, loxdb is the better fit.

## Quick start

**1. Add to your project:**
```cmake
add_subdirectory(loxdb)
target_link_libraries(your_app PRIVATE loxdb)
```

**2. Configure and initialize:**
```c
#define LOX_RAM_KB 32
#include "lox.h"

static lox_t db;

lox_cfg_t cfg = {
    .storage = NULL,   // RAM-only; provide HAL for persistence
    .now     = NULL,   // provide timestamp fn for TTL support
};
lox_init(&db, &cfg);
```

## C++ wrapper (incremental)

Header:
- `include/lox_cpp.hpp`

Current wrapper surface:
- lifecycle: `init/deinit/flush`
- startup gating: `preflight`
- diagnostics: `stats`, `db_stats`, `kv_stats`, `ts_stats`, `rel_stats`, `effective_capacity`, `pressure`
- KV: `kv_set/kv_put/kv_get/kv_del/kv_exists/kv_iter/kv_clear/kv_purge_expired`, `admit_kv_set`
- TS: `ts_register/ts_insert/ts_last/ts_query/ts_query_buf/ts_count/ts_clear`, `admit_ts_insert`
- REL: schema/table helpers + `rel_insert/find/find_by/delete/iter/count/clear`, `admit_rel_insert`
- txn: `txn_begin/txn_commit/txn_rollback`

Minimal example:
```cpp
#include "lox_cpp.hpp"

loxdb::cpp::Database db;
lox_cfg_t cfg{};
cfg.ram_kb = 32u;
if (db.init(cfg) != LOX_OK) { /* handle error */ }

uint8_t v = 7u, out = 0u;
db.kv_put("k", &v, 1u);
db.kv_get("k", &out, 1u);

db.txn_begin();
db.kv_put("k2", &v, 1u);
db.txn_commit();

db.deinit();
```

Preflight before init:
```cpp
#include "lox_cpp.hpp"

lox_cfg_t cfg{};
cfg.ram_kb = 64u;
lox_preflight_report_t rep{};
if (loxdb::cpp::preflight(cfg, &rep) != LOX_OK) {
    // use rep.status + sizing fields to pick fallback profile
}
```

## Optional wrappers and adapter modules

Core `loxdb` is intentionally lean. Extra wrappers/adapters are separate modules and can be toggled in CMake.

Build toggles:
- `LOX_BUILD_JSON_WRAPPER` (default `ON`)
- `LOX_BUILD_IMPORT_EXPORT` (default `ON`)
- `LOX_BUILD_OPTIONAL_BACKENDS` (default `ON`)
- `LOX_BUILD_BACKEND_ALIGNED_STUB` / `LOX_BUILD_BACKEND_NAND_STUB` / `LOX_BUILD_BACKEND_EMMC_STUB` / `LOX_BUILD_BACKEND_SD_STUB` / `LOX_BUILD_BACKEND_FS_STUB` / `LOX_BUILD_BACKEND_BLOCK_STUB`

Wrapper targets:
- `lox_json_wrapper`
- `lox_import_export` (links to `lox_json_wrapper` when available)
- `lox_backend_registry`
- `lox_backend_compat`
- `lox_backend_decision`
- `lox_backend_aligned_adapter`
- `lox_backend_managed_adapter`
- `lox_backend_fs_adapter`
- `lox_backend_open`

Core contract:
- optional modules are not linked into `loxdb` core by default.
- `loxdb` must remain independent from optional wrapper/backend targets.

**3. Use all three engines:**
```c
// Key-value
float temp = 23.5f;
lox_kv_put(&db, "temperature", &temp, sizeof(temp));

// Time-series
lox_ts_register(&db, "sensor", LOX_TS_F32, 0);
lox_ts_insert(&db, "sensor", time_now(), &temp);

// Relational
lox_schema_t schema;
lox_schema_init(&schema, "devices", 32);
lox_schema_add(&schema, "id",   LOX_COL_U16, 2, true);
lox_schema_add(&schema, "name", LOX_COL_STR, 16, false);
lox_schema_seal(&schema);
lox_table_create(&db, &schema);
```

## Configuration

Configuration is compile-time first, with a small runtime override surface in `lox_cfg_t`.

- `LOX_RAM_KB` sets the total heap budget
- `LOX_RAM_KV_PCT`, `LOX_RAM_TS_PCT`, `LOX_RAM_REL_PCT` set default engine slices
- `cfg.ram_kb` overrides the total budget per instance
- `cfg.kv_pct`, `cfg.ts_pct`, `cfg.rel_pct` override the slice split per instance
- `LOX_ENABLE_WAL` toggles WAL persistence when a storage HAL is present
- `cfg.wal_sync_mode` selects WAL durability/latency mode:
  - `LOX_WAL_SYNC_ALWAYS` (default): sync on each append, strongest per-op durability
  - `LOX_WAL_SYNC_FLUSH_ONLY`: sync on explicit `lox_flush()`, lower write latency
  - see measured ESP32 tradeoffs in `bench/loxdb_esp32_s3_bench_head/README.md` ("WAL Sync Mode Decision Table")
- `LOX_LOG(level, fmt, ...)` enables internal diagnostic logging
- smallest-size variant is available as CMake target `lox_tiny` (KV-only, TS/REL/WAL disabled, weaker power-fail durability)
- strict smallest **durable** profile is available as `LOX_PROFILE_FOOTPRINT_MIN` (KV + WAL + reopen/recovery contract)

Storage budget (separate from RAM budget):
- storage capacity comes from `lox_storage_t.capacity` (bytes)
- geometry comes from `lox_storage_t.erase_size` and `lox_storage_t.write_size`
- current fail-fast storage contract requires `erase_size > 0` and `write_size == 1`
- use `tools/lox_capacity_estimator.html` for storage/layout planning (`2/4/8/16/32 MiB` profiles)

## KV engine

The KV engine stores short keys with binary values and optional TTL.

- fixed-size hash table with overwrite or reject overflow policy
- LRU eviction for `LOX_KV_POLICY_OVERWRITE`
- TTL expiration checked on access
- WAL-backed persistence for set, delete, and clear

## Time-series engine

The time-series engine stores named streams of `F32`, `I32`, `U32`, or raw samples.

- one ring buffer per registered stream
- range queries by timestamp
- overflow policies: drop oldest, reject, downsample, or logarithmic retain
- per-stream extended registration via `lox_ts_register_ex(...)`
- WAL-backed persistence for inserts and stream metadata

## Relational engine

The relational engine stores small fixed schemas with packed rows.

- one indexed column per table
- binary-search index on the indexed field
- linear scans for non-index lookups
- insertion-order iteration
- WAL-backed persistence for inserts, deletes, and table metadata

## Storage HAL

loxdb supports three storage modes:

- RAM-only: `cfg.storage = NULL`
- POSIX file HAL for tests and simulation
- ESP32 partition HAL via `esp_partition_*`
- RTOS skeleton templates: `examples/freertos_port/`, `examples/zephyr_port/`
- SD + FatFS glue skeleton: `examples/sd_fatfs_port/`

## Supported Platforms

Verified hardware:
- ESP32-S3 N16R8 (`run_real` PASS, benchmarked)

Commonly compatible targets:
- direct byte-write flash ports (ESP32 family, STM32H7/F4, RP2040, nRF52, SAMD51)
- aligned-write media via `lox_backend_aligned_adapter` (`write_size > 1`)

Current hard limits:
- core durable path expects byte-write storage (`write_size == 1`)
- AVR/MSP430-class tiny targets are out of scope for current memory/storage contract

Notes:
- latency numbers are board/flash dependent; treat all values as directional and measure on target hardware
- for full platform matrix, adapter contracts, and managed media notes, see [BACKEND_INTEGRATION_GUIDE.md](docs/BACKEND_INTEGRATION_GUIDE.md) and [PORT_AUTHORING_GUIDE.md](docs/PORT_AUTHORING_GUIDE.md)

## Read-only diagnostics API

System stats are exposed through read-only APIs (not user KV keys):

- `lox_get_db_stats(...)`
- `lox_get_kv_stats(...)`
- `lox_get_ts_stats(...)`
- `lox_get_rel_stats(...)`
- `lox_get_effective_capacity(...)`
- `lox_get_pressure(...)`
- `lox_selfcheck(...)`
- `lox_admit_kv_set(...)`
- `lox_admit_ts_insert(...)`
- `lox_admit_rel_insert(...)`

Semantics:
- `lox_admission_t.status` carries operation-level decision
- `lox_get_pressure(...)` exposes `kv/ts/rel/wal` pressure and near-full risk
- see [PROGRAMMER_MANUAL.md](docs/PROGRAMMER_MANUAL.md) for detailed field-level behavior

## Migrations vs Snapshots

Three separate concepts:
1. schema migration API (`schema_version` + `cfg.on_migrate`) for REL tables
2. internal durable snapshot banks for WAL/compact/recovery (not public user snapshots)
3. query-time consistency checks (returns `LOX_ERR_MODIFIED` on concurrent mutation)

Detailed behavior: [SCHEMA_MIGRATION_GUIDE.md](docs/SCHEMA_MIGRATION_GUIDE.md) and [PROGRAMMER_MANUAL.md](docs/PROGRAMMER_MANUAL.md)

## RAM budget guide

| LOX_RAM_KB | KV entries (est.) | TS samples/stream (est.) | REL rows (est.) | Typical use |
|---------------|-------------------|--------------------------|-----------------|-------------|
| 8 KB          | ~3                | ~32                      | ~4              | Ultra-tiny KV-focused profile |
| 16 KB         | ~40               | ~136                     | ~8              | Small MCU baseline |
| 32 KB         | ~64               | ~1 500                   | ~30             | General embedded node |
| 64 KB         | ~150              | ~3 000                   | ~80             | Rich sensing / control node |
| 128 KB        | ~300              | ~6 000                   | ~160            | MCU + external RAM |
| 256 KB        | ~600              | ~12 000                  | ~320            | High-retention edge node |
| 512 KB        | ~1 200            | ~24 000                  | ~640            | Linux embedded |
| 1024 KB       | ~2 500            | ~48 000                  | ~1 300          | Resource-rich MCU |
| txn staging overhead | `LOX_TXN_STAGE_KEYS * sizeof(lox_txn_stage_entry_t)` bytes | same | same | Reserved from KV slice |

Estimates assume default 40/40/20 RAM split and default column sizes.
Override with `LOX_RAM_KV_PCT`, `LOX_RAM_TS_PCT`, `LOX_RAM_REL_PCT`.

Capacity planning helper:
- open `tools/lox_capacity_estimator.html` for profile-based storage/layout estimation (`2/4/8/16/32 MiB`) and rough record-fit planning.

## Design decisions and known limitations

- single `malloc` in `lox_init()` (predictable memory, no allocator churn)
- fixed RAM slices per engine (no runtime redistribution)
- one index per REL table (secondary indexes not planned for v1.x)
- KV overwrite mode uses O(n) LRU scan
- thread safety is hook-based (`LOX_THREAD_SAFE=1` + lock callbacks)
- no built-in compression/encryption (application-layer responsibility)

Detailed rationale: [PRODUCT_POSITIONING.md](docs/PRODUCT_POSITIONING.md) and [PROGRAMMER_MANUAL.md](docs/PROGRAMMER_MANUAL.md)

## Test coverage

Coverage includes KV/TS/REL behavior, WAL recovery/corruption paths, RAM-profile variants, and footprint/profile contract gates.
Current CTest inventory (as configured in CI debug presets): **76 registered tests per platform**.
CI execution volume per `ci.yml` run is higher because the same suite runs on multiple lanes:
- `build` matrix (`linux`, `windows`, `macOS`): `3 x 76 = 228` test executions
- `sanitize-linux` lane: additional `76` test executions
- total in `ci.yml`: **304 test executions** (same test set across environments/instrumentation)
Nightly soak (`nightly-soak.yml`) is benchmark-oriented (not CTest-count-oriented):
- lanes: `linux-debug`, `windows-debug`
- per lane: `3` worstcase-matrix profile runs + `3` long soak profile runs
- total per nightly run: **12 benchmark runs** (`2 x (3 + 3)`)

See CI and deep docs for current matrix:
- [ci.yml](.github/workflows/ci.yml)
- [PROGRAMMER_MANUAL.md](docs/PROGRAMMER_MANUAL.md)
- [docs/results/](docs/results/)

## Integration note

loxdb is storage-focused. Transport, serialization, and cryptography are handled by surrounding application components.

## Wiki

GitHub Wiki source pages are stored in [`wiki/`](wiki).
That keeps documentation versioned in the main repository and ready to publish into the GitHub wiki repo.

## License

MIT.

License details and file-level SPDX policy:

- [LICENSE](LICENSE)
- [docs/FREE_EDITION_LICENSING.md](docs/FREE_EDITION_LICENSING.md)
- SPDX tooling:
  - `tools/apply_spdx_headers.ps1`
  - `tools/check_spdx_headers.ps1`


