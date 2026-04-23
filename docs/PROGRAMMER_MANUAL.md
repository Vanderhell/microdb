# loxdb Programmer Manual

A practical, developer-friendly guide to understanding and using `loxdb`.

This manual is written from the current source of truth in this repository:

- public API: `include/lox.h`
- C++ wrapper: `include/lox_cpp.hpp`
- core implementation: `src/*.c`
- optional backend modules: `include/lox_backend_*.h`, `src/backends/*.c`

## 1. What loxdb is

`loxdb` is an embedded C99 storage library with one unified API over three data engines:

- KV (key-value)
- TS (time-series)
- REL (fixed-schema relational tables)

It is designed for MCU/edge systems where predictable memory behavior matters more than SQL-style flexibility.

## 2. Core architecture and layers

Think of `loxdb` as layered building blocks:

1. Public API layer (`include/lox.h`)
2. Core engine layer (`src/loxdb.c`, `src/lox_kv.c`, `src/lox_ts.c`, `src/lox_rel.c`, `src/lox_wal.c`)
3. Storage abstraction layer (`lox_storage_t` callbacks)
4. Port/adaptation layer (`port/*` + optional adapters in `src/backends/*`)

### 2.1 Layer map

```text
Application
  |
  |  (C API / C++ wrapper)
  v
loxdb public API (lox.h / lox_cpp.hpp)
  |
  +--> KV engine
  +--> TS engine
  +--> REL engine
  +--> WAL + recovery + compaction
  |
  v
Storage HAL (read/write/erase/sync)
  |
  +--> RAM port
  +--> POSIX port
  +--> ESP32 port
  +--> Optional backend adapters (aligned/managed/fs)
```

### 2.2 How layers connect at runtime

`lox_init()` wires everything together:

- validates config and storage contract
- performs one internal allocation for engine arenas
- loads durable state (if storage is present)
- replays recovery path when needed

All runtime operations (`kv_*`, `ts_*`, `rel_*`, `txn_*`) use that initialized state.

## 3. Dependencies and external libraries

## 3.1 Runtime dependencies

Core library runtime dependencies are intentionally minimal:

- C standard headers (`stdbool.h`, `stddef.h`, `stdint.h` and standard C runtime)
- no required third-party runtime library

## 3.2 Optional integration dependencies

Depending on your platform/path:

- POSIX test/simulation path uses the repository POSIX port (`port/posix/*`)
- ESP32 path uses repository ESP32 port (`port/esp32/*`) and ESP-IDF APIs
- C++ convenience wrapper uses C++ standard library headers (`cstring`, `type_traits`)

## 3.3 Build tooling

- CMake (main build and variants)
- CTest (test execution)

## 4. Data model and engine semantics

## 4.1 KV engine

Use for config/state values and optional TTL-backed entries.

- binary-safe values
- bounded by configured key/value limits
- overflow policy controlled at compile time

## 4.2 TS engine

Use for timestamped telemetry streams.

- per-stream typed samples (`F32/I32/U32/RAW`)
- timestamp range query APIs
- overflow policy controlled at compile time
- packed internal sample storage (`timestamp + value_size`) to reduce scalar stream footprint

TS capacity note:

- TS arena bytes are split evenly across `LOX_TS_MAX_STREAMS` slots.
- Packed layout increases samples-per-stream according to stream value size.
- With homogeneous scalar streams (for example all `F32`) this gives near-maximum gain.
- With mixed stream types (`F32` + `RAW`), each stream still keeps an equal byte slice, so total gain is partial.

## 4.3 REL engine

Use for small fixed-schema table data.

- explicit schema declaration
- one indexed column per table
- row helper API for column packing/unpacking

## 4.4 WAL/recovery

When `LOX_ENABLE_WAL=1` and persistent storage is provided:

- updates are logged for recovery
- reopen paths recover durable state
- `lox_compact()` can reduce WAL pressure in maintenance windows

## 5. Configuration model

`loxdb` is compile-time first, with runtime overrides in `lox_cfg_t`.

## 5.1 Compile-time options (important)

Capacity and feature toggles:

- `LOX_RAM_KB`
- `LOX_RAM_KV_PCT`, `LOX_RAM_TS_PCT`, `LOX_RAM_REL_PCT` (must sum to 100)
- `LOX_ENABLE_KV`, `LOX_ENABLE_TS`, `LOX_ENABLE_REL`
- `LOX_ENABLE_WAL`
- `LOX_KV_MAX_KEYS`, `LOX_KV_KEY_MAX_LEN`, `LOX_KV_VAL_MAX_LEN`
- `LOX_TXN_STAGE_KEYS`
- `LOX_TS_MAX_STREAMS`, `LOX_TS_RAW_MAX`
- `LOX_REL_MAX_TABLES`, `LOX_REL_MAX_COLS`
- `LOX_THREAD_SAFE`

Profiles (enable exactly one):

- `LOX_PROFILE_CORE_MIN`
- `LOX_PROFILE_CORE_WAL`
- `LOX_PROFILE_CORE_PERF`
- `LOX_PROFILE_CORE_HIMEM`
- `LOX_PROFILE_FOOTPRINT_MIN`

If none is selected, `LOX_PROFILE_CORE_WAL` is used by default.

## 5.2 Runtime options (`lox_cfg_t`)

- `storage`: storage HAL pointer (`NULL` for RAM-only)
- `ram_kb`: runtime RAM budget override
- `now`: timestamp callback (used by TTL/time logic)
- `kv_pct`, `ts_pct`, `rel_pct`: runtime split override
- `lock_create/lock/unlock/lock_destroy`: lock hooks
- `wal_compact_auto`, `wal_compact_threshold_pct`: compaction behavior knobs
- `wal_sync_mode`: WAL sync policy (`LOX_WAL_SYNC_ALWAYS` default, `LOX_WAL_SYNC_FLUSH_ONLY` opt-in)
- `on_migrate`: schema migration callback

## 5.3 Storage contract (strict)

Current releases require:

- `erase_size > 0`
- `write_size == 1`

Violations fail initialization (`LOX_ERR_INVALID`).

## 5.4 Capacity planning

`loxdb` uses a three-arena RAM model computed in `lox_init()`:

- `kv_arena = floor((ram_kb * 1024) * kv_pct / 100)`
- `ts_arena = floor((ram_kb * 1024) * ts_pct / 100)`
- `rel_arena = remaining bytes after TS and pointer-alignment step`

KV usable entry limit:

- `kv_entries_usable = LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS`

KV value-store capacity (current build ABI):

- `bucket_count = next_pow2(ceil((kv_entries_usable * 4) / 3))`
- `bucket_array_bytes = bucket_count * sizeof(lox_kv_bucket_t)`
- `txn_stage_bytes = LOX_TXN_STAGE_KEYS * sizeof(lox_txn_stage_entry_t)`
- `value_store_bytes = kv_arena - align8(bucket_array_bytes) - txn_stage_bytes`

TS retention depends on registered streams and stream stride:

- `sample_stride = sizeof(timestamp) + value_size` (`F32/I32/U32=8`, `RAW16=20`)
- `samples_per_stream ~= floor((ts_arena / stream_count) / sample_stride)`
- `retention_hours = samples_per_stream / inserts_per_hour_per_stream`

Tooling:

- Capacity estimator: `tools/lox_capacity_estimator.html`

Wear model (storage enabled):

- WAL/dual-bank layout follows `src/lox_wal.c` (`wal_target=erase*8`, `wal_min=erase*2`, two superblocks, two banks)
- `wal_compact_threshold_pct` controls when automatic compaction triggers based on WAL fill percentage
- lower threshold => earlier, more frequent compactions; higher threshold => longer WAL growth before compaction

Worked example (ESP32 sensor node):

- 3 temperature streams (`F32`), 512 KB flash, 4096 B erase blocks, 10-year target lifetime
- set these values in `tools/lox_capacity_estimator.html`
- verify:
  1. RAM split leaves sufficient KV value-store for config keys
  2. TS retention satisfies required history horizon
  3. estimated flash lifetime is at or above target
  4. WAL and dual-bank footprint fits within available flash

## 6. Quick start (practical)

```c
#include "lox.h"

static lox_t db;

int main(void) {
    lox_cfg_t cfg = {0};
    cfg.storage = NULL;  /* RAM-only */
    cfg.ram_kb = 32u;

    if (lox_init(&db, &cfg) != LOX_OK) {
        return 1;
    }

    uint32_t value = 123u;
    (void)lox_kv_put(&db, "boot_count", &value, sizeof(value));

    (void)lox_flush(&db);
    (void)lox_deinit(&db);
    return 0;
}
```

## 7. Public C API: complete reference

This section covers all public C API functions in `include/lox.h`.

## 7.1 Lifecycle and diagnostics

### `const char *lox_err_to_string(lox_err_t err)`

Converts error code to stable symbolic text.

### `lox_err_t lox_init(lox_t *db, const lox_cfg_t *cfg)`

Initializes DB instance. Must be called before other DB operations.

### `lox_err_t lox_deinit(lox_t *db)`

Shuts down DB instance.

### `lox_err_t lox_flush(lox_t *db)`

Flush durability boundary to storage path.

### `lox_err_t lox_stats(const lox_t *db, lox_stats_t *out)`

Legacy aggregate stats snapshot.

### `lox_err_t lox_inspect(lox_t *db, lox_stats_t *out)`

Inspectable aggregate snapshot helper.

### Detailed read-only diagnostics

- `lox_get_db_stats`
- `lox_get_kv_stats`
- `lox_get_ts_stats`
- `lox_get_rel_stats`
- `lox_get_effective_capacity`
- `lox_get_pressure`
- `lox_selfcheck`

### Admission/preflight helpers

- `lox_admit_kv_set`
- `lox_admit_ts_insert`
- `lox_admit_rel_insert`

Use these to estimate acceptance/degradation/compaction pressure before writing.

### Maintenance

- `lox_compact`

## 7.2 KV API

### Callback type

```c
typedef bool (*lox_kv_iter_cb_t)(
    const char *key,
    const void *val,
    size_t val_len,
    uint32_t ttl_remaining,
    void *ctx
);
```

### Functions

- `lox_kv_set(db, key, val, len, ttl)`
- `lox_kv_get(db, key, buf, buf_len, out_len)`
- `lox_kv_del(db, key)`
- `lox_kv_exists(db, key)`
- `lox_kv_iter(db, cb, ctx)`
- `lox_kv_purge_expired(db)`
- `lox_kv_clear(db)`
- `lox_kv_put(db, key, val, len)` (macro, ttl=0)

### KV usage example

```c
uint8_t mode = 2u;
uint8_t out_mode = 0u;
size_t out_len = 0u;

(void)lox_kv_set(&db, "mode", &mode, sizeof(mode), 60u);
if (lox_kv_get(&db, "mode", &out_mode, sizeof(out_mode), &out_len) == LOX_OK) {
    /* use out_mode */
}
```

## 7.3 Transaction API

- `lox_txn_begin`
- `lox_txn_commit`
- `lox_txn_rollback`

Use around grouped writes where transactional behavior is required.

```c
if (lox_txn_begin(&db) == LOX_OK) {
    uint32_t v = 9u;
    (void)lox_kv_put(&db, "x", &v, sizeof(v));
    if (lox_txn_commit(&db) != LOX_OK) {
        (void)lox_txn_rollback(&db);
    }
}
```

## 7.4 TS API

### Types

`lox_ts_type_t`:

- `LOX_TS_F32`
- `LOX_TS_I32`
- `LOX_TS_U32`
- `LOX_TS_RAW`

`lox_ts_sample_t` holds timestamp + typed union payload.

Storage/capacity behavior:

- Public API remains `lox_ts_sample_t` for compatibility.
- Internal TS ring uses packed per-stream stride: `sizeof(timestamp) + value_size`.
- Stream capacity is computed from that stride inside each stream's fixed byte slice.

### Callback type

```c
typedef bool (*lox_ts_query_cb_t)(const lox_ts_sample_t *sample, void *ctx);
```

### Functions

- `lox_ts_register(db, name, type, raw_size)`
- `lox_ts_register_ex(db, name, type, raw_size, cfg)`
- `lox_ts_insert(db, name, ts, val)`
- `lox_ts_last(db, name, out)`
- `lox_ts_query(db, name, from, to, cb, ctx)`
- `lox_ts_query_buf(db, name, from, to, buf, max_count, out_count)`
- `lox_ts_count(db, name, from, to, out_count)`
- `lox_ts_clear(db, name)`

Overflow policy note:

- In addition to DROP/REJECT/DOWNSAMPLE, builds can enable `LOX_TS_POLICY_LOG_RETAIN`.
- `lox_ts_register_ex` allows per-stream logarithmic retention zone configuration.

TS lifecycle limitation:

- There is currently no `lox_ts_unregister` API.
- Stream slots are expected to be provisioned at boot and reused/cleared (`lox_ts_clear`) rather than removed at runtime.

### TS query callback mutation contract

`lox_ts_query` intentionally unlocks before invoking your callback and re-locks after callback return.

- If callback (or another thread/task) mutates TS state during the query window, `mutation_seq` changes.
- When that happens, `lox_ts_query` returns `LOX_ERR_MODIFIED`.
- Treat this return value as "iteration snapshot invalidated by concurrent mutation", not as storage corruption.

Safe caller pattern:

- do read-only work in callback, or
- if mutation is needed, stop callback (`return false`), then do write in a separate step, then re-run query.

### TS usage example

```c
float temp = 24.5f;
lox_ts_sample_t last;

(void)lox_ts_register(&db, "temp", LOX_TS_F32, 0u);
(void)lox_ts_insert(&db, "temp", 1001u, &temp);
(void)lox_ts_last(&db, "temp", &last);
```

## 7.5 REL API

### Callback type

```c
typedef bool (*lox_rel_iter_cb_t)(const void *row_buf, void *ctx);
```

### Schema/table functions

- `lox_schema_init(schema, name, max_rows)`
- `lox_schema_add(schema, col_name, type, size, is_index)`
- `lox_schema_seal(schema)`
- `lox_table_create(db, schema)`
- `lox_table_get(db, name, out_table)`
- `lox_table_row_size(table)`

### Row helpers

- `lox_row_set(table, row_buf, col_name, val)`
- `lox_row_get(table, row_buf, col_name, out, out_len)`

### Relational operations

- `lox_rel_insert(db, table, row_buf)`
- `lox_rel_find(db, table, search_val, cb, ctx)`
- `lox_rel_find_by(db, table, col_name, search_val, out_buf)`
- `lox_rel_delete(db, table, search_val, out_deleted)`
- `lox_rel_iter(db, table, cb, ctx)`
- `lox_rel_count(table, out_count)`
- `lox_rel_clear(db, table)`

### REL find callback lock and mutation contract

`lox_rel_find` also unlocks around callback invocation and validates table mutation sequence on re-lock.

- If rows are inserted/deleted/cleared while `lox_rel_find` callback is in progress, table `mutation_seq` changes.
- If `mutation_seq` changed, `lox_rel_find` returns `LOX_ERR_MODIFIED`.
- For caller logic, this means "search result traversal was invalidated", so restart the query under your chosen retry policy.

Practical rule:

- Do not call mutating REL APIs (`rel_insert`, `rel_delete`, `rel_clear`, schema/table create) from inside `rel_find` callback for the same table if you need deterministic traversal.

`lox_rel_iter` follows the same unlock/re-lock pattern and also returns `LOX_ERR_MODIFIED` when concurrent mutation is detected after callback return.

### REL usage example

```c
typedef struct {
    uint32_t id;
    char name[16];
} user_row_t;

lox_schema_t schema;
lox_table_t *table = NULL;
user_row_t row = {0};

(void)lox_schema_init(&schema, "users", 32u);
(void)lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true);
(void)lox_schema_add(&schema, "name", LOX_COL_STR, 16u, false);
(void)lox_schema_seal(&schema);
(void)lox_table_create(&db, &schema);
(void)lox_table_get(&db, "users", &table);

row.id = 1u;
(void)memcpy(row.name, "alice", 6u);
(void)lox_row_set(table, &row, "id", &row.id);
(void)lox_row_set(table, &row, "name", row.name);
(void)lox_rel_insert(&db, table, &row);
```

## 8. Common error handling pattern

```c
static int check_rc(const char *op, lox_err_t rc) {
    if (rc != LOX_OK) {
        printf("%s failed: %s (%d)\n", op, lox_err_to_string(rc), (int)rc);
        return 0;
    }
    return 1;
}
```

## 9. Optional backend adapter APIs

These are optional modules, not required for basic core usage.

## 9.1 Registry/capability APIs

From `lox_backend_adapter.h`:

- `lox_backend_registry_reset`
- `lox_backend_registry_register`
- `lox_backend_registry_count`
- `lox_backend_registry_get`
- `lox_backend_registry_find`

## 9.2 Open compatibility APIs

From `lox_backend_compat.h` and `lox_backend_decision.h`:

- `lox_backend_classify_open`
- `lox_backend_decide_by_name`

## 9.3 Adapter init APIs

Aligned:

- `lox_backend_aligned_adapter_init`
- `lox_backend_aligned_adapter_deinit`

Managed:

- `lox_backend_managed_expectations_default`
- `lox_backend_managed_adapter_init`
- `lox_backend_managed_adapter_init_with_expectations`
- `lox_backend_managed_adapter_deinit`

Filesystem-managed:

- `lox_backend_fs_expectations_default`
- `lox_backend_fs_adapter_init`
- `lox_backend_fs_adapter_init_with_expectations`
- `lox_backend_fs_adapter_deinit`

Unified open flow:

- `lox_backend_open_session_reset`
- `lox_backend_open_prepare`
- `lox_backend_open_release`

## 10. C++ wrapper usage (`include/lox_cpp.hpp`)

Wrapper class: `loxdb::cpp::Database`.

Main groups:

- lifecycle: `init`, `deinit`, `flush`
- stats/diagnostics: `stats`, `db_stats`, `kv_stats`, `ts_stats`, `rel_stats`, `effective_capacity`, `pressure`
- KV API + POD helpers: `kv_set/put/get/del/exists/iter/clear/purge_expired`, `kv_put_pod`, `kv_get_pod`
- TS API + typed helpers: `ts_register_*`, `ts_insert_*`, query/count helpers
- REL schema/table/row helpers + operations
- transactions: `txn_begin`, `txn_commit`, `txn_rollback`

Minimal wrapper example:

```cpp
#include "lox_cpp.hpp"

loxdb::cpp::Database db;
lox_cfg_t cfg{};
cfg.ram_kb = 32u;

if (db.init(cfg) == LOX_OK) {
    uint32_t n = 42u;
    db.kv_put_pod("n", n);
    db.deinit();
}
```

## 10.1 C++ wrapper: what it adds (on top of C API)

`loxdb::cpp::Database` is a thin convenience layer, not a different engine.

It adds:

- RAII-style lifetime handling (deinit in destructor when still initialized)
- safer ergonomics for init/handle state checks
- POD helper templates (`kv_put_pod`, `kv_get_pod`, `rel_row_set_pod`, `rel_row_get_pod`)
- typed TS helper methods (`ts_register_f32/i32/u32`, `ts_insert_f32/i32/u32`)

Use C++ wrapper when:

- application is C++
- you want cleaner callsites and fewer repetitive checks

Use pure C API when:

- firmware is C-only
- ABI-level C integration is required

## 10.2 Backend-open wrapper layer (`include/lox_backend_open.h`)

`lox_backend_open_*` is an orchestration wrapper for optional backend adapters.
It helps decide whether to open storage directly or via aligned/managed/fs adapter paths.

Main flow:

1. reset session: `lox_backend_open_session_reset`
2. prepare adapted storage: `lox_backend_open_prepare`
3. pass resulting storage to `lox_init`
4. release wrapper resources: `lox_backend_open_release`

This layer is useful when your product supports multiple storage backend classes and you want a single open path policy.

## 10.3 Capacity profile helper wrapper (`include/lox_capacity_profile.h`)

This header is a lightweight helper for standard storage-capacity presets:

- `2/4/8/16/32 MiB`
- `lox_storage_profile_capacity_bytes(profile)`
- `lox_storage_profile_name(profile)`

It does not change core DB behavior by itself. It helps keep app/tooling config consistent.

## 10.4 Port wrappers and platform bridges

Repository ports are integration wrappers around `lox_storage_t`:

- RAM port: `port/ram/lox_port_ram.*`
- POSIX file port: `port/posix/lox_port_posix.*`
- ESP32 partition port: `port/esp32/lox_port_esp32.*`

They adapt platform I/O into `read/write/erase/sync` hooks expected by core.

## 10.5 Tooling wrappers around the core

Supporting tools built around the core APIs:

- offline verifier: `tools/lox_verify.c`
- full validation runner: `tools/run_full_validation.ps1`
- benchmark harness: `bench/lox_esp32_s3_bench/*`
- POSIX stress/latency trend runners: `tests/soak_runner.c`, `tests/worstcase_matrix_runner.c`

These are operational wrappers for validation/verification, not replacements for the DB API.
POSIX benchmark numbers are trend signals for regression tracking (`wal_fill_*`, `compact_count_*`, `compactions_during_measure`),
not direct SPI/NOR flash latency proxies.

## 11. Ready-to-run examples in the repository

- POSIX simulation and persistence example:
  - `examples/posix_simulator/main.c`
  - `examples/posix_simulator/README.md`
- ESP32 sensor-node style example:
  - `examples/esp32_sensor_node/main.c`

## 12. Build variants you can pick quickly

Common CMake targets:

- `loxdb` (default)
- `lox_no_wal`
- `lox_kv_only`, `lox_ts_only`, `lox_rel_only`
- `lox_thread_safe`
- `lox_tiny`
- `lox_footprint_min`
- profile variants (`lox_profile_core_*`)

Choose variant by product constraints:

- tightest footprint: `lox_tiny`
- smallest durable profile: `lox_footprint_min`
- full API with WAL path: `loxdb` / `lox_profile_core_wal`

## 13. Practical recommendations

1. Start with RAM-only and a small reproducible test.
2. Enable persistent storage only after KV/TS/REL basics are stable.
3. Use diagnostics APIs (`pressure`, `effective_capacity`) early.
4. Add `flush()` at explicit durability checkpoints.
5. Schedule `compact()` in maintenance windows, not latency-critical code paths.
6. Validate reopen/recovery behavior on real target media.

## 13.1 Migration callback contract (`on_migrate`)

`on_migrate` is called when `lox_table_create` sees an existing table with the same name but different schema version.

- Callback is invoked outside the internal DB lock.
- `migration_in_progress` guard is active during callback; recursive migration via nested `lox_table_create` is rejected with `LOX_ERR_SCHEMA`.
- If callback returns non-`LOX_OK`, that error is propagated to caller and schema version is not advanced.
- If callback returns `LOX_OK`, caller is responsible for data transformation correctness done inside callback before version bump completes.

Recommended callback behavior:

- Keep migration idempotent.
- Prefer explicit read/transform/write sequence with clear fail path.
- Avoid creating additional schema-version transitions inside callback.

## 13.2 Known Risks / Operational Watchpoints

1. KV value-store compaction cost under churn
   Frequent updates with different value sizes can fragment KV value storage.
   When compaction is triggered, the implementation may perform a large `memmove` over value-store bytes.
   On real-time paths with larger values and high update rate, watch tail latency and schedule heavy write bursts outside critical sections.

2. `lox_core_t` size growth on 64-bit hosts
   `lox_t` stores internal state in fixed `_opaque` bytes and `LOX_STATIC_ASSERT(core_size_fits, ...)` protects overflow.
   Because `lox_core_t` includes multiple pointers/callbacks, 64-bit builds can grow faster than 32-bit MCU builds.
   Keep at least one 64-bit CI build as an early warning for structural growth.

3. WAL corruption fallback loss window
   Replay behavior intentionally stops at the last valid entry on partial/corrupt WAL entries.
   Corruption in superblock/bank metadata can force fallback to an older bank/checkpoint.
   For critical durability requirements, test explicit fault-injection scenarios around checkpoint rotation and bank selection.

4. REL indexing model (one index per table in v1.x)
   Current REL design supports one indexed column per table.
   This is typically fine for small tables but should be accounted for during schema design to avoid query-plan surprises later.

## 14. Related documents

- `docs/GETTING_STARTED_5_MIN.md`
- `docs/FAIL_CODE_CONTRACT.md`
- `docs/PROFILE_GUARANTEES.md`
- `docs/CORE_INVARIANTS.md`
- `docs/OFFLINE_VERIFIER.md`
- `docs/THREAD_SAFETY.md`
- `docs/FS_BLOCK_ADAPTER_CONTRACT.md`
- `docs/FREE_EDITION_LICENSING.md`
- `docs/WCET_ANALYSIS.md`
- `docs/SAFETY_READINESS.md`
