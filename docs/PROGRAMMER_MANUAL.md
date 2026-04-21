# microdb Programmer Manual

A practical, developer-friendly guide to understanding and using `microdb`.

This manual is written from the current source of truth in this repository:

- public API: `include/microdb.h`
- C++ wrapper: `include/microdb_cpp.hpp`
- core implementation: `src/*.c`
- optional backend modules: `include/microdb_backend_*.h`, `src/backends/*.c`

## 1. What microdb is

`microdb` is an embedded C99 storage library with one unified API over three data engines:

- KV (key-value)
- TS (time-series)
- REL (fixed-schema relational tables)

It is designed for MCU/edge systems where predictable memory behavior matters more than SQL-style flexibility.

## 2. Core architecture and layers

Think of `microdb` as layered building blocks:

1. Public API layer (`include/microdb.h`)
2. Core engine layer (`src/microdb.c`, `src/microdb_kv.c`, `src/microdb_ts.c`, `src/microdb_rel.c`, `src/microdb_wal.c`)
3. Storage abstraction layer (`microdb_storage_t` callbacks)
4. Port/adaptation layer (`port/*` + optional adapters in `src/backends/*`)

### 2.1 Layer map

```text
Application
  |
  |  (C API / C++ wrapper)
  v
microdb public API (microdb.h / microdb_cpp.hpp)
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

`microdb_init()` wires everything together:

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

## 4.3 REL engine

Use for small fixed-schema table data.

- explicit schema declaration
- one indexed column per table
- row helper API for column packing/unpacking

## 4.4 WAL/recovery

When `MICRODB_ENABLE_WAL=1` and persistent storage is provided:

- updates are logged for recovery
- reopen paths recover durable state
- `microdb_compact()` can reduce WAL pressure in maintenance windows

## 5. Configuration model

`microdb` is compile-time first, with runtime overrides in `microdb_cfg_t`.

## 5.1 Compile-time options (important)

Capacity and feature toggles:

- `MICRODB_RAM_KB`
- `MICRODB_RAM_KV_PCT`, `MICRODB_RAM_TS_PCT`, `MICRODB_RAM_REL_PCT` (must sum to 100)
- `MICRODB_ENABLE_KV`, `MICRODB_ENABLE_TS`, `MICRODB_ENABLE_REL`
- `MICRODB_ENABLE_WAL`
- `MICRODB_KV_MAX_KEYS`, `MICRODB_KV_KEY_MAX_LEN`, `MICRODB_KV_VAL_MAX_LEN`
- `MICRODB_TXN_STAGE_KEYS`
- `MICRODB_TS_MAX_STREAMS`, `MICRODB_TS_RAW_MAX`
- `MICRODB_REL_MAX_TABLES`, `MICRODB_REL_MAX_COLS`
- `MICRODB_THREAD_SAFE`

Profiles (enable exactly one):

- `MICRODB_PROFILE_CORE_MIN`
- `MICRODB_PROFILE_CORE_WAL`
- `MICRODB_PROFILE_CORE_PERF`
- `MICRODB_PROFILE_CORE_HIMEM`
- `MICRODB_PROFILE_FOOTPRINT_MIN`

If none is selected, `MICRODB_PROFILE_CORE_WAL` is used by default.

## 5.2 Runtime options (`microdb_cfg_t`)

- `storage`: storage HAL pointer (`NULL` for RAM-only)
- `ram_kb`: runtime RAM budget override
- `now`: timestamp callback (used by TTL/time logic)
- `kv_pct`, `ts_pct`, `rel_pct`: runtime split override
- `lock_create/lock/unlock/lock_destroy`: lock hooks
- `wal_compact_auto`, `wal_compact_threshold_pct`: compaction behavior knobs
- `wal_sync_mode`: WAL sync policy (`MICRODB_WAL_SYNC_ALWAYS` default, `MICRODB_WAL_SYNC_FLUSH_ONLY` opt-in)
- `on_migrate`: schema migration callback

## 5.3 Storage contract (strict)

Current releases require:

- `erase_size > 0`
- `write_size == 1`

Violations fail initialization (`MICRODB_ERR_INVALID`).

## 6. Quick start (practical)

```c
#include "microdb.h"

static microdb_t db;

int main(void) {
    microdb_cfg_t cfg = {0};
    cfg.storage = NULL;  /* RAM-only */
    cfg.ram_kb = 32u;

    if (microdb_init(&db, &cfg) != MICRODB_OK) {
        return 1;
    }

    uint32_t value = 123u;
    (void)microdb_kv_put(&db, "boot_count", &value, sizeof(value));

    (void)microdb_flush(&db);
    (void)microdb_deinit(&db);
    return 0;
}
```

## 7. Public C API: complete reference

This section covers all public C API functions in `include/microdb.h`.

## 7.1 Lifecycle and diagnostics

### `const char *microdb_err_to_string(microdb_err_t err)`

Converts error code to stable symbolic text.

### `microdb_err_t microdb_init(microdb_t *db, const microdb_cfg_t *cfg)`

Initializes DB instance. Must be called before other DB operations.

### `microdb_err_t microdb_deinit(microdb_t *db)`

Shuts down DB instance.

### `microdb_err_t microdb_flush(microdb_t *db)`

Flush durability boundary to storage path.

### `microdb_err_t microdb_stats(const microdb_t *db, microdb_stats_t *out)`

Legacy aggregate stats snapshot.

### `microdb_err_t microdb_inspect(microdb_t *db, microdb_stats_t *out)`

Inspectable aggregate snapshot helper.

### Detailed read-only diagnostics

- `microdb_get_db_stats`
- `microdb_get_kv_stats`
- `microdb_get_ts_stats`
- `microdb_get_rel_stats`
- `microdb_get_effective_capacity`
- `microdb_get_pressure`

### Admission/preflight helpers

- `microdb_admit_kv_set`
- `microdb_admit_ts_insert`
- `microdb_admit_rel_insert`

Use these to estimate acceptance/degradation/compaction pressure before writing.

### Maintenance

- `microdb_compact`

## 7.2 KV API

### Callback type

```c
typedef bool (*microdb_kv_iter_cb_t)(
    const char *key,
    const void *val,
    size_t val_len,
    uint32_t ttl_remaining,
    void *ctx
);
```

### Functions

- `microdb_kv_set(db, key, val, len, ttl)`
- `microdb_kv_get(db, key, buf, buf_len, out_len)`
- `microdb_kv_del(db, key)`
- `microdb_kv_exists(db, key)`
- `microdb_kv_iter(db, cb, ctx)`
- `microdb_kv_purge_expired(db)`
- `microdb_kv_clear(db)`
- `microdb_kv_put(db, key, val, len)` (macro, ttl=0)

### KV usage example

```c
uint8_t mode = 2u;
uint8_t out_mode = 0u;
size_t out_len = 0u;

(void)microdb_kv_set(&db, "mode", &mode, sizeof(mode), 60u);
if (microdb_kv_get(&db, "mode", &out_mode, sizeof(out_mode), &out_len) == MICRODB_OK) {
    /* use out_mode */
}
```

## 7.3 Transaction API

- `microdb_txn_begin`
- `microdb_txn_commit`
- `microdb_txn_rollback`

Use around grouped writes where transactional behavior is required.

```c
if (microdb_txn_begin(&db) == MICRODB_OK) {
    uint32_t v = 9u;
    (void)microdb_kv_put(&db, "x", &v, sizeof(v));
    if (microdb_txn_commit(&db) != MICRODB_OK) {
        (void)microdb_txn_rollback(&db);
    }
}
```

## 7.4 TS API

### Types

`microdb_ts_type_t`:

- `MICRODB_TS_F32`
- `MICRODB_TS_I32`
- `MICRODB_TS_U32`
- `MICRODB_TS_RAW`

`microdb_ts_sample_t` holds timestamp + typed union payload.

### Callback type

```c
typedef bool (*microdb_ts_query_cb_t)(const microdb_ts_sample_t *sample, void *ctx);
```

### Functions

- `microdb_ts_register(db, name, type, raw_size)`
- `microdb_ts_insert(db, name, ts, val)`
- `microdb_ts_last(db, name, out)`
- `microdb_ts_query(db, name, from, to, cb, ctx)`
- `microdb_ts_query_buf(db, name, from, to, buf, max_count, out_count)`
- `microdb_ts_count(db, name, from, to, out_count)`
- `microdb_ts_clear(db, name)`

### TS query callback mutation contract

`microdb_ts_query` intentionally unlocks before invoking your callback and re-locks after callback return.

- If callback (or another thread/task) mutates TS state during the query window, `mutation_seq` changes.
- When that happens, `microdb_ts_query` returns `MICRODB_ERR_INVALID`.
- Treat this return value as "iteration snapshot invalidated by concurrent mutation", not as storage corruption.

Safe caller pattern:

- do read-only work in callback, or
- if mutation is needed, stop callback (`return false`), then do write in a separate step, then re-run query.

### TS usage example

```c
float temp = 24.5f;
microdb_ts_sample_t last;

(void)microdb_ts_register(&db, "temp", MICRODB_TS_F32, 0u);
(void)microdb_ts_insert(&db, "temp", 1001u, &temp);
(void)microdb_ts_last(&db, "temp", &last);
```

## 7.5 REL API

### Callback type

```c
typedef bool (*microdb_rel_iter_cb_t)(const void *row_buf, void *ctx);
```

### Schema/table functions

- `microdb_schema_init(schema, name, max_rows)`
- `microdb_schema_add(schema, col_name, type, size, is_index)`
- `microdb_schema_seal(schema)`
- `microdb_table_create(db, schema)`
- `microdb_table_get(db, name, out_table)`
- `microdb_table_row_size(table)`

### Row helpers

- `microdb_row_set(table, row_buf, col_name, val)`
- `microdb_row_get(table, row_buf, col_name, out, out_len)`

### Relational operations

- `microdb_rel_insert(db, table, row_buf)`
- `microdb_rel_find(db, table, search_val, cb, ctx)`
- `microdb_rel_find_by(db, table, col_name, search_val, out_buf)`
- `microdb_rel_delete(db, table, search_val, out_deleted)`
- `microdb_rel_iter(db, table, cb, ctx)`
- `microdb_rel_count(table, out_count)`
- `microdb_rel_clear(db, table)`

### REL find callback lock and mutation contract

`microdb_rel_find` also unlocks around callback invocation and validates table mutation sequence on re-lock.

- If rows are inserted/deleted/cleared while `microdb_rel_find` callback is in progress, table `mutation_seq` changes.
- If `mutation_seq` changed, `microdb_rel_find` returns `MICRODB_ERR_INVALID`.
- For caller logic, this means "search result traversal was invalidated", so restart the query under your chosen retry policy.

Practical rule:

- Do not call mutating REL APIs (`rel_insert`, `rel_delete`, `rel_clear`, schema/table create) from inside `rel_find` callback for the same table if you need deterministic traversal.

### REL usage example

```c
typedef struct {
    uint32_t id;
    char name[16];
} user_row_t;

microdb_schema_t schema;
microdb_table_t *table = NULL;
user_row_t row = {0};

(void)microdb_schema_init(&schema, "users", 32u);
(void)microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true);
(void)microdb_schema_add(&schema, "name", MICRODB_COL_STR, 16u, false);
(void)microdb_schema_seal(&schema);
(void)microdb_table_create(&db, &schema);
(void)microdb_table_get(&db, "users", &table);

row.id = 1u;
(void)memcpy(row.name, "alice", 6u);
(void)microdb_row_set(table, &row, "id", &row.id);
(void)microdb_row_set(table, &row, "name", row.name);
(void)microdb_rel_insert(&db, table, &row);
```

## 8. Common error handling pattern

```c
static int check_rc(const char *op, microdb_err_t rc) {
    if (rc != MICRODB_OK) {
        printf("%s failed: %s (%d)\n", op, microdb_err_to_string(rc), (int)rc);
        return 0;
    }
    return 1;
}
```

## 9. Optional backend adapter APIs

These are optional modules, not required for basic core usage.

## 9.1 Registry/capability APIs

From `microdb_backend_adapter.h`:

- `microdb_backend_registry_reset`
- `microdb_backend_registry_register`
- `microdb_backend_registry_count`
- `microdb_backend_registry_get`
- `microdb_backend_registry_find`

## 9.2 Open compatibility APIs

From `microdb_backend_compat.h` and `microdb_backend_decision.h`:

- `microdb_backend_classify_open`
- `microdb_backend_decide_by_name`

## 9.3 Adapter init APIs

Aligned:

- `microdb_backend_aligned_adapter_init`
- `microdb_backend_aligned_adapter_deinit`

Managed:

- `microdb_backend_managed_expectations_default`
- `microdb_backend_managed_adapter_init`
- `microdb_backend_managed_adapter_init_with_expectations`
- `microdb_backend_managed_adapter_deinit`

Filesystem-managed:

- `microdb_backend_fs_expectations_default`
- `microdb_backend_fs_adapter_init`
- `microdb_backend_fs_adapter_init_with_expectations`
- `microdb_backend_fs_adapter_deinit`

Unified open flow:

- `microdb_backend_open_session_reset`
- `microdb_backend_open_prepare`
- `microdb_backend_open_release`

## 10. C++ wrapper usage (`include/microdb_cpp.hpp`)

Wrapper class: `microdb::cpp::Database`.

Main groups:

- lifecycle: `init`, `deinit`, `flush`
- stats/diagnostics: `stats`, `db_stats`, `kv_stats`, `ts_stats`, `rel_stats`, `effective_capacity`, `pressure`
- KV API + POD helpers: `kv_set/put/get/del/exists/iter/clear/purge_expired`, `kv_put_pod`, `kv_get_pod`
- TS API + typed helpers: `ts_register_*`, `ts_insert_*`, query/count helpers
- REL schema/table/row helpers + operations
- transactions: `txn_begin`, `txn_commit`, `txn_rollback`

Minimal wrapper example:

```cpp
#include "microdb_cpp.hpp"

microdb::cpp::Database db;
microdb_cfg_t cfg{};
cfg.ram_kb = 32u;

if (db.init(cfg) == MICRODB_OK) {
    uint32_t n = 42u;
    db.kv_put_pod("n", n);
    db.deinit();
}
```

## 10.1 C++ wrapper: what it adds (on top of C API)

`microdb::cpp::Database` is a thin convenience layer, not a different engine.

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

## 10.2 Backend-open wrapper layer (`include/microdb_backend_open.h`)

`microdb_backend_open_*` is an orchestration wrapper for optional backend adapters.
It helps decide whether to open storage directly or via aligned/managed/fs adapter paths.

Main flow:

1. reset session: `microdb_backend_open_session_reset`
2. prepare adapted storage: `microdb_backend_open_prepare`
3. pass resulting storage to `microdb_init`
4. release wrapper resources: `microdb_backend_open_release`

This layer is useful when your product supports multiple storage backend classes and you want a single open path policy.

## 10.3 Capacity profile helper wrapper (`include/microdb_capacity_profile.h`)

This header is a lightweight helper for standard storage-capacity presets:

- `2/4/8/16/32 MiB`
- `microdb_storage_profile_capacity_bytes(profile)`
- `microdb_storage_profile_name(profile)`

It does not change core DB behavior by itself. It helps keep app/tooling config consistent.

## 10.4 Port wrappers and platform bridges

Repository ports are integration wrappers around `microdb_storage_t`:

- RAM port: `port/ram/microdb_port_ram.*`
- POSIX file port: `port/posix/microdb_port_posix.*`
- ESP32 partition port: `port/esp32/microdb_port_esp32.*`

They adapt platform I/O into `read/write/erase/sync` hooks expected by core.

## 10.5 Tooling wrappers around the core

Supporting tools built around the core APIs:

- offline verifier: `tools/microdb_verify.c`
- full validation runner: `tools/run_full_validation.ps1`
- benchmark harness: `bench/microdb_esp32_s3_bench/*`

These are operational wrappers for validation/verification, not replacements for the DB API.

## 11. Ready-to-run examples in the repository

- POSIX simulation and persistence example:
  - `examples/posix_simulator/main.c`
  - `examples/posix_simulator/README.md`
- ESP32 sensor-node style example:
  - `examples/esp32_sensor_node/main.c`

## 12. Build variants you can pick quickly

Common CMake targets:

- `microdb` (default)
- `microdb_no_wal`
- `microdb_kv_only`, `microdb_ts_only`, `microdb_rel_only`
- `microdb_thread_safe`
- `microdb_tiny`
- `microdb_footprint_min`
- profile variants (`microdb_profile_core_*`)

Choose variant by product constraints:

- tightest footprint: `microdb_tiny`
- smallest durable profile: `microdb_footprint_min`
- full API with WAL path: `microdb` / `microdb_profile_core_wal`

## 13. Practical recommendations

1. Start with RAM-only and a small reproducible test.
2. Enable persistent storage only after KV/TS/REL basics are stable.
3. Use diagnostics APIs (`pressure`, `effective_capacity`) early.
4. Add `flush()` at explicit durability checkpoints.
5. Schedule `compact()` in maintenance windows, not latency-critical code paths.
6. Validate reopen/recovery behavior on real target media.

## 13.1 Migration callback contract (`on_migrate`)

`on_migrate` is called when `microdb_table_create` sees an existing table with the same name but different schema version.

- Callback is invoked outside the internal DB lock.
- `migration_in_progress` guard is active during callback; recursive migration via nested `microdb_table_create` is rejected with `MICRODB_ERR_SCHEMA`.
- If callback returns non-`MICRODB_OK`, that error is propagated to caller and schema version is not advanced.
- If callback returns `MICRODB_OK`, caller is responsible for data transformation correctness done inside callback before version bump completes.

Recommended callback behavior:

- Keep migration idempotent.
- Prefer explicit read/transform/write sequence with clear fail path.
- Avoid creating additional schema-version transitions inside callback.

## 14. Related documents

- `docs/GETTING_STARTED_5_MIN.md`
- `docs/FAIL_CODE_CONTRACT.md`
- `docs/PROFILE_GUARANTEES.md`
- `docs/CORE_INVARIANTS.md`
- `docs/OFFLINE_VERIFIER.md`
- `docs/THREAD_SAFETY.md`
- `docs/FS_BLOCK_ADAPTER_CONTRACT.md`
- `docs/FREE_EDITION_LICENSING.md`
