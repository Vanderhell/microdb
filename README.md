# microdb

> Embedded database for microcontrollers.
> Three engines. One malloc. Zero dependencies.

[![CI](https://github.com/Vanderhell/microdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Vanderhell/microdb/actions/workflows/ci.yml)
[![Language: C99](https://img.shields.io/badge/language-C99-blue)](https://en.wikipedia.org/wiki/C99)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Platform: ESP32](https://img.shields.io/badge/platform-ESP32-orange)](https://github.com/Vanderhell/microdb)
[![Tests: 175+](https://img.shields.io/badge/tests-175%2B-brightgreen)](https://github.com/Vanderhell/microdb)

## What is microdb?

microdb is a compact embedded database written in C99 for firmware and small edge runtimes.
It combines three storage models behind one API surface:

- KV for configuration, caches, and TTL-backed state
- Time-series for sensor samples and rolling telemetry
- Relational for small indexed tables

The library allocates exactly once in `microdb_init()`, runs without external dependencies,
and can operate either in RAM-only mode or with a storage HAL for persistence and WAL recovery.

## Why not SQLite?

SQLite is excellent, but it targets a different operating point.
microdb is intentionally narrower:

- one malloc at init, no allocator churn during normal operation
- fixed RAM budgeting across engines
- tiny integration surface for MCUs and RTOS firmware
- simpler persistence model for flash partitions and file-backed simulation

If you need SQL, dynamic schemas, concurrent access, or large secondary indexes, use SQLite.
If you need predictable memory and embedded-first behavior, microdb is the better fit.

## Quick start

**1. Add to your project:**
```cmake
add_subdirectory(microdb)
target_link_libraries(your_app PRIVATE microdb)
```

**2. Configure and initialize:**
```c
#define MICRODB_RAM_KB 32
#include "microdb.h"

static microdb_t db;

microdb_cfg_t cfg = {
    .storage = NULL,   // RAM-only; provide HAL for persistence
    .now     = NULL,   // provide timestamp fn for TTL support
};
microdb_init(&db, &cfg);
```

**3. Use all three engines:**
```c
// Key-value
float temp = 23.5f;
microdb_kv_put(&db, "temperature", &temp, sizeof(temp));

// Time-series
microdb_ts_register(&db, "sensor", MICRODB_TS_F32, 0);
microdb_ts_insert(&db, "sensor", time_now(), &temp);

// Relational
microdb_schema_t schema;
microdb_schema_init(&schema, "devices", 32);
microdb_schema_add(&schema, "id",   MICRODB_COL_U16, 2, true);
microdb_schema_add(&schema, "name", MICRODB_COL_STR, 16, false);
microdb_schema_seal(&schema);
microdb_table_create(&db, &schema);
```

## Configuration

Configuration is compile-time first, with a small runtime override surface in `microdb_cfg_t`.

- `MICRODB_RAM_KB` sets the total heap budget
- `MICRODB_RAM_KV_PCT`, `MICRODB_RAM_TS_PCT`, `MICRODB_RAM_REL_PCT` set default engine slices
- `cfg.ram_kb` overrides the total budget per instance
- `cfg.kv_pct`, `cfg.ts_pct`, `cfg.rel_pct` override the slice split per instance
- `MICRODB_ENABLE_WAL` toggles WAL persistence when a storage HAL is present
- `MICRODB_LOG(level, fmt, ...)` enables internal diagnostic logging

## KV engine

The KV engine stores short keys with binary values and optional TTL.

- fixed-size hash table with overwrite or reject overflow policy
- LRU eviction for `MICRODB_KV_POLICY_OVERWRITE`
- TTL expiration checked on access
- WAL-backed persistence for set, delete, and clear

## Time-series engine

The time-series engine stores named streams of `F32`, `I32`, `U32`, or raw samples.

- one ring buffer per registered stream
- range queries by timestamp
- overflow policies: drop oldest, reject, or downsample
- WAL-backed persistence for inserts and stream metadata

## Relational engine

The relational engine stores small fixed schemas with packed rows.

- one indexed column per table
- binary-search index on the indexed field
- linear scans for non-index lookups
- insertion-order iteration
- WAL-backed persistence for inserts, deletes, and table metadata

## Storage HAL

microdb supports three storage modes:

- RAM-only: `cfg.storage = NULL`
- POSIX file HAL for tests and simulation
- ESP32 partition HAL via `esp_partition_*`

Persistent layout starts with a WAL region and then separate KV, TS, and REL regions aligned to the storage erase size.

## RAM budget guide

| MICRODB_RAM_KB | KV entries (est.) | TS samples/stream (est.) | REL rows (est.) | Typical use |
|---------------|-------------------|--------------------------|-----------------|-------------|
| 8 KB          | ~20               | ~200                     | ~10             | Minimal sensor node |
| 32 KB         | ~64               | ~1 500                   | ~30             | Default IoT device |
| 64 KB         | ~150              | ~3 000                   | ~80             | Gateway node |
| 128 KB        | ~300              | ~6 000                   | ~160            | ESP32 + PSRAM |
| 256 KB        | ~600              | ~12 000                  | ~320            | ESP32-S3 + PSRAM |
| 512 KB        | ~1 200            | ~24 000                  | ~640            | Linux embedded |
| 1024 KB       | ~2 500            | ~48 000                  | ~1 300          | Resource-rich MCU |

Estimates assume default 40/40/20 RAM split and default column sizes.
Override with `MICRODB_RAM_KV_PCT`, `MICRODB_RAM_TS_PCT`, `MICRODB_RAM_REL_PCT`.

## Design decisions and known limitations

**Single malloc at init.**
microdb allocates exactly once in `microdb_init()` and never again.
This makes memory usage predictable and eliminates heap fragmentation -
a critical property for long-running embedded systems.

**Fixed RAM slices per engine.**
Each engine gets a fixed percentage of the RAM budget at init time.
There is no automatic redistribution if one engine fills up while another
has free space. This is intentional - dynamic redistribution would require
a runtime allocator, breaking the single-malloc guarantee.
Workaround: tune `kv_pct`, `ts_pct`, `rel_pct` in `microdb_cfg_t` for your use case.

**One index per relational table.**
Each table supports one indexed column for O(log n) lookups.
All other column lookups are O(n) linear scans.
For embedded tables with <= 100 rows this is acceptable (microseconds on ESP32).
Secondary indexes are not planned for v1.x.

**LRU eviction is O(n).**
When KV store is full and overflow policy is OVERWRITE, finding the LRU entry
requires scanning all buckets. At `MICRODB_KV_MAX_KEYS=64` this is 64 comparisons.
For embedded use cases this is negligible. Not suitable for `MICRODB_KV_MAX_KEYS > 1000`.

**No thread safety.**
All functions assume single-threaded access. Wrap with mutex at application level
for multi-core MCUs (e.g. dual-core ESP32-S3).

**No compression, no encryption.**
Use `microcodec` for compression and `microcrypt` for encryption before storing.
Both are part of the micro-toolkit ecosystem.

## Test coverage

The repository covers:

- KV engine behavior and overflow variants
- TS engine behavior and overflow variants
- REL schema, indexing, and iteration
- WAL recovery, corruption handling, and disabled-WAL mode
- integration flows across RAM-only and persistent modes
- RAM budget variants from 8 KB through 1024 KB
- compile-fail validation for invalid percentage sums

## Part of micro-toolkit

microdb is intended to sit alongside the rest of the micro-toolkit stack:

- `microcodec` for compact serialization and compression
- `microcrypt` for encryption and authentication
- transport or protocol layers above the database as needed by the application

## Wiki

GitHub Wiki source pages are stored in [`wiki/`](wiki).
That keeps documentation versioned in the main repository and ready to publish into the GitHub wiki repo.

## License

MIT.
