# Getting Started

## Add microdb to a CMake project

```cmake
add_subdirectory(microdb)
target_link_libraries(your_app PRIVATE microdb)
```

## Minimal initialization

```c
#define MICRODB_RAM_KB 32
#include "microdb.h"

static microdb_t db;

microdb_cfg_t cfg = {
    .storage = NULL,
    .now = NULL,
};

microdb_init(&db, &cfg);
```

`cfg.storage = NULL` means RAM-only mode.
Provide a storage HAL when you want persistence and WAL recovery.

## Basic usage

```c
float temp = 23.5f;

microdb_kv_put(&db, "temperature", &temp, sizeof(temp));

microdb_ts_register(&db, "sensor", MICRODB_TS_F32, 0);
microdb_ts_insert(&db, "sensor", time_now(), &temp);
```

For relational data:

```c
microdb_schema_t schema;
microdb_schema_init(&schema, "devices", 32);
microdb_schema_add(&schema, "id", MICRODB_COL_U16, 2, true);
microdb_schema_add(&schema, "name", MICRODB_COL_STR, 16, false);
microdb_schema_seal(&schema);
microdb_table_create(&db, &schema);
```

## When to use microdb

Use microdb when you want:

- predictable memory usage
- a fixed RAM budget
- simple persistence for embedded devices
- one small library covering KV, TS, and relational storage

Use SQLite instead when you need dynamic schemas, SQL, concurrent access, or large indexes.
