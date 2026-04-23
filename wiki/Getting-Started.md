# Getting Started

## Add loxdb to a CMake project

```cmake
add_subdirectory(loxdb)
target_link_libraries(your_app PRIVATE loxdb)
```

## Minimal initialization

```c
#define LOX_RAM_KB 32
#include "lox.h"

static lox_t db;

lox_cfg_t cfg = {
    .storage = NULL,
    .now = NULL,
};

lox_init(&db, &cfg);
```

`cfg.storage = NULL` means RAM-only mode.
Provide a storage HAL when you want persistence and WAL recovery.

## Basic usage

```c
float temp = 23.5f;

lox_kv_put(&db, "temperature", &temp, sizeof(temp));

lox_ts_register(&db, "sensor", LOX_TS_F32, 0);
lox_ts_insert(&db, "sensor", time_now(), &temp);
```

For relational data:

```c
lox_schema_t schema;
lox_schema_init(&schema, "devices", 32);
lox_schema_add(&schema, "id", LOX_COL_U16, 2, true);
lox_schema_add(&schema, "name", LOX_COL_STR, 16, false);
lox_schema_seal(&schema);
lox_table_create(&db, &schema);
```

## When to use loxdb

Use loxdb when you want:

- predictable memory usage
- a fixed RAM budget
- simple persistence for embedded devices
- one small library covering KV, TS, and relational storage

Use SQLite instead when you need dynamic schemas, SQL, concurrent access, or large indexes.

For full API and integration details, use repository doc:

- `docs/PROGRAMMER_MANUAL.md`
