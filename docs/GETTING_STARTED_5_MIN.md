# Getting Started (5 min)

This is the fastest path to a working persistent `loxdb` integration.

## 1. Add the library

```cmake
add_subdirectory(loxdb)
target_link_libraries(your_app PRIVATE loxdb)
```

## 2. Provide storage HAL (or start RAM-only)

- RAM-only:
  - set `cfg.storage = NULL`
  - useful for feature bring-up
- persistent mode:
  - provide `read/write/erase/sync` callbacks in `lox_storage_t`
  - set correct `capacity`, `erase_size`, `write_size`
  - current storage contract: `erase_size > 0`, `write_size == 1` (other `write_size` values fail init)

## 3. Initialize with explicit profile-like budget

```c
#include "lox.h"

static lox_t db;
static lox_storage_t st; /* fill if persistent */

lox_cfg_t cfg = {0};
cfg.storage = &st;   /* or NULL for RAM-only */
cfg.ram_kb = 64;     /* start with balanced-like budget */
cfg.kv_pct = 40;
cfg.ts_pct = 40;
cfg.rel_pct = 20;

lox_err_t rc = lox_init(&db, &cfg);
```

## 4. Basic API flow

```c
/* KV */
uint32_t v = 123;
lox_kv_put(&db, "boot_count", &v, sizeof(v));

/* TS */
lox_ts_register(&db, "temp", LOX_TS_U32, 0);
lox_ts_insert(&db, "temp", 1, &v);

/* REL */
lox_schema_t s;
lox_table_t *t = NULL;
lox_schema_init(&s, "devices", 32);
lox_schema_add(&s, "id", LOX_COL_U32, sizeof(uint32_t), true);
lox_schema_add(&s, "state", LOX_COL_U8, sizeof(uint8_t), false);
lox_schema_seal(&s);
lox_table_create(&db, &s);
lox_table_get(&db, "devices", &t);
```

## 5. Shutdown / durability points

- call `lox_flush(&db)` at controlled checkpoints
- call `lox_compact(&db)` in maintenance windows
- call `lox_deinit(&db)` on clean shutdown

## 6. Read contracts before release

- fail codes: `docs/FAIL_CODE_CONTRACT.md`
- profile guarantees: `docs/PROFILE_GUARANTEES.md`
- hard verdict report: `docs/results/hard_verdict_20260412.md`

## 7. ESP32 bench quick check

Use the terminal bench and run:
- `run_det`
- `profile balanced` + `run`
- `profile stress` + `run`

Then check `metrics` and `config` output.

## 8. Full programmer guide

For complete API and integration coverage, see:

- `docs/PROGRAMMER_MANUAL.md`
