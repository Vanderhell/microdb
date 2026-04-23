# Thread Safety Hooks

loxdb exposes four optional lock hooks in `lox_cfg_t`:

- `lock_create`: called once during `lox_init`.
- `lock`: called at entry of selected public API calls when `LOX_THREAD_SAFE=1`.
- `unlock`: called at exit of those API calls.
- `lock_destroy`: called once during `lox_deinit`.

If hooks are `NULL`, locking is a no-op.

## Callback And Copying Notes

- `lox_kv_iter`, `lox_ts_query`, `lox_rel_find`, and `lox_rel_iter` invoke callbacks without DB lock held, then re-lock before continuing.
- `lox_rel_find` and `lox_rel_iter` detect concurrent table mutation after re-lock and return `LOX_ERR_MODIFIED`.
- `lox_rel_find_by` copies row bytes into caller `out_buf` while lock is still held. For larger row sizes this can increase lock hold time and create latency spikes for contending threads.

## FreeRTOS Example

```c
#include "lox.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

static void *db_lock_create(void) {
    return (void *)xSemaphoreCreateMutex();
}

static void db_lock(void *hdl) {
    xSemaphoreTake((SemaphoreHandle_t)hdl, portMAX_DELAY);
}

static void db_unlock(void *hdl) {
    xSemaphoreGive((SemaphoreHandle_t)hdl);
}

static void db_lock_destroy(void *hdl) {
    vSemaphoreDelete((SemaphoreHandle_t)hdl);
}

lox_cfg_t cfg = {
    .storage = NULL,
    .ram_kb = 32u,
    .lock_create = db_lock_create,
    .lock = db_lock,
    .unlock = db_unlock,
    .lock_destroy = db_lock_destroy
};
```

## Bare-Metal No-Op Example

```c
#include "lox.h"

lox_cfg_t cfg = {
    .storage = NULL,
    .ram_kb = 32u,
    .lock_create = NULL,
    .lock = NULL,
    .unlock = NULL,
    .lock_destroy = NULL
};
```
