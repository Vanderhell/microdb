// SPDX-License-Identifier: MIT
#include "lox.h"
#include "lox_backend_aligned_adapter.h"

#include <stdint.h>
#include <string.h>

/*
 * Non-byte-write integration skeleton (aligned/block media).
 *
 * Goal:
 * - raw driver exposes write granularity > 1 (for example 4 or 8 bytes)
 * - aligned adapter wraps raw storage and exposes byte-write contract to loxdb core
 *
 * This file is a reference template; it is not wired into repository CMake targets.
 */

typedef struct {
    /* TODO: replace with your real block/flash driver handle. */
    void *driver_ctx;
    /* Optional: keep runtime state for sync lifecycle tracking. */
    uint8_t dirty;
} app_block_ctx_t;

static app_block_ctx_t g_block_ctx;
static lox_storage_t g_raw_storage;
static lox_storage_t g_opened_storage;
static lox_backend_aligned_adapter_ctx_t g_aligned_ctx;
static lox_t g_db;

static lox_err_t app_raw_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    app_block_ctx_t *c = (app_block_ctx_t *)ctx;
    (void)c;
    (void)offset;
    (void)buf;
    (void)len;
    /* TODO: map to raw block driver read. */
    return LOX_ERR_DISABLED;
}

static lox_err_t app_raw_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    app_block_ctx_t *c = (app_block_ctx_t *)ctx;
    (void)c;
    (void)offset;
    (void)buf;
    (void)len;
    /*
     * TODO:
     * - map to raw block driver write with native alignment constraints,
     * - mark internal state dirty if your driver defers durability.
     */
    return LOX_ERR_DISABLED;
}

static lox_err_t app_raw_erase(void *ctx, uint32_t offset) {
    app_block_ctx_t *c = (app_block_ctx_t *)ctx;
    (void)c;
    (void)offset;
    /* TODO: erase exactly one erase block at offset. */
    return LOX_ERR_DISABLED;
}

static lox_err_t app_raw_sync(void *ctx) {
    app_block_ctx_t *c = (app_block_ctx_t *)ctx;
    (void)c;
    /*
     * TODO:
     * - flush deferred writes if needed,
     * - return LOX_OK only when durability contract is satisfied.
     */
    return LOX_ERR_DISABLED;
}

static void app_fill_raw_storage(lox_storage_t *raw, app_block_ctx_t *ctx) {
    memset(raw, 0, sizeof(*raw));
    raw->read = app_raw_read;
    raw->write = app_raw_write;
    raw->erase = app_raw_erase;
    raw->sync = app_raw_sync;
    raw->capacity = 256u * 1024u; /* Placeholder: replace with real medium capacity */
    raw->erase_size = 4096u;      /* Placeholder: replace with real erase block size (>0) */
    raw->write_size = 8u;  /* Example: non-byte-write medium */
    raw->ctx = ctx;
}

int app_lox_init_aligned(void) {
    lox_cfg_t cfg;
    lox_err_t rc;

    memset(&cfg, 0, sizeof(cfg));
    memset(&g_block_ctx, 0, sizeof(g_block_ctx));
    memset(&g_aligned_ctx, 0, sizeof(g_aligned_ctx));
    memset(&g_opened_storage, 0, sizeof(g_opened_storage));

    app_fill_raw_storage(&g_raw_storage, &g_block_ctx);

    /*
     * Adapter lifecycle:
     * 1) init adapter over raw non-byte-write storage
     * 2) pass adapted storage to lox_init
     * 3) deinit adapter at shutdown
     */
    rc = lox_backend_aligned_adapter_init(&g_opened_storage, &g_aligned_ctx, &g_raw_storage);
    if (rc != LOX_OK) {
        return rc;
    }

    cfg.storage = &g_opened_storage;
    cfg.ram_kb = 32u;

    rc = lox_init(&g_db, &cfg);
    if (rc != LOX_OK) {
        lox_backend_aligned_adapter_deinit(&g_opened_storage);
        return rc;
    }
    return LOX_OK;
}

int app_lox_deinit_aligned(void) {
    lox_err_t rc = lox_deinit(&g_db);
    lox_backend_aligned_adapter_deinit(&g_opened_storage);
    return rc;
}
