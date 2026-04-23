// SPDX-License-Identifier: MIT
#include "lox.h"

#include <stdint.h>
#include <string.h>

/*
 * Zephyr port skeleton for loxdb.
 *
 * Replace TODO blocks with real Zephyr storage + mutex integration.
 * This file is intentionally not wired into repo CMake targets; use it as a template.
 */

typedef struct {
    /* TODO: replace with your flash/partition handle context. */
    void *driver_ctx;
    /* TODO: replace with struct k_mutex (or pointer managed by your app). */
    void *mutex;
} app_zephyr_ctx_t;

static app_zephyr_ctx_t g_ctx;
static lox_storage_t g_storage;
static lox_t g_db;

static lox_err_t app_storage_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    app_zephyr_ctx_t *c = (app_zephyr_ctx_t *)ctx;
    (void)c;
    (void)offset;
    (void)buf;
    (void)len;
    /* TODO: map to Zephyr flash read API. */
    return LOX_ERR_DISABLED;
}

static lox_err_t app_storage_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    app_zephyr_ctx_t *c = (app_zephyr_ctx_t *)ctx;
    (void)c;
    (void)offset;
    (void)buf;
    (void)len;
    /* TODO: map to Zephyr flash write API. */
    return LOX_ERR_DISABLED;
}

static lox_err_t app_storage_erase(void *ctx, uint32_t offset) {
    app_zephyr_ctx_t *c = (app_zephyr_ctx_t *)ctx;
    (void)c;
    (void)offset;
    /* TODO: erase one block at offset via Zephyr API. */
    return LOX_ERR_DISABLED;
}

static lox_err_t app_storage_sync(void *ctx) {
    app_zephyr_ctx_t *c = (app_zephyr_ctx_t *)ctx;
    (void)c;
    /*
     * TODO:
     * - return LOX_OK only after pending writes/erase are durable,
     * - or map policy through backend-open adapter path.
     */
    return LOX_ERR_DISABLED;
}

/*
 * Lock hook mapping.
 * Replace with:
 * - lock_create: initialize k_mutex-backed handle
 * - lock: k_mutex_lock
 * - unlock: k_mutex_unlock
 * - lock_destroy: optional teardown
 */
static void *app_lock_create(void) {
    /* TODO: return lock handle used by lock/unlock callbacks. */
    return g_ctx.mutex;
}

static void app_lock(void *hdl) {
    (void)hdl;
    /* TODO: k_mutex_lock(...) */
}

static void app_unlock(void *hdl) {
    (void)hdl;
    /* TODO: k_mutex_unlock(...) */
}

static void app_lock_destroy(void *hdl) {
    (void)hdl;
    /* Optional: no-op for static mutexes. */
}

static void app_storage_fill(lox_storage_t *s, app_zephyr_ctx_t *ctx) {
    memset(s, 0, sizeof(*s));
    s->read = app_storage_read;
    s->write = app_storage_write;
    s->erase = app_storage_erase;
    s->sync = app_storage_sync;
    s->capacity = 0u;   /* TODO: set bytes from selected Zephyr partition. */
    s->erase_size = 0u; /* TODO: set erase block size (>0). */
    s->write_size = 1u; /* Core contract for direct mode. */
    s->ctx = ctx;
}

int app_lox_init(void) {
    lox_cfg_t cfg;

    memset(&cfg, 0, sizeof(cfg));
    memset(&g_ctx, 0, sizeof(g_ctx));
    app_storage_fill(&g_storage, &g_ctx);

    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.lock_create = app_lock_create;
    cfg.lock = app_lock;
    cfg.unlock = app_unlock;
    cfg.lock_destroy = app_lock_destroy;

    return lox_init(&g_db, &cfg);
}

