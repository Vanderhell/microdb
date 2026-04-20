// SPDX-License-Identifier: MIT
#include "microdb.h"

#include <stdint.h>
#include <string.h>

/*
 * FreeRTOS port skeleton for microdb.
 *
 * Replace TODO blocks with your platform driver + FreeRTOS mutex primitives.
 * This file is intentionally not wired into repo CMake targets; use it as a template.
 */

typedef struct {
    /* TODO: replace with your real storage driver context. */
    void *driver_ctx;
    /* TODO: replace with real SemaphoreHandle_t (from FreeRTOS semphr.h). */
    void *mutex;
} app_freertos_ctx_t;

static app_freertos_ctx_t g_ctx;
static microdb_storage_t g_storage;
static microdb_t g_db;

static microdb_err_t app_storage_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    app_freertos_ctx_t *c = (app_freertos_ctx_t *)ctx;
    (void)c;
    (void)offset;
    (void)buf;
    (void)len;
    /* TODO: map to your platform read API. */
    return MICRODB_ERR_DISABLED;
}

static microdb_err_t app_storage_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    app_freertos_ctx_t *c = (app_freertos_ctx_t *)ctx;
    (void)c;
    (void)offset;
    (void)buf;
    (void)len;
    /* TODO: map to your platform write API. */
    return MICRODB_ERR_DISABLED;
}

static microdb_err_t app_storage_erase(void *ctx, uint32_t offset) {
    app_freertos_ctx_t *c = (app_freertos_ctx_t *)ctx;
    (void)c;
    (void)offset;
    /* TODO: erase one block at offset. */
    return MICRODB_ERR_DISABLED;
}

static microdb_err_t app_storage_sync(void *ctx) {
    app_freertos_ctx_t *c = (app_freertos_ctx_t *)ctx;
    (void)c;
    /* TODO: ensure pending storage ops are durable before return. */
    return MICRODB_ERR_DISABLED;
}

/*
 * Lock hook mapping.
 * Replace with:
 * - lock_create: xSemaphoreCreateMutex
 * - lock: xSemaphoreTake
 * - unlock: xSemaphoreGive
 * - lock_destroy: vSemaphoreDelete
 */
static void *app_lock_create(void) {
    /* TODO: return created mutex handle. */
    return g_ctx.mutex;
}

static void app_lock(void *hdl) {
    (void)hdl;
    /* TODO: xSemaphoreTake(...) */
}

static void app_unlock(void *hdl) {
    (void)hdl;
    /* TODO: xSemaphoreGive(...) */
}

static void app_lock_destroy(void *hdl) {
    (void)hdl;
    /* TODO: vSemaphoreDelete(...) */
}

static void app_storage_fill(microdb_storage_t *s, app_freertos_ctx_t *ctx) {
    memset(s, 0, sizeof(*s));
    s->read = app_storage_read;
    s->write = app_storage_write;
    s->erase = app_storage_erase;
    s->sync = app_storage_sync;
    s->capacity = 0u;   /* TODO: set bytes from your partition/device. */
    s->erase_size = 0u; /* TODO: set erase block size (>0). */
    s->write_size = 1u; /* Core contract for direct mode. */
    s->ctx = ctx;
}

int app_microdb_init(void) {
    microdb_cfg_t cfg;

    memset(&cfg, 0, sizeof(cfg));
    memset(&g_ctx, 0, sizeof(g_ctx));
    app_storage_fill(&g_storage, &g_ctx);

    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.lock_create = app_lock_create;
    cfg.lock = app_lock;
    cfg.unlock = app_unlock;
    cfg.lock_destroy = app_lock_destroy;

    return microdb_init(&g_db, &cfg);
}

