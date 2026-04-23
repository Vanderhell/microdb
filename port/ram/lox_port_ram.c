// SPDX-License-Identifier: MIT
#include "lox_port_ram.h"

#include <stdlib.h>
#include <string.h>

static lox_err_t lox_port_ram_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    lox_port_ram_ctx_t *ram = (lox_port_ram_ctx_t *)ctx;

    if (ram == NULL || buf == NULL || ((size_t)offset + len) > ram->capacity) {
        return LOX_ERR_STORAGE;
    }

    memcpy(buf, ram->buf + offset, len);
    return LOX_OK;
}

static lox_err_t lox_port_ram_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    lox_port_ram_ctx_t *ram = (lox_port_ram_ctx_t *)ctx;

    if (ram == NULL || buf == NULL || ((size_t)offset + len) > ram->capacity) {
        return LOX_ERR_STORAGE;
    }

    memcpy(ram->buf + offset, buf, len);
    return LOX_OK;
}

static lox_err_t lox_port_ram_erase(void *ctx, uint32_t offset) {
    lox_port_ram_ctx_t *ram = (lox_port_ram_ctx_t *)ctx;
    uint32_t block_start;

    if (ram == NULL || offset >= ram->capacity) {
        return LOX_ERR_STORAGE;
    }

    block_start = (offset / ram->erase_size) * ram->erase_size;
    memset(ram->buf + block_start, 0xFF, ram->erase_size);
    return LOX_OK;
}

static lox_err_t lox_port_ram_sync(void *ctx) {
    if (ctx == NULL) {
        return LOX_ERR_STORAGE;
    }
    return LOX_OK;
}

lox_err_t lox_port_ram_init(lox_storage_t *storage, uint32_t capacity) {
    lox_port_ram_ctx_t *ctx;

    if (storage == NULL || capacity == 0u) {
        return LOX_ERR_INVALID;
    }

    memset(storage, 0, sizeof(*storage));

    ctx = (lox_port_ram_ctx_t *)malloc(sizeof(*ctx));
    if (ctx == NULL) {
        return LOX_ERR_NO_MEM;
    }

    ctx->buf = (uint8_t *)malloc(capacity);
    if (ctx->buf == NULL) {
        free(ctx);
        return LOX_ERR_NO_MEM;
    }

    ctx->capacity = capacity;
    ctx->erase_size = 256u;
    memset(ctx->buf, 0xFF, capacity);

    storage->read = lox_port_ram_read;
    storage->write = lox_port_ram_write;
    storage->erase = lox_port_ram_erase;
    storage->sync = lox_port_ram_sync;
    storage->capacity = capacity;
    storage->erase_size = ctx->erase_size;
    storage->write_size = 1u;
    storage->ctx = ctx;
    return LOX_OK;
}

void lox_port_ram_deinit(lox_storage_t *storage) {
    lox_port_ram_ctx_t *ctx;

    if (storage == NULL || storage->ctx == NULL) {
        return;
    }

    ctx = (lox_port_ram_ctx_t *)storage->ctx;
    free(ctx->buf);
    free(ctx);
    memset(storage, 0, sizeof(*storage));
}
