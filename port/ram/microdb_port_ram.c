// SPDX-License-Identifier: MIT
#include "microdb_port_ram.h"

#include <stdlib.h>
#include <string.h>

static microdb_err_t microdb_port_ram_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    microdb_port_ram_ctx_t *ram = (microdb_port_ram_ctx_t *)ctx;

    if (ram == NULL || buf == NULL || ((size_t)offset + len) > ram->capacity) {
        return MICRODB_ERR_STORAGE;
    }

    memcpy(buf, ram->buf + offset, len);
    return MICRODB_OK;
}

static microdb_err_t microdb_port_ram_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    microdb_port_ram_ctx_t *ram = (microdb_port_ram_ctx_t *)ctx;

    if (ram == NULL || buf == NULL || ((size_t)offset + len) > ram->capacity) {
        return MICRODB_ERR_STORAGE;
    }

    memcpy(ram->buf + offset, buf, len);
    return MICRODB_OK;
}

static microdb_err_t microdb_port_ram_erase(void *ctx, uint32_t offset) {
    microdb_port_ram_ctx_t *ram = (microdb_port_ram_ctx_t *)ctx;
    uint32_t block_start;

    if (ram == NULL || offset >= ram->capacity) {
        return MICRODB_ERR_STORAGE;
    }

    block_start = (offset / ram->erase_size) * ram->erase_size;
    memset(ram->buf + block_start, 0xFF, ram->erase_size);
    return MICRODB_OK;
}

static microdb_err_t microdb_port_ram_sync(void *ctx) {
    if (ctx == NULL) {
        return MICRODB_ERR_STORAGE;
    }
    return MICRODB_OK;
}

microdb_err_t microdb_port_ram_init(microdb_storage_t *storage, uint32_t capacity) {
    microdb_port_ram_ctx_t *ctx;

    if (storage == NULL || capacity == 0u) {
        return MICRODB_ERR_INVALID;
    }

    memset(storage, 0, sizeof(*storage));

    ctx = (microdb_port_ram_ctx_t *)malloc(sizeof(*ctx));
    if (ctx == NULL) {
        return MICRODB_ERR_NO_MEM;
    }

    ctx->buf = (uint8_t *)malloc(capacity);
    if (ctx->buf == NULL) {
        free(ctx);
        return MICRODB_ERR_NO_MEM;
    }

    ctx->capacity = capacity;
    ctx->erase_size = 256u;
    memset(ctx->buf, 0xFF, capacity);

    storage->read = microdb_port_ram_read;
    storage->write = microdb_port_ram_write;
    storage->erase = microdb_port_ram_erase;
    storage->sync = microdb_port_ram_sync;
    storage->capacity = capacity;
    storage->erase_size = ctx->erase_size;
    storage->write_size = 1u;
    storage->ctx = ctx;
    return MICRODB_OK;
}

void microdb_port_ram_deinit(microdb_storage_t *storage) {
    microdb_port_ram_ctx_t *ctx;

    if (storage == NULL || storage->ctx == NULL) {
        return;
    }

    ctx = (microdb_port_ram_ctx_t *)storage->ctx;
    free(ctx->buf);
    free(ctx);
    memset(storage, 0, sizeof(*storage));
}
