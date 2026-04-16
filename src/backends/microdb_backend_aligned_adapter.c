#include "microdb_backend_aligned_adapter.h"

#include <stdlib.h>
#include <string.h>

static microdb_err_t aligned_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    microdb_backend_aligned_adapter_ctx_t *adapter = (microdb_backend_aligned_adapter_ctx_t *)ctx;
    microdb_storage_t *raw;
    uint32_t unit;
    uint64_t pos;
    uint64_t end;
    uint8_t *dst = (uint8_t *)buf;
    if (adapter == NULL || adapter->raw_storage == NULL || buf == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (len == 0u) {
        return MICRODB_OK;
    }

    raw = adapter->raw_storage;
    unit = raw->write_size;
    if (unit == 0u || adapter->bounce_buf == NULL || adapter->bounce_len != unit) {
        return MICRODB_ERR_INVALID;
    }
    if ((uint64_t)offset + (uint64_t)len > (uint64_t)raw->capacity) {
        return MICRODB_ERR_STORAGE;
    }

    if ((offset % unit) == 0u && (len % unit) == 0u) {
        return raw->read(raw->ctx, offset, buf, len);
    }

    pos = (uint64_t)offset;
    end = pos + (uint64_t)len;
    while (pos < end) {
        uint32_t block_base = (uint32_t)((pos / unit) * unit);
        uint32_t block_end = block_base + unit;
        uint64_t copy_start = pos;
        uint32_t copy_len = block_end - (uint32_t)copy_start;
        microdb_err_t rc;

        if ((uint64_t)block_end > (uint64_t)raw->capacity) {
            return MICRODB_ERR_STORAGE;
        }
        if ((uint64_t)copy_len > (end - copy_start)) {
            copy_len = (uint32_t)(end - copy_start);
        }

        rc = raw->read(raw->ctx, block_base, adapter->bounce_buf, unit);
        if (rc != MICRODB_OK) {
            return rc;
        }
        memcpy(dst + (copy_start - offset), adapter->bounce_buf + ((uint32_t)copy_start - block_base), copy_len);
        pos = copy_start + (uint64_t)copy_len;
    }

    return MICRODB_OK;
}

static microdb_err_t aligned_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    microdb_backend_aligned_adapter_ctx_t *adapter = (microdb_backend_aligned_adapter_ctx_t *)ctx;
    const uint8_t *src = (const uint8_t *)buf;
    microdb_storage_t *raw;
    uint32_t unit;
    uint64_t pos;
    uint64_t end;

    if (adapter == NULL || adapter->raw_storage == NULL || src == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (len == 0u) {
        return MICRODB_OK;
    }

    raw = adapter->raw_storage;
    unit = raw->write_size;
    if (unit == 0u || adapter->bounce_buf == NULL || adapter->bounce_len != unit) {
        return MICRODB_ERR_INVALID;
    }

    if ((uint64_t)offset + (uint64_t)len > (uint64_t)raw->capacity) {
        return MICRODB_ERR_STORAGE;
    }

    if ((offset % unit) == 0u && (len % unit) == 0u) {
        return raw->write(raw->ctx, offset, buf, len);
    }

    pos = (uint64_t)offset;
    end = pos + (uint64_t)len;
    while (pos < end) {
        uint32_t block_base = (uint32_t)((pos / unit) * unit);
        uint32_t block_end = block_base + unit;
        uint64_t copy_start = pos;
        uint32_t copy_len = block_end - (uint32_t)copy_start;
        microdb_err_t rc;

        if ((uint64_t)block_end > (uint64_t)raw->capacity) {
            return MICRODB_ERR_STORAGE;
        }
        if ((uint64_t)copy_len > (end - copy_start)) {
            copy_len = (uint32_t)(end - copy_start);
        }

        rc = raw->read(raw->ctx, block_base, adapter->bounce_buf, unit);
        if (rc != MICRODB_OK) {
            return rc;
        }

        memcpy(adapter->bounce_buf + ((uint32_t)copy_start - block_base), src + (copy_start - offset), copy_len);

        rc = raw->write(raw->ctx, block_base, adapter->bounce_buf, unit);
        if (rc != MICRODB_OK) {
            return rc;
        }

        pos = copy_start + (uint64_t)copy_len;
    }

    return MICRODB_OK;
}

static microdb_err_t aligned_erase(void *ctx, uint32_t offset) {
    microdb_backend_aligned_adapter_ctx_t *adapter = (microdb_backend_aligned_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL) {
        return MICRODB_ERR_INVALID;
    }
    return adapter->raw_storage->erase(adapter->raw_storage->ctx, offset);
}

static microdb_err_t aligned_sync(void *ctx) {
    microdb_backend_aligned_adapter_ctx_t *adapter = (microdb_backend_aligned_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL) {
        return MICRODB_ERR_INVALID;
    }
    return adapter->raw_storage->sync(adapter->raw_storage->ctx);
}

microdb_err_t microdb_backend_aligned_adapter_init(microdb_storage_t *out_storage,
                                                   microdb_backend_aligned_adapter_ctx_t *adapter_ctx,
                                                   microdb_storage_t *raw_storage) {
    if (out_storage == NULL || adapter_ctx == NULL || raw_storage == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (raw_storage->read == NULL || raw_storage->write == NULL || raw_storage->erase == NULL || raw_storage->sync == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (raw_storage->capacity == 0u || raw_storage->erase_size == 0u || raw_storage->write_size <= 1u) {
        return MICRODB_ERR_INVALID;
    }
    if ((raw_storage->capacity % raw_storage->write_size) != 0u) {
        return MICRODB_ERR_INVALID;
    }
    if ((raw_storage->erase_size % raw_storage->write_size) != 0u) {
        return MICRODB_ERR_INVALID;
    }

    memset(out_storage, 0, sizeof(*out_storage));
    memset(adapter_ctx, 0, sizeof(*adapter_ctx));

    adapter_ctx->bounce_buf = (uint8_t *)malloc(raw_storage->write_size);
    if (adapter_ctx->bounce_buf == NULL) {
        return MICRODB_ERR_NO_MEM;
    }

    adapter_ctx->raw_storage = raw_storage;
    adapter_ctx->bounce_len = raw_storage->write_size;

    out_storage->read = aligned_read;
    out_storage->write = aligned_write;
    out_storage->erase = aligned_erase;
    out_storage->sync = aligned_sync;
    out_storage->capacity = raw_storage->capacity;
    out_storage->erase_size = raw_storage->erase_size;
    out_storage->write_size = 1u;
    out_storage->ctx = adapter_ctx;
    return MICRODB_OK;
}

void microdb_backend_aligned_adapter_deinit(microdb_storage_t *out_storage) {
    microdb_backend_aligned_adapter_ctx_t *adapter;

    if (out_storage == NULL || out_storage->ctx == NULL) {
        return;
    }

    adapter = (microdb_backend_aligned_adapter_ctx_t *)out_storage->ctx;
    free(adapter->bounce_buf);
    adapter->bounce_buf = NULL;
    adapter->bounce_len = 0u;
    adapter->raw_storage = NULL;

    memset(out_storage, 0, sizeof(*out_storage));
}
