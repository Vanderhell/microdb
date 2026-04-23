// SPDX-License-Identifier: MIT
#include "lox_backend_managed_adapter.h"

#include <string.h>

void lox_backend_managed_expectations_default(lox_backend_managed_expectations_t *out_expectations) {
    if (out_expectations == NULL) {
        return;
    }
    memset(out_expectations, 0, sizeof(*out_expectations));
    out_expectations->require_byte_write = 1u;
    out_expectations->require_sync_probe_on_mount = 1u;
}

static lox_err_t managed_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    lox_backend_managed_adapter_ctx_t *adapter = (lox_backend_managed_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL || adapter->mounted == 0u || buf == NULL) {
        return LOX_ERR_INVALID;
    }
    return adapter->raw_storage->read(adapter->raw_storage->ctx, offset, buf, len);
}

static lox_err_t managed_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    lox_backend_managed_adapter_ctx_t *adapter = (lox_backend_managed_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL || adapter->mounted == 0u || buf == NULL) {
        return LOX_ERR_INVALID;
    }
    if (len == 0u) {
        return LOX_OK;
    }
    return adapter->raw_storage->write(adapter->raw_storage->ctx, offset, buf, len);
}

static lox_err_t managed_erase(void *ctx, uint32_t offset) {
    lox_backend_managed_adapter_ctx_t *adapter = (lox_backend_managed_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL || adapter->mounted == 0u) {
        return LOX_ERR_INVALID;
    }
    return adapter->raw_storage->erase(adapter->raw_storage->ctx, offset);
}

static lox_err_t managed_sync(void *ctx) {
    lox_backend_managed_adapter_ctx_t *adapter = (lox_backend_managed_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL || adapter->mounted == 0u) {
        return LOX_ERR_INVALID;
    }
    return adapter->raw_storage->sync(adapter->raw_storage->ctx);
}

lox_err_t lox_backend_managed_adapter_init(lox_storage_t *out_storage,
                                                   lox_backend_managed_adapter_ctx_t *adapter_ctx,
                                                   lox_storage_t *raw_storage) {
    lox_backend_managed_expectations_t defaults;
    lox_backend_managed_expectations_default(&defaults);
    return lox_backend_managed_adapter_init_with_expectations(out_storage, adapter_ctx, raw_storage, &defaults);
}

lox_err_t lox_backend_managed_adapter_init_with_expectations(
    lox_storage_t *out_storage,
    lox_backend_managed_adapter_ctx_t *adapter_ctx,
    lox_storage_t *raw_storage,
    const lox_backend_managed_expectations_t *expectations) {
    lox_backend_managed_expectations_t defaults;

    if (out_storage == NULL || adapter_ctx == NULL || raw_storage == NULL) {
        return LOX_ERR_INVALID;
    }
    if (expectations == NULL) {
        lox_backend_managed_expectations_default(&defaults);
        expectations = &defaults;
    }
    if (raw_storage->read == NULL || raw_storage->write == NULL || raw_storage->erase == NULL || raw_storage->sync == NULL) {
        return LOX_ERR_INVALID;
    }
    if (raw_storage->capacity == 0u || raw_storage->erase_size == 0u) {
        return LOX_ERR_INVALID;
    }
    if (expectations->require_byte_write != 0u && raw_storage->write_size != 1u) {
        return LOX_ERR_INVALID;
    }
    if (expectations->require_sync_probe_on_mount != 0u) {
        lox_err_t probe_rc = raw_storage->sync(raw_storage->ctx);
        if (probe_rc != LOX_OK) {
            return LOX_ERR_STORAGE;
        }
    }

    memset(out_storage, 0, sizeof(*out_storage));
    memset(adapter_ctx, 0, sizeof(*adapter_ctx));

    adapter_ctx->raw_storage = raw_storage;
    adapter_ctx->mounted = 1u;

    out_storage->read = managed_read;
    out_storage->write = managed_write;
    out_storage->erase = managed_erase;
    out_storage->sync = managed_sync;
    out_storage->capacity = raw_storage->capacity;
    out_storage->erase_size = raw_storage->erase_size;
    out_storage->write_size = 1u;
    out_storage->ctx = adapter_ctx;
    return LOX_OK;
}

void lox_backend_managed_adapter_deinit(lox_storage_t *out_storage) {
    lox_backend_managed_adapter_ctx_t *adapter;

    if (out_storage == NULL || out_storage->ctx == NULL) {
        return;
    }

    adapter = (lox_backend_managed_adapter_ctx_t *)out_storage->ctx;
    adapter->mounted = 0u;
    adapter->raw_storage = NULL;

    memset(out_storage, 0, sizeof(*out_storage));
}
