// SPDX-License-Identifier: MIT
#include "microdb_backend_managed_adapter.h"

#include <string.h>

void microdb_backend_managed_expectations_default(microdb_backend_managed_expectations_t *out_expectations) {
    if (out_expectations == NULL) {
        return;
    }
    memset(out_expectations, 0, sizeof(*out_expectations));
    out_expectations->require_byte_write = 1u;
    out_expectations->require_sync_probe_on_mount = 1u;
}

static microdb_err_t managed_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    microdb_backend_managed_adapter_ctx_t *adapter = (microdb_backend_managed_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL || adapter->mounted == 0u || buf == NULL) {
        return MICRODB_ERR_INVALID;
    }
    return adapter->raw_storage->read(adapter->raw_storage->ctx, offset, buf, len);
}

static microdb_err_t managed_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    microdb_backend_managed_adapter_ctx_t *adapter = (microdb_backend_managed_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL || adapter->mounted == 0u || buf == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (len == 0u) {
        return MICRODB_OK;
    }
    return adapter->raw_storage->write(adapter->raw_storage->ctx, offset, buf, len);
}

static microdb_err_t managed_erase(void *ctx, uint32_t offset) {
    microdb_backend_managed_adapter_ctx_t *adapter = (microdb_backend_managed_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL || adapter->mounted == 0u) {
        return MICRODB_ERR_INVALID;
    }
    return adapter->raw_storage->erase(adapter->raw_storage->ctx, offset);
}

static microdb_err_t managed_sync(void *ctx) {
    microdb_backend_managed_adapter_ctx_t *adapter = (microdb_backend_managed_adapter_ctx_t *)ctx;
    if (adapter == NULL || adapter->raw_storage == NULL || adapter->mounted == 0u) {
        return MICRODB_ERR_INVALID;
    }
    return adapter->raw_storage->sync(adapter->raw_storage->ctx);
}

microdb_err_t microdb_backend_managed_adapter_init(microdb_storage_t *out_storage,
                                                   microdb_backend_managed_adapter_ctx_t *adapter_ctx,
                                                   microdb_storage_t *raw_storage) {
    microdb_backend_managed_expectations_t defaults;
    microdb_backend_managed_expectations_default(&defaults);
    return microdb_backend_managed_adapter_init_with_expectations(out_storage, adapter_ctx, raw_storage, &defaults);
}

microdb_err_t microdb_backend_managed_adapter_init_with_expectations(
    microdb_storage_t *out_storage,
    microdb_backend_managed_adapter_ctx_t *adapter_ctx,
    microdb_storage_t *raw_storage,
    const microdb_backend_managed_expectations_t *expectations) {
    microdb_backend_managed_expectations_t defaults;

    if (out_storage == NULL || adapter_ctx == NULL || raw_storage == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (expectations == NULL) {
        microdb_backend_managed_expectations_default(&defaults);
        expectations = &defaults;
    }
    if (raw_storage->read == NULL || raw_storage->write == NULL || raw_storage->erase == NULL || raw_storage->sync == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (raw_storage->capacity == 0u || raw_storage->erase_size == 0u) {
        return MICRODB_ERR_INVALID;
    }
    if (expectations->require_byte_write != 0u && raw_storage->write_size != 1u) {
        return MICRODB_ERR_INVALID;
    }
    if (expectations->require_sync_probe_on_mount != 0u) {
        microdb_err_t probe_rc = raw_storage->sync(raw_storage->ctx);
        if (probe_rc != MICRODB_OK) {
            return MICRODB_ERR_STORAGE;
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
    return MICRODB_OK;
}

void microdb_backend_managed_adapter_deinit(microdb_storage_t *out_storage) {
    microdb_backend_managed_adapter_ctx_t *adapter;

    if (out_storage == NULL || out_storage->ctx == NULL) {
        return;
    }

    adapter = (microdb_backend_managed_adapter_ctx_t *)out_storage->ctx;
    adapter->mounted = 0u;
    adapter->raw_storage = NULL;

    memset(out_storage, 0, sizeof(*out_storage));
}
