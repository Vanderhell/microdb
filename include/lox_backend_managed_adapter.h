// SPDX-License-Identifier: MIT
#ifndef LOX_BACKEND_MANAGED_ADAPTER_H
#define LOX_BACKEND_MANAGED_ADAPTER_H

#include "lox.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    lox_storage_t *raw_storage;
    uint8_t mounted;
} lox_backend_managed_adapter_ctx_t;

typedef struct {
    uint8_t require_byte_write;
    uint8_t require_sync_probe_on_mount;
} lox_backend_managed_expectations_t;

void lox_backend_managed_expectations_default(lox_backend_managed_expectations_t *out_expectations);

lox_err_t lox_backend_managed_adapter_init(lox_storage_t *out_storage,
                                                   lox_backend_managed_adapter_ctx_t *adapter_ctx,
                                                   lox_storage_t *raw_storage);

lox_err_t lox_backend_managed_adapter_init_with_expectations(
    lox_storage_t *out_storage,
    lox_backend_managed_adapter_ctx_t *adapter_ctx,
    lox_storage_t *raw_storage,
    const lox_backend_managed_expectations_t *expectations);

void lox_backend_managed_adapter_deinit(lox_storage_t *out_storage);

#ifdef __cplusplus
}
#endif

#endif
