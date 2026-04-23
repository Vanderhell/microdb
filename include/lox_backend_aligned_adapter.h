// SPDX-License-Identifier: MIT
#ifndef LOX_BACKEND_ALIGNED_ADAPTER_H
#define LOX_BACKEND_ALIGNED_ADAPTER_H

#include "lox.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    lox_storage_t *raw_storage;
    uint8_t *bounce_buf;
    uint32_t bounce_len;
} lox_backend_aligned_adapter_ctx_t;

lox_err_t lox_backend_aligned_adapter_init(lox_storage_t *out_storage,
                                                   lox_backend_aligned_adapter_ctx_t *adapter_ctx,
                                                   lox_storage_t *raw_storage);

void lox_backend_aligned_adapter_deinit(lox_storage_t *out_storage);

#ifdef __cplusplus
}
#endif

#endif
