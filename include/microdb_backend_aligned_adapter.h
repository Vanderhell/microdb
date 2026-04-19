// SPDX-License-Identifier: MIT
#ifndef MICRODB_BACKEND_ALIGNED_ADAPTER_H
#define MICRODB_BACKEND_ALIGNED_ADAPTER_H

#include "microdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    microdb_storage_t *raw_storage;
    uint8_t *bounce_buf;
    uint32_t bounce_len;
} microdb_backend_aligned_adapter_ctx_t;

microdb_err_t microdb_backend_aligned_adapter_init(microdb_storage_t *out_storage,
                                                   microdb_backend_aligned_adapter_ctx_t *adapter_ctx,
                                                   microdb_storage_t *raw_storage);

void microdb_backend_aligned_adapter_deinit(microdb_storage_t *out_storage);

#ifdef __cplusplus
}
#endif

#endif
