// SPDX-License-Identifier: MIT
#ifndef MICRODB_BACKEND_FS_ADAPTER_H
#define MICRODB_BACKEND_FS_ADAPTER_H

#include "microdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    MICRODB_BACKEND_FS_SYNC_POLICY_NONE = 0,
    MICRODB_BACKEND_FS_SYNC_POLICY_EXPLICIT = 1,
    MICRODB_BACKEND_FS_SYNC_POLICY_WRITE_THROUGH = 2
} microdb_backend_fs_sync_policy_t;

typedef struct {
    microdb_storage_t *raw_storage;
    microdb_backend_fs_sync_policy_t sync_policy;
    uint8_t mounted;
} microdb_backend_fs_adapter_ctx_t;

typedef struct {
    uint8_t require_byte_write;
    uint8_t require_sync_probe_on_mount;
    microdb_backend_fs_sync_policy_t sync_policy;
} microdb_backend_fs_expectations_t;

void microdb_backend_fs_expectations_default(microdb_backend_fs_expectations_t *out_expectations);

microdb_err_t microdb_backend_fs_adapter_init(microdb_storage_t *out_storage,
                                              microdb_backend_fs_adapter_ctx_t *adapter_ctx,
                                              microdb_storage_t *raw_storage);

microdb_err_t microdb_backend_fs_adapter_init_with_expectations(microdb_storage_t *out_storage,
                                                                microdb_backend_fs_adapter_ctx_t *adapter_ctx,
                                                                microdb_storage_t *raw_storage,
                                                                const microdb_backend_fs_expectations_t *expectations);

void microdb_backend_fs_adapter_deinit(microdb_storage_t *out_storage);

#ifdef __cplusplus
}
#endif

#endif
