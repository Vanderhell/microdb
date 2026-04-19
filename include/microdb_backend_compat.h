// SPDX-License-Identifier: MIT
#ifndef MICRODB_BACKEND_COMPAT_H
#define MICRODB_BACKEND_COMPAT_H

#include <stdint.h>

#include "microdb_backend_adapter.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    MICRODB_BACKEND_OPEN_DIRECT = 0,
    MICRODB_BACKEND_OPEN_VIA_ADAPTER = 1,
    MICRODB_BACKEND_OPEN_UNSUPPORTED = 2
} microdb_backend_open_mode_t;

typedef enum {
    MICRODB_BACKEND_REASON_NONE = 0,
    MICRODB_BACKEND_REASON_BACKEND_NOT_REGISTERED = 1,
    MICRODB_BACKEND_REASON_INVALID_CAPABILITY = 2,
    MICRODB_BACKEND_REASON_INVALID_STORAGE_CONTRACT = 3,
    MICRODB_BACKEND_REASON_BYTE_WRITE_NOT_SUPPORTED = 4,
    MICRODB_BACKEND_REASON_MISSING_ALIGNED_ADAPTER = 5,
    MICRODB_BACKEND_REASON_MISSING_MANAGED_ADAPTER = 6
} microdb_backend_open_reason_t;

typedef struct {
    microdb_backend_open_mode_t mode;
    microdb_backend_open_reason_t reason;
} microdb_backend_open_result_t;

microdb_backend_open_result_t microdb_backend_classify_open(const microdb_storage_capability_t *capability,
                                                            uint32_t storage_write_size,
                                                            uint32_t storage_erase_size,
                                                            uint8_t has_aligned_adapter,
                                                            uint8_t has_managed_adapter);

#ifdef __cplusplus
}
#endif

#endif
