// SPDX-License-Identifier: MIT
#ifndef LOX_BACKEND_COMPAT_H
#define LOX_BACKEND_COMPAT_H

#include <stdint.h>

#include "lox_backend_adapter.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    LOX_BACKEND_OPEN_DIRECT = 0,
    LOX_BACKEND_OPEN_VIA_ADAPTER = 1,
    LOX_BACKEND_OPEN_UNSUPPORTED = 2
} lox_backend_open_mode_t;

typedef enum {
    LOX_BACKEND_REASON_NONE = 0,
    LOX_BACKEND_REASON_BACKEND_NOT_REGISTERED = 1,
    LOX_BACKEND_REASON_INVALID_CAPABILITY = 2,
    LOX_BACKEND_REASON_INVALID_STORAGE_CONTRACT = 3,
    LOX_BACKEND_REASON_BYTE_WRITE_NOT_SUPPORTED = 4,
    LOX_BACKEND_REASON_MISSING_ALIGNED_ADAPTER = 5,
    LOX_BACKEND_REASON_MISSING_MANAGED_ADAPTER = 6
} lox_backend_open_reason_t;

typedef struct {
    lox_backend_open_mode_t mode;
    lox_backend_open_reason_t reason;
} lox_backend_open_result_t;

lox_backend_open_result_t lox_backend_classify_open(const lox_storage_capability_t *capability,
                                                            uint32_t storage_write_size,
                                                            uint32_t storage_erase_size,
                                                            uint8_t has_aligned_adapter,
                                                            uint8_t has_managed_adapter);

#ifdef __cplusplus
}
#endif

#endif
