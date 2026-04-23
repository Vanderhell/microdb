// SPDX-License-Identifier: MIT
#ifndef LOX_BACKEND_DECISION_H
#define LOX_BACKEND_DECISION_H

#include <stdint.h>

#include "lox_backend_compat.h"

#ifdef __cplusplus
extern "C" {
#endif

lox_backend_open_result_t lox_backend_decide_by_name(const char *backend_name,
                                                             uint32_t storage_write_size,
                                                             uint32_t storage_erase_size,
                                                             uint8_t has_aligned_adapter,
                                                             uint8_t has_managed_adapter);

#ifdef __cplusplus
}
#endif

#endif
