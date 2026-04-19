// SPDX-License-Identifier: MIT
#ifndef MICRODB_BACKEND_DECISION_H
#define MICRODB_BACKEND_DECISION_H

#include <stdint.h>

#include "microdb_backend_compat.h"

#ifdef __cplusplus
extern "C" {
#endif

microdb_backend_open_result_t microdb_backend_decide_by_name(const char *backend_name,
                                                             uint32_t storage_write_size,
                                                             uint32_t storage_erase_size,
                                                             uint8_t has_aligned_adapter,
                                                             uint8_t has_managed_adapter);

#ifdef __cplusplus
}
#endif

#endif
