// SPDX-License-Identifier: MIT
#ifndef LOX_BACKEND_OPEN_H
#define LOX_BACKEND_OPEN_H

#include <stdint.h>

#include "lox.h"
#include "lox_backend_aligned_adapter.h"
#include "lox_backend_decision.h"
#include "lox_backend_fs_adapter.h"
#include "lox_backend_managed_adapter.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    lox_storage_t adapted_storage;
    lox_backend_aligned_adapter_ctx_t aligned_ctx;
    lox_backend_fs_adapter_ctx_t fs_ctx;
    lox_backend_managed_adapter_ctx_t managed_ctx;
    uint8_t using_aligned_adapter;
    uint8_t using_fs_adapter;
    uint8_t using_managed_adapter;
    lox_backend_open_result_t last_decision;
} lox_backend_open_session_t;

void lox_backend_open_session_reset(lox_backend_open_session_t *session);

lox_err_t lox_backend_open_prepare(const char *backend_name,
                                           lox_storage_t *raw_storage,
                                           uint8_t has_aligned_adapter,
                                           uint8_t has_managed_adapter,
                                           lox_backend_open_session_t *session,
                                           lox_storage_t **out_storage);

void lox_backend_open_release(lox_backend_open_session_t *session);

#ifdef __cplusplus
}
#endif

#endif
