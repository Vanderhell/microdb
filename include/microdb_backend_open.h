#ifndef MICRODB_BACKEND_OPEN_H
#define MICRODB_BACKEND_OPEN_H

#include <stdint.h>

#include "microdb.h"
#include "microdb_backend_aligned_adapter.h"
#include "microdb_backend_decision.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    microdb_storage_t adapted_storage;
    microdb_backend_aligned_adapter_ctx_t aligned_ctx;
    uint8_t using_aligned_adapter;
    microdb_backend_open_result_t last_decision;
} microdb_backend_open_session_t;

void microdb_backend_open_session_reset(microdb_backend_open_session_t *session);

microdb_err_t microdb_backend_open_prepare(const char *backend_name,
                                           microdb_storage_t *raw_storage,
                                           uint8_t has_aligned_adapter,
                                           uint8_t has_managed_adapter,
                                           microdb_backend_open_session_t *session,
                                           microdb_storage_t **out_storage);

void microdb_backend_open_release(microdb_backend_open_session_t *session);

#ifdef __cplusplus
}
#endif

#endif
