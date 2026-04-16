#include "microdb_backend_open.h"

#include <string.h>

#include "microdb_backend_adapter.h"

void microdb_backend_open_session_reset(microdb_backend_open_session_t *session) {
    if (session == NULL) {
        return;
    }
    memset(session, 0, sizeof(*session));
    session->last_decision.mode = MICRODB_BACKEND_OPEN_UNSUPPORTED;
    session->last_decision.reason = MICRODB_BACKEND_REASON_INVALID_CAPABILITY;
}

microdb_err_t microdb_backend_open_prepare(const char *backend_name,
                                           microdb_storage_t *raw_storage,
                                           uint8_t has_aligned_adapter,
                                           uint8_t has_managed_adapter,
                                           microdb_backend_open_session_t *session,
                                           microdb_storage_t **out_storage) {
    const microdb_backend_adapter_t *adapter;

    if (backend_name == NULL || backend_name[0] == '\0' || raw_storage == NULL || session == NULL || out_storage == NULL) {
        return MICRODB_ERR_INVALID;
    }

    microdb_backend_open_session_reset(session);
    *out_storage = NULL;

    session->last_decision = microdb_backend_decide_by_name(backend_name,
                                                            raw_storage->write_size,
                                                            raw_storage->erase_size,
                                                            has_aligned_adapter,
                                                            has_managed_adapter);

    if (session->last_decision.mode == MICRODB_BACKEND_OPEN_UNSUPPORTED) {
        return MICRODB_ERR_INVALID;
    }
    if (session->last_decision.mode == MICRODB_BACKEND_OPEN_DIRECT) {
        *out_storage = raw_storage;
        return MICRODB_OK;
    }

    adapter = microdb_backend_registry_find(backend_name);
    if (adapter == NULL) {
        session->last_decision.mode = MICRODB_BACKEND_OPEN_UNSUPPORTED;
        session->last_decision.reason = MICRODB_BACKEND_REASON_BACKEND_NOT_REGISTERED;
        return MICRODB_ERR_NOT_FOUND;
    }

    if (adapter->capability.backend_class == MICRODB_BACKEND_CLASS_ALIGNED) {
        microdb_err_t rc =
            microdb_backend_aligned_adapter_init(&session->adapted_storage, &session->aligned_ctx, raw_storage);
        if (rc != MICRODB_OK) {
            return rc;
        }
        session->using_aligned_adapter = 1u;
        *out_storage = &session->adapted_storage;
        return MICRODB_OK;
    }

    if (adapter->capability.backend_class == MICRODB_BACKEND_CLASS_MANAGED) {
        return MICRODB_ERR_DISABLED;
    }

    return MICRODB_ERR_INVALID;
}

void microdb_backend_open_release(microdb_backend_open_session_t *session) {
    if (session == NULL) {
        return;
    }
    if (session->using_aligned_adapter != 0u) {
        microdb_backend_aligned_adapter_deinit(&session->adapted_storage);
    }
    microdb_backend_open_session_reset(session);
}
