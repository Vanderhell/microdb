// SPDX-License-Identifier: MIT
#include "lox_backend_open.h"

#include <string.h>

#include "lox_backend_adapter.h"

void lox_backend_open_session_reset(lox_backend_open_session_t *session) {
    if (session == NULL) {
        return;
    }
    memset(session, 0, sizeof(*session));
    session->last_decision.mode = LOX_BACKEND_OPEN_UNSUPPORTED;
    session->last_decision.reason = LOX_BACKEND_REASON_INVALID_CAPABILITY;
}

lox_err_t lox_backend_open_prepare(const char *backend_name,
                                           lox_storage_t *raw_storage,
                                           uint8_t has_aligned_adapter,
                                           uint8_t has_managed_adapter,
                                           lox_backend_open_session_t *session,
                                           lox_storage_t **out_storage) {
    const lox_backend_adapter_t *adapter;

    if (backend_name == NULL || backend_name[0] == '\0' || raw_storage == NULL || session == NULL || out_storage == NULL) {
        return LOX_ERR_INVALID;
    }

    lox_backend_open_session_reset(session);
    *out_storage = NULL;

    session->last_decision = lox_backend_decide_by_name(backend_name,
                                                            raw_storage->write_size,
                                                            raw_storage->erase_size,
                                                            has_aligned_adapter,
                                                            has_managed_adapter);

    if (session->last_decision.mode == LOX_BACKEND_OPEN_UNSUPPORTED) {
        return LOX_ERR_INVALID;
    }
    if (session->last_decision.mode == LOX_BACKEND_OPEN_DIRECT) {
        *out_storage = raw_storage;
        return LOX_OK;
    }

    adapter = lox_backend_registry_find(backend_name);
    if (adapter == NULL) {
        session->last_decision.mode = LOX_BACKEND_OPEN_UNSUPPORTED;
        session->last_decision.reason = LOX_BACKEND_REASON_BACKEND_NOT_REGISTERED;
        return LOX_ERR_NOT_FOUND;
    }

    if (adapter->capability.backend_class == LOX_BACKEND_CLASS_ALIGNED) {
        lox_err_t rc =
            lox_backend_aligned_adapter_init(&session->adapted_storage, &session->aligned_ctx, raw_storage);
        if (rc != LOX_OK) {
            return rc;
        }
        session->using_aligned_adapter = 1u;
        *out_storage = &session->adapted_storage;
        return LOX_OK;
    }

    if (adapter->capability.backend_class == LOX_BACKEND_CLASS_MANAGED) {
        if (adapter->capability.sync_semantics == LOX_SYNC_SEMANTICS_DURABLE_SYNC) {
            lox_err_t rc =
                lox_backend_managed_adapter_init(&session->adapted_storage, &session->managed_ctx, raw_storage);
            if (rc != LOX_OK) {
                return rc;
            }
            session->using_managed_adapter = 1u;
            *out_storage = &session->adapted_storage;
            return LOX_OK;
        } else {
            lox_backend_fs_expectations_t fs_expectations;
            lox_err_t rc;
            lox_backend_fs_expectations_default(&fs_expectations);
            if (adapter->capability.sync_semantics == LOX_SYNC_SEMANTICS_NONE) {
                fs_expectations.sync_policy = LOX_BACKEND_FS_SYNC_POLICY_NONE;
                fs_expectations.require_sync_probe_on_mount = 0u;
            } else {
                fs_expectations.sync_policy = LOX_BACKEND_FS_SYNC_POLICY_EXPLICIT;
            }
            rc = lox_backend_fs_adapter_init_with_expectations(
                &session->adapted_storage, &session->fs_ctx, raw_storage, &fs_expectations);
            if (rc != LOX_OK) {
                return rc;
            }
            session->using_fs_adapter = 1u;
            *out_storage = &session->adapted_storage;
            return LOX_OK;
        }
    }

    return LOX_ERR_INVALID;
}

void lox_backend_open_release(lox_backend_open_session_t *session) {
    if (session == NULL) {
        return;
    }
    if (session->using_aligned_adapter != 0u) {
        lox_backend_aligned_adapter_deinit(&session->adapted_storage);
    }
    if (session->using_fs_adapter != 0u) {
        lox_backend_fs_adapter_deinit(&session->adapted_storage);
    }
    if (session->using_managed_adapter != 0u) {
        lox_backend_managed_adapter_deinit(&session->adapted_storage);
    }
    lox_backend_open_session_reset(session);
}
