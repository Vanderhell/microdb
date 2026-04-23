// SPDX-License-Identifier: MIT
#include "lox_backend_decision.h"

#include "lox_backend_adapter.h"

lox_backend_open_result_t lox_backend_decide_by_name(const char *backend_name,
                                                             uint32_t storage_write_size,
                                                             uint32_t storage_erase_size,
                                                             uint8_t has_aligned_adapter,
                                                             uint8_t has_managed_adapter) {
    const lox_backend_adapter_t *adapter = lox_backend_registry_find(backend_name);
    if (adapter == NULL) {
        lox_backend_open_result_t out;
        out.mode = LOX_BACKEND_OPEN_UNSUPPORTED;
        out.reason = LOX_BACKEND_REASON_BACKEND_NOT_REGISTERED;
        return out;
    }

    return lox_backend_classify_open(&adapter->capability,
                                         storage_write_size,
                                         storage_erase_size,
                                         has_aligned_adapter,
                                         has_managed_adapter);
}
