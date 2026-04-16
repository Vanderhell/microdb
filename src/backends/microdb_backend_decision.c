#include "microdb_backend_decision.h"

#include "microdb_backend_adapter.h"

microdb_backend_open_result_t microdb_backend_decide_by_name(const char *backend_name,
                                                             uint32_t storage_write_size,
                                                             uint32_t storage_erase_size,
                                                             uint8_t has_aligned_adapter,
                                                             uint8_t has_managed_adapter) {
    const microdb_backend_adapter_t *adapter = microdb_backend_registry_find(backend_name);
    if (adapter == NULL) {
        microdb_backend_open_result_t out;
        out.mode = MICRODB_BACKEND_OPEN_UNSUPPORTED;
        out.reason = MICRODB_BACKEND_REASON_INVALID_CAPABILITY;
        return out;
    }

    return microdb_backend_classify_open(&adapter->capability,
                                         storage_write_size,
                                         storage_erase_size,
                                         has_aligned_adapter,
                                         has_managed_adapter);
}
