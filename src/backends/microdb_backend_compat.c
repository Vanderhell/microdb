// SPDX-License-Identifier: MIT
#include "microdb_backend_compat.h"

static microdb_backend_open_result_t make_result(microdb_backend_open_mode_t mode, microdb_backend_open_reason_t reason) {
    microdb_backend_open_result_t out;
    out.mode = mode;
    out.reason = reason;
    return out;
}

static int microdb_capability_shape_valid(const microdb_storage_capability_t *capability) {
    if (capability == 0) {
        return 0;
    }
    if (capability->minimal_write_unit == 0u || capability->erase_granularity == 0u ||
        capability->atomic_write_granularity == 0u) {
        return 0;
    }
    if (capability->atomic_write_granularity < capability->minimal_write_unit) {
        return 0;
    }
    if ((capability->atomic_write_granularity % capability->minimal_write_unit) != 0u) {
        return 0;
    }
    return 1;
}

microdb_backend_open_result_t microdb_backend_classify_open(const microdb_storage_capability_t *capability,
                                                            uint32_t storage_write_size,
                                                            uint32_t storage_erase_size,
                                                            uint8_t has_aligned_adapter,
                                                            uint8_t has_managed_adapter) {
    if (!microdb_capability_shape_valid(capability)) {
        return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_INVALID_CAPABILITY);
    }

    if (storage_write_size == 0u || storage_erase_size == 0u) {
        return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_INVALID_STORAGE_CONTRACT);
    }
    if (storage_write_size < capability->minimal_write_unit ||
        (storage_write_size % capability->minimal_write_unit) != 0u) {
        return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_INVALID_STORAGE_CONTRACT);
    }
    if (storage_erase_size < capability->erase_granularity ||
        (storage_erase_size % capability->erase_granularity) != 0u) {
        return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_INVALID_STORAGE_CONTRACT);
    }

    if (capability->backend_class == MICRODB_BACKEND_CLASS_BYTE) {
        if (storage_write_size == 1u && capability->minimal_write_unit == 1u) {
            return make_result(MICRODB_BACKEND_OPEN_DIRECT, MICRODB_BACKEND_REASON_NONE);
        }
        return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_BYTE_WRITE_NOT_SUPPORTED);
    }

    if (capability->backend_class == MICRODB_BACKEND_CLASS_ALIGNED) {
        if (has_aligned_adapter != 0u) {
            return make_result(MICRODB_BACKEND_OPEN_VIA_ADAPTER, MICRODB_BACKEND_REASON_NONE);
        }
        return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_MISSING_ALIGNED_ADAPTER);
    }

    if (capability->backend_class == MICRODB_BACKEND_CLASS_MANAGED) {
        if (storage_write_size != 1u) {
            return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_BYTE_WRITE_NOT_SUPPORTED);
        }
        if (has_managed_adapter != 0u) {
            return make_result(MICRODB_BACKEND_OPEN_VIA_ADAPTER, MICRODB_BACKEND_REASON_NONE);
        }
        return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_MISSING_MANAGED_ADAPTER);
    }

    return make_result(MICRODB_BACKEND_OPEN_UNSUPPORTED, MICRODB_BACKEND_REASON_INVALID_CAPABILITY);
}
