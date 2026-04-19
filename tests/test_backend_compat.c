// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb_backend_compat.h"

#include <string.h>

static microdb_storage_capability_t base_capability(microdb_backend_class_t cls) {
    microdb_storage_capability_t cap;
    memset(&cap, 0, sizeof(cap));
    cap.backend_class = cls;
    cap.minimal_write_unit = 1u;
    cap.erase_granularity = 256u;
    cap.atomic_write_granularity = 1u;
    cap.sync_semantics = MICRODB_SYNC_SEMANTICS_DURABLE_SYNC;
    cap.is_managed = (cls == MICRODB_BACKEND_CLASS_MANAGED) ? 1u : 0u;
    return cap;
}

static void setup_noop(void) {
}

static void teardown_noop(void) {
}

MDB_TEST(backend_compat_byte_direct) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_BYTE);
    microdb_backend_open_result_t r = microdb_backend_classify_open(&cap, 1u, 256u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_DIRECT);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_NONE);
}

MDB_TEST(backend_compat_byte_write_gt1_unsupported) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_BYTE);
    microdb_backend_open_result_t r = microdb_backend_classify_open(&cap, 4u, 256u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_BYTE_WRITE_NOT_SUPPORTED);
}

MDB_TEST(backend_compat_aligned_via_adapter) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_ALIGNED);
    cap.minimal_write_unit = 16u;
    cap.atomic_write_granularity = 16u;
    microdb_backend_open_result_t r = microdb_backend_classify_open(&cap, 16u, 4096u, 1u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_VIA_ADAPTER);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_NONE);
}

MDB_TEST(backend_compat_aligned_missing_adapter) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_ALIGNED);
    cap.minimal_write_unit = 16u;
    cap.atomic_write_granularity = 16u;
    microdb_backend_open_result_t r = microdb_backend_classify_open(&cap, 16u, 4096u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_MISSING_ALIGNED_ADAPTER);
}

MDB_TEST(backend_compat_aligned_storage_contract_mismatch) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_ALIGNED);
    microdb_backend_open_result_t r;
    cap.minimal_write_unit = 16u;
    cap.atomic_write_granularity = 16u;
    cap.erase_granularity = 4096u;

    r = microdb_backend_classify_open(&cap, 8u, 4096u, 1u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_INVALID_STORAGE_CONTRACT);

    r = microdb_backend_classify_open(&cap, 16u, 2048u, 1u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_INVALID_STORAGE_CONTRACT);
}

MDB_TEST(backend_compat_managed_via_adapter) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_MANAGED);
    cap.minimal_write_unit = 1u;
    microdb_backend_open_result_t r = microdb_backend_classify_open(&cap, 1u, 4096u, 0u, 1u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_VIA_ADAPTER);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_NONE);
}

MDB_TEST(backend_compat_managed_missing_adapter) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_MANAGED);
    microdb_backend_open_result_t r = microdb_backend_classify_open(&cap, 1u, 4096u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_MISSING_MANAGED_ADAPTER);
}

MDB_TEST(backend_compat_managed_requires_byte_write) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_MANAGED);
    microdb_backend_open_result_t r = microdb_backend_classify_open(&cap, 4u, 4096u, 0u, 1u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_BYTE_WRITE_NOT_SUPPORTED);
}

MDB_TEST(backend_compat_invalid_capability) {
    microdb_backend_open_result_t r = microdb_backend_classify_open(NULL, 1u, 256u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_INVALID_CAPABILITY);
}

MDB_TEST(backend_compat_invalid_capability_atomic_lt_minimal) {
    microdb_storage_capability_t cap = base_capability(MICRODB_BACKEND_CLASS_ALIGNED);
    microdb_backend_open_result_t r;
    cap.minimal_write_unit = 16u;
    cap.atomic_write_granularity = 8u;
    r = microdb_backend_classify_open(&cap, 16u, 4096u, 1u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_INVALID_CAPABILITY);
}

int main(void) {
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_byte_direct);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_byte_write_gt1_unsupported);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_aligned_via_adapter);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_aligned_missing_adapter);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_aligned_storage_contract_mismatch);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_managed_via_adapter);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_managed_missing_adapter);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_managed_requires_byte_write);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_invalid_capability);
    MDB_RUN_TEST(setup_noop, teardown_noop, backend_compat_invalid_capability_atomic_lt_minimal);
    return MDB_RESULT();
}
