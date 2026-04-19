// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb_backend_decision.h"
#include "microdb_backend_adapter.h"

int microdb_backend_nand_stub_register(void);
int microdb_backend_emmc_stub_register(void);
int microdb_backend_sd_stub_register(void);
int microdb_backend_fs_stub_register(void);
int microdb_backend_block_stub_register(void);

static microdb_storage_capability_t byte_capability(void) {
    microdb_storage_capability_t cap;
    cap.backend_class = MICRODB_BACKEND_CLASS_BYTE;
    cap.minimal_write_unit = 1u;
    cap.erase_granularity = 256u;
    cap.atomic_write_granularity = 1u;
    cap.sync_semantics = MICRODB_SYNC_SEMANTICS_DURABLE_SYNC;
    cap.is_managed = 0u;
    return cap;
}

static void setup_registry(void) {
    microdb_backend_registry_reset();
}

static void teardown_registry(void) {
    microdb_backend_registry_reset();
}

MDB_TEST(backend_decision_unknown_backend_is_unsupported) {
    microdb_backend_open_result_t r =
        microdb_backend_decide_by_name("missing", 1u, 256u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_BACKEND_NOT_REGISTERED);
}

MDB_TEST(backend_decision_managed_requires_adapter) {
    microdb_backend_open_result_t r;
    ASSERT_EQ(microdb_backend_nand_stub_register(), 0);

    r = microdb_backend_decide_by_name("nand_stub", 1u, 4096u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_MISSING_MANAGED_ADAPTER);

    r = microdb_backend_decide_by_name("nand_stub", 1u, 4096u, 0u, 1u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_VIA_ADAPTER);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_NONE);
}

MDB_TEST(backend_decision_emmc_and_sd_require_managed_adapter) {
    microdb_backend_open_result_t r;
    ASSERT_EQ(microdb_backend_emmc_stub_register(), 0);
    ASSERT_EQ(microdb_backend_sd_stub_register(), 0);

    r = microdb_backend_decide_by_name("emmc_stub", 1u, 4096u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_MISSING_MANAGED_ADAPTER);

    r = microdb_backend_decide_by_name("sd_stub", 1u, 512u, 0u, 1u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_VIA_ADAPTER);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_NONE);
}

MDB_TEST(backend_decision_fs_and_block_require_managed_adapter) {
    microdb_backend_open_result_t r;
    ASSERT_EQ(microdb_backend_fs_stub_register(), 0);
    ASSERT_EQ(microdb_backend_block_stub_register(), 0);

    r = microdb_backend_decide_by_name("fs_stub", 1u, 4096u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_MISSING_MANAGED_ADAPTER);

    r = microdb_backend_decide_by_name("block_stub", 1u, 512u, 0u, 1u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_VIA_ADAPTER);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_NONE);
}

MDB_TEST(backend_decision_byte_direct_when_contract_is_valid) {
    microdb_backend_adapter_t byte_adapter = { "byte_stub", { 0 } };
    microdb_backend_open_result_t r;

    byte_adapter.capability = byte_capability();
    ASSERT_EQ(microdb_backend_registry_register(&byte_adapter), 0);
    r = microdb_backend_decide_by_name("byte_stub", 1u, 256u, 0u, 0u);
    ASSERT_EQ(r.mode, MICRODB_BACKEND_OPEN_DIRECT);
    ASSERT_EQ(r.reason, MICRODB_BACKEND_REASON_NONE);
}

int main(void) {
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_unknown_backend_is_unsupported);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_managed_requires_adapter);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_emmc_and_sd_require_managed_adapter);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_fs_and_block_require_managed_adapter);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_byte_direct_when_contract_is_valid);
    return MDB_RESULT();
}
