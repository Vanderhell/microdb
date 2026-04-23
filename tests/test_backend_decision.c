// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox_backend_decision.h"
#include "lox_backend_adapter.h"

int lox_backend_nand_stub_register(void);
int lox_backend_emmc_stub_register(void);
int lox_backend_sd_stub_register(void);
int lox_backend_fs_stub_register(void);
int lox_backend_block_stub_register(void);

static lox_storage_capability_t byte_capability(void) {
    lox_storage_capability_t cap;
    cap.backend_class = LOX_BACKEND_CLASS_BYTE;
    cap.minimal_write_unit = 1u;
    cap.erase_granularity = 256u;
    cap.atomic_write_granularity = 1u;
    cap.sync_semantics = LOX_SYNC_SEMANTICS_DURABLE_SYNC;
    cap.is_managed = 0u;
    return cap;
}

static void setup_registry(void) {
    lox_backend_registry_reset();
}

static void teardown_registry(void) {
    lox_backend_registry_reset();
}

MDB_TEST(backend_decision_unknown_backend_is_unsupported) {
    lox_backend_open_result_t r =
        lox_backend_decide_by_name("missing", 1u, 256u, 0u, 0u);
    ASSERT_EQ(r.mode, LOX_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, LOX_BACKEND_REASON_BACKEND_NOT_REGISTERED);
}

MDB_TEST(backend_decision_managed_requires_adapter) {
    lox_backend_open_result_t r;
    ASSERT_EQ(lox_backend_nand_stub_register(), 0);

    r = lox_backend_decide_by_name("nand_stub", 1u, 4096u, 0u, 0u);
    ASSERT_EQ(r.mode, LOX_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, LOX_BACKEND_REASON_MISSING_MANAGED_ADAPTER);

    r = lox_backend_decide_by_name("nand_stub", 1u, 4096u, 0u, 1u);
    ASSERT_EQ(r.mode, LOX_BACKEND_OPEN_VIA_ADAPTER);
    ASSERT_EQ(r.reason, LOX_BACKEND_REASON_NONE);
}

MDB_TEST(backend_decision_emmc_and_sd_require_managed_adapter) {
    lox_backend_open_result_t r;
    ASSERT_EQ(lox_backend_emmc_stub_register(), 0);
    ASSERT_EQ(lox_backend_sd_stub_register(), 0);

    r = lox_backend_decide_by_name("emmc_stub", 1u, 4096u, 0u, 0u);
    ASSERT_EQ(r.mode, LOX_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, LOX_BACKEND_REASON_MISSING_MANAGED_ADAPTER);

    r = lox_backend_decide_by_name("sd_stub", 1u, 512u, 0u, 1u);
    ASSERT_EQ(r.mode, LOX_BACKEND_OPEN_VIA_ADAPTER);
    ASSERT_EQ(r.reason, LOX_BACKEND_REASON_NONE);
}

MDB_TEST(backend_decision_fs_and_block_require_managed_adapter) {
    lox_backend_open_result_t r;
    ASSERT_EQ(lox_backend_fs_stub_register(), 0);
    ASSERT_EQ(lox_backend_block_stub_register(), 0);

    r = lox_backend_decide_by_name("fs_stub", 1u, 4096u, 0u, 0u);
    ASSERT_EQ(r.mode, LOX_BACKEND_OPEN_UNSUPPORTED);
    ASSERT_EQ(r.reason, LOX_BACKEND_REASON_MISSING_MANAGED_ADAPTER);

    r = lox_backend_decide_by_name("block_stub", 1u, 512u, 0u, 1u);
    ASSERT_EQ(r.mode, LOX_BACKEND_OPEN_VIA_ADAPTER);
    ASSERT_EQ(r.reason, LOX_BACKEND_REASON_NONE);
}

MDB_TEST(backend_decision_byte_direct_when_contract_is_valid) {
    lox_backend_adapter_t byte_adapter = { "byte_stub", { 0 } };
    lox_backend_open_result_t r;

    byte_adapter.capability = byte_capability();
    ASSERT_EQ(lox_backend_registry_register(&byte_adapter), 0);
    r = lox_backend_decide_by_name("byte_stub", 1u, 256u, 0u, 0u);
    ASSERT_EQ(r.mode, LOX_BACKEND_OPEN_DIRECT);
    ASSERT_EQ(r.reason, LOX_BACKEND_REASON_NONE);
}

int main(void) {
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_unknown_backend_is_unsupported);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_managed_requires_adapter);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_emmc_and_sd_require_managed_adapter);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_fs_and_block_require_managed_adapter);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_decision_byte_direct_when_contract_is_valid);
    return MDB_RESULT();
}
