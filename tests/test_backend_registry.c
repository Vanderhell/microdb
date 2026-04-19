// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb_backend_adapter.h"

int microdb_backend_nand_stub_register(void);
int microdb_backend_emmc_stub_register(void);
int microdb_backend_sd_stub_register(void);
int microdb_backend_fs_stub_register(void);
int microdb_backend_block_stub_register(void);

static void setup_registry(void) {
    microdb_backend_registry_reset();
}

static void teardown_registry(void) {
    microdb_backend_registry_reset();
}

MDB_TEST(backend_registry_registers_stub_modules) {
    const microdb_backend_adapter_t *adapter;

    ASSERT_EQ(microdb_backend_nand_stub_register(), 0);
    ASSERT_EQ(microdb_backend_emmc_stub_register(), 0);
    ASSERT_EQ(microdb_backend_sd_stub_register(), 0);
    ASSERT_EQ(microdb_backend_fs_stub_register(), 0);
    ASSERT_EQ(microdb_backend_block_stub_register(), 0);
    ASSERT_EQ((long long)microdb_backend_registry_count(), 5);

    adapter = microdb_backend_registry_find("nand_stub");
    ASSERT_EQ(adapter != NULL, 1);
    ASSERT_EQ(adapter->capability.backend_class, MICRODB_BACKEND_CLASS_MANAGED);
    ASSERT_EQ(adapter->capability.is_managed, 1);

    adapter = microdb_backend_registry_find("block_stub");
    ASSERT_EQ(adapter != NULL, 1);
    ASSERT_EQ(adapter->capability.backend_class, MICRODB_BACKEND_CLASS_MANAGED);
}

MDB_TEST(backend_registry_duplicate_register_is_idempotent) {
    ASSERT_EQ(microdb_backend_nand_stub_register(), 0);
    ASSERT_EQ(microdb_backend_nand_stub_register(), 0);
    ASSERT_EQ((long long)microdb_backend_registry_count(), 1);
}

MDB_TEST(backend_registry_get_out_of_range_returns_null) {
    ASSERT_EQ(microdb_backend_nand_stub_register(), 0);
    ASSERT_EQ(microdb_backend_registry_get(99u) == NULL, 1);
}

int main(void) {
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_registry_registers_stub_modules);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_registry_duplicate_register_is_idempotent);
    MDB_RUN_TEST(setup_registry, teardown_registry, backend_registry_get_out_of_range_returns_null);
    return MDB_RESULT();
}
