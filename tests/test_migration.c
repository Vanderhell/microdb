// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "../port/posix/microdb_port_posix.h"

#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_storage;
static const char *g_path = "migration_test.bin";

static uint32_t g_migrate_calls = 0u;
static uint16_t g_migrate_old = 0u;
static uint16_t g_migrate_new = 0u;
static microdb_err_t g_migrate_result = MICRODB_OK;

static microdb_err_t on_migrate_cb(microdb_t *db, const char *table_name, uint16_t old_version, uint16_t new_version) {
    (void)db;
    (void)table_name;
    g_migrate_calls++;
    g_migrate_old = old_version;
    g_migrate_new = new_version;
    return g_migrate_result;
}

static void open_db(microdb_err_t (*on_migrate)(microdb_t *, const char *, uint16_t, uint16_t)) {
    microdb_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 65536u), MICRODB_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.on_migrate = on_migrate;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void reopen_db(microdb_err_t (*on_migrate)(microdb_t *, const char *, uint16_t, uint16_t)) {
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_OK);
    microdb_port_posix_deinit(&g_storage);
    open_db(on_migrate);
}

static void setup_db(void) {
    microdb_port_posix_remove(g_path);
    g_migrate_calls = 0u;
    g_migrate_old = 0u;
    g_migrate_new = 0u;
    g_migrate_result = MICRODB_OK;
    open_db(NULL);
}

static void teardown_db(void) {
    (void)microdb_deinit(&g_db);
    microdb_port_posix_deinit(&g_storage);
    microdb_port_posix_remove(g_path);
}

static microdb_err_t create_users_table(uint16_t schema_version) {
    microdb_schema_t schema;
    microdb_err_t err;

    err = microdb_schema_init(&schema, "users", 8u);
    if (err != MICRODB_OK) {
        return err;
    }
    schema.schema_version = schema_version;
    err = microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_seal(&schema);
    if (err != MICRODB_OK) {
        return err;
    }
    return microdb_table_create(&g_db, &schema);
}

MDB_TEST(test_migration_called_on_version_bump) {
    microdb_table_t *table = NULL;
    uint8_t row[32] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 9u;

    ASSERT_EQ(create_users_table(1u), MICRODB_OK);
    reopen_db(on_migrate_cb);
    ASSERT_EQ(create_users_table(2u), MICRODB_OK);
    ASSERT_EQ(g_migrate_calls, 1u);
    ASSERT_EQ(g_migrate_old, 1u);
    ASSERT_EQ(g_migrate_new, 2u);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
}

MDB_TEST(test_no_migration_on_matching_version) {
    ASSERT_EQ(create_users_table(1u), MICRODB_OK);
    reopen_db(on_migrate_cb);
    ASSERT_EQ(create_users_table(1u), MICRODB_OK);
    ASSERT_EQ(g_migrate_calls, 0u);
}

MDB_TEST(test_mismatch_without_callback_returns_err_schema) {
    ASSERT_EQ(create_users_table(1u), MICRODB_OK);
    reopen_db(NULL);
    ASSERT_EQ(create_users_table(2u), MICRODB_ERR_SCHEMA);
}

MDB_TEST(test_migration_error_propagated) {
    ASSERT_EQ(create_users_table(1u), MICRODB_OK);
    g_migrate_result = MICRODB_ERR_INVALID;
    reopen_db(on_migrate_cb);
    ASSERT_EQ(create_users_table(2u), MICRODB_ERR_INVALID);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, test_migration_called_on_version_bump);
    MDB_RUN_TEST(setup_db, teardown_db, test_no_migration_on_matching_version);
    MDB_RUN_TEST(setup_db, teardown_db, test_mismatch_without_callback_returns_err_schema);
    MDB_RUN_TEST(setup_db, teardown_db, test_migration_error_propagated);
    return MDB_RESULT();
}
