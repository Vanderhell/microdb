// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"

#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static const char *g_path = "migration_test.bin";

static uint32_t g_migrate_calls = 0u;
static uint16_t g_migrate_old = 0u;
static uint16_t g_migrate_new = 0u;
static lox_err_t g_migrate_result = LOX_OK;
static uint32_t g_nested_migrate_calls = 0u;
static lox_err_t g_nested_migrate_result = LOX_OK;

static lox_err_t create_users_table(uint16_t schema_version);

static lox_err_t on_migrate_cb(lox_t *db, const char *table_name, uint16_t old_version, uint16_t new_version) {
    (void)db;
    (void)table_name;
    g_migrate_calls++;
    g_migrate_old = old_version;
    g_migrate_new = new_version;
    return g_migrate_result;
}

static lox_err_t on_migrate_recursive_cb(lox_t *db, const char *table_name, uint16_t old_version, uint16_t new_version) {
    (void)db;
    (void)table_name;
    g_migrate_calls++;
    g_migrate_old = old_version;
    g_migrate_new = new_version;
    g_nested_migrate_calls++;
    g_nested_migrate_result = create_users_table((uint16_t)(new_version + 1u));
    return g_nested_migrate_result;
}

static void open_db(lox_err_t (*on_migrate)(lox_t *, const char *, uint16_t, uint16_t)) {
    lox_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.on_migrate = on_migrate;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void reopen_db(lox_err_t (*on_migrate)(lox_t *, const char *, uint16_t, uint16_t)) {
    ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    lox_port_posix_deinit(&g_storage);
    open_db(on_migrate);
}

static void setup_db(void) {
    lox_port_posix_remove(g_path);
    g_migrate_calls = 0u;
    g_migrate_old = 0u;
    g_migrate_new = 0u;
    g_migrate_result = LOX_OK;
    g_nested_migrate_calls = 0u;
    g_nested_migrate_result = LOX_OK;
    open_db(NULL);
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
    lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(g_path);
}

static lox_err_t create_users_table(uint16_t schema_version) {
    lox_schema_t schema;
    lox_err_t err;

    err = lox_schema_init(&schema, "users", 8u);
    if (err != LOX_OK) {
        return err;
    }
    schema.schema_version = schema_version;
    err = lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(&schema, "age", LOX_COL_U8, sizeof(uint8_t), false);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_seal(&schema);
    if (err != LOX_OK) {
        return err;
    }
    return lox_table_create(&g_db, &schema);
}

MDB_TEST(test_migration_called_on_version_bump) {
    lox_table_t *table = NULL;
    uint8_t row[32] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 9u;

    ASSERT_EQ(create_users_table(1u), LOX_OK);
    reopen_db(on_migrate_cb);
    ASSERT_EQ(create_users_table(2u), LOX_OK);
    ASSERT_EQ(g_migrate_calls, 1u);
    ASSERT_EQ(g_migrate_old, 1u);
    ASSERT_EQ(g_migrate_new, 2u);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
}

MDB_TEST(test_no_migration_on_matching_version) {
    ASSERT_EQ(create_users_table(1u), LOX_OK);
    reopen_db(on_migrate_cb);
    ASSERT_EQ(create_users_table(1u), LOX_OK);
    ASSERT_EQ(g_migrate_calls, 0u);
}

MDB_TEST(test_mismatch_without_callback_returns_err_schema) {
    ASSERT_EQ(create_users_table(1u), LOX_OK);
    reopen_db(NULL);
    ASSERT_EQ(create_users_table(2u), LOX_ERR_SCHEMA);
}

MDB_TEST(test_migration_error_propagated) {
    ASSERT_EQ(create_users_table(1u), LOX_OK);
    g_migrate_result = LOX_ERR_INVALID;
    reopen_db(on_migrate_cb);
    ASSERT_EQ(create_users_table(2u), LOX_ERR_INVALID);
}

MDB_TEST(test_recursive_migration_callback_rejected) {
    ASSERT_EQ(create_users_table(1u), LOX_OK);
    reopen_db(on_migrate_recursive_cb);
    ASSERT_EQ(create_users_table(2u), LOX_ERR_SCHEMA);
    ASSERT_EQ(g_migrate_calls, 1u);
    ASSERT_EQ(g_nested_migrate_calls, 1u);
    ASSERT_EQ(g_nested_migrate_result, LOX_ERR_SCHEMA);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, test_migration_called_on_version_bump);
    MDB_RUN_TEST(setup_db, teardown_db, test_no_migration_on_matching_version);
    MDB_RUN_TEST(setup_db, teardown_db, test_mismatch_without_callback_returns_err_schema);
    MDB_RUN_TEST(setup_db, teardown_db, test_migration_error_propagated);
    MDB_RUN_TEST(setup_db, teardown_db, test_recursive_migration_callback_rejected);
    return MDB_RESULT();
}
