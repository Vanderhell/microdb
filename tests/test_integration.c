// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <string.h>

#ifndef LOX_TEST_WITH_8KB
#define LOX_TEST_WITH_8KB 0
#endif
#ifndef LOX_TEST_LARGE_RAM_VARIANT
#define LOX_TEST_LARGE_RAM_VARIANT 0
#endif
#ifndef LOX_TEST_DB_PATH
#define LOX_TEST_DB_PATH "integration_test.bin"
#endif

static lox_t g_db;
static lox_storage_t g_storage;
static char g_path[128];
static unsigned g_path_seq = 0u;
static uint32_t g_now = 1000u;

typedef struct {
    uint32_t count;
    uint32_t ids[16];
} rel_ids_t;

static lox_timestamp_t mock_now(void) {
    return g_now;
}

static void open_storage_db(lox_t *db, lox_storage_t *storage, uint32_t ram_kb) {
    lox_cfg_t cfg;
    uint32_t capacity = 131072u;

    if (ram_kb > 64u) {
        capacity = ram_kb * 2048u;
    }

    memset(db, 0, sizeof(*db));
    memset(storage, 0, sizeof(*storage));
    ASSERT_EQ(lox_port_posix_init(storage, g_path, capacity), LOX_OK);

    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = storage;
    cfg.ram_kb = ram_kb;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(db, &cfg), LOX_OK);
}

static void open_ram_db(lox_t *db, uint32_t ram_kb) {
    lox_cfg_t cfg;

    memset(db, 0, sizeof(*db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = NULL;
    cfg.ram_kb = ram_kb;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(db, &cfg), LOX_OK);
}

static void setup_db(void) {
    g_now = 1000u;
    g_path_seq++;
    (void)snprintf(g_path, sizeof(g_path), "%s.%u", LOX_TEST_DB_PATH, g_path_seq);
    lox_port_posix_remove(g_path);
    open_storage_db(&g_db, &g_storage, LOX_RAM_KB);
}

static void close_db_safe(bool expect_ok) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        lox_err_t rc = lox_deinit(&g_db);
        if (expect_ok) {
            ASSERT_EQ(rc, LOX_OK);
        }
        if (rc != LOX_OK && lox_core_const(&g_db)->magic == LOX_MAGIC) {
            free(lox_core(&g_db)->heap);
            memset(&g_db, 0, sizeof(g_db));
        }
    }
}

static void crash_drop_db_heap(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
        memset(&g_db, 0, sizeof(g_db));
    }
}

static void teardown_db(void) {
    close_db_safe(false);
    lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(g_path);
}

static void reopen_db(uint32_t ram_kb) {
    close_db_safe(true);
    lox_port_posix_deinit(&g_storage);
    open_storage_db(&g_db, &g_storage, ram_kb);
}

static void make_rel_table(lox_t *db, lox_table_t **out) {
    lox_schema_t schema;

    ASSERT_EQ(lox_schema_init(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "age", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(lox_table_create(db, &schema), LOX_OK);
    ASSERT_EQ(lox_table_get(db, "users", out), LOX_OK);
}

static void populate_all_engines(lox_t *db, uint32_t ttl) {
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint8_t kv = 7u;
    float temp = 12.5f;
    uint32_t id = 11u;
    uint8_t age = 42u;

    ASSERT_EQ(lox_kv_set(db, "mode", &kv, 1u, ttl), LOX_OK);
    ASSERT_EQ(lox_ts_register(db, "temp", LOX_TS_F32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(db, "temp", 123u, &temp), LOX_OK);
    make_rel_table(db, &table);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(db, table, row), LOX_OK);
}

static void assert_all_engines_present(lox_t *db) {
    lox_ts_sample_t sample;
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint8_t kv = 0u;
    uint32_t id = 11u;

    ASSERT_EQ(lox_kv_get(db, "mode", &kv, 1u, NULL), LOX_OK);
    ASSERT_EQ(kv, 7u);
    ASSERT_EQ(lox_ts_last(db, "temp", &sample), LOX_OK);
    ASSERT_EQ(sample.ts, 123u);
    ASSERT_EQ(sample.v.f32 == 12.5f, 1);
    ASSERT_EQ(lox_table_get(db, "users", &table), LOX_OK);
    ASSERT_EQ(lox_rel_find_by(db, table, "id", &id, row), LOX_OK);
}

static bool collect_rel_ids(const void *row_buf, void *ctx) {
    rel_ids_t *ids = (rel_ids_t *)ctx;
    uint32_t id = 0u;

    memcpy(&id, row_buf, sizeof(id));
    ids->ids[ids->count++] = id;
    return true;
}

static void run_budget_smoke(uint32_t ram_kb) {
    lox_t db;

    open_ram_db(&db, ram_kb);
    populate_all_engines(&db, 0u);
    assert_all_engines_present(&db);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
}

MDB_TEST(integration_all_three_engines_same_session) {
    populate_all_engines(&g_db, 0u);
    assert_all_engines_present(&g_db);
}

MDB_TEST(integration_flush_between_operations) {
    uint8_t kv = 1u;
    float temp = 5.0f;
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 2u;
    uint8_t age = 9u;

    ASSERT_EQ(lox_kv_set(&g_db, "a", &kv, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    ASSERT_EQ(lox_ts_register(&g_db, "f", LOX_TS_F32, 0u), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "f", 10u, &temp), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    make_rel_table(&g_db, &table);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    reopen_db(LOX_RAM_KB);
    ASSERT_EQ(lox_kv_get(&g_db, "a", &kv, 1u, NULL), LOX_OK);
    ASSERT_EQ(lox_ts_last(&g_db, "f", &(lox_ts_sample_t){ 0 }), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
}

MDB_TEST(integration_storage_reinit_persists_all_engines) {
    populate_all_engines(&g_db, 0u);
    reopen_db(LOX_RAM_KB);
    assert_all_engines_present(&g_db);
}

MDB_TEST(integration_ram_only_loses_state_after_deinit) {
    lox_t db;
    uint8_t value = 3u;
    uint8_t out = 0u;

    open_ram_db(&db, LOX_RAM_KB);
    ASSERT_EQ(lox_kv_set(&db, "volatile", &value, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
    open_ram_db(&db, LOX_RAM_KB);
    ASSERT_EQ(lox_kv_get(&db, "volatile", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
}

#if LOX_TEST_WITH_8KB
MDB_TEST(integration_budget_8kb_functional) {
    run_budget_smoke(8u);
}
#endif

MDB_TEST(integration_budget_32kb_functional) {
    run_budget_smoke(LOX_RAM_KB);
}

MDB_TEST(integration_budget_64kb_functional) {
    run_budget_smoke(64u);
}

MDB_TEST(integration_budget_128kb_functional) {
    run_budget_smoke(128u);
}

MDB_TEST(integration_repeated_flush_idempotent) {
    populate_all_engines(&g_db, 0u);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    reopen_db(LOX_RAM_KB);
    assert_all_engines_present(&g_db);
}

MDB_TEST(integration_delete_then_reload) {
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint8_t kv = 1u;
    uint32_t id = 5u;
    uint8_t age = 8u;

    make_rel_table(&g_db, &table);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "gone", &kv, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    ASSERT_EQ(lox_kv_del(&g_db, "gone"), LOX_OK);
    ASSERT_EQ(lox_rel_delete(&g_db, table, &id, NULL), LOX_OK);
    reopen_db(LOX_RAM_KB);
    ASSERT_EQ(lox_kv_exists(&g_db, "gone"), LOX_ERR_NOT_FOUND);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    ASSERT_EQ(lox_rel_find_by(&g_db, table, "id", &id, row), LOX_ERR_NOT_FOUND);
}

MDB_TEST(integration_rel_order_persists_with_other_engines) {
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    rel_ids_t ids = { 0 };
    uint32_t id;
    uint8_t age = 1u;

    populate_all_engines(&g_db, 0u);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    for (id = 12u; id <= 14u; ++id) {
        memset(row, 0, sizeof(row));
        ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
        ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
        ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    }
    reopen_db(LOX_RAM_KB);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    ASSERT_EQ(lox_rel_iter(&g_db, table, collect_rel_ids, &ids), LOX_OK);
    ASSERT_EQ(ids.count, 4u);
    ASSERT_EQ(ids.ids[0], 11u);
    ASSERT_EQ(ids.ids[1], 12u);
    ASSERT_EQ(ids.ids[2], 13u);
    ASSERT_EQ(ids.ids[3], 14u);
}

MDB_TEST(integration_kv_ttl_persists_after_reload) {
    uint8_t out = 0u;

    populate_all_engines(&g_db, 5u);
    reopen_db(LOX_RAM_KB);
    g_now = 1004u;
    ASSERT_EQ(lox_kv_get(&g_db, "mode", &out, 1u, NULL), LOX_OK);
    g_now = 1005u;
    ASSERT_EQ(lox_kv_get(&g_db, "mode", &out, 1u, NULL), LOX_ERR_EXPIRED);
}

MDB_TEST(integration_multiple_reinit_cycles_preserve_data) {
    populate_all_engines(&g_db, 0u);
    reopen_db(LOX_RAM_KB);
    reopen_db(LOX_RAM_KB);
    reopen_db(LOX_RAM_KB);
    assert_all_engines_present(&g_db);
}

MDB_TEST(integration_stats_track_mixed_usage) {
    lox_stats_t stats;

    populate_all_engines(&g_db, 0u);
    ASSERT_EQ(lox_stats(&g_db, &stats), LOX_OK);
    ASSERT_EQ(stats.kv_entries_used, 1u);
    ASSERT_EQ(stats.ts_streams_registered, 1u);
    ASSERT_EQ(stats.rel_tables_count, 1u);
    ASSERT_GT(stats.kv_entries_max, 0u);
}

MDB_TEST(cfg_kv_heavy_split) {
    lox_t default_db;
    lox_t heavy_db;
    lox_cfg_t default_cfg;
    lox_cfg_t heavy_cfg;
    lox_stats_t default_stats;
    lox_stats_t heavy_stats;

    memset(&default_db, 0, sizeof(default_db));
    memset(&heavy_db, 0, sizeof(heavy_db));
    memset(&default_cfg, 0, sizeof(default_cfg));
    memset(&heavy_cfg, 0, sizeof(heavy_cfg));

    default_cfg.storage = NULL;
    default_cfg.ram_kb = 32u;
    default_cfg.now = NULL;
    ASSERT_EQ(lox_init(&default_db, &default_cfg), LOX_OK);
    ASSERT_EQ(lox_stats(&default_db, &default_stats), LOX_OK);

    heavy_cfg.storage = NULL;
    heavy_cfg.ram_kb = 32u;
    heavy_cfg.now = NULL;
    heavy_cfg.kv_pct = 80u;
    heavy_cfg.ts_pct = 15u;
    heavy_cfg.rel_pct = 5u;
    ASSERT_EQ(lox_init(&heavy_db, &heavy_cfg), LOX_OK);
    ASSERT_EQ(lox_stats(&heavy_db, &heavy_stats), LOX_OK);
    ASSERT_GT(heavy_stats.kv_entries_max, 0u);
    ASSERT_GT(default_stats.kv_entries_max, 0u);

    ASSERT_EQ(lox_deinit(&heavy_db), LOX_OK);
    ASSERT_EQ(lox_deinit(&default_db), LOX_OK);
}

MDB_TEST(cfg_invalid_pct_sum) {
    lox_t db;
    lox_cfg_t cfg;

    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;
    cfg.kv_pct = 50u;
    cfg.ts_pct = 50u;
    cfg.rel_pct = 50u;
    ASSERT_EQ(lox_init(&db, &cfg), LOX_ERR_INVALID);
}

MDB_TEST(cfg_zero_pct_uses_compile_defaults) {
    lox_t db;
    lox_cfg_t cfg;

    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;
    cfg.kv_pct = 0u;
    cfg.ts_pct = 0u;
    cfg.rel_pct = 0u;
    ASSERT_EQ(lox_init(&db, &cfg), LOX_OK);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
}

MDB_TEST(integration_clear_then_reload_empty_state) {
    lox_table_t *table = NULL;
    uint32_t count = 0u;

    populate_all_engines(&g_db, 0u);
    ASSERT_EQ(lox_kv_clear(&g_db), LOX_OK);
    ASSERT_EQ(lox_ts_clear(&g_db, "temp"), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    ASSERT_EQ(lox_rel_clear(&g_db, table), LOX_OK);
    reopen_db(LOX_RAM_KB);
    ASSERT_EQ(lox_kv_exists(&g_db, "mode"), LOX_ERR_NOT_FOUND);
    ASSERT_EQ(lox_ts_last(&g_db, "temp", &(lox_ts_sample_t){ 0 }), LOX_ERR_NOT_FOUND);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    ASSERT_EQ(lox_rel_count(table, &count), LOX_OK);
    ASSERT_EQ(count, 0u);
}

MDB_TEST(integration_storage_bytes_written_increase) {
    lox_stats_t before;
    lox_stats_t after;

    ASSERT_EQ(lox_stats(&g_db, &before), LOX_OK);
    populate_all_engines(&g_db, 0u);
    ASSERT_EQ(lox_stats(&g_db, &after), LOX_OK);
    ASSERT_EQ(after.wal_bytes_used >= before.wal_bytes_used, 1);
}

MDB_TEST(integration_reload_without_explicit_flush_uses_wal) {
    populate_all_engines(&g_db, 0u);
    lox_port_posix_simulate_power_loss(&g_storage);
    crash_drop_db_heap();
    lox_port_posix_deinit(&g_storage);
    open_storage_db(&g_db, &g_storage, LOX_RAM_KB);
    assert_all_engines_present(&g_db);
}

MDB_TEST(integration_ts_query_after_reload) {
    lox_ts_sample_t buf[4];
    size_t count = 0u;

    populate_all_engines(&g_db, 0u);
    reopen_db(LOX_RAM_KB);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "temp", 100u, 200u, buf, 4u, &count), LOX_OK);
    ASSERT_EQ(count, 1u);
    ASSERT_EQ(buf[0].ts, 123u);
}

MDB_TEST(integration_rel_find_after_reload) {
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 11u;

    populate_all_engines(&g_db, 0u);
    reopen_db(LOX_RAM_KB);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    ASSERT_EQ(lox_rel_find_by(&g_db, table, "id", &id, row), LOX_OK);
}

MDB_TEST(integration_flush_after_reload_keeps_data) {
    populate_all_engines(&g_db, 0u);
    reopen_db(LOX_RAM_KB);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    reopen_db(LOX_RAM_KB);
    assert_all_engines_present(&g_db);
}

MDB_TEST(integration_rel_count_after_reload) {
    lox_table_t *table = NULL;
    uint32_t count = 0u;

    populate_all_engines(&g_db, 0u);
    reopen_db(LOX_RAM_KB);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    ASSERT_EQ(lox_rel_count(table, &count), LOX_OK);
    ASSERT_EQ(count, 1u);
}

MDB_TEST(integration_kv_disabled_returns_err_disabled) {
    uint8_t value = 1u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_set(&g_db, "x", &value, 1u, 0u), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_kv_get(&g_db, "x", &out, 1u, NULL), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_kv_del(&g_db, "x"), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_kv_exists(&g_db, "x"), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_kv_purge_expired(&g_db), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_kv_clear(&g_db), LOX_ERR_DISABLED);
}

MDB_TEST(integration_ts_disabled_returns_err_disabled) {
    lox_ts_sample_t sample;
    size_t count = 0u;

    ASSERT_EQ(lox_ts_register(&g_db, "x", LOX_TS_U32, 0u), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_ts_insert(&g_db, "x", 1u, &(uint32_t){ 1u }), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_ts_last(&g_db, "x", &sample), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "x", 0u, 1u, &sample, 1u, &count), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_ts_count(&g_db, "x", 0u, 1u, &count), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_ts_clear(&g_db, "x"), LOX_ERR_DISABLED);
}

MDB_TEST(integration_rel_disabled_returns_err_disabled) {
    lox_schema_t schema;
    uint8_t row[16] = { 0 };
    lox_table_t *table = NULL;
    uint32_t count = 0u;

    ASSERT_EQ(lox_schema_init(&schema, "x", 1u), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_table_get(&g_db, "x", &table), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_row_set(table, row, "id", row), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_row_get(table, row, "id", row, NULL), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_rel_find_by(&g_db, table, "id", row, row), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_rel_delete(&g_db, table, row, &count), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_rel_count(table, &count), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_rel_clear(&g_db, table), LOX_ERR_DISABLED);
}

int main(void) {
#if !LOX_ENABLE_KV
    MDB_RUN_TEST(setup_db, teardown_db, integration_kv_disabled_returns_err_disabled);
#elif !LOX_ENABLE_TS
    MDB_RUN_TEST(setup_db, teardown_db, integration_ts_disabled_returns_err_disabled);
#elif !LOX_ENABLE_REL
    MDB_RUN_TEST(setup_db, teardown_db, integration_rel_disabled_returns_err_disabled);
#elif LOX_TEST_LARGE_RAM_VARIANT
    MDB_RUN_TEST(setup_db, teardown_db, integration_all_three_engines_same_session);
    MDB_RUN_TEST(setup_db, teardown_db, integration_storage_reinit_persists_all_engines);
    MDB_RUN_TEST(setup_db, teardown_db, integration_repeated_flush_idempotent);
    MDB_RUN_TEST(setup_db, teardown_db, integration_stats_track_mixed_usage);
    MDB_RUN_TEST(setup_db, teardown_db, integration_flush_after_reload_keeps_data);
#else
    MDB_RUN_TEST(setup_db, teardown_db, integration_all_three_engines_same_session);
    MDB_RUN_TEST(setup_db, teardown_db, integration_flush_between_operations);
    MDB_RUN_TEST(setup_db, teardown_db, integration_storage_reinit_persists_all_engines);
    MDB_RUN_TEST(setup_db, teardown_db, integration_ram_only_loses_state_after_deinit);
#if LOX_TEST_WITH_8KB
    MDB_RUN_TEST(setup_db, teardown_db, integration_budget_8kb_functional);
#endif
    MDB_RUN_TEST(setup_db, teardown_db, integration_budget_32kb_functional);
    MDB_RUN_TEST(setup_db, teardown_db, integration_budget_64kb_functional);
    MDB_RUN_TEST(setup_db, teardown_db, integration_budget_128kb_functional);
    MDB_RUN_TEST(setup_db, teardown_db, integration_repeated_flush_idempotent);
    MDB_RUN_TEST(setup_db, teardown_db, integration_delete_then_reload);
    MDB_RUN_TEST(setup_db, teardown_db, integration_rel_order_persists_with_other_engines);
    MDB_RUN_TEST(setup_db, teardown_db, integration_kv_ttl_persists_after_reload);
    MDB_RUN_TEST(setup_db, teardown_db, integration_multiple_reinit_cycles_preserve_data);
    MDB_RUN_TEST(setup_db, teardown_db, integration_stats_track_mixed_usage);
    MDB_RUN_TEST(setup_db, teardown_db, cfg_kv_heavy_split);
    MDB_RUN_TEST(setup_db, teardown_db, cfg_invalid_pct_sum);
    MDB_RUN_TEST(setup_db, teardown_db, cfg_zero_pct_uses_compile_defaults);
    MDB_RUN_TEST(setup_db, teardown_db, integration_clear_then_reload_empty_state);
    MDB_RUN_TEST(setup_db, teardown_db, integration_storage_bytes_written_increase);
    MDB_RUN_TEST(setup_db, teardown_db, integration_reload_without_explicit_flush_uses_wal);
    MDB_RUN_TEST(setup_db, teardown_db, integration_ts_query_after_reload);
    MDB_RUN_TEST(setup_db, teardown_db, integration_rel_find_after_reload);
    MDB_RUN_TEST(setup_db, teardown_db, integration_flush_after_reload_keeps_data);
    MDB_RUN_TEST(setup_db, teardown_db, integration_rel_count_after_reload);
#endif
    return MDB_RESULT();
}
