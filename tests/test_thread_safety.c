// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"

#include <string.h>

static microdb_t g_db;
static uint32_t g_lock_count = 0u;
static uint32_t g_unlock_count = 0u;
static uint32_t g_lock_depth = 0u;
static uint32_t g_reentrant_lock = 0u;

static void *mock_lock_create(void) {
    return &g_lock_count;
}

static void mock_lock(void *hdl) {
    (void)hdl;
    if (g_lock_depth != 0u) {
        g_reentrant_lock++;
    }
    g_lock_depth++;
    g_lock_count++;
}

static void mock_unlock(void *hdl) {
    (void)hdl;
    if (g_lock_depth != 0u) {
        g_lock_depth--;
    }
    g_unlock_count++;
}

static void mock_lock_destroy(void *hdl) {
    (void)hdl;
}

static void setup_db(void) {
    microdb_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    memset(&cfg, 0, sizeof(cfg));
    g_lock_count = 0u;
    g_unlock_count = 0u;
    g_lock_depth = 0u;
    g_reentrant_lock = 0u;
    cfg.ram_kb = 32u;
    cfg.lock_create = mock_lock_create;
    cfg.lock = mock_lock;
    cfg.unlock = mock_unlock;
    cfg.lock_destroy = mock_lock_destroy;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    (void)microdb_deinit(&g_db);
}

MDB_TEST(test_lock_called_on_kv_put) {
    uint8_t value = 1u;

    ASSERT_EQ(microdb_kv_put(&g_db, "a", &value, 1u), MICRODB_OK);
    ASSERT_EQ(g_lock_count, 1u);
    ASSERT_EQ(g_unlock_count, 1u);
}

MDB_TEST(test_lock_called_on_kv_get) {
    uint8_t value = 1u;
    uint8_t out = 0u;

    ASSERT_EQ(microdb_kv_put(&g_db, "a", &value, 1u), MICRODB_OK);
    g_lock_count = 0u;
    g_unlock_count = 0u;
    ASSERT_EQ(microdb_kv_get(&g_db, "a", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(g_lock_count, 1u);
    ASSERT_EQ(g_unlock_count, 1u);
}

MDB_TEST(test_counts_balanced_after_sequence) {
    uint8_t value = 1u;
    uint8_t out = 0u;
    uint32_t i;

    ASSERT_EQ(microdb_ts_register(&g_db, "s", MICRODB_TS_U32, 0u), MICRODB_OK);
    g_lock_count = 0u;
    g_unlock_count = 0u;

    for (i = 0u; i < 10u; ++i) {
        uint32_t tsv = i;
        ASSERT_EQ(microdb_kv_put(&g_db, "a", &value, 1u), MICRODB_OK);
        ASSERT_EQ(microdb_kv_get(&g_db, "a", &out, 1u, NULL), MICRODB_OK);
        ASSERT_EQ(microdb_ts_insert(&g_db, "s", i, &tsv), MICRODB_OK);
    }

    ASSERT_EQ(g_lock_count, g_unlock_count);
}

MDB_TEST(test_null_hooks_are_safe) {
    microdb_t db;
    microdb_cfg_t cfg;
    uint8_t value = 1u;

    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    cfg.lock_create = mock_lock_create;
    cfg.lock = NULL;
    cfg.unlock = NULL;
    cfg.lock_destroy = mock_lock_destroy;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_OK);
    ASSERT_EQ(microdb_kv_put(&db, "n", &value, 1u), MICRODB_OK);
    ASSERT_EQ(microdb_deinit(&db), MICRODB_OK);
}

MDB_TEST(test_lock_called_on_compact) {
    uint8_t value = 1u;
    ASSERT_EQ(microdb_kv_put(&g_db, "c", &value, 1u), MICRODB_OK);
    g_lock_count = 0u;
    g_unlock_count = 0u;
    ASSERT_EQ(microdb_compact(&g_db), MICRODB_OK);
    ASSERT_EQ(g_lock_count, 1u);
    ASSERT_EQ(g_unlock_count, 1u);
}

static bool kv_reenter_cb(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    uint8_t out = 0u;
    (void)key;
    (void)val;
    (void)val_len;
    (void)ttl_remaining;
    (void)ctx;
    (void)microdb_kv_get(&g_db, "r", &out, 1u, NULL);
    return true;
}

MDB_TEST(test_kv_iter_callback_reentry_no_recursive_lock) {
    uint8_t value = 1u;
    ASSERT_EQ(microdb_kv_put(&g_db, "r", &value, 1u), MICRODB_OK);
    g_reentrant_lock = 0u;
    ASSERT_EQ(microdb_kv_iter(&g_db, kv_reenter_cb, NULL), MICRODB_OK);
    ASSERT_EQ(g_reentrant_lock, 0u);
}

static bool ts_reenter_cb(const microdb_ts_sample_t *sample, void *ctx) {
    microdb_ts_sample_t out;
    (void)sample;
    (void)ctx;
    (void)microdb_ts_last(&g_db, "rt", &out);
    return true;
}

MDB_TEST(test_ts_query_callback_reentry_no_recursive_lock) {
    uint32_t value = 1u;
    ASSERT_EQ(microdb_ts_register(&g_db, "rt", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "rt", 1u, &value), MICRODB_OK);
    g_reentrant_lock = 0u;
    ASSERT_EQ(microdb_ts_query(&g_db, "rt", 0u, 10u, ts_reenter_cb, NULL), MICRODB_OK);
    ASSERT_EQ(g_reentrant_lock, 0u);
}

static bool rel_reenter_cb(const void *row_buf, void *ctx) {
    microdb_table_t **table_ptr = (microdb_table_t **)ctx;
    uint8_t out[64] = { 0 };
    uint32_t id = 1u;
    (void)row_buf;
    (void)microdb_rel_find_by(&g_db, *table_ptr, "id", &id, out);
    return true;
}

MDB_TEST(test_rel_iter_callback_reentry_no_recursive_lock) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 2u;

    ASSERT_EQ(microdb_schema_init(&schema, "t", 8u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_get(&g_db, "t", &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);

    g_reentrant_lock = 0u;
    ASSERT_EQ(microdb_rel_iter(&g_db, table, rel_reenter_cb, &table), MICRODB_OK);
    ASSERT_EQ(g_reentrant_lock, 0u);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, test_lock_called_on_kv_put);
    MDB_RUN_TEST(setup_db, teardown_db, test_lock_called_on_kv_get);
    MDB_RUN_TEST(setup_db, teardown_db, test_counts_balanced_after_sequence);
    MDB_RUN_TEST(setup_db, teardown_db, test_null_hooks_are_safe);
    MDB_RUN_TEST(setup_db, teardown_db, test_lock_called_on_compact);
    MDB_RUN_TEST(setup_db, teardown_db, test_kv_iter_callback_reentry_no_recursive_lock);
    MDB_RUN_TEST(setup_db, teardown_db, test_ts_query_callback_reentry_no_recursive_lock);
    MDB_RUN_TEST(setup_db, teardown_db, test_rel_iter_callback_reentry_no_recursive_lock);
    return MDB_RESULT();
}
