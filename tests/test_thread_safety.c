// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"

#include <string.h>

static lox_t g_db;
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
    lox_cfg_t cfg;

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
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
}

MDB_TEST(test_lock_called_on_kv_put) {
    uint8_t value = 1u;

    ASSERT_EQ(lox_kv_put(&g_db, "a", &value, 1u), LOX_OK);
    ASSERT_EQ(g_lock_count, 1u);
    ASSERT_EQ(g_unlock_count, 1u);
}

MDB_TEST(test_lock_called_on_kv_get) {
    uint8_t value = 1u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_put(&g_db, "a", &value, 1u), LOX_OK);
    g_lock_count = 0u;
    g_unlock_count = 0u;
    ASSERT_EQ(lox_kv_get(&g_db, "a", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(g_lock_count, 1u);
    ASSERT_EQ(g_unlock_count, 1u);
}

MDB_TEST(test_counts_balanced_after_sequence) {
    uint8_t value = 1u;
    uint8_t out = 0u;
    uint32_t i;

    ASSERT_EQ(lox_ts_register(&g_db, "s", LOX_TS_U32, 0u), LOX_OK);
    g_lock_count = 0u;
    g_unlock_count = 0u;

    for (i = 0u; i < 10u; ++i) {
        uint32_t tsv = i;
        ASSERT_EQ(lox_kv_put(&g_db, "a", &value, 1u), LOX_OK);
        ASSERT_EQ(lox_kv_get(&g_db, "a", &out, 1u, NULL), LOX_OK);
        ASSERT_EQ(lox_ts_insert(&g_db, "s", i, &tsv), LOX_OK);
    }

    ASSERT_EQ(g_lock_count, g_unlock_count);
}

MDB_TEST(test_null_hooks_are_safe) {
    lox_t db;
    lox_cfg_t cfg;
    uint8_t value = 1u;

    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    cfg.lock_create = mock_lock_create;
    cfg.lock = NULL;
    cfg.unlock = NULL;
    cfg.lock_destroy = mock_lock_destroy;
    ASSERT_EQ(lox_init(&db, &cfg), LOX_OK);
    ASSERT_EQ(lox_kv_put(&db, "n", &value, 1u), LOX_OK);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
}

MDB_TEST(test_lock_called_on_compact) {
    uint8_t value = 1u;
    ASSERT_EQ(lox_kv_put(&g_db, "c", &value, 1u), LOX_OK);
    g_lock_count = 0u;
    g_unlock_count = 0u;
    ASSERT_EQ(lox_compact(&g_db), LOX_OK);
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
    (void)lox_kv_get(&g_db, "r", &out, 1u, NULL);
    return true;
}

MDB_TEST(test_kv_iter_callback_reentry_no_recursive_lock) {
    uint8_t value = 1u;
    ASSERT_EQ(lox_kv_put(&g_db, "r", &value, 1u), LOX_OK);
    g_reentrant_lock = 0u;
    ASSERT_EQ(lox_kv_iter(&g_db, kv_reenter_cb, NULL), LOX_OK);
    ASSERT_EQ(g_reentrant_lock, 0u);
}

static bool ts_reenter_cb(const lox_ts_sample_t *sample, void *ctx) {
    lox_ts_sample_t out;
    (void)sample;
    (void)ctx;
    (void)lox_ts_last(&g_db, "rt", &out);
    return true;
}

MDB_TEST(test_ts_query_callback_reentry_no_recursive_lock) {
    uint32_t value = 1u;
    ASSERT_EQ(lox_ts_register(&g_db, "rt", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "rt", 1u, &value), LOX_OK);
    g_reentrant_lock = 0u;
    ASSERT_EQ(lox_ts_query(&g_db, "rt", 0u, 10u, ts_reenter_cb, NULL), LOX_OK);
    ASSERT_EQ(g_reentrant_lock, 0u);
}

static bool rel_reenter_cb(const void *row_buf, void *ctx) {
    lox_table_t **table_ptr = (lox_table_t **)ctx;
    uint8_t out[64] = { 0 };
    uint32_t id = 1u;
    (void)row_buf;
    (void)lox_rel_find_by(&g_db, *table_ptr, "id", &id, out);
    return true;
}

MDB_TEST(test_rel_iter_callback_reentry_no_recursive_lock) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 2u;

    ASSERT_EQ(lox_schema_init(&schema, "t", 8u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "age", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "t", &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);

    g_reentrant_lock = 0u;
    ASSERT_EQ(lox_rel_iter(&g_db, table, rel_reenter_cb, &table), LOX_OK);
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
