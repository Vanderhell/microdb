// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"

#include <string.h>

static lox_t g_db;

static void setup_db(void) {
    lox_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    memset(&g_db, 0, sizeof(g_db));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
}

static void make_rel_table(lox_table_t **out_table) {
    lox_schema_t schema;
    lox_err_t rc;

    rc = lox_table_get(&g_db, "m", out_table);
    if (rc == LOX_OK) return;
    ASSERT_EQ(rc, LOX_ERR_NOT_FOUND);

    ASSERT_EQ(lox_schema_init(&schema, "m", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "v", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "m", out_table), LOX_OK);
}

/* Core */
MDB_TEST(core_init_null_db_invalid) {
    lox_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(NULL, &cfg), LOX_ERR_INVALID);
}
MDB_TEST(core_init_null_cfg_invalid) { ASSERT_EQ(lox_init(&g_db, NULL), LOX_ERR_INVALID); }
MDB_TEST(core_deinit_null_db_invalid) { ASSERT_EQ(lox_deinit(NULL), LOX_ERR_INVALID); }
MDB_TEST(core_flush_null_db_invalid) { ASSERT_EQ(lox_flush(NULL), LOX_ERR_INVALID); }
MDB_TEST(core_inspect_null_db_invalid) {
    lox_stats_t st;
    ASSERT_EQ(lox_inspect(NULL, &st), LOX_ERR_INVALID);
}
MDB_TEST(core_inspect_null_out_invalid) { ASSERT_EQ(lox_inspect(&g_db, NULL), LOX_ERR_INVALID); }
MDB_TEST(core_stats_null_db_invalid) {
    lox_stats_t st;
    ASSERT_EQ(lox_stats(NULL, &st), LOX_ERR_INVALID);
}
MDB_TEST(core_stats_null_out_invalid) { ASSERT_EQ(lox_stats(&g_db, NULL), LOX_ERR_INVALID); }

/* TXN */
MDB_TEST(txn_begin_null_db_invalid) { ASSERT_EQ(lox_txn_begin(NULL), LOX_ERR_INVALID); }
MDB_TEST(txn_commit_null_db_invalid) { ASSERT_EQ(lox_txn_commit(NULL), LOX_ERR_INVALID); }
MDB_TEST(txn_rollback_null_db_invalid) { ASSERT_EQ(lox_txn_rollback(NULL), LOX_ERR_INVALID); }

/* KV null db */
MDB_TEST(kv_set_null_db_invalid) {
    uint8_t v = 1u;
    ASSERT_EQ(lox_kv_set(NULL, "k", &v, 1u, 0u), LOX_ERR_INVALID);
}
MDB_TEST(kv_get_null_db_invalid) {
    uint8_t out = 0u;
    size_t out_len = 0u;
    ASSERT_EQ(lox_kv_get(NULL, "k", &out, 1u, &out_len), LOX_ERR_INVALID);
}
MDB_TEST(kv_del_null_db_invalid) { ASSERT_EQ(lox_kv_del(NULL, "k"), LOX_ERR_INVALID); }
MDB_TEST(kv_exists_null_db_invalid) { ASSERT_EQ(lox_kv_exists(NULL, "k"), LOX_ERR_INVALID); }
MDB_TEST(kv_iter_null_db_invalid) { ASSERT_EQ(lox_kv_iter(NULL, (lox_kv_iter_cb_t)1, NULL), LOX_ERR_INVALID); }
MDB_TEST(kv_purge_null_db_invalid) { ASSERT_EQ(lox_kv_purge_expired(NULL), LOX_ERR_INVALID); }
MDB_TEST(kv_clear_null_db_invalid) { ASSERT_EQ(lox_kv_clear(NULL), LOX_ERR_INVALID); }

/* KV arg validation */
MDB_TEST(kv_set_null_key_invalid) {
    uint8_t v = 1u;
    ASSERT_EQ(lox_kv_set(&g_db, NULL, &v, 1u, 0u), LOX_ERR_INVALID);
}
MDB_TEST(kv_set_null_val_invalid) { ASSERT_EQ(lox_kv_set(&g_db, "k", NULL, 1u, 0u), LOX_ERR_INVALID); }
MDB_TEST(kv_get_null_buf_invalid) {
    size_t out_len = 0u;
    ASSERT_EQ(lox_kv_get(&g_db, "k", NULL, 1u, &out_len), LOX_ERR_INVALID);
}
MDB_TEST(kv_del_null_key_invalid) { ASSERT_EQ(lox_kv_del(&g_db, NULL), LOX_ERR_INVALID); }
MDB_TEST(kv_exists_null_key_invalid) { ASSERT_EQ(lox_kv_exists(&g_db, NULL), LOX_ERR_INVALID); }
MDB_TEST(kv_iter_null_cb_invalid) { ASSERT_EQ(lox_kv_iter(&g_db, NULL, NULL), LOX_ERR_INVALID); }

/* TS null db */
MDB_TEST(ts_register_null_db_invalid) { ASSERT_EQ(lox_ts_register(NULL, "s", LOX_TS_U32, 0u), LOX_ERR_INVALID); }
MDB_TEST(ts_insert_null_db_invalid) {
    uint32_t v = 1u;
    ASSERT_EQ(lox_ts_insert(NULL, "s", 1u, &v), LOX_ERR_INVALID);
}
MDB_TEST(ts_last_null_db_invalid) {
    lox_ts_sample_t s;
    ASSERT_EQ(lox_ts_last(NULL, "s", &s), LOX_ERR_INVALID);
}
MDB_TEST(ts_query_null_db_invalid) {
    ASSERT_EQ(lox_ts_query(NULL, "s", 0u, 1u, (lox_ts_query_cb_t)1, NULL), LOX_ERR_INVALID);
}
MDB_TEST(ts_query_buf_null_db_invalid) {
    lox_ts_sample_t b[1];
    size_t n = 0u;
    ASSERT_EQ(lox_ts_query_buf(NULL, "s", 0u, 1u, b, 1u, &n), LOX_ERR_INVALID);
}
MDB_TEST(ts_count_null_db_invalid) {
    size_t n = 0u;
    ASSERT_EQ(lox_ts_count(NULL, "s", 0u, 1u, &n), LOX_ERR_INVALID);
}
MDB_TEST(ts_clear_null_db_invalid) { ASSERT_EQ(lox_ts_clear(NULL, "s"), LOX_ERR_INVALID); }

/* TS arg validation */
MDB_TEST(ts_register_null_name_invalid) { ASSERT_EQ(lox_ts_register(&g_db, NULL, LOX_TS_U32, 0u), LOX_ERR_INVALID); }
MDB_TEST(ts_insert_null_val_invalid) { ASSERT_EQ(lox_ts_insert(&g_db, "s", 1u, NULL), LOX_ERR_INVALID); }
MDB_TEST(ts_last_null_out_invalid) { ASSERT_EQ(lox_ts_last(&g_db, "s", NULL), LOX_ERR_INVALID); }
MDB_TEST(ts_query_null_cb_invalid) { ASSERT_EQ(lox_ts_query(&g_db, "s", 0u, 1u, NULL, NULL), LOX_ERR_INVALID); }
MDB_TEST(ts_query_buf_null_buf_invalid) {
    size_t n = 0u;
    ASSERT_EQ(lox_ts_query_buf(&g_db, "s", 0u, 1u, NULL, 1u, &n), LOX_ERR_INVALID);
}
MDB_TEST(ts_count_null_out_invalid) { ASSERT_EQ(lox_ts_count(&g_db, "s", 0u, 1u, NULL), LOX_ERR_INVALID); }

/* REL schema validation */
MDB_TEST(rel_schema_init_null_schema_invalid) { ASSERT_EQ(lox_schema_init(NULL, "t", 1u), LOX_ERR_INVALID); }
MDB_TEST(rel_schema_init_null_name_invalid) {
    lox_schema_t s;
    ASSERT_EQ(lox_schema_init(&s, NULL, 1u), LOX_ERR_INVALID);
}
MDB_TEST(rel_schema_add_null_schema_invalid) {
    ASSERT_EQ(lox_schema_add(NULL, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_ERR_INVALID);
}
MDB_TEST(rel_schema_add_null_name_invalid) {
    lox_schema_t s;
    ASSERT_EQ(lox_schema_init(&s, "t", 1u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&s, NULL, LOX_COL_U32, sizeof(uint32_t), true), LOX_ERR_INVALID);
}
MDB_TEST(rel_schema_seal_null_schema_invalid) { ASSERT_EQ(lox_schema_seal(NULL), LOX_ERR_INVALID); }

/* REL table validation */
MDB_TEST(rel_table_create_null_db_invalid) {
    lox_schema_t s;
    ASSERT_EQ(lox_schema_init(&s, "t", 1u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&s, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&s), LOX_OK);
    ASSERT_EQ(lox_table_create(NULL, &s), LOX_ERR_INVALID);
}
MDB_TEST(rel_table_create_null_schema_invalid) { ASSERT_EQ(lox_table_create(&g_db, NULL), LOX_ERR_INVALID); }
MDB_TEST(rel_table_get_null_db_invalid) {
    lox_table_t *t = NULL;
    ASSERT_EQ(lox_table_get(NULL, "t", &t), LOX_ERR_INVALID);
}
MDB_TEST(rel_table_get_null_name_invalid) {
    lox_table_t *t = NULL;
    ASSERT_EQ(lox_table_get(&g_db, NULL, &t), LOX_ERR_INVALID);
}
MDB_TEST(rel_table_get_null_out_invalid) { ASSERT_EQ(lox_table_get(&g_db, "t", NULL), LOX_ERR_INVALID); }

/* REL row buffer validation */
MDB_TEST(rel_row_set_null_table_invalid) {
    uint8_t row[8] = {0};
    uint8_t v = 1u;
    ASSERT_EQ(lox_row_set(NULL, row, "id", &v), LOX_ERR_INVALID);
}
MDB_TEST(rel_row_set_null_row_invalid) {
    lox_table_t *t = NULL;
    uint8_t v = 1u;
    make_rel_table(&t);
    ASSERT_EQ(lox_row_set(t, NULL, "id", &v), LOX_ERR_INVALID);
}
MDB_TEST(rel_row_set_null_col_invalid) {
    lox_table_t *t = NULL;
    uint8_t row[64] = {0};
    uint8_t v = 1u;
    make_rel_table(&t);
    ASSERT_EQ(lox_row_set(t, row, NULL, &v), LOX_ERR_INVALID);
}
MDB_TEST(rel_row_set_null_val_invalid) {
    lox_table_t *t = NULL;
    uint8_t row[64] = {0};
    make_rel_table(&t);
    ASSERT_EQ(lox_row_set(t, row, "id", NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_row_get_null_table_invalid) {
    uint8_t row[8] = {0};
    uint8_t out = 0u;
    ASSERT_EQ(lox_row_get(NULL, row, "id", &out, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_row_get_null_row_invalid) {
    lox_table_t *t = NULL;
    uint8_t out = 0u;
    make_rel_table(&t);
    ASSERT_EQ(lox_row_get(t, NULL, "id", &out, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_row_get_null_col_invalid) {
    lox_table_t *t = NULL;
    uint8_t row[64] = {0};
    uint8_t out = 0u;
    make_rel_table(&t);
    ASSERT_EQ(lox_row_get(t, row, NULL, &out, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_row_get_null_out_invalid) {
    lox_table_t *t = NULL;
    uint8_t row[64] = {0};
    make_rel_table(&t);
    ASSERT_EQ(lox_row_get(t, row, "id", NULL, NULL), LOX_ERR_INVALID);
}

/* REL runtime null db */
MDB_TEST(rel_insert_null_db_invalid) {
    uint8_t row[8] = {0};
    ASSERT_EQ(lox_rel_insert(NULL, NULL, row), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_null_db_invalid) {
    uint32_t id = 1u;
    ASSERT_EQ(lox_rel_find(NULL, NULL, &id, (lox_rel_iter_cb_t)1, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_by_null_db_invalid) {
    uint32_t id = 1u;
    uint8_t row[8] = {0};
    ASSERT_EQ(lox_rel_find_by(NULL, NULL, "id", &id, row), LOX_ERR_INVALID);
}
MDB_TEST(rel_delete_null_db_invalid) {
    uint32_t id = 1u;
    ASSERT_EQ(lox_rel_delete(NULL, NULL, &id, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_iter_null_db_invalid) { ASSERT_EQ(lox_rel_iter(NULL, NULL, (lox_rel_iter_cb_t)1, NULL), LOX_ERR_INVALID); }
MDB_TEST(rel_clear_null_db_invalid) { ASSERT_EQ(lox_rel_clear(NULL, NULL), LOX_ERR_INVALID); }

/* REL runtime arg validation with valid table */
MDB_TEST(rel_insert_null_table_invalid) {
    uint8_t row[64] = {0};
    ASSERT_EQ(lox_rel_insert(&g_db, NULL, row), LOX_ERR_INVALID);
}
MDB_TEST(rel_insert_null_row_invalid) {
    lox_table_t *t = NULL;
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_insert(&g_db, t, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_null_table_invalid) {
    uint32_t id = 1u;
    ASSERT_EQ(lox_rel_find(&g_db, NULL, &id, (lox_rel_iter_cb_t)1, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_null_search_invalid) {
    lox_table_t *t = NULL;
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_find(&g_db, t, NULL, (lox_rel_iter_cb_t)1, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_null_cb_invalid) {
    lox_table_t *t = NULL;
    uint32_t id = 1u;
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_find(&g_db, t, &id, NULL, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_by_null_table_invalid) {
    uint32_t id = 1u;
    uint8_t out[64] = {0};
    ASSERT_EQ(lox_rel_find_by(&g_db, NULL, "id", &id, out), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_by_null_col_invalid) {
    lox_table_t *t = NULL;
    uint32_t id = 1u;
    uint8_t out[64] = {0};
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_find_by(&g_db, t, NULL, &id, out), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_by_null_search_invalid) {
    lox_table_t *t = NULL;
    uint8_t out[64] = {0};
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_find_by(&g_db, t, "id", NULL, out), LOX_ERR_INVALID);
}
MDB_TEST(rel_find_by_null_out_invalid) {
    lox_table_t *t = NULL;
    uint32_t id = 1u;
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_find_by(&g_db, t, "id", &id, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_delete_null_table_invalid) {
    uint32_t id = 1u;
    ASSERT_EQ(lox_rel_delete(&g_db, NULL, &id, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_delete_null_search_invalid) {
    lox_table_t *t = NULL;
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_delete(&g_db, t, NULL, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_iter_null_table_invalid) { ASSERT_EQ(lox_rel_iter(&g_db, NULL, (lox_rel_iter_cb_t)1, NULL), LOX_ERR_INVALID); }
MDB_TEST(rel_iter_null_cb_invalid) {
    lox_table_t *t = NULL;
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_iter(&g_db, t, NULL, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_count_null_table_invalid) { ASSERT_EQ(lox_rel_count(NULL, &(uint32_t){0u}), LOX_ERR_INVALID); }
MDB_TEST(rel_count_null_out_invalid) {
    lox_table_t *t = NULL;
    make_rel_table(&t);
    ASSERT_EQ(lox_rel_count(t, NULL), LOX_ERR_INVALID);
}
MDB_TEST(rel_clear_null_table_invalid) { ASSERT_EQ(lox_rel_clear(&g_db, NULL), LOX_ERR_INVALID); }

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, core_init_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, core_init_null_cfg_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, core_deinit_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, core_flush_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, core_inspect_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, core_inspect_null_out_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, core_stats_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, core_stats_null_out_invalid);

    MDB_RUN_TEST(setup_db, teardown_db, txn_begin_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, txn_commit_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, txn_rollback_null_db_invalid);

    MDB_RUN_TEST(setup_db, teardown_db, kv_set_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_get_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_del_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_exists_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_iter_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_purge_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_clear_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_set_null_key_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_set_null_val_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_get_null_buf_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_del_null_key_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_exists_null_key_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, kv_iter_null_cb_invalid);

    MDB_RUN_TEST(setup_db, teardown_db, ts_register_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_insert_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_last_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_query_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_query_buf_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_count_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_clear_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_register_null_name_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_insert_null_val_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_last_null_out_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_query_null_cb_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_query_buf_null_buf_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, ts_count_null_out_invalid);

    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_init_null_schema_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_init_null_name_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_add_null_schema_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_add_null_name_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_seal_null_schema_invalid);

    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_null_schema_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_get_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_get_null_name_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_get_null_out_invalid);

    MDB_RUN_TEST(setup_db, teardown_db, rel_row_set_null_table_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_set_null_row_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_set_null_col_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_set_null_val_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_get_null_table_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_get_null_row_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_get_null_col_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_get_null_out_invalid);

    MDB_RUN_TEST(setup_db, teardown_db, rel_insert_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_iter_null_db_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_clear_null_db_invalid);

    MDB_RUN_TEST(setup_db, teardown_db, rel_insert_null_table_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_insert_null_row_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_null_table_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_null_search_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_null_cb_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_null_table_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_null_col_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_null_search_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_null_out_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_null_table_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_null_search_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_iter_null_table_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_iter_null_cb_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_count_null_table_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_count_null_out_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_clear_null_table_invalid);
    return MDB_RESULT();
}
