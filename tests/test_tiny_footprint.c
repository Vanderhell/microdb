// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"

#include <string.h>

static lox_t g_db;

static void setup_db(void) {
    lox_cfg_t cfg;
    memset(&g_db, 0, sizeof(g_db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 8u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
}

MDB_TEST(tiny_kv_works) {
    uint8_t in = 33u;
    uint8_t out = 0u;
    size_t out_len = 0u;
    ASSERT_EQ(lox_kv_set(&g_db, "k", &in, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "k", &out, sizeof(out), &out_len), LOX_OK);
    ASSERT_EQ(out_len, 1u);
    ASSERT_EQ(out, in);
}

MDB_TEST(tiny_ts_rel_disabled) {
    lox_table_t *t = NULL;
    uint32_t v = 1u;
    ASSERT_EQ(lox_ts_register(&g_db, "s", LOX_TS_U32, 0u), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_table_get(&g_db, "t", &t), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_admit_ts_insert(&g_db, "s", sizeof(v), &(lox_admission_t){0}), LOX_ERR_DISABLED);
    ASSERT_EQ(lox_admit_rel_insert(&g_db, "t", sizeof(v), &(lox_admission_t){0}), LOX_ERR_DISABLED);
}

MDB_TEST(tiny_wal_off) {
    lox_db_stats_t dbs;
    ASSERT_EQ(lox_get_db_stats(&g_db, &dbs), LOX_OK);
    ASSERT_EQ(dbs.wal_bytes_total, 0u);
    ASSERT_EQ(dbs.wal_bytes_used, 0u);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, tiny_kv_works);
    MDB_RUN_TEST(setup_db, teardown_db, tiny_ts_rel_disabled);
    MDB_RUN_TEST(setup_db, teardown_db, tiny_wal_off);
    return MDB_RESULT();
}
