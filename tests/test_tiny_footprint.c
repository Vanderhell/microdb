// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"

#include <string.h>

static microdb_t g_db;

static void setup_db(void) {
    microdb_cfg_t cfg;
    memset(&g_db, 0, sizeof(g_db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 8u;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    (void)microdb_deinit(&g_db);
}

MDB_TEST(tiny_kv_works) {
    uint8_t in = 33u;
    uint8_t out = 0u;
    size_t out_len = 0u;
    ASSERT_EQ(microdb_kv_set(&g_db, "k", &in, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_get(&g_db, "k", &out, sizeof(out), &out_len), MICRODB_OK);
    ASSERT_EQ(out_len, 1u);
    ASSERT_EQ(out, in);
}

MDB_TEST(tiny_ts_rel_disabled) {
    microdb_table_t *t = NULL;
    uint32_t v = 1u;
    ASSERT_EQ(microdb_ts_register(&g_db, "s", MICRODB_TS_U32, 0u), MICRODB_ERR_DISABLED);
    ASSERT_EQ(microdb_table_get(&g_db, "t", &t), MICRODB_ERR_DISABLED);
    ASSERT_EQ(microdb_admit_ts_insert(&g_db, "s", sizeof(v), &(microdb_admission_t){0}), MICRODB_ERR_DISABLED);
    ASSERT_EQ(microdb_admit_rel_insert(&g_db, "t", sizeof(v), &(microdb_admission_t){0}), MICRODB_ERR_DISABLED);
}

MDB_TEST(tiny_wal_off) {
    microdb_db_stats_t dbs;
    ASSERT_EQ(microdb_get_db_stats(&g_db, &dbs), MICRODB_OK);
    ASSERT_EQ(dbs.wal_bytes_total, 0u);
    ASSERT_EQ(dbs.wal_bytes_used, 0u);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, tiny_kv_works);
    MDB_RUN_TEST(setup_db, teardown_db, tiny_ts_rel_disabled);
    MDB_RUN_TEST(setup_db, teardown_db, tiny_wal_off);
    return MDB_RESULT();
}
