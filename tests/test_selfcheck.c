// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../src/lox_internal.h"

#include <string.h>

static lox_t g_db;

typedef struct {
    uint16_t id;
    uint8_t v;
} rel_row_t;

static void setup_db(void) {
    lox_cfg_t cfg;
    memset(&g_db, 0, sizeof(g_db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
}

static void create_rel_table(lox_table_t **out_table) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    ASSERT_EQ(lox_schema_init(&schema, "t", 8u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U16, sizeof(uint16_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "v", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "t", &table), LOX_OK);
    *out_table = table;
}

MDB_TEST(selfcheck_clean_db_returns_ok) {
    lox_selfcheck_result_t sc;
    uint32_t tsv = 0u;
    lox_table_t *table;
    rel_row_t row;

    ASSERT_EQ(lox_kv_put(&g_db, "k1", &(uint8_t){1u}, 1u), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "k2", &(uint8_t){2u}, 1u), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "k3", &(uint8_t){3u}, 1u), LOX_OK);

    ASSERT_EQ(lox_ts_register(&g_db, "s", LOX_TS_U32, 0u), LOX_OK);
    for (tsv = 0u; tsv < 5u; ++tsv) {
        ASSERT_EQ(lox_ts_insert(&g_db, "s", tsv + 1u, &tsv), LOX_OK);
    }

    create_rel_table(&table);
    row.id = 1u; row.v = 11u; ASSERT_EQ(lox_rel_insert(&g_db, table, &row), LOX_OK);
    row.id = 2u; row.v = 12u; ASSERT_EQ(lox_rel_insert(&g_db, table, &row), LOX_OK);
    row.id = 3u; row.v = 13u; ASSERT_EQ(lox_rel_insert(&g_db, table, &row), LOX_OK);

    memset(&sc, 0, sizeof(sc));
    ASSERT_EQ(lox_selfcheck(&g_db, &sc), LOX_OK);
    ASSERT_EQ(sc.kv_ok, 1u);
    ASSERT_EQ(sc.ts_ok, 1u);
    ASSERT_EQ(sc.rel_ok, 1u);
    ASSERT_EQ(sc.wal_ok, 1u);
    ASSERT_EQ(sc.kv_anomalies, 0u);
    ASSERT_EQ(sc.ts_anomalies, 0u);
    ASSERT_EQ(sc.rel_anomalies, 0u);
}

MDB_TEST(selfcheck_kv_live_bytes_mismatch) {
    lox_selfcheck_result_t sc;
    lox_core_t *core = lox_core(&g_db);
    ASSERT_EQ(lox_kv_put(&g_db, "k", &(uint8_t){1u}, 1u), LOX_OK);
    core->kv.live_value_bytes += 999u;
    ASSERT_EQ(lox_selfcheck(&g_db, &sc), LOX_ERR_CORRUPT);
    ASSERT_GT(sc.kv_anomalies, 0u);
}

MDB_TEST(selfcheck_ts_count_exceeds_capacity) {
    lox_selfcheck_result_t sc;
    lox_core_t *core = lox_core(&g_db);
    ASSERT_EQ(lox_ts_register(&g_db, "s", LOX_TS_U32, 0u), LOX_OK);
    core->ts.streams[0].count = core->ts.streams[0].capacity + 1u;
    ASSERT_EQ(lox_selfcheck(&g_db, &sc), LOX_ERR_CORRUPT);
    ASSERT_GT(sc.ts_anomalies, 0u);
}

MDB_TEST(selfcheck_rel_bitmap_mismatch) {
    lox_selfcheck_result_t sc;
    lox_core_t *core = lox_core(&g_db);
    lox_table_t *table = NULL;
    create_rel_table(&table);
    rel_row_t row;
    row.id = 1u; row.v = 1u; ASSERT_EQ(lox_rel_insert(&g_db, table, &row), LOX_OK);
    row.id = 2u; row.v = 2u; ASSERT_EQ(lox_rel_insert(&g_db, table, &row), LOX_OK);
    core->rel.tables[0].live_count = 5u;
    ASSERT_EQ(lox_selfcheck(&g_db, &sc), LOX_ERR_CORRUPT);
    ASSERT_GT(sc.rel_anomalies, 0u);
}

MDB_TEST(selfcheck_rel_index_not_sorted) {
    lox_selfcheck_result_t sc;
    lox_core_t *core = lox_core(&g_db);
    lox_table_t *table = NULL;
    create_rel_table(&table);
    rel_row_t row;
    lox_index_entry_t tmp;

    row.id = 10u; row.v = 1u; ASSERT_EQ(lox_rel_insert(&g_db, table, &row), LOX_OK);
    row.id = 20u; row.v = 2u; ASSERT_EQ(lox_rel_insert(&g_db, table, &row), LOX_OK);
    row.id = 30u; row.v = 3u; ASSERT_EQ(lox_rel_insert(&g_db, table, &row), LOX_OK);

    tmp = core->rel.tables[0].index[0];
    core->rel.tables[0].index[0] = core->rel.tables[0].index[2];
    core->rel.tables[0].index[2] = tmp;

    ASSERT_EQ(lox_selfcheck(&g_db, &sc), LOX_ERR_CORRUPT);
    ASSERT_GT(sc.rel_anomalies, 0u);
}

MDB_TEST(selfcheck_first_anomaly_string_populated) {
    lox_selfcheck_result_t sc;
    lox_core_t *core = lox_core(&g_db);
    ASSERT_EQ(lox_kv_put(&g_db, "k", &(uint8_t){1u}, 1u), LOX_OK);
    core->kv.live_value_bytes += 1u;
    ASSERT_EQ(lox_selfcheck(&g_db, &sc), LOX_ERR_CORRUPT);
    ASSERT_EQ(sc.first_anomaly[0] != '\0', 1);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, selfcheck_clean_db_returns_ok);
    MDB_RUN_TEST(setup_db, teardown_db, selfcheck_kv_live_bytes_mismatch);
    MDB_RUN_TEST(setup_db, teardown_db, selfcheck_ts_count_exceeds_capacity);
    MDB_RUN_TEST(setup_db, teardown_db, selfcheck_rel_bitmap_mismatch);
    MDB_RUN_TEST(setup_db, teardown_db, selfcheck_rel_index_not_sorted);
    MDB_RUN_TEST(setup_db, teardown_db, selfcheck_first_anomaly_string_populated);
    return MDB_RESULT();
}
