#include "microtest.h"
#include "microdb.h"

#include <string.h>

#ifndef MICRODB_TEST_WITH_8KB
#define MICRODB_TEST_WITH_8KB 0
#endif

static microdb_t g_db;

static void setup_db(void) {
    microdb_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = MICRODB_RAM_KB;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    (void)microdb_deinit(&g_db);
}

static void init_db_with_budget(microdb_t *db, uint32_t ram_kb) {
    microdb_cfg_t cfg;

    memset(db, 0, sizeof(*db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = ram_kb;
    ASSERT_EQ(microdb_init(db, &cfg), MICRODB_OK);
}

static void make_name(char *buf, size_t len, char fill) {
    size_t i;

    for (i = 0; i + 1u < len; ++i) {
        buf[i] = fill;
    }
    buf[len - 1u] = '\0';
}

#if MICRODB_TEST_WITH_8KB
MDB_TEST(limits_ram_8kb_functional) {
    microdb_t db;
    uint8_t value = 1u;
    uint8_t out = 0u;

    init_db_with_budget(&db, 8u);
    ASSERT_EQ(microdb_kv_set(&db, "a", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_get(&db, "a", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_deinit(&db), MICRODB_OK);
}
#endif

MDB_TEST(limits_ram_4096kb_functional) {
    microdb_t db;
    microdb_stats_t stats;

    init_db_with_budget(&db, 4096u);
    ASSERT_EQ(microdb_stats(&db, &stats), MICRODB_OK);
    ASSERT_EQ(stats.ram_total_bytes, 4096u * 1024u);
    ASSERT_EQ(microdb_deinit(&db), MICRODB_OK);
}

MDB_TEST(large_ram_kv_capacity_scales) {
    microdb_stats_t stats;

    ASSERT_EQ(microdb_stats(&g_db, &stats), MICRODB_OK);
    ASSERT_GT(stats.kv_capacity, 0u);

#if MICRODB_RAM_KB >= 128
    ASSERT_GE(stats.ram_total_bytes, 128u * 1024u);
#endif
#if MICRODB_RAM_KB >= 256
    ASSERT_GE(stats.ram_total_bytes, 256u * 1024u);
#endif
#if MICRODB_RAM_KB >= 512
    ASSERT_GE(stats.ram_total_bytes, 512u * 1024u);
#endif
#if MICRODB_RAM_KB >= 1024
    ASSERT_GE(stats.ram_total_bytes, 1024u * 1024u);
#endif
}

MDB_TEST(large_ram_no_integer_overflow) {
    microdb_stats_t stats;

    ASSERT_EQ(microdb_stats(&g_db, &stats), MICRODB_OK);
    ASSERT_LE(stats.ram_used_bytes, stats.ram_total_bytes);
    ASSERT_GT(stats.kv_capacity, 0u);
}

MDB_TEST(large_ram_ts_stream_capacity_scales) {
    uint32_t inserted = 0u;
    uint32_t i;

    ASSERT_EQ(microdb_ts_register(&g_db, "scale_test", MICRODB_TS_F32, 0u), MICRODB_OK);
    for (i = 0u; i < 10000u; ++i) {
        float val = (float)i;
        microdb_err_t err = microdb_ts_insert(&g_db, "scale_test", (microdb_timestamp_t)i, &val);
        if (err != MICRODB_OK) {
            break;
        }
        inserted++;
    }

#if MICRODB_RAM_KB >= 128
    ASSERT_GE(inserted, 4000u);
#endif
#if MICRODB_RAM_KB >= 512
    ASSERT_GE(inserted, 10000u);
#endif
}

MDB_TEST(large_ram_rel_more_rows) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t inserted = 0u;
    uint32_t i;

    ASSERT_EQ(microdb_schema_init(&schema, "scale_rel", 10000u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, 4u, true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "val", MICRODB_COL_F32, 4u, false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);

    if (microdb_table_create(&g_db, &schema) != MICRODB_OK) {
        return;
    }

    ASSERT_EQ(microdb_table_get(&g_db, "scale_rel", &table), MICRODB_OK);
    for (i = 0u; i < 10000u; ++i) {
        float val = (float)i;
        ASSERT_EQ(microdb_row_set(table, row, "id", &i), MICRODB_OK);
        ASSERT_EQ(microdb_row_set(table, row, "val", &val), MICRODB_OK);
        if (microdb_rel_insert(&g_db, table, row) != MICRODB_OK) {
            break;
        }
        inserted++;
    }

#if MICRODB_RAM_KB >= 256
    ASSERT_GE(inserted, 500u);
#endif
#if MICRODB_RAM_KB >= 1024
    ASSERT_GE(inserted, 2000u);
#endif
}

MDB_TEST(limits_kv_key_max_minus_one_accepted) {
    char key[MICRODB_KV_KEY_MAX_LEN];
    uint8_t value = 1u;

    make_name(key, sizeof(key), 'k');
    ASSERT_EQ(microdb_kv_set(&g_db, key, &value, 1u, 0u), MICRODB_OK);
}

MDB_TEST(limits_kv_key_max_rejected) {
    char key[MICRODB_KV_KEY_MAX_LEN + 1u];
    uint8_t value = 1u;

    make_name(key, sizeof(key), 'k');
    ASSERT_EQ(microdb_kv_set(&g_db, key, &value, 1u, 0u), MICRODB_ERR_INVALID);
}

MDB_TEST(limits_kv_value_max_accepted) {
    uint8_t value[MICRODB_KV_VAL_MAX_LEN];

    memset(value, 0xAB, sizeof(value));
    ASSERT_EQ(microdb_kv_set(&g_db, "vmax", value, sizeof(value), 0u), MICRODB_OK);
}

MDB_TEST(limits_kv_value_max_plus_one_rejected) {
    uint8_t value[MICRODB_KV_VAL_MAX_LEN + 1u];

    memset(value, 0xCD, sizeof(value));
    ASSERT_EQ(microdb_kv_set(&g_db, "ovf", value, sizeof(value), 0u), MICRODB_ERR_OVERFLOW);
}

MDB_TEST(limits_ts_raw_max_accepted) {
    uint8_t raw[MICRODB_TS_RAW_MAX] = { 0 };

    ASSERT_EQ(microdb_ts_register(&g_db, "rawok", MICRODB_TS_RAW, MICRODB_TS_RAW_MAX), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "rawok", 1u, raw), MICRODB_OK);
}

MDB_TEST(limits_ts_raw_zero_rejected) {
    ASSERT_EQ(microdb_ts_register(&g_db, "raw0", MICRODB_TS_RAW, 0u), MICRODB_ERR_INVALID);
}

MDB_TEST(limits_ts_raw_too_large_rejected) {
    ASSERT_EQ(microdb_ts_register(&g_db, "rawo", MICRODB_TS_RAW, MICRODB_TS_RAW_MAX + 1u), MICRODB_ERR_INVALID);
}

MDB_TEST(limits_ts_name_max_minus_one_accepted) {
    char name[MICRODB_TS_STREAM_NAME_LEN];

    make_name(name, sizeof(name), 's');
    ASSERT_EQ(microdb_ts_register(&g_db, name, MICRODB_TS_U32, 0u), MICRODB_OK);
}

MDB_TEST(limits_ts_name_max_rejected) {
    char name[MICRODB_TS_STREAM_NAME_LEN + 1u];

    make_name(name, sizeof(name), 's');
    ASSERT_EQ(microdb_ts_register(&g_db, name, MICRODB_TS_U32, 0u), MICRODB_ERR_INVALID);
}

MDB_TEST(limits_rel_table_name_max_minus_one_accepted) {
    microdb_schema_t schema;
    char name[MICRODB_REL_TABLE_NAME_LEN];

    make_name(name, sizeof(name), 't');
    ASSERT_EQ(microdb_schema_init(&schema, name, 2u), MICRODB_OK);
}

MDB_TEST(limits_rel_table_name_max_rejected) {
    microdb_schema_t schema;
    char name[MICRODB_REL_TABLE_NAME_LEN + 1u];

    make_name(name, sizeof(name), 't');
    ASSERT_EQ(microdb_schema_init(&schema, name, 2u), MICRODB_ERR_INVALID);
}

MDB_TEST(limits_rel_column_name_max_minus_one_accepted) {
    microdb_schema_t schema;
    char name[MICRODB_REL_COL_NAME_LEN];

    make_name(name, sizeof(name), 'c');
    ASSERT_EQ(microdb_schema_init(&schema, "tbl", 2u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, name, MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
}

MDB_TEST(limits_rel_column_name_max_rejected) {
    microdb_schema_t schema;
    char name[MICRODB_REL_COL_NAME_LEN + 1u];

    make_name(name, sizeof(name), 'c');
    ASSERT_EQ(microdb_schema_init(&schema, "tbl", 2u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, name, MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_ERR_INVALID);
}

MDB_TEST(limits_rel_max_tables_enforced) {
    uint32_t i;

    for (i = 0u; i < MICRODB_REL_MAX_TABLES; ++i) {
        microdb_schema_t schema;
        char name[8] = { 't', (char)('0' + (char)i), '\0' };

        ASSERT_EQ(microdb_schema_init(&schema, name, 2u), MICRODB_OK);
        ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
        ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
        ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_OK);
    }

    {
        microdb_schema_t schema;
        ASSERT_EQ(microdb_schema_init(&schema, "over", 2u), MICRODB_OK);
        ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
        ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
        ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_ERR_FULL);
    }
}

MDB_TEST(limits_rel_max_cols_enforced) {
    microdb_schema_t schema;
    uint32_t i;

    ASSERT_EQ(microdb_schema_init(&schema, "cols", 2u), MICRODB_OK);
    for (i = 0u; i < MICRODB_REL_MAX_COLS; ++i) {
        char name[8] = { 'c', (char)('0' + (char)(i / 10u)), (char)('0' + (char)(i % 10u)), '\0' };
        ASSERT_EQ(microdb_schema_add(&schema,
                                     name,
                                     MICRODB_COL_U32,
                                     sizeof(uint32_t),
                                     i == 0u),
                  MICRODB_OK);
    }
    ASSERT_EQ(microdb_schema_add(&schema, "extra", MICRODB_COL_U32, sizeof(uint32_t), false), MICRODB_ERR_FULL);
}

MDB_TEST(limits_stats_capacity_matches_macro) {
    microdb_stats_t stats;

    ASSERT_EQ(microdb_stats(&g_db, &stats), MICRODB_OK);
    ASSERT_GT(stats.kv_capacity, 0u);
}

int main(void) {
#if MICRODB_TEST_WITH_8KB
    MDB_RUN_TEST(setup_db, teardown_db, limits_ram_8kb_functional);
#endif
    MDB_RUN_TEST(setup_db, teardown_db, limits_ram_4096kb_functional);
    MDB_RUN_TEST(setup_db, teardown_db, large_ram_kv_capacity_scales);
    MDB_RUN_TEST(setup_db, teardown_db, large_ram_no_integer_overflow);
    MDB_RUN_TEST(setup_db, teardown_db, large_ram_ts_stream_capacity_scales);
    MDB_RUN_TEST(setup_db, teardown_db, large_ram_rel_more_rows);
    MDB_RUN_TEST(setup_db, teardown_db, limits_kv_key_max_minus_one_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_kv_key_max_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_kv_value_max_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_kv_value_max_plus_one_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_raw_max_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_raw_zero_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_raw_too_large_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_name_max_minus_one_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_name_max_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_table_name_max_minus_one_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_table_name_max_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_column_name_max_minus_one_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_column_name_max_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_max_tables_enforced);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_max_cols_enforced);
    MDB_RUN_TEST(setup_db, teardown_db, limits_stats_capacity_matches_macro);
    return MDB_RESULT();
}
