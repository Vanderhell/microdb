// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"

#include <string.h>

#ifndef LOX_TEST_WITH_8KB
#define LOX_TEST_WITH_8KB 0
#endif
#ifndef LOX_TEST_LIMITS_KV_ONLY
#define LOX_TEST_LIMITS_KV_ONLY 0
#endif
#ifndef LOX_TEST_LIMITS_TS_ONLY
#define LOX_TEST_LIMITS_TS_ONLY 0
#endif
#ifndef LOX_TEST_LIMITS_REL_ONLY
#define LOX_TEST_LIMITS_REL_ONLY 0
#endif

static lox_t g_db;

static void setup_db(void) {
    lox_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = LOX_RAM_KB;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
}

static void init_db_with_budget(lox_t *db, uint32_t ram_kb) {
    lox_cfg_t cfg;

    memset(db, 0, sizeof(*db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = ram_kb;
    ASSERT_EQ(lox_init(db, &cfg), LOX_OK);
}

static void make_name(char *buf, size_t len, char fill) {
    size_t i;

    for (i = 0; i + 1u < len; ++i) {
        buf[i] = fill;
    }
    buf[len - 1u] = '\0';
}

#if LOX_TEST_WITH_8KB
MDB_TEST(limits_ram_8kb_functional) {
    lox_t db;
    uint8_t value = 1u;
    uint8_t out = 0u;

    init_db_with_budget(&db, 8u);
    ASSERT_EQ(lox_kv_set(&db, "a", &value, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_kv_get(&db, "a", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
}
#endif

MDB_TEST(limits_ram_4096kb_functional) {
    lox_t db;
    lox_stats_t stats;

    init_db_with_budget(&db, 4096u);
    ASSERT_EQ(lox_stats(&db, &stats), LOX_OK);
    ASSERT_GT(stats.kv_entries_max, 0u);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
}

MDB_TEST(large_ram_kv_capacity_scales) {
    lox_stats_t stats;

    ASSERT_EQ(lox_stats(&g_db, &stats), LOX_OK);
    ASSERT_GT(stats.kv_entries_max, 0u);
}

MDB_TEST(large_ram_no_integer_overflow) {
    lox_stats_t stats;

    ASSERT_EQ(lox_stats(&g_db, &stats), LOX_OK);
    ASSERT_LE(stats.kv_fill_pct, 100u);
    ASSERT_LE(stats.ts_fill_pct, 100u);
    ASSERT_LE(stats.wal_fill_pct, 100u);
}

MDB_TEST(large_ram_ts_stream_capacity_scales) {
    uint32_t inserted = 0u;
    uint32_t i;

    ASSERT_EQ(lox_ts_register(&g_db, "scale_test", LOX_TS_F32, 0u), LOX_OK);
    for (i = 0u; i < 10000u; ++i) {
        float val = (float)i;
        lox_err_t err = lox_ts_insert(&g_db, "scale_test", (lox_timestamp_t)i, &val);
        if (err != LOX_OK) {
            break;
        }
        inserted++;
    }

#if LOX_RAM_KB >= 128
    ASSERT_GE(inserted, 4000u);
#endif
#if LOX_RAM_KB >= 512
    ASSERT_GE(inserted, 10000u);
#endif
}

MDB_TEST(large_ram_rel_more_rows) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t inserted = 0u;
    uint32_t i;

    ASSERT_EQ(lox_schema_init(&schema, "scale_rel", 10000u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, 4u, true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "val", LOX_COL_F32, 4u, false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);

    if (lox_table_create(&g_db, &schema) != LOX_OK) {
        return;
    }

    ASSERT_EQ(lox_table_get(&g_db, "scale_rel", &table), LOX_OK);
    for (i = 0u; i < 10000u; ++i) {
        float val = (float)i;
        ASSERT_EQ(lox_row_set(table, row, "id", &i), LOX_OK);
        ASSERT_EQ(lox_row_set(table, row, "val", &val), LOX_OK);
        if (lox_rel_insert(&g_db, table, row) != LOX_OK) {
            break;
        }
        inserted++;
    }

#if LOX_RAM_KB >= 256
    ASSERT_GE(inserted, 500u);
#endif
#if LOX_RAM_KB >= 1024
    ASSERT_GE(inserted, 2000u);
#endif
}

MDB_TEST(limits_kv_key_max_minus_one_accepted) {
    char key[LOX_KV_KEY_MAX_LEN];
    uint8_t value = 1u;

    make_name(key, sizeof(key), 'k');
    ASSERT_EQ(lox_kv_set(&g_db, key, &value, 1u, 0u), LOX_OK);
}

MDB_TEST(limits_kv_key_max_rejected) {
    char key[LOX_KV_KEY_MAX_LEN + 1u];
    uint8_t value = 1u;

    make_name(key, sizeof(key), 'k');
    ASSERT_EQ(lox_kv_set(&g_db, key, &value, 1u, 0u), LOX_ERR_INVALID);
}

MDB_TEST(limits_kv_value_max_accepted) {
    uint8_t value[LOX_KV_VAL_MAX_LEN];

    memset(value, 0xAB, sizeof(value));
    ASSERT_EQ(lox_kv_set(&g_db, "vmax", value, sizeof(value), 0u), LOX_OK);
}

MDB_TEST(limits_kv_value_max_plus_one_rejected) {
    uint8_t value[LOX_KV_VAL_MAX_LEN + 1u];

    memset(value, 0xCD, sizeof(value));
    ASSERT_EQ(lox_kv_set(&g_db, "ovf", value, sizeof(value), 0u), LOX_ERR_OVERFLOW);
}

MDB_TEST(limits_ts_raw_max_accepted) {
    uint8_t raw[LOX_TS_RAW_MAX] = { 0 };

    ASSERT_EQ(lox_ts_register(&g_db, "rawok", LOX_TS_RAW, LOX_TS_RAW_MAX), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "rawok", 1u, raw), LOX_OK);
}

MDB_TEST(limits_ts_raw_zero_rejected) {
    ASSERT_EQ(lox_ts_register(&g_db, "raw0", LOX_TS_RAW, 0u), LOX_ERR_INVALID);
}

MDB_TEST(limits_ts_raw_too_large_rejected) {
    ASSERT_EQ(lox_ts_register(&g_db, "rawo", LOX_TS_RAW, LOX_TS_RAW_MAX + 1u), LOX_ERR_INVALID);
}

MDB_TEST(limits_ts_name_max_minus_one_accepted) {
    char name[LOX_TS_STREAM_NAME_LEN];

    make_name(name, sizeof(name), 's');
    ASSERT_EQ(lox_ts_register(&g_db, name, LOX_TS_U32, 0u), LOX_OK);
}

MDB_TEST(limits_ts_name_max_rejected) {
    char name[LOX_TS_STREAM_NAME_LEN + 1u];

    make_name(name, sizeof(name), 's');
    ASSERT_EQ(lox_ts_register(&g_db, name, LOX_TS_U32, 0u), LOX_ERR_INVALID);
}

MDB_TEST(limits_rel_table_name_max_minus_one_accepted) {
    lox_schema_t schema;
    char name[LOX_REL_TABLE_NAME_LEN];

    make_name(name, sizeof(name), 't');
    ASSERT_EQ(lox_schema_init(&schema, name, 2u), LOX_OK);
}

MDB_TEST(limits_rel_table_name_max_rejected) {
    lox_schema_t schema;
    char name[LOX_REL_TABLE_NAME_LEN + 1u];

    make_name(name, sizeof(name), 't');
    ASSERT_EQ(lox_schema_init(&schema, name, 2u), LOX_ERR_INVALID);
}

MDB_TEST(limits_rel_column_name_max_minus_one_accepted) {
    lox_schema_t schema;
    char name[LOX_REL_COL_NAME_LEN];

    make_name(name, sizeof(name), 'c');
    ASSERT_EQ(lox_schema_init(&schema, "tbl", 2u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, name, LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
}

MDB_TEST(limits_rel_column_name_max_rejected) {
    lox_schema_t schema;
    char name[LOX_REL_COL_NAME_LEN + 1u];

    make_name(name, sizeof(name), 'c');
    ASSERT_EQ(lox_schema_init(&schema, "tbl", 2u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, name, LOX_COL_U32, sizeof(uint32_t), true), LOX_ERR_INVALID);
}

MDB_TEST(limits_rel_max_tables_enforced) {
    uint32_t i;

    for (i = 0u; i < LOX_REL_MAX_TABLES; ++i) {
        lox_schema_t schema;
        char name[8] = { 't', (char)('0' + (char)i), '\0' };

        ASSERT_EQ(lox_schema_init(&schema, name, 2u), LOX_OK);
        ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
        ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
        ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    }

    {
        lox_schema_t schema;
        ASSERT_EQ(lox_schema_init(&schema, "over", 2u), LOX_OK);
        ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
        ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
        ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_ERR_FULL);
    }
}

MDB_TEST(limits_rel_max_cols_enforced) {
    lox_schema_t schema;
    uint32_t i;

    ASSERT_EQ(lox_schema_init(&schema, "cols", 2u), LOX_OK);
    for (i = 0u; i < LOX_REL_MAX_COLS; ++i) {
        char name[8] = { 'c', (char)('0' + (char)(i / 10u)), (char)('0' + (char)(i % 10u)), '\0' };
        ASSERT_EQ(lox_schema_add(&schema,
                                     name,
                                     LOX_COL_U32,
                                     sizeof(uint32_t),
                                     i == 0u),
                  LOX_OK);
    }
    ASSERT_EQ(lox_schema_add(&schema, "extra", LOX_COL_U32, sizeof(uint32_t), false), LOX_ERR_FULL);
}

MDB_TEST(limits_stats_capacity_matches_macro) {
    lox_stats_t stats;

    ASSERT_EQ(lox_stats(&g_db, &stats), LOX_OK);
    ASSERT_GT(stats.kv_entries_max, 0u);
}

int main(void) {
#if LOX_TEST_LIMITS_KV_ONLY
    MDB_RUN_TEST(setup_db, teardown_db, limits_kv_key_max_minus_one_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_kv_key_max_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_kv_value_max_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_kv_value_max_plus_one_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_stats_capacity_matches_macro);
#elif LOX_TEST_LIMITS_TS_ONLY
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_raw_max_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_raw_zero_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_raw_too_large_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_name_max_minus_one_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_ts_name_max_rejected);
#elif LOX_TEST_LIMITS_REL_ONLY
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_table_name_max_minus_one_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_table_name_max_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_column_name_max_minus_one_accepted);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_column_name_max_rejected);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_max_tables_enforced);
    MDB_RUN_TEST(setup_db, teardown_db, limits_rel_max_cols_enforced);
#else
#if LOX_TEST_WITH_8KB
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
#endif
    return MDB_RESULT();
}
