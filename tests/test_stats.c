// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"

#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static const char *g_path = "stats_test.bin";

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

static void setup_storage_db(void) {
    lox_cfg_t cfg;

    lox_port_posix_remove(g_path);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void setup_storage_low_wal_threshold_db(void) {
    lox_cfg_t cfg;

    lox_port_posix_remove(g_path);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.wal_compact_auto = 1u;
    cfg.wal_compact_threshold_pct = 1u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_storage_db(void) {
    (void)lox_deinit(&g_db);
    lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(g_path);
}

MDB_TEST(test_inspect_empty_db) {
    lox_stats_t stats;

    ASSERT_EQ(lox_inspect(&g_db, &stats), LOX_OK);
    ASSERT_EQ(stats.kv_entries_used, 0u);
    ASSERT_EQ(stats.kv_collision_count, 0u);
    ASSERT_EQ(stats.kv_eviction_count, 0u);
    ASSERT_EQ(stats.ts_streams_registered, 0u);
    ASSERT_EQ(stats.ts_samples_total, 0u);
    ASSERT_EQ(stats.wal_bytes_used, 0u);
    ASSERT_EQ(stats.rel_tables_count, 0u);
    ASSERT_EQ(stats.rel_rows_total, 0u);
    ASSERT_EQ(stats.kv_fill_pct, 0u);
    ASSERT_EQ(stats.ts_fill_pct, 0u);
    ASSERT_EQ(stats.wal_fill_pct, 0u);
}

MDB_TEST(test_inspect_kv_entries) {
    lox_stats_t stats;
    uint8_t value = 1u;
    uint32_t i;

    for (i = 0u; i < 8u; ++i) {
        char key[8] = { 0 };
        key[0] = 'k';
        key[1] = (char)('0' + (char)i);
        ASSERT_EQ(lox_kv_set(&g_db, key, &value, 1u, 0u), LOX_OK);
    }

    ASSERT_EQ(lox_inspect(&g_db, &stats), LOX_OK);
    ASSERT_EQ(stats.kv_entries_used, 8u);
}

MDB_TEST(test_inspect_kv_eviction) {
    lox_stats_t stats;
    uint8_t value = 1u;
    uint32_t i;

    for (i = 0u; i < LOX_KV_MAX_KEYS + 8u; ++i) {
        char key[12] = { 0 };
        key[0] = 'e';
        key[1] = (char)('0' + (char)((i / 100u) % 10u));
        key[2] = (char)('0' + (char)((i / 10u) % 10u));
        key[3] = (char)('0' + (char)(i % 10u));
        ASSERT_EQ(lox_kv_set(&g_db, key, &value, 1u, 0u), LOX_OK);
    }

    ASSERT_EQ(lox_inspect(&g_db, &stats), LOX_OK);
    ASSERT_GT(stats.kv_eviction_count, 0u);
}

MDB_TEST(test_inspect_ts_samples) {
    lox_stats_t stats;
    uint32_t i;

    ASSERT_EQ(lox_ts_register(&g_db, "temp", LOX_TS_U32, 0u), LOX_OK);
    for (i = 0u; i < 6u; ++i) {
        uint32_t v = i + 1u;
        ASSERT_EQ(lox_ts_insert(&g_db, "temp", (lox_timestamp_t)i, &v), LOX_OK);
    }

    ASSERT_EQ(lox_inspect(&g_db, &stats), LOX_OK);
    ASSERT_EQ(stats.ts_streams_registered, 1u);
    ASSERT_EQ(stats.ts_samples_total, 6u);
}

MDB_TEST(test_inspect_wal) {
    lox_stats_t stats;
    uint8_t value = 7u;

    ASSERT_EQ(lox_kv_set(&g_db, "wal", &value, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_inspect(&g_db, &stats), LOX_OK);
    ASSERT_GT(stats.wal_bytes_used, 0u);
}

MDB_TEST(test_split_stats_api_empty_db) {
    lox_db_stats_t dbs;
    lox_kv_stats_t kvs;
    lox_ts_stats_t tss;
    lox_rel_stats_t rls;

    ASSERT_EQ(lox_get_db_stats(&g_db, &dbs), LOX_OK);
    ASSERT_EQ(lox_get_kv_stats(&g_db, &kvs), LOX_OK);
    ASSERT_EQ(lox_get_ts_stats(&g_db, &tss), LOX_OK);
    ASSERT_EQ(lox_get_rel_stats(&g_db, &rls), LOX_OK);

    ASSERT_EQ(dbs.last_runtime_error, LOX_OK);
    ASSERT_EQ(dbs.last_recovery_status, LOX_OK);
    ASSERT_EQ(kvs.live_keys, 0u);
    ASSERT_EQ(kvs.tombstones, 0u);
    ASSERT_EQ(tss.stream_count, 0u);
    ASSERT_EQ(tss.retained_samples, 0u);
    ASSERT_EQ(rls.table_count, 0u);
    ASSERT_EQ(rls.rows_live, 0u);
}

MDB_TEST(test_split_stats_api_kv_ts_rel) {
    lox_schema_t s;
    lox_table_t *t = NULL;
    uint8_t row[32] = {0};
    uint32_t id = 1u;
    uint8_t state = 3u;
    lox_kv_stats_t kvs;
    lox_ts_stats_t tss;
    lox_rel_stats_t rls;
    uint32_t deleted = 0u;
    uint8_t v = 42u;

    ASSERT_EQ(lox_kv_set(&g_db, "k", &v, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_kv_del(&g_db, "k"), LOX_OK);
    ASSERT_EQ(lox_get_kv_stats(&g_db, &kvs), LOX_OK);
    ASSERT_EQ(kvs.live_keys, 0u);
    ASSERT_GT(kvs.tombstones, 0u);

    ASSERT_EQ(lox_ts_register(&g_db, "s", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "s", 1u, &id), LOX_OK);
    ASSERT_EQ(lox_get_ts_stats(&g_db, &tss), LOX_OK);
    ASSERT_EQ(tss.stream_count, 1u);
    ASSERT_EQ(tss.retained_samples, 1u);

    ASSERT_EQ(lox_schema_init(&s, "devices", 2u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&s, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&s, "state", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&s), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &s), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "devices", &t), LOX_OK);
    ASSERT_EQ(lox_row_set(t, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(t, row, "state", &state), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, t, row), LOX_OK);
    ASSERT_EQ(lox_rel_delete(&g_db, t, &id, &deleted), LOX_OK);
    ASSERT_EQ(deleted, 1u);
    ASSERT_EQ(lox_get_rel_stats(&g_db, &rls), LOX_OK);
    ASSERT_EQ(rls.table_count, 1u);
    ASSERT_EQ(rls.rows_live, 0u);
    ASSERT_EQ(rls.rows_free, 2u);
    ASSERT_EQ(rls.indexed_tables, 1u);
}

MDB_TEST(test_split_db_stats_storage_fields) {
    lox_db_stats_t dbs;
    uint32_t compact_before = 0u;
    uint8_t value = 9u;

    ASSERT_EQ(lox_get_db_stats(&g_db, &dbs), LOX_OK);
    ASSERT_GT(dbs.effective_capacity_bytes, 0u);
    compact_before = dbs.compact_count;

    ASSERT_EQ(lox_kv_set(&g_db, "wal2", &value, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    ASSERT_EQ(lox_get_db_stats(&g_db, &dbs), LOX_OK);
    ASSERT_GT(dbs.compact_count, compact_before);
    ASSERT_EQ(dbs.last_runtime_error, LOX_OK);
}

MDB_TEST(test_effective_capacity_api) {
    lox_effective_capacity_t cap;

    ASSERT_EQ(lox_get_effective_capacity(&g_db, &cap), LOX_OK);
    ASSERT_GT(cap.kv_entries_usable, 0u);
    ASSERT_GE(cap.kv_entries_usable, cap.kv_entries_free);
    ASSERT_GT(cap.kv_value_bytes_usable, 0u);
    ASSERT_GE(cap.kv_value_bytes_usable, cap.kv_value_bytes_free_now);
    ASSERT_GE(cap.ts_samples_usable, cap.ts_samples_retained);
}

MDB_TEST(test_admission_api_kv_ts_rel) {
    lox_admission_t a;
    lox_schema_t s;
    lox_table_t *t = NULL;
    uint8_t row[32] = {0};
    uint32_t id = 1u;
    uint8_t state = 1u;
    uint32_t ts_val = 3u;

    ASSERT_EQ(lox_admit_kv_set(&g_db, "preflight", 8u, &a), LOX_OK);
    ASSERT_EQ(a.status, LOX_OK);
    ASSERT_EQ(a.deterministic_budget_ok, 1u);

    ASSERT_EQ(lox_ts_register(&g_db, "pre_ts", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_admit_ts_insert(&g_db, "pre_ts", sizeof(ts_val), &a), LOX_OK);
    ASSERT_EQ(a.status, LOX_OK);

    ASSERT_EQ(lox_schema_init(&s, "pre_rel", 2u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&s, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&s, "state", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&s), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &s), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "pre_rel", &t), LOX_OK);
    ASSERT_EQ(lox_row_set(t, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(t, row, "state", &state), LOX_OK);
    ASSERT_EQ(lox_admit_rel_insert(&g_db, "pre_rel", lox_table_row_size(t), &a), LOX_OK);
    ASSERT_EQ(a.status, LOX_OK);
}

MDB_TEST(test_admission_rel_would_compact_clears_deterministic_budget) {
    lox_admission_t a;
    lox_schema_t s;
    lox_table_t *t = NULL;
    uint8_t row[32] = {0};
    uint8_t kv_fill[64];
    uint32_t i;
    uint32_t id = 1u;
    uint8_t state = 1u;

    memset(kv_fill, 0xA5, sizeof(kv_fill));
    ASSERT_EQ(lox_schema_init(&s, "pre_rel_wc", 2u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&s, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&s, "state", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&s), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &s), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "pre_rel_wc", &t), LOX_OK);
    ASSERT_EQ(lox_row_set(t, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(t, row, "state", &state), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, t, row), LOX_OK);

    memset(&a, 0, sizeof(a));
    for (i = 0u; i < 512u; ++i) {
        char key[6];
        key[0] = 'w';
        key[1] = (char)('0' + (char)((i / 1000u) % 10u));
        key[2] = (char)('0' + (char)((i / 100u) % 10u));
        key[3] = (char)('0' + (char)((i / 10u) % 10u));
        key[4] = (char)('0' + (char)(i % 10u));
        key[5] = '\0';
        ASSERT_EQ(lox_kv_set(&g_db, key, kv_fill, sizeof(kv_fill), 0u), LOX_OK);
        ASSERT_EQ(lox_admit_rel_insert(&g_db, "pre_rel_wc", lox_table_row_size(t), &a), LOX_OK);
        ASSERT_EQ(a.status, LOX_OK);
        if (a.would_compact != 0u) {
            break;
        }
    }

    ASSERT_EQ(a.would_compact, 1u);
    ASSERT_EQ(a.deterministic_budget_ok, 0u);
}

MDB_TEST(test_pressure_api) {
    lox_pressure_t p;
    uint8_t value = 7u;
    uint32_t ts_val = 11u;

    ASSERT_EQ(lox_kv_set(&g_db, "pressure_k", &value, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_register(&g_db, "pressure_s", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "pressure_s", 1u, &ts_val), LOX_OK);

    ASSERT_EQ(lox_get_pressure(&g_db, &p), LOX_OK);
    ASSERT_LE(p.kv_fill_pct, 100u);
    ASSERT_LE(p.ts_fill_pct, 100u);
    ASSERT_LE(p.rel_fill_pct, 100u);
    ASSERT_LE(p.wal_fill_pct, 100u);
    ASSERT_LE(p.compact_pressure_pct, 100u);
    ASSERT_LE(p.near_full_risk_pct, 100u);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, test_inspect_empty_db);
    MDB_RUN_TEST(setup_db, teardown_db, test_inspect_kv_entries);
    MDB_RUN_TEST(setup_db, teardown_db, test_inspect_kv_eviction);
    MDB_RUN_TEST(setup_db, teardown_db, test_inspect_ts_samples);
    MDB_RUN_TEST(setup_db, teardown_db, test_split_stats_api_empty_db);
    MDB_RUN_TEST(setup_db, teardown_db, test_split_stats_api_kv_ts_rel);
    MDB_RUN_TEST(setup_db, teardown_db, test_effective_capacity_api);
    MDB_RUN_TEST(setup_db, teardown_db, test_admission_api_kv_ts_rel);
    MDB_RUN_TEST(setup_db, teardown_db, test_pressure_api);
    MDB_RUN_TEST(setup_storage_db, teardown_storage_db, test_inspect_wal);
    MDB_RUN_TEST(setup_storage_db, teardown_storage_db, test_split_db_stats_storage_fields);
    MDB_RUN_TEST(setup_storage_low_wal_threshold_db, teardown_storage_db, test_admission_rel_would_compact_clears_deterministic_budget);
    return MDB_RESULT();
}
