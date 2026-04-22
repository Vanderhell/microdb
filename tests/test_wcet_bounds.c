// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "microdb_wcet.h"
#include "../src/microdb_internal.h"

#include <stdio.h>
#include <string.h>

MICRODB_STATIC_ASSERT(wcet_probe_nonzero, MICRODB_WCET_KV_PROBE_MAX > 0u);
MICRODB_STATIC_ASSERT(wcet_kv_wal_nonzero, MICRODB_WCET_WAL_KV_SET_MAX > 0u);
MICRODB_STATIC_ASSERT(wcet_ts_wal_nonzero, MICRODB_WCET_WAL_TS_F32_INSERT_MAX > 0u);
MICRODB_STATIC_ASSERT(wcet_ts_raw_wal_nonzero, MICRODB_WCET_WAL_TS_RAW_INSERT_MAX > 0u);
MICRODB_STATIC_ASSERT(wcet_kv_memmove_nonzero, MICRODB_WCET_KV_MEMMOVE_MAX > 0u);

static microdb_t g_db;

static uint32_t hash32(const char *s) {
    uint32_t h = 2166136261u;
    while (*s != '\0') {
        h ^= (uint8_t)*s++;
        h *= 16777619u;
    }
    return h;
}

static uint32_t probe_count_for_missing(const microdb_core_t *core, const char *key) {
    uint32_t probes = 0u;
    uint32_t idx = hash32(key) & (core->kv.bucket_count - 1u);
    uint32_t mask = core->kv.bucket_count - 1u;
    while (probes < core->kv.bucket_count) {
        const microdb_kv_bucket_t *b = &core->kv.buckets[idx];
        probes++;
        if (b->state == 0u) {
            break;
        }
        idx = (idx + 1u) & mask;
    }
    return probes;
}

typedef struct {
    uint16_t id;
    uint8_t v;
} rel_row_t;

static void setup_db(void) {
    microdb_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    memset(&g_db, 0, sizeof(g_db));
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    (void)microdb_deinit(&g_db);
}

MDB_TEST(wcet_kv_probe_never_exceeds_bound) {
    microdb_core_t *core;
    uint32_t i;
    uint32_t probes;
    for (i = 0u; i < MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS; ++i) {
        char key[24];
        uint8_t v = (uint8_t)i;
        (void)snprintf(key, sizeof(key), "k%u", i);
        ASSERT_EQ(microdb_kv_set(&g_db, key, &v, 1u, 0u), MICRODB_OK);
    }
    core = microdb_core(&g_db);
    probes = probe_count_for_missing(core, "__missing_key_for_wcet__");
    ASSERT_LE(probes, MICRODB_WCET_KV_PROBE_MAX);
}

MDB_TEST(wcet_rel_insert_index_shifts_bounded) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    rel_row_t row;
    uint32_t before;
    uint32_t max_rows = 16u;
    ASSERT_EQ(microdb_schema_init(&schema, "t", max_rows), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U16, sizeof(uint16_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "v", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_get(&g_db, "t", &table), MICRODB_OK);

    row.id = 10u; row.v = 1u; ASSERT_EQ(microdb_rel_insert(&g_db, table, &row), MICRODB_OK);
    row.id = 20u; row.v = 2u; ASSERT_EQ(microdb_rel_insert(&g_db, table, &row), MICRODB_OK);
    row.id = 30u; row.v = 3u; ASSERT_EQ(microdb_rel_insert(&g_db, table, &row), MICRODB_OK);
    before = table->index_count;

    row.id = 1u; row.v = 9u;
    ASSERT_EQ(microdb_rel_insert(&g_db, table, &row), MICRODB_OK);
    ASSERT_LE(before, MICRODB_WCET_REL_INDEX_SHIFTS_MAX(max_rows));
}

MDB_TEST(wcet_macros_compile_time_nonzero) {
    ASSERT_GT(MICRODB_WCET_KV_PROBE_MAX, 0u);
    ASSERT_GT(MICRODB_WCET_WAL_KV_SET_MAX, 0u);
    ASSERT_GT(MICRODB_WCET_WAL_TS_F32_INSERT_MAX, 0u);
    ASSERT_GT(MICRODB_WCET_WAL_TS_RAW_INSERT_MAX, 0u);
    ASSERT_GT(MICRODB_WCET_KV_MEMMOVE_MAX, 0u);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, wcet_kv_probe_never_exceeds_bound);
    MDB_RUN_TEST(setup_db, teardown_db, wcet_rel_insert_index_shifts_bounded);
    MDB_RUN_TEST(setup_db, teardown_db, wcet_macros_compile_time_nonzero);
    return MDB_RESULT();
}
