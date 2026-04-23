// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/ram/lox_port_ram.h"
#include "../src/lox_internal.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static int g_storage_inited = 0;
static int g_db_inited = 0;

static uint32_t align_u32(uint32_t value, uint32_t align) {
    return (value + (align - 1u)) & ~(align - 1u);
}

static uint32_t kv_bucket_count(uint32_t entry_limit) {
    uint32_t required = (entry_limit * 4u + 2u) / 3u;
    uint32_t buckets = 1u;
    while (buckets < required) {
        buckets <<= 1u;
    }
    return buckets;
}

static void close_db(void) {
    if (g_db_inited) {
        (void)lox_deinit(&g_db);
        g_db_inited = 0;
    }
    if (g_storage_inited) {
        lox_port_ram_deinit(&g_storage);
        g_storage_inited = 0;
    }
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
}

static lox_err_t open_db(uint32_t ram_kb,
                             uint8_t kv_pct,
                             uint8_t ts_pct,
                             uint8_t rel_pct,
                             int enable_storage) {
    lox_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));

    if (enable_storage) {
        lox_err_t s = lox_port_ram_init(&g_storage, 2u * 1024u * 1024u);
        if (s != LOX_OK) {
            return s;
        }
        g_storage_inited = 1;
        cfg.storage = &g_storage;
    }

    cfg.ram_kb = ram_kb;
    cfg.kv_pct = kv_pct;
    cfg.ts_pct = ts_pct;
    cfg.rel_pct = rel_pct;
    cfg.wal_compact_auto = 1u;
    cfg.wal_compact_threshold_pct = 75u;
    {
        lox_err_t rc = lox_init(&g_db, &cfg);
        if (rc != LOX_OK) {
            return rc;
        }
    }
    g_db_inited = 1;
    return LOX_OK;
}

static void setup_noop(void) {
}

static void teardown_noop(void) {
    close_db();
}

MDB_TEST(estimator_model_core_wal_default) {
    lox_effective_capacity_t cap;
    memset(&cap, 0, sizeof(cap));

    ASSERT_EQ(open_db(32u, 40u, 40u, 20u, 1), LOX_OK);
    ASSERT_EQ(lox_get_effective_capacity(&g_db, &cap), LOX_OK);
    ASSERT_EQ(cap.kv_entries_usable, (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS));
    ASSERT_GT(cap.kv_value_bytes_usable, 0u);
}

MDB_TEST(estimator_model_footprint_min) {
    lox_effective_capacity_t cap;
    memset(&cap, 0, sizeof(cap));

    {
        lox_err_t rc = open_db(8u, 40u, 40u, 20u, 1);
#if (LOX_KV_MAX_KEYS == 16u) && (LOX_TXN_STAGE_KEYS == 2u)
        ASSERT_EQ(rc, LOX_OK);
        ASSERT_EQ(lox_get_effective_capacity(&g_db, &cap), LOX_OK);
#else
        ASSERT_EQ(rc, LOX_ERR_NO_MEM);
        return;
#endif
    }

#if (LOX_KV_MAX_KEYS == 16u) && (LOX_TXN_STAGE_KEYS == 2u)
    ASSERT_EQ(cap.kv_entries_usable, 14u);
#else
    ASSERT_EQ(cap.kv_entries_usable, (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS));
#endif

    ASSERT_EQ(cap.ts_samples_usable, 0u);
}

MDB_TEST(estimator_model_kv_value_store_exact) {
    lox_effective_capacity_t cap;
    uint32_t total_bytes = 32u * 1024u;
    uint32_t kv_arena_bytes = (total_bytes * 40u) / 100u;
    uint32_t entry_limit = LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS;
    uint32_t bucket_count = kv_bucket_count(entry_limit);
    uint32_t bucket_array_bytes = bucket_count * (uint32_t)sizeof(lox_kv_bucket_t);
    uint32_t stage_bytes = LOX_TXN_STAGE_KEYS * (uint32_t)sizeof(lox_txn_stage_entry_t);
    uint32_t expected_value_store = 0u;

    if (kv_arena_bytes > align_u32(bucket_array_bytes, 8u) + stage_bytes) {
        expected_value_store = kv_arena_bytes - align_u32(bucket_array_bytes, 8u) - stage_bytes;
    }

    memset(&cap, 0, sizeof(cap));
    ASSERT_EQ(open_db(32u, 40u, 40u, 20u, 0), LOX_OK);
    ASSERT_EQ(lox_get_effective_capacity(&g_db, &cap), LOX_OK);
    ASSERT_EQ(cap.kv_value_bytes_usable, expected_value_store);
}

MDB_TEST(estimator_model_custom_heavy_ts) {
    lox_effective_capacity_t cap;
    uint32_t i;
    char stream_name[16];
    uint32_t stream_count = LOX_TS_MAX_STREAMS;

    memset(&cap, 0, sizeof(cap));
    ASSERT_EQ(open_db(64u, 20u, 70u, 10u, 1), LOX_OK);

    for (i = 0u; i < stream_count; ++i) {
        (void)snprintf(stream_name, sizeof(stream_name), "s%u", i);
        ASSERT_EQ(lox_ts_register(&g_db, stream_name, LOX_TS_U32, 0u), LOX_OK);
    }

    ASSERT_EQ(lox_get_effective_capacity(&g_db, &cap), LOX_OK);
    ASSERT_GT(cap.ts_samples_usable, 0u);
    ASSERT_EQ(cap.kv_entries_usable, (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS));
}

MDB_TEST(estimator_struct_size_constants) {
    fprintf(stderr,
            "sizeof(lox_kv_bucket_t)=%u sizeof(lox_ts_stream_t)=%u sizeof(lox_table_t)=%u\n",
            (unsigned)sizeof(lox_kv_bucket_t),
            (unsigned)sizeof(lox_ts_stream_t),
            (unsigned)sizeof(lox_table_t));
    ASSERT_GT(sizeof(lox_kv_bucket_t), 0u);
}

int main(void) {
    MDB_RUN_TEST(setup_noop, teardown_noop, estimator_model_core_wal_default);
    MDB_RUN_TEST(setup_noop, teardown_noop, estimator_model_footprint_min);
    MDB_RUN_TEST(setup_noop, teardown_noop, estimator_model_kv_value_store_exact);
    MDB_RUN_TEST(setup_noop, teardown_noop, estimator_model_custom_heavy_ts);
    MDB_RUN_TEST(setup_noop, teardown_noop, estimator_struct_size_constants);
    return MDB_RESULT();
}
