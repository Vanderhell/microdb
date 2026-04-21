// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "../port/ram/microdb_port_ram.h"
#include "../src/microdb_internal.h"

#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_ram_storage;

static microdb_core_t *test_core(void) {
    return microdb_core(&g_db);
}

static void setup_basic(void) {
    microdb_cfg_t cfg;

    ASSERT_EQ(microdb_port_ram_init(&g_ram_storage, 65536u), MICRODB_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_ram_storage;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    microdb_deinit(&g_db);
    microdb_port_ram_deinit(&g_ram_storage);
}

MDB_TEST(ts_downsample_keeps_count_at_capacity) {
    uint32_t i;
    uint32_t value;
    size_t count = 0u;
    uint32_t capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "down", MICRODB_TS_U32, 0u), MICRODB_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        value = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "down", i, &value), MICRODB_OK);
    }
    ASSERT_EQ(microdb_ts_count(&g_db, "down", 0u, 1000u, &count), MICRODB_OK);
    ASSERT_EQ(count, capacity);
}

MDB_TEST(ts_downsample_merges_two_oldest_u32_samples) {
    uint32_t i;
    uint32_t value;
    microdb_ts_sample_t out[256];
    size_t out_count = 0u;
    uint32_t capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "down", MICRODB_TS_U32, 0u), MICRODB_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        value = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "down", i, &value), MICRODB_OK);
    }
    ASSERT_EQ(microdb_ts_query_buf(&g_db, "down", 0u, 1000u, out, 256u, &out_count), MICRODB_OK);
    ASSERT_EQ(out_count, capacity);
    ASSERT_EQ(out[0].ts, 0u);
    ASSERT_EQ(out[0].v.u32, 0u);
}

MDB_TEST(ts_downsample_merges_two_oldest_i32_samples) {
    int32_t values[5] = { 10, 20, 30, 40, 50 };
    microdb_ts_sample_t out[256];
    size_t out_count = 0u;
    uint32_t i;
    uint32_t capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "downi", MICRODB_TS_I32, 0u), MICRODB_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        ASSERT_EQ(microdb_ts_insert(&g_db, "downi", i, &values[i % 5u]), MICRODB_OK);
    }
    ASSERT_EQ(microdb_ts_query_buf(&g_db, "downi", 0u, 1000u, out, 256u, &out_count), MICRODB_OK);
    ASSERT_EQ(out_count, capacity);
}

MDB_TEST(ts_downsample_merges_two_oldest_f32_samples) {
    float value = 2.0f;
    microdb_ts_sample_t out[256];
    size_t out_count = 0u;
    uint32_t i;
    uint32_t capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "downf", MICRODB_TS_F32, 0u), MICRODB_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        value = (float)i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "downf", i, &value), MICRODB_OK);
    }
    ASSERT_EQ(microdb_ts_query_buf(&g_db, "downf", 0u, 1000u, out, 256u, &out_count), MICRODB_OK);
    ASSERT_EQ(out_count, capacity);
}

MDB_TEST(ts_downsample_raw_merges_oldest_pair_bytes) {
    uint8_t raw_a[3] = { 1u, 2u, 3u };
    uint8_t raw_b[3] = { 9u, 9u, 9u };
    uint8_t raw_c[3] = { 4u, 4u, 4u };
    uint8_t merged[3] = { 5u, 5u, 6u };
    microdb_ts_sample_t out[256];
    size_t out_count = 0u;
    uint32_t i;
    uint32_t capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "raw", MICRODB_TS_RAW, 3u), MICRODB_OK);
    capacity = test_core()->ts.streams[0].capacity;
    ASSERT_EQ(microdb_ts_insert(&g_db, "raw", 0u, raw_a), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "raw", 1u, raw_b), MICRODB_OK);
    for (i = 2u; i < capacity; ++i) {
        ASSERT_EQ(microdb_ts_insert(&g_db, "raw", i, raw_c), MICRODB_OK);
    }
    ASSERT_EQ(microdb_ts_insert(&g_db, "raw", 99u, raw_c), MICRODB_OK);
    ASSERT_EQ(microdb_ts_query_buf(&g_db, "raw", 0u, 1000u, out, 256u, &out_count), MICRODB_OK);
    ASSERT_EQ(out_count, capacity);
    ASSERT_MEM_EQ(out[0].v.raw, merged, 3u);
}

int main(void) {
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_keeps_count_at_capacity);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_merges_two_oldest_u32_samples);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_merges_two_oldest_i32_samples);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_merges_two_oldest_f32_samples);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_raw_merges_oldest_pair_bytes);
    return MDB_RESULT();
}
