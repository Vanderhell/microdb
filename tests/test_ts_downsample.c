// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/ram/lox_port_ram.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_ram_storage;

static lox_core_t *test_core(void) {
    return lox_core(&g_db);
}

static void setup_basic(void) {
    lox_cfg_t cfg;

    ASSERT_EQ(lox_port_ram_init(&g_ram_storage, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    lox_deinit(&g_db);
    lox_port_ram_deinit(&g_ram_storage);
}

MDB_TEST(ts_downsample_keeps_count_at_capacity) {
    uint32_t i;
    uint32_t value;
    size_t count = 0u;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "down", LOX_TS_U32, 0u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        value = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "down", i, &value), LOX_OK);
    }
    ASSERT_EQ(lox_ts_count(&g_db, "down", 0u, UINT32_MAX, &count), LOX_OK);
    ASSERT_EQ(count, capacity);
}

MDB_TEST(ts_downsample_merges_two_oldest_u32_samples) {
    uint32_t i;
    uint32_t value;
    lox_ts_sample_t *out;
    size_t out_count = 0u;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "down", LOX_TS_U32, 0u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        value = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "down", i, &value), LOX_OK);
    }
    out = (lox_ts_sample_t *)malloc((size_t)capacity * sizeof(*out));
    ASSERT_EQ(out != NULL, 1);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "down", 0u, UINT32_MAX, out, capacity, &out_count), LOX_OK);
    ASSERT_EQ(out_count, capacity);
    ASSERT_EQ(out[0].ts, 0u);
    ASSERT_EQ(out[0].v.u32, 0u);
    free(out);
}

MDB_TEST(ts_downsample_merges_two_oldest_i32_samples) {
    int32_t values[5] = { 10, 20, 30, 40, 50 };
    lox_ts_sample_t *out;
    size_t out_count = 0u;
    uint32_t i;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "downi", LOX_TS_I32, 0u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        ASSERT_EQ(lox_ts_insert(&g_db, "downi", i, &values[i % 5u]), LOX_OK);
    }
    out = (lox_ts_sample_t *)malloc((size_t)capacity * sizeof(*out));
    ASSERT_EQ(out != NULL, 1);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "downi", 0u, UINT32_MAX, out, capacity, &out_count), LOX_OK);
    ASSERT_EQ(out_count, capacity);
    free(out);
}

MDB_TEST(ts_downsample_merges_two_oldest_f32_samples) {
    float value = 2.0f;
    lox_ts_sample_t *out;
    size_t out_count = 0u;
    uint32_t i;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "downf", LOX_TS_F32, 0u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        value = (float)i;
        ASSERT_EQ(lox_ts_insert(&g_db, "downf", i, &value), LOX_OK);
    }
    out = (lox_ts_sample_t *)malloc((size_t)capacity * sizeof(*out));
    ASSERT_EQ(out != NULL, 1);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "downf", 0u, UINT32_MAX, out, capacity, &out_count), LOX_OK);
    ASSERT_EQ(out_count, capacity);
    free(out);
}

MDB_TEST(ts_downsample_raw_merges_oldest_pair_bytes) {
    uint8_t raw_a[3] = { 1u, 2u, 3u };
    uint8_t raw_b[3] = { 9u, 9u, 9u };
    uint8_t raw_c[3] = { 4u, 4u, 4u };
    uint8_t merged[3] = { 5u, 5u, 6u };
    lox_ts_sample_t *out;
    size_t out_count = 0u;
    uint32_t i;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "raw", LOX_TS_RAW, 3u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    ASSERT_EQ(lox_ts_insert(&g_db, "raw", 0u, raw_a), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "raw", 1u, raw_b), LOX_OK);
    for (i = 2u; i < capacity; ++i) {
        ASSERT_EQ(lox_ts_insert(&g_db, "raw", i, raw_c), LOX_OK);
    }
    ASSERT_EQ(lox_ts_insert(&g_db, "raw", 99u, raw_c), LOX_OK);
    out = (lox_ts_sample_t *)malloc((size_t)capacity * sizeof(*out));
    ASSERT_EQ(out != NULL, 1);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "raw", 0u, UINT32_MAX, out, capacity, &out_count), LOX_OK);
    ASSERT_EQ(out_count, capacity);
    ASSERT_MEM_EQ(out[0].v.raw, merged, 3u);
    free(out);
}

int main(void) {
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_keeps_count_at_capacity);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_merges_two_oldest_u32_samples);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_merges_two_oldest_i32_samples);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_merges_two_oldest_f32_samples);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_downsample_raw_merges_oldest_pair_bytes);
    return MDB_RESULT();
}
