// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/ram/lox_port_ram.h"
#include "../src/lox_internal.h"

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

MDB_TEST(ts_reject_policy_returns_full_when_buffer_full) {
    uint32_t i;
    uint32_t value;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "reject", LOX_TS_U32, 0u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity; ++i) {
        value = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "reject", i, &value), LOX_OK);
    }
    value = 999u;
    ASSERT_EQ(lox_ts_insert(&g_db, "reject", 999u, &value), LOX_ERR_FULL);
}

MDB_TEST(ts_reject_policy_preserves_oldest_data) {
    uint32_t i;
    uint32_t value;
    lox_ts_sample_t sample;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "reject", LOX_TS_U32, 0u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity; ++i) {
        value = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "reject", i, &value), LOX_OK);
    }
    value = 999u;
    ASSERT_EQ(lox_ts_insert(&g_db, "reject", 999u, &value), LOX_ERR_FULL);
    ASSERT_EQ(lox_ts_last(&g_db, "reject", &sample), LOX_OK);
    ASSERT_EQ(sample.v.u32, capacity - 1u);
}

MDB_TEST(ts_reject_policy_count_stays_at_capacity) {
    uint32_t i;
    uint32_t value;
    size_t count = 0u;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "reject", LOX_TS_U32, 0u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity; ++i) {
        value = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "reject", i, &value), LOX_OK);
    }
    value = 999u;
    ASSERT_EQ(lox_ts_insert(&g_db, "reject", 999u, &value), LOX_ERR_FULL);
    ASSERT_EQ(lox_ts_count(&g_db, "reject", 0u, 2000u, &count), LOX_OK);
    ASSERT_EQ(count, capacity);
}

MDB_TEST(ts_reject_policy_raw_full_returns_full) {
    uint8_t raw[3] = { 1u, 2u, 3u };
    uint32_t i;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "raw", LOX_TS_RAW, 3u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity; ++i) {
        ASSERT_EQ(lox_ts_insert(&g_db, "raw", i, raw), LOX_OK);
    }
    ASSERT_EQ(lox_ts_insert(&g_db, "raw", 99u, raw), LOX_ERR_FULL);
}

MDB_TEST(ts_reject_policy_query_still_reads_existing_samples) {
    uint32_t value = 5u;
    lox_ts_sample_t out[1];
    size_t out_count = 0u;

    ASSERT_EQ(lox_ts_register(&g_db, "reject", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "reject", 10u, &value), LOX_OK);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "reject", 0u, 20u, out, 1u, &out_count), LOX_OK);
    ASSERT_EQ(out_count, 1u);
    ASSERT_EQ(out[0].v.u32, 5u);
}

int main(void) {
    MDB_RUN_TEST(setup_basic, teardown_db, ts_reject_policy_returns_full_when_buffer_full);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_reject_policy_preserves_oldest_data);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_reject_policy_count_stays_at_capacity);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_reject_policy_raw_full_returns_full);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_reject_policy_query_still_reads_existing_samples);
    return MDB_RESULT();
}
