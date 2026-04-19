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

MDB_TEST(ts_reject_policy_returns_full_when_buffer_full) {
    uint32_t i;
    uint32_t value;
    uint32_t capacity = test_core()->ts.streams[0].capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "reject", MICRODB_TS_U32, 0u), MICRODB_OK);
    for (i = 0; i < capacity; ++i) {
        value = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "reject", i, &value), MICRODB_OK);
    }
    value = 999u;
    ASSERT_EQ(microdb_ts_insert(&g_db, "reject", 999u, &value), MICRODB_ERR_FULL);
}

MDB_TEST(ts_reject_policy_preserves_oldest_data) {
    uint32_t i;
    uint32_t value;
    microdb_ts_sample_t sample;
    uint32_t capacity = test_core()->ts.streams[0].capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "reject", MICRODB_TS_U32, 0u), MICRODB_OK);
    for (i = 0; i < capacity; ++i) {
        value = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "reject", i, &value), MICRODB_OK);
    }
    value = 999u;
    ASSERT_EQ(microdb_ts_insert(&g_db, "reject", 999u, &value), MICRODB_ERR_FULL);
    ASSERT_EQ(microdb_ts_last(&g_db, "reject", &sample), MICRODB_OK);
    ASSERT_EQ(sample.v.u32, capacity - 1u);
}

MDB_TEST(ts_reject_policy_count_stays_at_capacity) {
    uint32_t i;
    uint32_t value;
    size_t count = 0u;
    uint32_t capacity = test_core()->ts.streams[0].capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "reject", MICRODB_TS_U32, 0u), MICRODB_OK);
    for (i = 0; i < capacity; ++i) {
        value = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "reject", i, &value), MICRODB_OK);
    }
    value = 999u;
    ASSERT_EQ(microdb_ts_insert(&g_db, "reject", 999u, &value), MICRODB_ERR_FULL);
    ASSERT_EQ(microdb_ts_count(&g_db, "reject", 0u, 2000u, &count), MICRODB_OK);
    ASSERT_EQ(count, capacity);
}

MDB_TEST(ts_reject_policy_raw_full_returns_full) {
    uint8_t raw[3] = { 1u, 2u, 3u };
    uint32_t i;
    uint32_t capacity = test_core()->ts.streams[0].capacity;

    ASSERT_EQ(microdb_ts_register(&g_db, "raw", MICRODB_TS_RAW, 3u), MICRODB_OK);
    for (i = 0; i < capacity; ++i) {
        ASSERT_EQ(microdb_ts_insert(&g_db, "raw", i, raw), MICRODB_OK);
    }
    ASSERT_EQ(microdb_ts_insert(&g_db, "raw", 99u, raw), MICRODB_ERR_FULL);
}

MDB_TEST(ts_reject_policy_query_still_reads_existing_samples) {
    uint32_t value = 5u;
    microdb_ts_sample_t out[1];
    size_t out_count = 0u;

    ASSERT_EQ(microdb_ts_register(&g_db, "reject", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "reject", 10u, &value), MICRODB_OK);
    ASSERT_EQ(microdb_ts_query_buf(&g_db, "reject", 0u, 20u, out, 1u, &out_count), MICRODB_OK);
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
