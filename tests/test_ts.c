#include "microtest.h"
#include "microdb.h"
#include "../port/ram/microdb_port_ram.h"

#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_ram_storage;

typedef struct {
    size_t count;
    microdb_timestamp_t last_ts;
} ts_iter_ctx_t;

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

static bool ts_collect_cb(const microdb_ts_sample_t *sample, void *ctx) {
    ts_iter_ctx_t *iter = (ts_iter_ctx_t *)ctx;

    iter->count++;
    iter->last_ts = sample->ts;
    return true;
}

MDB_TEST(ts_register_stream_ok) {
    ASSERT_EQ(microdb_ts_register(&g_db, "temp", MICRODB_TS_F32, 0u), MICRODB_OK);
}

MDB_TEST(ts_insert_and_last_ok) {
    microdb_ts_sample_t sample;
    float value = 21.5f;

    ASSERT_EQ(microdb_ts_register(&g_db, "temp", MICRODB_TS_F32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "temp", 100u, &value), MICRODB_OK);
    ASSERT_EQ(microdb_ts_last(&g_db, "temp", &sample), MICRODB_OK);
    ASSERT_EQ(sample.ts, 100u);
    ASSERT_EQ(sample.v.f32 == value, 1);
}

MDB_TEST(ts_query_returns_matching_range) {
    uint32_t a = 1u;
    uint32_t b = 2u;
    uint32_t c = 3u;
    ts_iter_ctx_t ctx;

    ASSERT_EQ(microdb_ts_register(&g_db, "count", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "count", 10u, &a), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "count", 20u, &b), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "count", 30u, &c), MICRODB_OK);
    ctx.count = 0u;
    ctx.last_ts = 0u;
    ASSERT_EQ(microdb_ts_query(&g_db, "count", 15u, 30u, ts_collect_cb, &ctx), MICRODB_OK);
    ASSERT_EQ(ctx.count, 2u);
    ASSERT_EQ(ctx.last_ts, 30u);
}

MDB_TEST(ts_count_and_clear_work) {
    int32_t a = 11;
    int32_t b = 12;
    size_t count = 0u;

    ASSERT_EQ(microdb_ts_register(&g_db, "i32", MICRODB_TS_I32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "i32", 1u, &a), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "i32", 2u, &b), MICRODB_OK);
    ASSERT_EQ(microdb_ts_count(&g_db, "i32", 0u, 10u, &count), MICRODB_OK);
    ASSERT_EQ(count, 2u);
    ASSERT_EQ(microdb_ts_clear(&g_db, "i32"), MICRODB_OK);
    ASSERT_EQ(microdb_ts_count(&g_db, "i32", 0u, 10u, &count), MICRODB_OK);
    ASSERT_EQ(count, 0u);
}

MDB_TEST(ts_raw_roundtrip_query_buf) {
    microdb_ts_sample_t out[2];
    uint8_t raw[3] = { 0xAAu, 0xBBu, 0xCCu };
    size_t out_count = 0u;

    ASSERT_EQ(microdb_ts_register(&g_db, "raw", MICRODB_TS_RAW, 3u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "raw", 55u, raw), MICRODB_OK);
    ASSERT_EQ(microdb_ts_query_buf(&g_db, "raw", 0u, 100u, out, 2u, &out_count), MICRODB_OK);
    ASSERT_EQ(out_count, 1u);
    ASSERT_EQ(out[0].ts, 55u);
    ASSERT_MEM_EQ(out[0].v.raw, raw, 3u);
}

int main(void) {
    MDB_RUN_TEST(setup_basic, teardown_db, ts_register_stream_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_insert_and_last_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_returns_matching_range);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_count_and_clear_work);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_raw_roundtrip_query_buf);
    return MDB_RESULT();
}
