// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/ram/lox_port_ram.h"
#include "../src/lox_internal.h"

#include <stdio.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_ram_storage;
static bool g_ts_mutate_once = false;

typedef struct {
    size_t count;
    lox_timestamp_t ts[256];
    uint32_t u32[256];
} ts_collect_ctx_t;

static lox_core_t *test_core(void) {
    return lox_core(&g_db);
}

static void make_stream_name(char *buf, size_t buf_len, uint32_t index) {
    (void)snprintf(buf, buf_len, "s%02u", (unsigned)index);
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

static bool ts_collect_cb(const lox_ts_sample_t *sample, void *ctx) {
    ts_collect_ctx_t *out = (ts_collect_ctx_t *)ctx;
    size_t cap = sizeof(out->ts) / sizeof(out->ts[0]);

    if (out->count < cap) {
        out->ts[out->count] = sample->ts;
        out->u32[out->count] = sample->v.u32;
    }
    out->count++;
    return true;
}

static bool ts_collect_stop_after_two(const lox_ts_sample_t *sample, void *ctx) {
    ts_collect_ctx_t *out = (ts_collect_ctx_t *)ctx;
    out->ts[out->count] = sample->ts;
    out->count++;
    (void)sample;
    return out->count < 2u;
}

static bool ts_query_mutating_cb(const lox_ts_sample_t *sample, void *ctx) {
    uint32_t value = 999u;
    (void)sample;
    (void)ctx;
    if (!g_ts_mutate_once) {
        g_ts_mutate_once = true;
        (void)lox_ts_insert(&g_db, "mut", 42u, &value);
    }
    return true;
}

MDB_TEST(ts_register_stream_ok) {
    ASSERT_EQ(lox_ts_register(&g_db, "temp", LOX_TS_F32, 0u), LOX_OK);
}

MDB_TEST(ts_register_same_name_twice_exists) {
    ASSERT_EQ(lox_ts_register(&g_db, "temp", LOX_TS_F32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_register(&g_db, "temp", LOX_TS_F32, 0u), LOX_ERR_EXISTS);
}

MDB_TEST(ts_register_beyond_max_streams_full) {
    char name[16];
    uint32_t i;

    for (i = 0; i < LOX_TS_MAX_STREAMS; ++i) {
        make_stream_name(name, sizeof(name), i);
        ASSERT_EQ(lox_ts_register(&g_db, name, LOX_TS_U32, 0u), LOX_OK);
    }

    ASSERT_EQ(lox_ts_register(&g_db, "overflow", LOX_TS_U32, 0u), LOX_ERR_FULL);
}

MDB_TEST(ts_insert_unregistered_not_found) {
    uint32_t value = 1u;
    ASSERT_EQ(lox_ts_insert(&g_db, "missing", 1u, &value), LOX_ERR_NOT_FOUND);
}

MDB_TEST(ts_insert_and_last_f32_ok) {
    lox_ts_sample_t sample;
    float value = 21.5f;

    ASSERT_EQ(lox_ts_register(&g_db, "temp", LOX_TS_F32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "temp", 100u, &value), LOX_OK);
    ASSERT_EQ(lox_ts_last(&g_db, "temp", &sample), LOX_OK);
    ASSERT_EQ(sample.ts, 100u);
    ASSERT_EQ(sample.v.f32 == value, 1);
}

MDB_TEST(ts_insert_and_last_i32_ok) {
    lox_ts_sample_t sample;
    int32_t value = -12;

    ASSERT_EQ(lox_ts_register(&g_db, "i32", LOX_TS_I32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "i32", 11u, &value), LOX_OK);
    ASSERT_EQ(lox_ts_last(&g_db, "i32", &sample), LOX_OK);
    ASSERT_EQ(sample.ts, 11u);
    ASSERT_EQ(sample.v.i32, -12);
}

MDB_TEST(ts_insert_and_last_u32_ok) {
    lox_ts_sample_t sample;
    uint32_t value = 77u;

    ASSERT_EQ(lox_ts_register(&g_db, "u32", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "u32", 9u, &value), LOX_OK);
    ASSERT_EQ(lox_ts_last(&g_db, "u32", &sample), LOX_OK);
    ASSERT_EQ(sample.ts, 9u);
    ASSERT_EQ(sample.v.u32, 77u);
}

MDB_TEST(ts_raw_type_roundtrip_ok) {
    lox_ts_sample_t sample;
    uint8_t raw[4] = { 0xAAu, 0xBBu, 0xCCu, 0xDDu };

    ASSERT_EQ(lox_ts_register(&g_db, "raw", LOX_TS_RAW, 4u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "raw", 5u, raw), LOX_OK);
    ASSERT_EQ(lox_ts_last(&g_db, "raw", &sample), LOX_OK);
    ASSERT_EQ(sample.ts, 5u);
    ASSERT_MEM_EQ(sample.v.raw, raw, 4u);
}

MDB_TEST(ts_last_on_empty_stream_not_found) {
    lox_ts_sample_t sample;

    ASSERT_EQ(lox_ts_register(&g_db, "empty", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_last(&g_db, "empty", &sample), LOX_ERR_NOT_FOUND);
}

MDB_TEST(ts_query_empty_stream_ok_no_callback) {
    ts_collect_ctx_t ctx;

    ASSERT_EQ(lox_ts_register(&g_db, "empty", LOX_TS_U32, 0u), LOX_OK);
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(lox_ts_query(&g_db, "empty", 0u, 100u, ts_collect_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 0u);
}

MDB_TEST(ts_query_all_samples_chronological) {
    uint32_t a = 1u;
    uint32_t b = 2u;
    uint32_t c = 3u;
    ts_collect_ctx_t ctx;

    ASSERT_EQ(lox_ts_register(&g_db, "count", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "count", 10u, &a), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "count", 20u, &b), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "count", 30u, &c), LOX_OK);
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(lox_ts_query(&g_db, "count", 0u, 100u, ts_collect_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 3u);
    ASSERT_EQ(ctx.ts[0], 10u);
    ASSERT_EQ(ctx.ts[1], 20u);
    ASSERT_EQ(ctx.ts[2], 30u);
}

MDB_TEST(ts_query_from_greater_than_to_returns_zero) {
    uint32_t value = 5u;
    ts_collect_ctx_t ctx;

    ASSERT_EQ(lox_ts_register(&g_db, "range", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "range", 10u, &value), LOX_OK);
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(lox_ts_query(&g_db, "range", 30u, 20u, ts_collect_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 0u);
}

MDB_TEST(ts_query_exact_boundaries_inclusive) {
    uint32_t a = 1u;
    uint32_t b = 2u;
    uint32_t c = 3u;
    ts_collect_ctx_t ctx;

    ASSERT_EQ(lox_ts_register(&g_db, "inc", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "inc", 10u, &a), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "inc", 20u, &b), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "inc", 30u, &c), LOX_OK);
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(lox_ts_query(&g_db, "inc", 10u, 30u, ts_collect_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 3u);
}

MDB_TEST(ts_query_buf_exact_fit_ok) {
    uint32_t a = 11u;
    uint32_t b = 12u;
    lox_ts_sample_t out[2];
    size_t out_count = 0u;

    ASSERT_EQ(lox_ts_register(&g_db, "buf", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "buf", 1u, &a), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "buf", 2u, &b), LOX_OK);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "buf", 0u, 10u, out, 2u, &out_count), LOX_OK);
    ASSERT_EQ(out_count, 2u);
    ASSERT_EQ(out[0].v.u32, 11u);
    ASSERT_EQ(out[1].v.u32, 12u);
}

MDB_TEST(ts_query_buf_too_small_overflow) {
    uint32_t a = 1u;
    uint32_t b = 2u;
    uint32_t c = 3u;
    lox_ts_sample_t out[2];
    size_t out_count = 0u;

    ASSERT_EQ(lox_ts_register(&g_db, "small", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "small", 1u, &a), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "small", 2u, &b), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "small", 3u, &c), LOX_OK);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "small", 0u, 10u, out, 2u, &out_count), LOX_ERR_OVERFLOW);
    ASSERT_EQ(out_count, 2u);
    ASSERT_EQ(out[0].v.u32, 1u);
    ASSERT_EQ(out[1].v.u32, 2u);
}

MDB_TEST(ts_count_returns_correct_count) {
    uint32_t a = 1u;
    uint32_t b = 2u;
    size_t count = 0u;

    ASSERT_EQ(lox_ts_register(&g_db, "cnt", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "cnt", 10u, &a), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "cnt", 20u, &b), LOX_OK);
    ASSERT_EQ(lox_ts_count(&g_db, "cnt", 0u, 15u, &count), LOX_OK);
    ASSERT_EQ(count, 1u);
}

MDB_TEST(ts_clear_resets_count_but_preserves_registration) {
    uint32_t value = 1u;
    size_t count = 99u;

    ASSERT_EQ(lox_ts_register(&g_db, "clr", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "clr", 1u, &value), LOX_OK);
    ASSERT_EQ(lox_ts_clear(&g_db, "clr"), LOX_OK);
    ASSERT_EQ(lox_ts_count(&g_db, "clr", 0u, 10u, &count), LOX_OK);
    ASSERT_EQ(count, 0u);
    ASSERT_EQ(lox_ts_register(&g_db, "clr", LOX_TS_U32, 0u), LOX_ERR_EXISTS);
}

MDB_TEST(ts_callback_false_stops_iteration_immediately) {
    uint32_t a = 1u;
    uint32_t b = 2u;
    uint32_t c = 3u;
    ts_collect_ctx_t ctx;

    ASSERT_EQ(lox_ts_register(&g_db, "stop", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "stop", 1u, &a), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "stop", 2u, &b), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "stop", 3u, &c), LOX_OK);
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(lox_ts_query(&g_db, "stop", 0u, 10u, ts_collect_stop_after_two, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 2u);
}

MDB_TEST(ts_all_streams_isolated) {
    char name[16];
    uint32_t value;
    lox_ts_sample_t sample;
    uint32_t i;

    for (i = 0; i < LOX_TS_MAX_STREAMS; ++i) {
        make_stream_name(name, sizeof(name), i);
        ASSERT_EQ(lox_ts_register(&g_db, name, LOX_TS_U32, 0u), LOX_OK);
        value = i + 100u;
        ASSERT_EQ(lox_ts_insert(&g_db, name, i, &value), LOX_OK);
    }

    for (i = 0; i < LOX_TS_MAX_STREAMS; ++i) {
        make_stream_name(name, sizeof(name), i);
        ASSERT_EQ(lox_ts_last(&g_db, name, &sample), LOX_OK);
        ASSERT_EQ(sample.v.u32, i + 100u);
    }
}

MDB_TEST(ts_name_exact_max_minus_one_ok) {
    char name[LOX_TS_STREAM_NAME_LEN];

    memset(name, 'x', sizeof(name));
    name[LOX_TS_STREAM_NAME_LEN - 1u] = '\0';
    ASSERT_EQ(lox_ts_register(&g_db, name, LOX_TS_U32, 0u), LOX_OK);
}

MDB_TEST(ts_null_stream_name_invalid) {
    ASSERT_EQ(lox_ts_register(&g_db, NULL, LOX_TS_U32, 0u), LOX_ERR_INVALID);
}

MDB_TEST(ts_raw_invalid_size_zero) {
    ASSERT_EQ(lox_ts_register(&g_db, "raw0", LOX_TS_RAW, 0u), LOX_ERR_INVALID);
}

MDB_TEST(ts_raw_invalid_size_too_large) {
    ASSERT_EQ(lox_ts_register(&g_db, "rawbig", LOX_TS_RAW, LOX_TS_RAW_MAX + 1u), LOX_ERR_INVALID);
}

MDB_TEST(ts_query_null_callback_invalid) {
    ASSERT_EQ(lox_ts_register(&g_db, "cb", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_query(&g_db, "cb", 0u, 10u, NULL, NULL), LOX_ERR_INVALID);
}

MDB_TEST(ts_query_mutation_during_callback_returns_modified) {
    uint32_t a = 1u;
    uint32_t b = 2u;

    ASSERT_EQ(lox_ts_register(&g_db, "mut", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "mut", 1u, &a), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "mut", 2u, &b), LOX_OK);
    g_ts_mutate_once = false;
    ASSERT_EQ(lox_ts_query(&g_db, "mut", 0u, 100u, ts_query_mutating_cb, NULL), LOX_ERR_MODIFIED);
}

MDB_TEST(ts_drop_oldest_when_ring_buffer_full) {
    uint32_t i;
    uint32_t value;
    lox_ts_sample_t last;
    ts_collect_ctx_t ctx;
    uint32_t capacity;

    ASSERT_EQ(lox_ts_register(&g_db, "drop", LOX_TS_U32, 0u), LOX_OK);
    capacity = test_core()->ts.streams[0].capacity;
    for (i = 0; i < capacity + 1u; ++i) {
        value = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "drop", i, &value), LOX_OK);
    }

    ASSERT_EQ(lox_ts_last(&g_db, "drop", &last), LOX_OK);
    ASSERT_EQ(last.v.u32, capacity);
    memset(&ctx, 0, sizeof(ctx));
    ASSERT_EQ(lox_ts_query(&g_db, "drop", 0u, capacity + 1u, ts_collect_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, capacity);
    ASSERT_EQ(ctx.ts[0], 1u);
}

MDB_TEST(ts_adaptive_partition_mixed_types_preserves_data) {
    uint32_t u = 111u;
    uint8_t raw[16];
    uint8_t raw2[16];
    uint32_t u2 = 222u;
    lox_ts_sample_t sample;
    uint32_t cap_u;
    uint32_t cap_raw;

    memset(raw, 0xA5, sizeof(raw));
    memset(raw2, 0x5A, sizeof(raw2));

    ASSERT_EQ(lox_ts_register(&g_db, "u", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_register(&g_db, "raw", LOX_TS_RAW, 16u), LOX_OK);
    cap_u = test_core()->ts.streams[0].capacity;
    cap_raw = test_core()->ts.streams[1].capacity;
    ASSERT_EQ(cap_u >= 4u, 1);
    ASSERT_EQ(cap_raw >= 4u, 1);
    ASSERT_EQ((cap_u + 1u >= cap_raw) && (cap_raw + 1u >= cap_u), 1);

    ASSERT_EQ(lox_ts_insert(&g_db, "u", 1u, &u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "raw", 2u, raw), LOX_OK);
    ASSERT_EQ(lox_ts_register(&g_db, "u2", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "raw", 3u, raw2), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "u2", 4u, &u2), LOX_OK);

    ASSERT_EQ(lox_ts_last(&g_db, "u", &sample), LOX_OK);
    ASSERT_EQ(sample.v.u32, u);
    ASSERT_EQ(lox_ts_last(&g_db, "raw", &sample), LOX_OK);
    ASSERT_MEM_EQ(sample.v.raw, raw2, 16u);
    ASSERT_EQ(lox_ts_last(&g_db, "u2", &sample), LOX_OK);
    ASSERT_EQ(sample.v.u32, u2);
}

int main(void) {
    MDB_RUN_TEST(setup_basic, teardown_db, ts_register_stream_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_register_same_name_twice_exists);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_register_beyond_max_streams_full);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_insert_unregistered_not_found);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_insert_and_last_f32_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_insert_and_last_i32_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_insert_and_last_u32_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_raw_type_roundtrip_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_last_on_empty_stream_not_found);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_empty_stream_ok_no_callback);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_all_samples_chronological);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_from_greater_than_to_returns_zero);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_exact_boundaries_inclusive);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_buf_exact_fit_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_buf_too_small_overflow);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_count_returns_correct_count);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_clear_resets_count_but_preserves_registration);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_callback_false_stops_iteration_immediately);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_all_streams_isolated);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_name_exact_max_minus_one_ok);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_null_stream_name_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_raw_invalid_size_zero);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_raw_invalid_size_too_large);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_null_callback_invalid);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_query_mutation_during_callback_returns_modified);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_drop_oldest_when_ring_buffer_full);
    MDB_RUN_TEST(setup_basic, teardown_db, ts_adaptive_partition_mixed_types_preserves_data);
    return MDB_RESULT();
}
