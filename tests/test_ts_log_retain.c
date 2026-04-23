// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"
#include "../src/lox_internal.h"

#include <stdio.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static char g_path[128];
static unsigned g_seq = 0u;

static void setup_noop(void) {
}

static void teardown_noop(void) {
}

static void open_db(int with_storage) {
    lox_cfg_t cfg;
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    if (with_storage) {
        g_seq++;
        (void)snprintf(g_path, sizeof(g_path), "ts_log_retain_%u.bin", g_seq);
        lox_port_posix_remove(g_path);
        ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 131072u), LOX_OK);
        cfg.storage = &g_storage;
        cfg.wal_compact_auto = 1u;
        cfg.wal_compact_threshold_pct = 75u;
    }
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void reopen_db(void) {
    lox_cfg_t cfg;
    ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    lox_port_posix_deinit(&g_storage);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 131072u), LOX_OK);
    cfg.storage = &g_storage;
    cfg.wal_compact_auto = 1u;
    cfg.wal_compact_threshold_pct = 75u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void close_db(void) {
    (void)lox_deinit(&g_db);
    if (g_storage.ctx != NULL) {
        lox_port_posix_deinit(&g_storage);
        lox_port_posix_remove(g_path);
    }
}

static uint32_t stream_capacity(const char *name) {
    lox_core_t *core = lox_core(&g_db);
    uint32_t i;
    for (i = 0u; i < LOX_TS_MAX_STREAMS; ++i) {
        if (core->ts.streams[i].registered && strcmp(core->ts.streams[i].name, name) == 0) {
            return core->ts.streams[i].capacity;
        }
    }
    return 0u;
}

static void query_oldest_ts(const char *name, uint32_t *out_oldest_ts) {
    lox_ts_sample_t buf[2048];
    size_t out_count = 0u;
    ASSERT_EQ(lox_ts_query_buf(&g_db, name, 0u, UINT32_MAX, buf, 2048u, &out_count), LOX_OK);
    ASSERT_GT(out_count, 0u);
    *out_oldest_ts = buf[0].ts;
}

static void query_count(const char *name, size_t *out_value) {
    size_t out_count = 0u;
    ASSERT_EQ(lox_ts_count(&g_db, name, 0u, UINT32_MAX, &out_count), LOX_OK);
    *out_value = out_count;
}

MDB_TEST(log_retain_horizon_longer_than_drop_oldest) {
    uint32_t i;
    uint32_t v = 0u;
    uint32_t cap_a;
    uint32_t cap_b;
    uint32_t inserts;
    lox_ts_log_retain_cfg_t cfg = {4u, 25u};

    open_db(0);
    ASSERT_EQ(lox_ts_register(&g_db, "a", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_register_ex(&g_db, "b", LOX_TS_U32, 0u, &cfg), LOX_OK);
    cap_a = stream_capacity("a");
    cap_b = stream_capacity("b");
    inserts = (cap_a > cap_b ? cap_a : cap_b) * 4u;
    for (i = 0u; i < inserts; ++i) {
        v = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "a", i, &v), LOX_OK);
        ASSERT_EQ(lox_ts_insert(&g_db, "b", i, &v), LOX_OK);
    }
    {
        uint32_t oldest_a = 0u;
        uint32_t oldest_b = 0u;
        query_oldest_ts("a", &oldest_a);
        query_oldest_ts("b", &oldest_b);
        ASSERT_EQ(oldest_b < oldest_a, 1);
    }
    close_db();
}

MDB_TEST(log_retain_zone_boundary_correct) {
    uint32_t cap;
    uint32_t i;
    uint32_t v;
    lox_ts_log_retain_cfg_t cfg = {2u, 50u};
    open_db(0);
    ASSERT_EQ(lox_ts_register_ex(&g_db, "z", LOX_TS_U32, 0u, &cfg), LOX_OK);
    cap = stream_capacity("z");
    ASSERT_GT(cap, 0u);
    for (i = 0u; i < cap; ++i) {
        v = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "z", i, &v), LOX_OK);
    }
    v = cap;
    ASSERT_EQ(lox_ts_insert(&g_db, "z", cap, &v), LOX_OK);
    {
        uint32_t oldest = 0u;
        query_oldest_ts("z", &oldest);
        ASSERT_EQ(oldest <= 1u, 1);
    }
    close_db();
}

MDB_TEST(log_retain_wal_recovery) {
    uint32_t i;
    uint32_t v = 0u;
    uint32_t before_oldest;
    size_t before_count;
    lox_ts_log_retain_cfg_t cfg = {4u, 25u};

    open_db(1);
    ASSERT_EQ(lox_ts_register_ex(&g_db, "r", LOX_TS_U32, 0u, &cfg), LOX_OK);
    for (i = 0u; i < 300u; ++i) {
        v = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "r", i, &v), LOX_OK);
    }
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    query_oldest_ts("r", &before_oldest);
    query_count("r", &before_count);
    reopen_db();
    {
        uint32_t after_oldest = 0u;
        size_t after_count = 0u;
        query_oldest_ts("r", &after_oldest);
        query_count("r", &after_count);
        ASSERT_EQ(after_oldest, before_oldest);
        ASSERT_EQ(after_count, before_count);
    }
    close_db();
}

MDB_TEST(log_retain_query_covers_full_horizon) {
    uint32_t cap;
    uint32_t i;
    uint32_t v;
    uint32_t oldest;
    lox_ts_log_retain_cfg_t cfg = {4u, 25u};
    open_db(0);
    ASSERT_EQ(lox_ts_register_ex(&g_db, "h", LOX_TS_U32, 0u, &cfg), LOX_OK);
    cap = stream_capacity("h");
    for (i = 0u; i < (cap * 3u); ++i) {
        v = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "h", i, &v), LOX_OK);
    }
    query_oldest_ts("h", &oldest);
    ASSERT_EQ(oldest < (cap * 2u), 1);
    close_db();
}

MDB_TEST(log_retain_disabled_when_zones_zero) {
    uint32_t i;
    uint32_t v;
    uint32_t oldest_a;
    uint32_t oldest_b;
    lox_ts_log_retain_cfg_t cfg = {0u, 0u};
    open_db(0);
    ASSERT_EQ(lox_ts_register(&g_db, "a", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_register_ex(&g_db, "b", LOX_TS_U32, 0u, &cfg), LOX_OK);
    for (i = 0u; i < 400u; ++i) {
        v = i;
        ASSERT_EQ(lox_ts_insert(&g_db, "a", i, &v), LOX_OK);
        ASSERT_EQ(lox_ts_insert(&g_db, "b", i, &v), LOX_OK);
    }
    query_oldest_ts("a", &oldest_a);
    query_oldest_ts("b", &oldest_b);
    ASSERT_EQ(oldest_a, oldest_b);
    close_db();
}

MDB_TEST(log_retain_invalid_cfg_rejected) {
    lox_ts_log_retain_cfg_t cfg = {4u, 30u};
    open_db(0);
    ASSERT_EQ(lox_ts_register_ex(&g_db, "bad", LOX_TS_U32, 0u, &cfg), LOX_ERR_INVALID);
    close_db();
}

int main(void) {
    MDB_RUN_TEST(setup_noop, teardown_noop, log_retain_horizon_longer_than_drop_oldest);
    MDB_RUN_TEST(setup_noop, teardown_noop, log_retain_zone_boundary_correct);
    MDB_RUN_TEST(setup_noop, teardown_noop, log_retain_wal_recovery);
    MDB_RUN_TEST(setup_noop, teardown_noop, log_retain_query_covers_full_horizon);
    MDB_RUN_TEST(setup_noop, teardown_noop, log_retain_disabled_when_zones_zero);
    MDB_RUN_TEST(setup_noop, teardown_noop, log_retain_invalid_cfg_rejected);
    return MDB_RESULT();
}
