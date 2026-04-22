// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "../port/posix/microdb_port_posix.h"
#include "../src/microdb_internal.h"

#include <stdio.h>
#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_storage;
static char g_path[128];
static unsigned g_seq = 0u;

static void setup_noop(void) {
}

static void teardown_noop(void) {
}

static void open_db(int with_storage) {
    microdb_cfg_t cfg;
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    if (with_storage) {
        g_seq++;
        (void)snprintf(g_path, sizeof(g_path), "ts_log_retain_%u.bin", g_seq);
        microdb_port_posix_remove(g_path);
        ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 131072u), MICRODB_OK);
        cfg.storage = &g_storage;
        cfg.wal_compact_auto = 1u;
        cfg.wal_compact_threshold_pct = 75u;
    }
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void reopen_db(void) {
    microdb_cfg_t cfg;
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_OK);
    microdb_port_posix_deinit(&g_storage);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 131072u), MICRODB_OK);
    cfg.storage = &g_storage;
    cfg.wal_compact_auto = 1u;
    cfg.wal_compact_threshold_pct = 75u;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void close_db(void) {
    (void)microdb_deinit(&g_db);
    if (g_storage.ctx != NULL) {
        microdb_port_posix_deinit(&g_storage);
        microdb_port_posix_remove(g_path);
    }
}

static uint32_t stream_capacity(const char *name) {
    microdb_core_t *core = microdb_core(&g_db);
    uint32_t i;
    for (i = 0u; i < MICRODB_TS_MAX_STREAMS; ++i) {
        if (core->ts.streams[i].registered && strcmp(core->ts.streams[i].name, name) == 0) {
            return core->ts.streams[i].capacity;
        }
    }
    return 0u;
}

static void query_oldest_ts(const char *name, uint32_t *out_oldest_ts) {
    microdb_ts_sample_t buf[2048];
    size_t out_count = 0u;
    ASSERT_EQ(microdb_ts_query_buf(&g_db, name, 0u, UINT32_MAX, buf, 2048u, &out_count), MICRODB_OK);
    ASSERT_GT(out_count, 0u);
    *out_oldest_ts = buf[0].ts;
}

static void query_count(const char *name, size_t *out_value) {
    size_t out_count = 0u;
    ASSERT_EQ(microdb_ts_count(&g_db, name, 0u, UINT32_MAX, &out_count), MICRODB_OK);
    *out_value = out_count;
}

MDB_TEST(log_retain_horizon_longer_than_drop_oldest) {
    uint32_t i;
    uint32_t v = 0u;
    uint32_t cap_a;
    uint32_t cap_b;
    uint32_t inserts;
    microdb_ts_log_retain_cfg_t cfg = {4u, 25u};

    open_db(0);
    ASSERT_EQ(microdb_ts_register(&g_db, "a", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_register_ex(&g_db, "b", MICRODB_TS_U32, 0u, &cfg), MICRODB_OK);
    cap_a = stream_capacity("a");
    cap_b = stream_capacity("b");
    inserts = (cap_a > cap_b ? cap_a : cap_b) * 4u;
    for (i = 0u; i < inserts; ++i) {
        v = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "a", i, &v), MICRODB_OK);
        ASSERT_EQ(microdb_ts_insert(&g_db, "b", i, &v), MICRODB_OK);
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
    microdb_ts_log_retain_cfg_t cfg = {2u, 50u};
    open_db(0);
    ASSERT_EQ(microdb_ts_register_ex(&g_db, "z", MICRODB_TS_U32, 0u, &cfg), MICRODB_OK);
    cap = stream_capacity("z");
    ASSERT_GT(cap, 0u);
    for (i = 0u; i < cap; ++i) {
        v = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "z", i, &v), MICRODB_OK);
    }
    v = cap;
    ASSERT_EQ(microdb_ts_insert(&g_db, "z", cap, &v), MICRODB_OK);
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
    microdb_ts_log_retain_cfg_t cfg = {4u, 25u};

    open_db(1);
    ASSERT_EQ(microdb_ts_register_ex(&g_db, "r", MICRODB_TS_U32, 0u, &cfg), MICRODB_OK);
    for (i = 0u; i < 300u; ++i) {
        v = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "r", i, &v), MICRODB_OK);
    }
    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
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
    microdb_ts_log_retain_cfg_t cfg = {4u, 25u};
    open_db(0);
    ASSERT_EQ(microdb_ts_register_ex(&g_db, "h", MICRODB_TS_U32, 0u, &cfg), MICRODB_OK);
    cap = stream_capacity("h");
    for (i = 0u; i < (cap * 3u); ++i) {
        v = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "h", i, &v), MICRODB_OK);
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
    microdb_ts_log_retain_cfg_t cfg = {0u, 0u};
    open_db(0);
    ASSERT_EQ(microdb_ts_register(&g_db, "a", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_register_ex(&g_db, "b", MICRODB_TS_U32, 0u, &cfg), MICRODB_OK);
    for (i = 0u; i < 400u; ++i) {
        v = i;
        ASSERT_EQ(microdb_ts_insert(&g_db, "a", i, &v), MICRODB_OK);
        ASSERT_EQ(microdb_ts_insert(&g_db, "b", i, &v), MICRODB_OK);
    }
    query_oldest_ts("a", &oldest_a);
    query_oldest_ts("b", &oldest_b);
    ASSERT_EQ(oldest_a, oldest_b);
    close_db();
}

MDB_TEST(log_retain_invalid_cfg_rejected) {
    microdb_ts_log_retain_cfg_t cfg = {4u, 30u};
    open_db(0);
    ASSERT_EQ(microdb_ts_register_ex(&g_db, "bad", MICRODB_TS_U32, 0u, &cfg), MICRODB_ERR_INVALID);
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
