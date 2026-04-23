// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"
#include "../src/lox_crc.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <string.h>

enum {
    WAL_MAGIC = 0x4D44424Cu,
    WAL_VERSION = 0x00010000u,
    WAL_ENTRY_MAGIC = 0x454E5452u,
    WAL_ENGINE_META = 0xFFu,
    WAL_ENGINE_KV = 0u,
    WAL_OP_SET_INSERT = 0u,
    WAL_OP_COMPACT_BEGIN = 3u,
    WAL_OP_COMPACT_COMMIT = 4u
};

static lox_t g_db;
static lox_storage_t g_storage;
static const char *g_path = "compact_test.bin";

static void put_u32(uint8_t *dst, uint32_t value) {
    memcpy(dst, &value, sizeof(value));
}

static void put_u16(uint8_t *dst, uint16_t value) {
    memcpy(dst, &value, sizeof(value));
}

static uint32_t write_wal_entry(uint32_t offset, uint32_t seq, uint8_t engine, uint8_t op, const uint8_t *payload, uint16_t payload_len) {
    uint8_t entry[16];
    uint8_t aligned_payload[256];
    uint32_t crc;
    uint32_t aligned_len = (uint32_t)((payload_len + 3u) & ~3u);

    memset(entry, 0, sizeof(entry));
    memset(aligned_payload, 0, sizeof(aligned_payload));
    put_u32(entry + 0u, WAL_ENTRY_MAGIC);
    put_u32(entry + 4u, seq);
    entry[8] = engine;
    entry[9] = op;
    put_u16(entry + 10u, payload_len);
    if (payload_len != 0u && payload != NULL) {
        memcpy(aligned_payload, payload, payload_len);
    }
    crc = LOX_CRC32(entry, 12u);
    if (payload_len != 0u && payload != NULL) {
        crc = lox_crc32(crc, aligned_payload, payload_len);
    }
    put_u32(entry + 12u, crc);
    if (g_storage.write(g_storage.ctx, offset, entry, sizeof(entry)) != LOX_OK) {
        return 0u;
    }
    if (aligned_len != 0u) {
        if (g_storage.write(g_storage.ctx, offset + 16u, aligned_payload, aligned_len) != LOX_OK) {
            return 0u;
        }
    }
    return offset + 16u + aligned_len;
}

static void open_db_with_cfg(const lox_cfg_t *cfg) {
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 65536u), LOX_OK);
    ASSERT_EQ(lox_init(&g_db, cfg), LOX_OK);
}

static void setup_db(void) {
    lox_cfg_t cfg;

    lox_port_posix_remove(g_path);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    open_db_with_cfg(&cfg);
}

static void setup_auto_db(void) {
    lox_cfg_t cfg;

    lox_port_posix_remove(g_path);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.wal_compact_auto = 1u;
    cfg.wal_compact_threshold_pct = 50u;
    open_db_with_cfg(&cfg);
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
    lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(g_path);
}

static void crash_drop_db_heap(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
    }
    memset(&g_db, 0, sizeof(g_db));
}

MDB_TEST(test_manual_compact_resets_wal) {
    lox_stats_t stats;
    uint8_t value = 1u;
    uint32_t i;

    for (i = 0u; i < 128u; ++i) {
        char key[12] = { 0 };
        key[0] = 'k';
        key[1] = (char)('0' + (char)((i / 100u) % 10u));
        key[2] = (char)('0' + (char)((i / 10u) % 10u));
        key[3] = (char)('0' + (char)(i % 10u));
        ASSERT_EQ(lox_kv_set(&g_db, key, &value, 1u, 0u), LOX_OK);
        ASSERT_EQ(lox_inspect(&g_db, &stats), LOX_OK);
        if (stats.wal_fill_pct > 50u) {
            break;
        }
    }

    ASSERT_GT(stats.wal_fill_pct, 50u);
    ASSERT_EQ(lox_compact(&g_db), LOX_OK);
    ASSERT_EQ(lox_inspect(&g_db, &stats), LOX_OK);
    ASSERT_EQ(stats.wal_fill_pct < 5u, 1);
}

MDB_TEST(test_data_intact_after_compact) {
    uint8_t a = 1u;
    uint8_t b = 2u;
    uint8_t c = 3u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_set(&g_db, "A", &a, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "B", &b, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "C", &c, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_compact(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 1u);
    ASSERT_EQ(lox_kv_get(&g_db, "B", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 2u);
    ASSERT_EQ(lox_kv_get(&g_db, "C", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 3u);
}

MDB_TEST(test_auto_compact_fires) {
    lox_stats_t stats;
    uint8_t value = 7u;
    uint32_t prev_fill = 0u;
    uint32_t i;
    uint32_t fired = 0u;

    for (i = 0u; i < 256u; ++i) {
        char key[12] = { 0 };
        key[0] = 'a';
        key[1] = (char)('0' + (char)((i / 100u) % 10u));
        key[2] = (char)('0' + (char)((i / 10u) % 10u));
        key[3] = (char)('0' + (char)(i % 10u));
        ASSERT_EQ(lox_kv_set(&g_db, key, &value, 1u, 0u), LOX_OK);
        ASSERT_EQ(lox_inspect(&g_db, &stats), LOX_OK);
        if (prev_fill >= 40u && stats.wal_fill_pct < 5u) {
            fired = 1u;
            break;
        }
        prev_fill = stats.wal_fill_pct;
    }

    ASSERT_EQ(fired, 1u);
}

MDB_TEST(test_recovery_after_interrupted_compact) {
    lox_cfg_t cfg;
    uint8_t a = 1u;
    uint8_t b = 2u;
    uint8_t c = 3u;
    uint8_t out = 0u;
    uint8_t wal_header[32];
    uint8_t wal_entry[16];
    uint32_t crc;

    ASSERT_EQ(lox_kv_set(&g_db, "A", &a, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "B", &b, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "C", &c, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);

    memset(wal_header, 0, sizeof(wal_header));
    put_u32(wal_header + 0u, WAL_MAGIC);
    put_u32(wal_header + 4u, WAL_VERSION);
    put_u32(wal_header + 8u, 1u);
    put_u32(wal_header + 12u, 1u);
    crc = LOX_CRC32(wal_header, 16u);
    put_u32(wal_header + 16u, crc);
    ASSERT_EQ(g_storage.write(g_storage.ctx, 0u, wal_header, sizeof(wal_header)), LOX_OK);

    memset(wal_entry, 0, sizeof(wal_entry));
    put_u32(wal_entry + 0u, WAL_ENTRY_MAGIC);
    put_u32(wal_entry + 4u, 1u);
    wal_entry[8] = (uint8_t)WAL_ENGINE_META;
    wal_entry[9] = (uint8_t)WAL_OP_COMPACT_BEGIN;
    put_u16(wal_entry + 10u, 0u);
    crc = LOX_CRC32(wal_entry, 12u);
    put_u32(wal_entry + 12u, crc);
    ASSERT_EQ(g_storage.write(g_storage.ctx, 32u, wal_entry, sizeof(wal_entry)), LOX_OK);
    ASSERT_EQ(g_storage.sync(g_storage.ctx), LOX_OK);

    lox_port_posix_simulate_power_loss(&g_storage);
    crash_drop_db_heap();
    lox_port_posix_deinit(&g_storage);

    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    open_db_with_cfg(&cfg);

    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 1u);
    ASSERT_EQ(lox_kv_get(&g_db, "B", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 2u);
    ASSERT_EQ(lox_kv_get(&g_db, "C", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 3u);
}

MDB_TEST(test_legacy_compact_markers_do_not_change_recovery_outcome) {
    lox_cfg_t cfg;
    uint8_t a = 1u;
    uint8_t b = 2u;
    uint8_t out = 0u;
    uint8_t wal_header[32];
    uint8_t kv_payload[32];
    uint32_t off = 32u;
    uint32_t crc;
    const char *key = "B";
    size_t key_len = strlen(key);

    ASSERT_EQ(lox_kv_set(&g_db, "A", &a, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);

    memset(wal_header, 0, sizeof(wal_header));
    put_u32(wal_header + 0u, WAL_MAGIC);
    put_u32(wal_header + 4u, WAL_VERSION);
    put_u32(wal_header + 8u, 3u);
    put_u32(wal_header + 12u, 2u);
    crc = LOX_CRC32(wal_header, 16u);
    put_u32(wal_header + 16u, crc);
    ASSERT_EQ(g_storage.write(g_storage.ctx, 0u, wal_header, sizeof(wal_header)), LOX_OK);

    off = write_wal_entry(off, 1u, WAL_ENGINE_META, WAL_OP_COMPACT_BEGIN, NULL, 0u);
    ASSERT_GT(off, 0u);
    off = write_wal_entry(off, 2u, WAL_ENGINE_META, WAL_OP_COMPACT_COMMIT, NULL, 0u);
    ASSERT_GT(off, 0u);
    memset(kv_payload, 0, sizeof(kv_payload));
    kv_payload[0] = (uint8_t)key_len;
    memcpy(kv_payload + 1u, key, key_len);
    put_u32(kv_payload + 1u + key_len, 1u);
    kv_payload[1u + key_len + 4u] = b;
    put_u32(kv_payload + 1u + key_len + 4u + 1u, 0u);
    off = write_wal_entry(off, 3u, WAL_ENGINE_KV, WAL_OP_SET_INSERT, kv_payload, (uint16_t)(1u + key_len + 4u + 1u + 4u));
    ASSERT_GT(off, 0u);
    ASSERT_EQ(g_storage.sync(g_storage.ctx), LOX_OK);

    lox_port_posix_simulate_power_loss(&g_storage);
    crash_drop_db_heap();
    lox_port_posix_deinit(&g_storage);

    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    open_db_with_cfg(&cfg);

    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 1u);
    ASSERT_EQ(lox_kv_get(&g_db, "B", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 2u);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, test_manual_compact_resets_wal);
    MDB_RUN_TEST(setup_db, teardown_db, test_data_intact_after_compact);
    MDB_RUN_TEST(setup_auto_db, teardown_db, test_auto_compact_fires);
    MDB_RUN_TEST(setup_db, teardown_db, test_recovery_after_interrupted_compact);
    MDB_RUN_TEST(setup_db, teardown_db, test_legacy_compact_markers_do_not_change_recovery_outcome);
    return MDB_RESULT();
}
