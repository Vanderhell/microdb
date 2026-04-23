// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static char g_path[128];
static unsigned g_seq = 0u;

static bool rel_iter_noop(const void *row_buf, void *ctx) {
    (void)row_buf;
    (void)ctx;
    return true;
}

static bool kv_iter_noop(const char *key, const void *val, size_t len, uint32_t ttl, void *ctx) {
    (void)key;
    (void)val;
    (void)len;
    (void)ttl;
    (void)ctx;
    return true;
}

static bool ts_query_noop(const lox_ts_sample_t *sample, void *ctx) {
    (void)sample;
    (void)ctx;
    return true;
}

static void make_path(const char *tag) {
    g_seq++;
    (void)snprintf(g_path, sizeof(g_path), "safety_inv_%s_%u.bin", tag, g_seq);
}

static void open_db_with_storage(const char *tag) {
    lox_cfg_t cfg;
    make_path(tag);
    lox_port_posix_remove(g_path);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 131072u), LOX_OK);
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.wal_compact_auto = 1u;
    cfg.wal_compact_threshold_pct = 75u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void reopen_db_with_storage(void) {
    lox_cfg_t cfg;
    lox_port_posix_deinit(&g_storage);
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&g_db, 0, sizeof(g_db));
    memset(&cfg, 0, sizeof(cfg));
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 131072u), LOX_OK);
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.wal_compact_auto = 1u;
    cfg.wal_compact_threshold_pct = 75u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void close_db_with_storage(void) {
    (void)lox_deinit(&g_db);
    (void)lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(g_path);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
}

static void open_db_ram_only(void) {
    lox_cfg_t cfg;
    memset(&g_db, 0, sizeof(g_db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void close_db_ram_only(void) {
    (void)lox_deinit(&g_db);
    memset(&g_db, 0, sizeof(g_db));
}

static void setup_noop(void) {
}

static void teardown_noop(void) {
    if (g_storage.ctx != NULL) {
        close_db_with_storage();
    } else {
        close_db_ram_only();
    }
}

MDB_TEST(invariant_magic_cleared_before_heap_free) {
    uint32_t magic = 0u;
    open_db_ram_only();
    ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    memcpy(&magic, &g_db._opaque[0], sizeof(magic));
    ASSERT_EQ(magic != 0x4D444230u, 1);
}

MDB_TEST(invariant_wal_replay_stops_at_corrupt_entry) {
    uint8_t one = 1u;
    uint8_t two = 2u;
    uint8_t out = 0u;
    uint8_t header[32];
    uint32_t entry_count;
    uint32_t off;
    uint32_t i;
    FILE *fp;

    open_db_with_storage("walstop");
    ASSERT_EQ(lox_kv_set(&g_db, "snap", &one, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "tail", &two, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_deinit(&g_db), LOX_OK);

    fp = fopen(g_path, "r+b");
    ASSERT_EQ(fp != NULL, 1);
    ASSERT_EQ(fread(header, 1u, sizeof(header), fp), sizeof(header));
    memcpy(&entry_count, header + 8u, sizeof(entry_count));
    off = 32u;
    for (i = 0u; i < entry_count; ++i) {
        uint8_t eh[16];
        uint16_t data_len;
        uint32_t aligned;
        ASSERT_EQ(fseek(fp, (long)off, SEEK_SET), 0);
        ASSERT_EQ(fread(eh, 1u, sizeof(eh), fp), sizeof(eh));
        memcpy(&data_len, eh + 10u, sizeof(data_len));
        aligned = ((uint32_t)data_len + 3u) & ~3u;
        if (i + 1u == entry_count) {
            uint32_t bad_crc = 0u;
            memcpy(&bad_crc, eh + 12u, sizeof(bad_crc));
            bad_crc ^= 0x00FF00FFu;
            ASSERT_EQ(fseek(fp, (long)(off + 12u), SEEK_SET), 0);
            ASSERT_EQ(fwrite(&bad_crc, 1u, sizeof(bad_crc), fp), sizeof(bad_crc));
            break;
        }
        off += 16u + aligned;
    }
    ASSERT_EQ(fclose(fp), 0);

    reopen_db_with_storage();
    ASSERT_EQ(lox_kv_get(&g_db, "snap", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, one);
}

MDB_TEST(invariant_txn_without_commit_not_visible_after_reopen) {
    uint8_t v = 7u;
    uint8_t out = 0u;
    open_db_with_storage("txn");
    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "txn_test", &v, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_txn_rollback(&g_db), LOX_OK);
    ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    reopen_db_with_storage();
    ASSERT_EQ(lox_kv_get(&g_db, "txn_test", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(invariant_superblock_switches_on_compact) {
    lox_db_stats_t before;
    lox_db_stats_t after;
    uint8_t v = 9u;
    open_db_with_storage("super");
    ASSERT_EQ(lox_get_db_stats(&g_db, &before), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "k", &v, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_compact(&g_db), LOX_OK);
    ASSERT_EQ(lox_get_db_stats(&g_db, &after), LOX_OK);
    ASSERT_EQ(after.active_bank != before.active_bank, 1);
    ASSERT_EQ(after.active_generation > before.active_generation, 1);
}

MDB_TEST(invariant_double_deinit_returns_invalid) {
    open_db_ram_only();
    ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    ASSERT_EQ(lox_deinit(&g_db), LOX_ERR_INVALID);
}

MDB_TEST(invariant_all_apis_reject_null_handle) {
    lox_stats_t stats;
    uint8_t b = 0u;
    lox_ts_sample_t ts;
    uint32_t deleted = 0u;
    uint32_t rel_count = 0u;
    size_t out_len = 0u;

    ASSERT_EQ(lox_flush(NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_deinit(NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_stats(NULL, &stats), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_set(NULL, "k", &b, 1u, 0u), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_get(NULL, "k", &b, 1u, &out_len), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_del(NULL, "k"), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_exists(NULL, "k"), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_iter(NULL, kv_iter_noop, NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_clear(NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_txn_begin(NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_txn_commit(NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_txn_rollback(NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_ts_register(NULL, "s", LOX_TS_U32, 0u), LOX_ERR_INVALID);
    ASSERT_EQ(lox_ts_insert(NULL, "s", 1u, &b), LOX_ERR_INVALID);
    ASSERT_EQ(lox_ts_last(NULL, "s", &ts), LOX_ERR_INVALID);
    ASSERT_EQ(lox_ts_query(NULL, "s", 0u, 1u, ts_query_noop, NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_ts_clear(NULL, "s"), LOX_ERR_INVALID);
    ASSERT_EQ(lox_rel_insert(NULL, NULL, &b), LOX_ERR_INVALID);
    ASSERT_EQ(lox_rel_find(NULL, NULL, &b, rel_iter_noop, NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_rel_delete(NULL, NULL, &b, &deleted), LOX_ERR_INVALID);
    ASSERT_EQ(lox_rel_iter(NULL, NULL, rel_iter_noop, NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_rel_clear(NULL, NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_rel_count(NULL, &rel_count), LOX_ERR_INVALID);
    ASSERT_EQ(lox_compact(NULL), LOX_ERR_INVALID);
}

int main(void) {
    MDB_RUN_TEST(setup_noop, teardown_noop, invariant_magic_cleared_before_heap_free);
    MDB_RUN_TEST(setup_noop, teardown_noop, invariant_wal_replay_stops_at_corrupt_entry);
    MDB_RUN_TEST(setup_noop, teardown_noop, invariant_txn_without_commit_not_visible_after_reopen);
    MDB_RUN_TEST(setup_noop, teardown_noop, invariant_superblock_switches_on_compact);
    MDB_RUN_TEST(setup_noop, teardown_noop, invariant_double_deinit_returns_invalid);
    MDB_RUN_TEST(setup_noop, teardown_noop, invariant_all_apis_reject_null_handle);
    return MDB_RESULT();
}
