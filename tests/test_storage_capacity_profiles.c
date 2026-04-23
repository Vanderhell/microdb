// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_capacity_profile.h"
#include "../port/posix/lox_port_posix.h"

#include <stdio.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static char g_path[128];

static void make_path(const char *suffix) {
    (void)snprintf(g_path, sizeof(g_path), "capacity_profile_%s.bin", suffix);
}

static void setup_noop(void) {
}

static void teardown_noop(void) {
}

static void close_db(void) {
    (void)lox_deinit(&g_db);
    lox_port_posix_deinit(&g_storage);
}

static void open_db_with_capacity(uint32_t capacity_bytes) {
    lox_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&cfg, 0, sizeof(cfg));

    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, capacity_bytes), LOX_OK);
    cfg.storage = &g_storage;
    cfg.ram_kb = 64u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void run_profile_reopen_roundtrip(lox_storage_profile_t profile, const char *file_suffix) {
    uint32_t expected_capacity = lox_storage_profile_capacity_bytes(profile);
#ifdef LOX_CAP_LIMIT_NONE
    lox_effective_capacity_t cap;
    lox_db_stats_t dbs;
#else
    lox_stats_t stats;
#endif
    uint8_t value = 42u;
    uint8_t out = 0u;

    make_path(file_suffix);
    lox_port_posix_remove(g_path);

    open_db_with_capacity(expected_capacity);
#ifdef LOX_CAP_LIMIT_NONE
    ASSERT_EQ(lox_get_effective_capacity(&g_db, &cap), LOX_OK);
    ASSERT_EQ(lox_get_db_stats(&g_db, &dbs), LOX_OK);
    ASSERT_EQ(cap.wal_budget_total > 0u, 1);
    ASSERT_EQ(dbs.effective_capacity_bytes, expected_capacity);
#else
    ASSERT_EQ(lox_stats(&g_db, &stats), LOX_OK);
    ASSERT_EQ(stats.wal_bytes_total > 0u, 1);
    ASSERT_EQ(g_storage.capacity, expected_capacity);
#endif
    ASSERT_EQ(lox_kv_set(&g_db, "k", &value, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    close_db();

    open_db_with_capacity(expected_capacity);
    ASSERT_EQ(lox_kv_get(&g_db, "k", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, value);
#ifdef LOX_CAP_LIMIT_NONE
    ASSERT_EQ(lox_get_effective_capacity(&g_db, &cap), LOX_OK);
    ASSERT_EQ(lox_get_db_stats(&g_db, &dbs), LOX_OK);
    ASSERT_EQ(dbs.effective_capacity_bytes, expected_capacity);
#else
    ASSERT_EQ(lox_stats(&g_db, &stats), LOX_OK);
    ASSERT_EQ(stats.wal_bytes_total > 0u, 1);
    ASSERT_EQ(g_storage.capacity, expected_capacity);
#endif
    close_db();

    lox_port_posix_remove(g_path);
}

MDB_TEST(capacity_profile_2_mib_reopen_roundtrip) {
    run_profile_reopen_roundtrip(LOX_STORAGE_PROFILE_2_MIB, "2mib");
}

MDB_TEST(capacity_profile_32_mib_reopen_roundtrip) {
    run_profile_reopen_roundtrip(LOX_STORAGE_PROFILE_32_MIB, "32mib");
}

MDB_TEST(capacity_profile_invalid_maps_to_zero) {
    ASSERT_EQ(lox_storage_profile_capacity_bytes((lox_storage_profile_t)99), 0u);
}

int main(void) {
    MDB_RUN_TEST(setup_noop, teardown_noop, capacity_profile_2_mib_reopen_roundtrip);
    MDB_RUN_TEST(setup_noop, teardown_noop, capacity_profile_32_mib_reopen_roundtrip);
    MDB_RUN_TEST(setup_noop, teardown_noop, capacity_profile_invalid_maps_to_zero);
    return MDB_RESULT();
}
