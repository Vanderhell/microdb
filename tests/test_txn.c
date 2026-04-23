// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"

#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static const char *g_path = "txn_test.bin";
static uint32_t g_now = 1000u;

static lox_timestamp_t mock_now(void) {
    return g_now;
}

static void open_db(void) {
    lox_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    g_now = 1000u;
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void setup_db(void) {
    lox_port_posix_remove(g_path);
    open_db();
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
    lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(g_path);
}

static void reopen_db(void) {
    ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    lox_port_posix_deinit(&g_storage);
    open_db();
}

MDB_TEST(test_txn_commit_makes_values_visible) {
    uint8_t out = 0u;

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "A", &(uint8_t){ 1u }, 1u), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "B", &(uint8_t){ 2u }, 1u), LOX_OK);
    ASSERT_EQ(lox_txn_commit(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 1u);
    ASSERT_EQ(lox_kv_get(&g_db, "B", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 2u);
}

MDB_TEST(test_txn_rollback_discards_values) {
    uint8_t out = 0u;

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "A", &(uint8_t){ 1u }, 1u), LOX_OK);
    ASSERT_EQ(lox_txn_rollback(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(test_txn_double_begin_returns_err) {
    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_txn_begin(&g_db), LOX_ERR_TXN_ACTIVE);
}

MDB_TEST(test_txn_get_sees_staged_value) {
    uint8_t out = 0u;
    uint8_t v1 = 1u;
    uint8_t v2 = 2u;

    ASSERT_EQ(lox_kv_put(&g_db, "A", &v1, 1u), LOX_OK);
    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "A", &v2, 1u), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 2u);
    ASSERT_EQ(lox_txn_rollback(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 1u);
}

MDB_TEST(test_txn_del_inside_txn) {
    uint8_t out = 0u;
    uint8_t v1 = 1u;

    ASSERT_EQ(lox_kv_put(&g_db, "A", &v1, 1u), LOX_OK);
    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_del(&g_db, "A"), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
    ASSERT_EQ(lox_txn_rollback(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 1u);
}

MDB_TEST(test_kv_put_outside_txn_unaffected) {
    uint8_t out = 0u;

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "A", &(uint8_t){ 1u }, 1u), LOX_OK);
    ASSERT_EQ(lox_txn_commit(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "B", &(uint8_t){ 5u }, 1u), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "B", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 5u);
}

MDB_TEST(test_txn_no_commit_survives_reinit) {
    uint8_t out = 0u;

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "A", &(uint8_t){ 1u }, 1u), LOX_OK);
    reopen_db();
    ASSERT_EQ(lox_kv_get(&g_db, "A", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(test_txn_commit_under_wal_pressure_replays_after_reopen) {
    lox_stats_t st;
    uint8_t filler = 9u;
    uint8_t out = 0u;
    uint32_t i;

    for (i = 0u; i < 256u; ++i) {
        char key[12] = { 0 };
        key[0] = 'p';
        key[1] = (char)('0' + (char)((i / 100u) % 10u));
        key[2] = (char)('0' + (char)((i / 10u) % 10u));
        key[3] = (char)('0' + (char)(i % 10u));
        ASSERT_EQ(lox_kv_set(&g_db, key, &filler, 1u, 0u), LOX_OK);
        ASSERT_EQ(lox_inspect(&g_db, &st), LOX_OK);
        if (st.wal_fill_pct >= 65u) {
            break;
        }
    }
    ASSERT_GE(st.wal_fill_pct, 65u);

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "T1", &(uint8_t){ 1u }, 1u), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "T2", &(uint8_t){ 2u }, 1u), LOX_OK);
    ASSERT_EQ(lox_txn_commit(&g_db), LOX_OK);

    reopen_db();
    ASSERT_EQ(lox_kv_get(&g_db, "T1", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 1u);
    ASSERT_EQ(lox_kv_get(&g_db, "T2", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 2u);
}

MDB_TEST(test_txn_flush_while_active_does_not_commit_staged) {
    uint8_t out = 0u;

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "X", &(uint8_t){ 1u }, 1u), LOX_OK);
    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    reopen_db();
    ASSERT_EQ(lox_kv_get(&g_db, "X", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(test_txn_commit_replay_idempotent_across_reboots) {
    uint8_t out = 0u;
    uint32_t i;

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "IDEMP", &(uint8_t){ 7u }, 1u), LOX_OK);
    ASSERT_EQ(lox_txn_commit(&g_db), LOX_OK);
    for (i = 0u; i < 5u; ++i) {
        reopen_db();
        ASSERT_EQ(lox_kv_get(&g_db, "IDEMP", &out, 1u, NULL), LOX_OK);
        ASSERT_EQ(out, 7u);
    }
}

MDB_TEST(test_txn_commit_preserves_ttl_expiration) {
    uint8_t out = 0u;

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_set(&g_db, "ttl_txn", &(uint8_t){ 9u }, 1u, 5u), LOX_OK);
    ASSERT_EQ(lox_txn_commit(&g_db), LOX_OK);

    g_now += 6u;
    ASSERT_EQ(lox_kv_get(&g_db, "ttl_txn", &out, 1u, NULL), LOX_ERR_EXPIRED);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_commit_makes_values_visible);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_rollback_discards_values);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_double_begin_returns_err);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_get_sees_staged_value);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_del_inside_txn);
    MDB_RUN_TEST(setup_db, teardown_db, test_kv_put_outside_txn_unaffected);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_no_commit_survives_reinit);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_commit_under_wal_pressure_replays_after_reopen);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_flush_while_active_does_not_commit_staged);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_commit_replay_idempotent_across_reboots);
    MDB_RUN_TEST(setup_db, teardown_db, test_txn_commit_preserves_ttl_expiration);
    return MDB_RESULT();
}
