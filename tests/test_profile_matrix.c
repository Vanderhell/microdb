// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_port_posix.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <string.h>

#ifndef LOX_PROFILE_TEST_DB_PATH
#define LOX_PROFILE_TEST_DB_PATH "profile_matrix.bin"
#endif

enum {
    PROFILE_STORAGE_CAP = 2u * 1024u * 1024u
};

static lox_t g_db;
static lox_storage_t g_storage;
static lox_cfg_t g_cfg;
static lox_port_posix_ctx_t g_posix_ctx;
static uint32_t g_now = 1000u;
static uint32_t g_lock_depth = 0u;
static uint32_t g_reentrant_lock = 0u;
static lox_err_t g_cb_error = LOX_OK;

static lox_timestamp_t mock_now(void) {
    return g_now++;
}

static void *test_lock_create(void) {
    return NULL;
}

static void test_lock(void *hdl) {
    (void)hdl;
    if (g_lock_depth != 0u) {
        g_reentrant_lock++;
    }
    g_lock_depth++;
}

static void test_unlock(void *hdl) {
    (void)hdl;
    if (g_lock_depth > 0u) {
        g_lock_depth--;
    }
}

static void test_lock_destroy(void *hdl) {
    (void)hdl;
}

static void setup_db(void) {
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    memset(&g_cfg, 0, sizeof(g_cfg));
    memset(&g_posix_ctx, 0, sizeof(g_posix_ctx));
    g_now = 1000u;
    g_lock_depth = 0u;
    g_reentrant_lock = 0u;
    g_cb_error = LOX_OK;

    lox_port_posix_remove(LOX_PROFILE_TEST_DB_PATH);
    ASSERT_EQ(lox_port_posix_init(&g_storage, LOX_PROFILE_TEST_DB_PATH, PROFILE_STORAGE_CAP), LOX_OK);

    g_cfg.storage = &g_storage;
    g_cfg.ram_kb = 0u;
    g_cfg.now = mock_now;
    g_cfg.lock_create = test_lock_create;
    g_cfg.lock = test_lock;
    g_cfg.unlock = test_unlock;
    g_cfg.lock_destroy = test_lock_destroy;
    ASSERT_EQ(lox_init(&g_db, &g_cfg), LOX_OK);
}

static void teardown_db(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        (void)lox_deinit(&g_db);
    }
    lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(LOX_PROFILE_TEST_DB_PATH);
}

static void crash_reopen_db(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
    }
    lox_port_posix_deinit(&g_storage);
    ASSERT_EQ(lox_port_posix_init(&g_storage, LOX_PROFILE_TEST_DB_PATH, PROFILE_STORAGE_CAP), LOX_OK);
    g_cfg.storage = &g_storage;
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(lox_init(&g_db, &g_cfg), LOX_OK);
}

static bool kv_reenter_cb(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    uint8_t out = 0u;
    (void)key;
    (void)val;
    (void)val_len;
    (void)ttl_remaining;
    (void)ctx;
    if (lox_kv_get(&g_db, "A", &out, 1u, NULL) != LOX_OK || out != 11u) {
        g_cb_error = LOX_ERR_INVALID;
    }
    return true;
}

static bool ts_reenter_cb(const lox_ts_sample_t *sample, void *ctx) {
    lox_ts_sample_t out;
    (void)sample;
    (void)ctx;
    if (lox_ts_last(&g_db, "s", &out) != LOX_OK || out.v.u32 != 22u) {
        g_cb_error = LOX_ERR_INVALID;
    }
    return true;
}

static bool rel_reenter_cb(const void *row_buf, void *ctx) {
    lox_table_t *table = (lox_table_t *)ctx;
    uint8_t out[64] = { 0 };
    uint32_t id = 1u;
    (void)row_buf;
    if (lox_rel_find_by(&g_db, table, "id", &id, out) != LOX_OK) {
        g_cb_error = LOX_ERR_INVALID;
    }
    return true;
}

MDB_TEST(test_profile_contract_end_to_end) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 33u;
    uint8_t a = 11u;
    uint32_t tsv = 22u;
    uint8_t txv = 7u;
    uint8_t out = 0u;
    lox_ts_sample_t sample;
    uint32_t corrupt_off_a;
    uint32_t corrupt_off_b;
    uint32_t super_off_a;
    uint32_t super_off_b;
    const lox_core_t *core;
    uint8_t zeros[32];

    ASSERT_EQ(lox_kv_set(&g_db, "A", &a, 1u, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_register(&g_db, "s", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "s", 1u, &tsv), LOX_OK);

    ASSERT_EQ(lox_schema_init(&schema, "t", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "age", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "t", &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);

    ASSERT_EQ(lox_txn_begin(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "TX", &txv, 1u), LOX_OK);
    ASSERT_EQ(lox_txn_commit(&g_db), LOX_OK);

    lox_port_posix_simulate_power_loss(&g_storage);
    crash_reopen_db();
    ASSERT_EQ(lox_kv_get(&g_db, "TX", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 7u);
    ASSERT_EQ(lox_ts_last(&g_db, "s", &sample), LOX_OK);
    ASSERT_EQ(sample.v.u32, 22u);
    ASSERT_EQ(lox_table_get(&g_db, "t", &table), LOX_OK);
    ASSERT_EQ(lox_rel_find_by(&g_db, table, "id", &id, row), LOX_OK);

    ASSERT_EQ(lox_compact(&g_db), LOX_OK);

    g_cb_error = LOX_OK;
    g_reentrant_lock = 0u;
    ASSERT_EQ(lox_kv_iter(&g_db, kv_reenter_cb, NULL), LOX_OK);
    ASSERT_EQ(lox_ts_query(&g_db, "s", 0u, 100u, ts_reenter_cb, NULL), LOX_OK);
    ASSERT_EQ(lox_rel_iter(&g_db, table, rel_reenter_cb, table), LOX_OK);
    ASSERT_EQ(g_cb_error, LOX_OK);
    ASSERT_EQ(g_reentrant_lock, 0u);

    core = lox_core_const(&g_db);
    corrupt_off_a = core->layout.bank_a_offset + 24u;
    corrupt_off_b = core->layout.bank_b_offset + 24u;
    super_off_a = core->layout.super_a_offset;
    super_off_b = core->layout.super_b_offset;
    memset(zeros, 0, sizeof(zeros));
    ASSERT_EQ(g_storage.write(g_storage.ctx, super_off_a, zeros, sizeof(zeros)), LOX_OK);
    ASSERT_EQ(g_storage.write(g_storage.ctx, super_off_b, zeros, sizeof(zeros)), LOX_OK);
    ASSERT_EQ(g_storage.write(g_storage.ctx, corrupt_off_a - 24u, zeros, sizeof(zeros)), LOX_OK);
    ASSERT_EQ(g_storage.write(g_storage.ctx, corrupt_off_b - 24u, zeros, sizeof(zeros)), LOX_OK);
    ASSERT_EQ(g_storage.sync(g_storage.ctx), LOX_OK);
    lox_port_posix_simulate_power_loss(&g_storage);
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
    }
    lox_port_posix_deinit(&g_storage);
    ASSERT_EQ(lox_port_posix_init(&g_storage, LOX_PROFILE_TEST_DB_PATH, PROFILE_STORAGE_CAP), LOX_OK);
    g_cfg.storage = &g_storage;
    ASSERT_EQ(g_storage.read(g_storage.ctx, super_off_a, &zeros[0], 1u), LOX_OK);
    ASSERT_EQ(g_storage.read(g_storage.ctx, super_off_b, &zeros[1], 1u), LOX_OK);
    ASSERT_EQ(g_storage.read(g_storage.ctx, corrupt_off_a - 24u, &zeros[2], 1u), LOX_OK);
    ASSERT_EQ(g_storage.read(g_storage.ctx, corrupt_off_b - 24u, &zeros[3], 1u), LOX_OK);
    ASSERT_EQ(zeros[0], 0u);
    ASSERT_EQ(zeros[1], 0u);
    ASSERT_EQ(zeros[2], 0u);
    ASSERT_EQ(zeros[3], 0u);
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(lox_init(&g_db, &g_cfg), LOX_ERR_CORRUPT);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, test_profile_contract_end_to_end);
    return MDB_RESULT();
}
