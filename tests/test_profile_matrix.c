// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "microdb_port_posix.h"
#include "../src/microdb_internal.h"

#include <stdlib.h>
#include <string.h>

#ifndef MICRODB_PROFILE_TEST_DB_PATH
#define MICRODB_PROFILE_TEST_DB_PATH "profile_matrix.bin"
#endif

enum {
    PROFILE_STORAGE_CAP = 2u * 1024u * 1024u
};

static microdb_t g_db;
static microdb_storage_t g_storage;
static microdb_cfg_t g_cfg;
static microdb_port_posix_ctx_t g_posix_ctx;
static uint32_t g_now = 1000u;
static uint32_t g_lock_depth = 0u;
static uint32_t g_reentrant_lock = 0u;
static microdb_err_t g_cb_error = MICRODB_OK;

static microdb_timestamp_t mock_now(void) {
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
    g_cb_error = MICRODB_OK;

    microdb_port_posix_remove(MICRODB_PROFILE_TEST_DB_PATH);
    ASSERT_EQ(microdb_port_posix_init(&g_storage, MICRODB_PROFILE_TEST_DB_PATH, PROFILE_STORAGE_CAP), MICRODB_OK);

    g_cfg.storage = &g_storage;
    g_cfg.ram_kb = 0u;
    g_cfg.now = mock_now;
    g_cfg.lock_create = test_lock_create;
    g_cfg.lock = test_lock;
    g_cfg.unlock = test_unlock;
    g_cfg.lock_destroy = test_lock_destroy;
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_OK);
}

static void teardown_db(void) {
    if (microdb_core_const(&g_db)->magic == MICRODB_MAGIC) {
        (void)microdb_deinit(&g_db);
    }
    microdb_port_posix_deinit(&g_storage);
    microdb_port_posix_remove(MICRODB_PROFILE_TEST_DB_PATH);
}

static void crash_reopen_db(void) {
    if (microdb_core_const(&g_db)->magic == MICRODB_MAGIC) {
        free(microdb_core(&g_db)->heap);
    }
    microdb_port_posix_deinit(&g_storage);
    ASSERT_EQ(microdb_port_posix_init(&g_storage, MICRODB_PROFILE_TEST_DB_PATH, PROFILE_STORAGE_CAP), MICRODB_OK);
    g_cfg.storage = &g_storage;
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_OK);
}

static bool kv_reenter_cb(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    uint8_t out = 0u;
    (void)key;
    (void)val;
    (void)val_len;
    (void)ttl_remaining;
    (void)ctx;
    if (microdb_kv_get(&g_db, "A", &out, 1u, NULL) != MICRODB_OK || out != 11u) {
        g_cb_error = MICRODB_ERR_INVALID;
    }
    return true;
}

static bool ts_reenter_cb(const microdb_ts_sample_t *sample, void *ctx) {
    microdb_ts_sample_t out;
    (void)sample;
    (void)ctx;
    if (microdb_ts_last(&g_db, "s", &out) != MICRODB_OK || out.v.u32 != 22u) {
        g_cb_error = MICRODB_ERR_INVALID;
    }
    return true;
}

static bool rel_reenter_cb(const void *row_buf, void *ctx) {
    microdb_table_t *table = (microdb_table_t *)ctx;
    uint8_t out[64] = { 0 };
    uint32_t id = 1u;
    (void)row_buf;
    if (microdb_rel_find_by(&g_db, table, "id", &id, out) != MICRODB_OK) {
        g_cb_error = MICRODB_ERR_INVALID;
    }
    return true;
}

MDB_TEST(test_profile_contract_end_to_end) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 33u;
    uint8_t a = 11u;
    uint32_t tsv = 22u;
    uint8_t txv = 7u;
    uint8_t out = 0u;
    microdb_ts_sample_t sample;
    uint32_t corrupt_off_a;
    uint32_t corrupt_off_b;
    uint32_t super_off_a;
    uint32_t super_off_b;
    const microdb_core_t *core;
    uint8_t zeros[32];

    ASSERT_EQ(microdb_kv_set(&g_db, "A", &a, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_register(&g_db, "s", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "s", 1u, &tsv), MICRODB_OK);

    ASSERT_EQ(microdb_schema_init(&schema, "t", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_get(&g_db, "t", &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);

    ASSERT_EQ(microdb_txn_begin(&g_db), MICRODB_OK);
    ASSERT_EQ(microdb_kv_put(&g_db, "TX", &txv, 1u), MICRODB_OK);
    ASSERT_EQ(microdb_txn_commit(&g_db), MICRODB_OK);

    microdb_port_posix_simulate_power_loss(&g_storage);
    crash_reopen_db();
    ASSERT_EQ(microdb_kv_get(&g_db, "TX", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(out, 7u);
    ASSERT_EQ(microdb_ts_last(&g_db, "s", &sample), MICRODB_OK);
    ASSERT_EQ(sample.v.u32, 22u);
    ASSERT_EQ(microdb_table_get(&g_db, "t", &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find_by(&g_db, table, "id", &id, row), MICRODB_OK);

    ASSERT_EQ(microdb_compact(&g_db), MICRODB_OK);

    g_cb_error = MICRODB_OK;
    g_reentrant_lock = 0u;
    ASSERT_EQ(microdb_kv_iter(&g_db, kv_reenter_cb, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_ts_query(&g_db, "s", 0u, 100u, ts_reenter_cb, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_rel_iter(&g_db, table, rel_reenter_cb, table), MICRODB_OK);
    ASSERT_EQ(g_cb_error, MICRODB_OK);
    ASSERT_EQ(g_reentrant_lock, 0u);

    core = microdb_core_const(&g_db);
    corrupt_off_a = core->layout.bank_a_offset + 24u;
    corrupt_off_b = core->layout.bank_b_offset + 24u;
    super_off_a = core->layout.super_a_offset;
    super_off_b = core->layout.super_b_offset;
    memset(zeros, 0, sizeof(zeros));
    ASSERT_EQ(g_storage.write(g_storage.ctx, super_off_a, zeros, sizeof(zeros)), MICRODB_OK);
    ASSERT_EQ(g_storage.write(g_storage.ctx, super_off_b, zeros, sizeof(zeros)), MICRODB_OK);
    ASSERT_EQ(g_storage.write(g_storage.ctx, corrupt_off_a - 24u, zeros, sizeof(zeros)), MICRODB_OK);
    ASSERT_EQ(g_storage.write(g_storage.ctx, corrupt_off_b - 24u, zeros, sizeof(zeros)), MICRODB_OK);
    ASSERT_EQ(g_storage.sync(g_storage.ctx), MICRODB_OK);
    microdb_port_posix_simulate_power_loss(&g_storage);
    if (microdb_core_const(&g_db)->magic == MICRODB_MAGIC) {
        free(microdb_core(&g_db)->heap);
    }
    microdb_port_posix_deinit(&g_storage);
    ASSERT_EQ(microdb_port_posix_init(&g_storage, MICRODB_PROFILE_TEST_DB_PATH, PROFILE_STORAGE_CAP), MICRODB_OK);
    g_cfg.storage = &g_storage;
    ASSERT_EQ(g_storage.read(g_storage.ctx, super_off_a, &zeros[0], 1u), MICRODB_OK);
    ASSERT_EQ(g_storage.read(g_storage.ctx, super_off_b, &zeros[1], 1u), MICRODB_OK);
    ASSERT_EQ(g_storage.read(g_storage.ctx, corrupt_off_a - 24u, &zeros[2], 1u), MICRODB_OK);
    ASSERT_EQ(g_storage.read(g_storage.ctx, corrupt_off_b - 24u, &zeros[3], 1u), MICRODB_OK);
    ASSERT_EQ(zeros[0], 0u);
    ASSERT_EQ(zeros[1], 0u);
    ASSERT_EQ(zeros[2], 0u);
    ASSERT_EQ(zeros[3], 0u);
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_ERR_CORRUPT);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, test_profile_contract_end_to_end);
    return MDB_RESULT();
}
