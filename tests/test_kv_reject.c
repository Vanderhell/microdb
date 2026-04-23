// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/ram/lox_port_ram.h"

#include <stdio.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_ram_storage;
static uint32_t g_mock_time = 0u;

static lox_timestamp_t mock_now(void) {
    return (lox_timestamp_t)g_mock_time;
}

static void make_key(char *buf, size_t buf_len, uint32_t index) {
    (void)snprintf(buf, buf_len, "r%03u", (unsigned)index);
}

static uint32_t kv_capacity(void) {
    return LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS;
}

static void setup_basic(void) {
    lox_cfg_t cfg;

    ASSERT_EQ(lox_port_ram_init(&g_ram_storage, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_ram_storage;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void setup_ttl(void) {
    lox_cfg_t cfg;

    g_mock_time = 1000u;
    ASSERT_EQ(lox_port_ram_init(&g_ram_storage, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_ram_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    lox_deinit(&g_db);
    lox_port_ram_deinit(&g_ram_storage);
}

MDB_TEST(kv_reject_policy_returns_full_when_store_is_full) {
    uint8_t value = 1u;
    char key[16];
    uint32_t i;

    for (i = 0; i < kv_capacity(); ++i) {
        make_key(key, sizeof(key), i);
        ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
    }

    ASSERT_EQ(lox_kv_put(&g_db, "extra", &value, sizeof(value)), LOX_ERR_FULL);
}

MDB_TEST(kv_reject_policy_allows_overwrite_when_full) {
    uint8_t value = 1u;
    uint8_t newer = 9u;
    uint8_t out = 0u;
    char key[16];
    uint32_t i;

    for (i = 0; i < kv_capacity(); ++i) {
        make_key(key, sizeof(key), i);
        ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
    }

    make_key(key, sizeof(key), 0u);
    ASSERT_EQ(lox_kv_put(&g_db, key, &newer, sizeof(newer)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, key, &out, sizeof(out), NULL), LOX_OK);
    ASSERT_EQ(out, 9u);
}

MDB_TEST(kv_reject_policy_insert_succeeds_after_delete) {
    uint8_t value = 1u;
    char key[16];
    uint32_t i;

    for (i = 0; i < kv_capacity(); ++i) {
        make_key(key, sizeof(key), i);
        ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
    }

    make_key(key, sizeof(key), 0u);
    ASSERT_EQ(lox_kv_del(&g_db, key), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "fresh", &value, sizeof(value)), LOX_OK);
}

MDB_TEST(kv_reject_policy_clear_allows_refill) {
    uint8_t value = 1u;
    char key[16];
    uint32_t i;

    for (i = 0; i < kv_capacity(); ++i) {
        make_key(key, sizeof(key), i);
        ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
    }

    ASSERT_EQ(lox_kv_clear(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "fresh", &value, sizeof(value)), LOX_OK);
}

MDB_TEST(kv_reject_policy_expired_key_frees_slot_for_insert) {
    uint8_t value = 3u;
    char key[16];
    uint32_t i;

    for (i = 0; i < kv_capacity() - 1u; ++i) {
        make_key(key, sizeof(key), i);
        ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
    }

    ASSERT_EQ(lox_kv_set(&g_db, "ttl", &value, sizeof(value), 1u), LOX_OK);
    g_mock_time = 1001u;
    ASSERT_EQ(lox_kv_exists(&g_db, "ttl"), LOX_ERR_EXPIRED);
    ASSERT_EQ(lox_kv_purge_expired(&g_db), LOX_OK);
    ASSERT_EQ(lox_kv_put(&g_db, "fresh", &value, sizeof(value)), LOX_OK);
}

int main(void) {
    MDB_RUN_TEST(setup_basic, teardown_db, kv_reject_policy_returns_full_when_store_is_full);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_reject_policy_allows_overwrite_when_full);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_reject_policy_insert_succeeds_after_delete);
    MDB_RUN_TEST(setup_basic, teardown_db, kv_reject_policy_clear_allows_refill);
    MDB_RUN_TEST(setup_ttl, teardown_db, kv_reject_policy_expired_key_frees_slot_for_insert);
    return MDB_RESULT();
}
