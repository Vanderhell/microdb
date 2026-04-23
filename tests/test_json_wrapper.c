// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_json_wrapper.h"
#include "../port/ram/lox_port_ram.h"

#include <string.h>

static lox_t g_db;
static lox_storage_t g_st;

static void setup_db(void) {
    lox_cfg_t cfg;
    ASSERT_EQ(lox_port_ram_init(&g_st, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_st;
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    lox_deinit(&g_db);
    lox_port_ram_deinit(&g_st);
}

MDB_TEST(json_u32_roundtrip) {
    uint32_t out = 0u;
    ASSERT_EQ(lox_json_kv_set_u32(&g_db, "n", 123456u, 0u), LOX_OK);
    ASSERT_EQ(lox_json_kv_get_u32(&g_db, "n", &out), LOX_OK);
    ASSERT_EQ(out, 123456u);
}

MDB_TEST(json_bool_roundtrip) {
    bool out = false;
    ASSERT_EQ(lox_json_kv_set_bool(&g_db, "b", true, 0u), LOX_OK);
    ASSERT_EQ(lox_json_kv_get_bool(&g_db, "b", &out), LOX_OK);
    ASSERT_EQ(out, 1);
}

MDB_TEST(json_cstr_roundtrip) {
    char out[64];
    size_t out_len = 0u;
    ASSERT_EQ(lox_json_kv_set_cstr(&g_db, "s", "hello-json", 0u), LOX_OK);
    ASSERT_EQ(lox_json_kv_get_cstr(&g_db, "s", out, sizeof(out), &out_len), LOX_OK);
    ASSERT_EQ(out_len, 10u);
    ASSERT_EQ(strcmp(out, "hello-json"), 0);
}

MDB_TEST(json_record_encode_decode) {
    const uint8_t in_val[] = {0x01u, 0xA2u, 0xFFu};
    char json[256];
    size_t used = 0u;
    char key[64];
    uint8_t out_val[16];
    size_t out_val_len = 0u;
    uint32_t ttl = 0u;

    ASSERT_EQ(lox_json_encode_kv_record("k\\\"x", in_val, sizeof(in_val), 77u, json, sizeof(json), &used), LOX_OK);
    ASSERT_GT(used, 0u);
    ASSERT_EQ(lox_json_decode_kv_record(json, key, sizeof(key), out_val, sizeof(out_val), &out_val_len, &ttl), LOX_OK);
    ASSERT_EQ(strcmp(key, "k\\\"x"), 0);
    ASSERT_EQ(out_val_len, sizeof(in_val));
    ASSERT_MEM_EQ(out_val, in_val, sizeof(in_val));
    ASSERT_EQ(ttl, 77u);
}

MDB_TEST(json_record_decode_invalid_rejected) {
    char key[32];
    uint8_t value[8];
    size_t value_len = 0u;
    uint32_t ttl = 0u;
    const char *bad = "{\"key\":\"a\",\"ttl\":1,\"value_hex\":\"0GG0\"}";
    ASSERT_EQ(lox_json_decode_kv_record(bad, key, sizeof(key), value, sizeof(value), &value_len, &ttl), LOX_ERR_INVALID);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, json_u32_roundtrip);
    MDB_RUN_TEST(setup_db, teardown_db, json_bool_roundtrip);
    MDB_RUN_TEST(setup_db, teardown_db, json_cstr_roundtrip);
    MDB_RUN_TEST(setup_db, teardown_db, json_record_encode_decode);
    MDB_RUN_TEST(setup_db, teardown_db, json_record_decode_invalid_rejected);
    return MDB_RESULT();
}
