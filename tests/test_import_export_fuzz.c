// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_import_export.h"
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
    ASSERT_EQ(lox_ts_register(&g_db, "s1", LOX_TS_U32, 0u), LOX_OK);
}

static void teardown_db(void) {
    lox_deinit(&g_db);
    lox_port_ram_deinit(&g_st);
}

static uint32_t fuzz_next(uint32_t *s) {
    *s = (*s * 1664525u) + 1013904223u;
    return *s;
}

static void fuzz_mutate(char *buf, size_t len, uint32_t *seed) {
    size_t k;
    uint32_t flips = (fuzz_next(seed) % 4u) + 1u;
    for (k = 0u; k < flips; ++k) {
        size_t idx = (size_t)(fuzz_next(seed) % (uint32_t)len);
        buf[idx] = (char)(32 + (fuzz_next(seed) % 95u));
    }
}

MDB_TEST(fuzz_import_parsers_do_not_crash) {
    static const char *valid_kv = "{\"format\":\"loxdb.kv.v1\",\"items\":[{\"key\":\"k1\",\"ttl\":0,\"value_hex\":\"0102\"}]}";
    static const char *valid_ts = "{\"format\":\"loxdb.ts.v1\",\"items\":[{\"stream\":\"s1\",\"type\":\"u32\",\"ts\":1,\"value_hex\":\"01000000\"}]}";
    static const char *valid_rel = "{\"format\":\"loxdb.rel.v1\",\"items\":[]}";
    lox_ie_options_t opts = lox_ie_default_options();
    lox_ie_ts_stream_desc_t ts_desc;
    uint32_t i;

    opts.skip_invalid_items = 1u;
    ts_desc.name = "s1";
    ts_desc.type = LOX_TS_U32;
    ts_desc.raw_size = 0u;

    for (i = 0u; i < 400u; ++i) {
        char kv[256];
        char ts[256];
        char rel[256];
        uint32_t imported = 0u, skipped = 0u;
        uint32_t seed = 0xA11CE55u + i;
        lox_err_t rc;

        memset(kv, 0, sizeof(kv));
        memset(ts, 0, sizeof(ts));
        memset(rel, 0, sizeof(rel));
        strncpy(kv, valid_kv, sizeof(kv) - 1u);
        strncpy(ts, valid_ts, sizeof(ts) - 1u);
        strncpy(rel, valid_rel, sizeof(rel) - 1u);
        fuzz_mutate(kv, strlen(kv), &seed);
        fuzz_mutate(ts, strlen(ts), &seed);
        fuzz_mutate(rel, strlen(rel), &seed);

        rc = lox_ie_import_kv_json(&g_db, kv, &opts, &imported, &skipped);
        ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_INVALID || rc == LOX_ERR_OVERFLOW), 1);
        rc = lox_ie_import_ts_json(&g_db, ts, &ts_desc, 1u, &opts, &imported, &skipped);
        ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_INVALID || rc == LOX_ERR_OVERFLOW), 1);
        rc = lox_ie_import_rel_json(&g_db, rel, NULL, 0u, &opts, &imported, &skipped);
        ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_INVALID || rc == LOX_ERR_OVERFLOW), 1);
    }
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, fuzz_import_parsers_do_not_crash);
    return MDB_RESULT();
}
