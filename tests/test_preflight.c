// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"

#include <string.h>

static lox_err_t st_read(void *ctx, uint32_t off, void *buf, size_t len) {
    (void)ctx; (void)off; (void)buf; (void)len;
    return LOX_OK;
}

static lox_err_t st_write(void *ctx, uint32_t off, const void *buf, size_t len) {
    (void)ctx; (void)off; (void)buf; (void)len;
    return LOX_OK;
}

static lox_err_t st_erase(void *ctx, uint32_t off) {
    (void)ctx; (void)off;
    return LOX_OK;
}

static lox_err_t st_sync(void *ctx) {
    (void)ctx;
    return LOX_OK;
}

static void noop(void) {
}

MDB_TEST(test_preflight_null_args_invalid) {
    lox_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    ASSERT_EQ(lox_preflight(NULL, NULL), LOX_ERR_INVALID);
    ASSERT_EQ(lox_preflight(&cfg, NULL), LOX_ERR_INVALID);
}

MDB_TEST(test_preflight_ram_only_ok) {
    lox_cfg_t cfg;
    lox_preflight_report_t rep;
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 64u;
    ASSERT_EQ(lox_preflight(&cfg, &rep), LOX_OK);
    ASSERT_EQ(rep.status, LOX_OK);
    ASSERT_EQ(rep.ram_kb, 64u);
    ASSERT_GT(rep.kv_arena_bytes, 0u);
    ASSERT_GT(rep.ts_arena_bytes, 0u);
    ASSERT_GT(rep.rel_arena_bytes, 0u);
}

MDB_TEST(test_preflight_pct_invalid) {
    lox_cfg_t cfg;
    lox_preflight_report_t rep;
    memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 64u;
    cfg.kv_pct = 50u;
    cfg.ts_pct = 50u;
    cfg.rel_pct = 1u;
    ASSERT_EQ(lox_preflight(&cfg, &rep), LOX_ERR_INVALID);
}

MDB_TEST(test_preflight_storage_contract_invalid) {
    lox_cfg_t cfg;
    lox_storage_t st;
    lox_preflight_report_t rep;
    memset(&cfg, 0, sizeof(cfg));
    memset(&st, 0, sizeof(st));
    st.read = st_read;
    st.write = st_write;
    st.erase = st_erase;
    st.sync = st_sync;
    st.capacity = 1024u * 1024u;
    st.erase_size = 4096u;
    st.write_size = 2u;
    cfg.ram_kb = 64u;
    cfg.storage = &st;
    ASSERT_EQ(lox_preflight(&cfg, &rep), LOX_ERR_INVALID);
}

MDB_TEST(test_preflight_storage_too_small) {
    lox_cfg_t cfg;
    lox_storage_t st;
    lox_preflight_report_t rep;
    memset(&cfg, 0, sizeof(cfg));
    memset(&st, 0, sizeof(st));
    st.read = st_read;
    st.write = st_write;
    st.erase = st_erase;
    st.sync = st_sync;
    st.capacity = 64u * 1024u;
    st.erase_size = 4096u;
    st.write_size = 1u;
    cfg.ram_kb = 64u;
    cfg.storage = &st;
    ASSERT_EQ(lox_preflight(&cfg, &rep), LOX_ERR_STORAGE);
    ASSERT_EQ(rep.status, LOX_ERR_STORAGE);
    ASSERT_GT(rep.storage_required_bytes, st.capacity);
}

MDB_TEST(test_preflight_storage_ok) {
    lox_cfg_t cfg;
    lox_storage_t st;
    lox_preflight_report_t rep;
    memset(&cfg, 0, sizeof(cfg));
    memset(&st, 0, sizeof(st));
    st.read = st_read;
    st.write = st_write;
    st.erase = st_erase;
    st.sync = st_sync;
    st.capacity = 16u * 1024u * 1024u;
    st.erase_size = 4096u;
    st.write_size = 1u;
    cfg.ram_kb = 64u;
    cfg.storage = &st;
    ASSERT_EQ(lox_preflight(&cfg, &rep), LOX_OK);
    ASSERT_EQ(rep.status, LOX_OK);
    ASSERT_GT(rep.storage_required_bytes, 0u);
    ASSERT_LE(rep.storage_required_bytes, st.capacity);
    ASSERT_GT(rep.wal_size, 0u);
}

int main(void) {
    MDB_RUN_TEST(noop, noop, test_preflight_null_args_invalid);
    MDB_RUN_TEST(noop, noop, test_preflight_ram_only_ok);
    MDB_RUN_TEST(noop, noop, test_preflight_pct_invalid);
    MDB_RUN_TEST(noop, noop, test_preflight_storage_contract_invalid);
    MDB_RUN_TEST(noop, noop, test_preflight_storage_too_small);
    MDB_RUN_TEST(noop, noop, test_preflight_storage_ok);
    return MDB_RESULT();
}
