// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "../src/microdb_internal.h"

#include <stdlib.h>
#include <string.h>

enum {
    FI_CAPACITY = 131072u,
    FI_ERASE = 256u,
    FI_TRACE_MAX = 2048u
};

typedef struct {
    uint8_t bytes[FI_CAPACITY];
    int32_t fail_at_step;
    int32_t step;
    uint16_t trace_count;
    char trace_op[FI_TRACE_MAX];
    uint32_t trace_off[FI_TRACE_MAX];
} fi_store_t;

static fi_store_t g_store;
static microdb_storage_t g_storage;
static microdb_t g_db;
static microdb_cfg_t g_cfg;
static uint32_t g_now = 1000u;

static microdb_timestamp_t mock_now(void) {
    return g_now++;
}

static void fi_reset_storage(void) {
    memset(&g_store, 0, sizeof(g_store));
    memset(g_store.bytes, 0xFF, sizeof(g_store.bytes));
    g_store.fail_at_step = -1;
    g_store.step = 0;
}

static void fi_record(char op, uint32_t off) {
    if (g_store.trace_count < FI_TRACE_MAX) {
        g_store.trace_op[g_store.trace_count] = op;
        g_store.trace_off[g_store.trace_count] = off;
        g_store.trace_count++;
    }
}

static microdb_err_t fi_maybe_fail(void) {
    g_store.step++;
    if (g_store.fail_at_step > 0 && g_store.step == g_store.fail_at_step) {
        return MICRODB_ERR_STORAGE;
    }
    return MICRODB_OK;
}

static microdb_err_t fi_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    (void)ctx;
    if ((size_t)offset + len > FI_CAPACITY) {
        return MICRODB_ERR_STORAGE;
    }
    memcpy(buf, &g_store.bytes[offset], len);
    return MICRODB_OK;
}

static microdb_err_t fi_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    (void)ctx;
    if ((size_t)offset + len > FI_CAPACITY) {
        return MICRODB_ERR_STORAGE;
    }
    fi_record('W', offset);
    if (fi_maybe_fail() != MICRODB_OK) {
        return MICRODB_ERR_STORAGE;
    }
    memcpy(&g_store.bytes[offset], buf, len);
    return MICRODB_OK;
}

static microdb_err_t fi_erase(void *ctx, uint32_t offset) {
    uint32_t start;
    (void)ctx;
    if (offset >= FI_CAPACITY) {
        return MICRODB_ERR_STORAGE;
    }
    fi_record('E', offset);
    if (fi_maybe_fail() != MICRODB_OK) {
        return MICRODB_ERR_STORAGE;
    }
    start = (offset / FI_ERASE) * FI_ERASE;
    if (start + FI_ERASE > FI_CAPACITY) {
        return MICRODB_ERR_STORAGE;
    }
    memset(&g_store.bytes[start], 0xFF, FI_ERASE);
    return MICRODB_OK;
}

static microdb_err_t fi_sync(void *ctx) {
    (void)ctx;
    fi_record('S', 0xFFFFFFFFu);
    if (fi_maybe_fail() != MICRODB_OK) {
        return MICRODB_ERR_STORAGE;
    }
    return MICRODB_OK;
}

static void setup_storage(void) {
    memset(&g_storage, 0, sizeof(g_storage));
    g_storage.read = fi_read;
    g_storage.write = fi_write;
    g_storage.erase = fi_erase;
    g_storage.sync = fi_sync;
    g_storage.capacity = FI_CAPACITY;
    g_storage.erase_size = FI_ERASE;
    g_storage.write_size = 1u;
    g_storage.ctx = &g_store;
}

static void setup_db(void) {
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_cfg, 0, sizeof(g_cfg));
    g_cfg.storage = &g_storage;
    g_cfg.ram_kb = 32u;
    g_cfg.now = mock_now;
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_OK);
}

static void crash_reopen(void) {
    if (microdb_core_const(&g_db)->magic == MICRODB_MAGIC) {
        free(microdb_core(&g_db)->heap);
    }
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_OK);
}

static void clean_reopen(void) {
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_OK);
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_OK);
}

static void teardown_db(void) {
    if (microdb_core_const(&g_db)->magic == MICRODB_MAGIC) {
        (void)microdb_deinit(&g_db);
    }
}

static void init_fixture(void) {
    fi_reset_storage();
    setup_storage();
    setup_db();
}

static void fini_fixture(void) {
    teardown_db();
}

static void create_rel_users(microdb_t *db, microdb_table_t **out) {
    microdb_schema_t schema;
    ASSERT_EQ(microdb_schema_init(&schema, "users", 128u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(db, &schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_get(db, "users", out), MICRODB_OK);
}

static void seed_mixed_state(void) {
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 7u;
    uint8_t age = 42u;
    uint8_t kv = 9u;
    uint32_t tsv = 77u;

    ASSERT_EQ(microdb_kv_set(&g_db, "anchor", &kv, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_register(&g_db, "stream", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "stream", 11u, &tsv), MICRODB_OK);
    create_rel_users(&g_db, &table);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
}

static void verify_anchor_state(void) {
    uint8_t kv = 0u;
    microdb_ts_sample_t sample;
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 7u;

    ASSERT_EQ(microdb_kv_get(&g_db, "anchor", &kv, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(kv, 9u);
    ASSERT_EQ(microdb_ts_last(&g_db, "stream", &sample), MICRODB_OK);
    ASSERT_EQ(sample.v.u32, 77u);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find_by(&g_db, table, "id", &id, row), MICRODB_OK);
}

MDB_TEST(power_cut_after_each_compact_step_keeps_committed_state) {
    uint8_t baseline[FI_CAPACITY];
    uint32_t fp;
    int saw_success = 0;

    seed_mixed_state();
    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
    memcpy(baseline, g_store.bytes, sizeof(baseline));
    clean_reopen();

    for (fp = 1u; fp < 512u; ++fp) {
        memcpy(g_store.bytes, baseline, sizeof(baseline));
        ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_OK);
        g_store.step = 0;
        g_store.trace_count = 0u;
        g_store.fail_at_step = (int32_t)fp;
        if (microdb_compact(&g_db) == MICRODB_OK) {
            saw_success = 1;
        }
        g_store.fail_at_step = -1;
        crash_reopen();
        verify_anchor_state();
        teardown_db();
        memset(&g_db, 0, sizeof(g_db));
        if (saw_success) {
            break;
        }
    }
    ASSERT_EQ(saw_success, 1);
}

MDB_TEST(power_cut_after_superblock_write_before_sync_keeps_old_state) {
    uint8_t baseline[FI_CAPACITY];
    uint32_t i;
    int32_t target_fail = -1;

    seed_mixed_state();
    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
    memcpy(baseline, g_store.bytes, sizeof(baseline));

    g_store.fail_at_step = -1;
    g_store.step = 0;
    g_store.trace_count = 0u;
    ASSERT_EQ(microdb_compact(&g_db), MICRODB_OK);
    for (i = 1u; i < g_store.trace_count; ++i) {
        if (g_store.trace_op[i - 1u] == 'W') {
            const microdb_core_t *core = microdb_core_const(&g_db);
            if (g_store.trace_off[i - 1u] == core->layout.super_a_offset ||
                g_store.trace_off[i - 1u] == core->layout.super_b_offset) {
                if (g_store.trace_op[i] == 'S') {
                    target_fail = (int32_t)(i + 1u);
                    break;
                }
            }
        }
    }
    ASSERT_GE(target_fail, 1);

    memcpy(g_store.bytes, baseline, sizeof(baseline));
    teardown_db();
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_OK);
    g_store.fail_at_step = target_fail;
    (void)microdb_compact(&g_db);
    g_store.fail_at_step = -1;
    crash_reopen();
    verify_anchor_state();
}

MDB_TEST(corrupt_superblock_a_only_boots_from_other_copy) {
    microdb_core_t *core;
    seed_mixed_state();
    core = microdb_core(&g_db);
    while ((core->layout.active_generation & 1u) != 0u) {
        ASSERT_EQ(microdb_compact(&g_db), MICRODB_OK);
        core = microdb_core(&g_db);
    }
    core = microdb_core(&g_db);
    g_store.bytes[core->layout.super_a_offset + 20u] ^= 0x5Au;
    crash_reopen();
    verify_anchor_state();
}

MDB_TEST(corrupt_superblock_b_only_boots_from_other_copy) {
    microdb_core_t *core;
    seed_mixed_state();
    ASSERT_EQ(microdb_compact(&g_db), MICRODB_OK);
    core = microdb_core(&g_db);
    g_store.bytes[core->layout.super_b_offset + 20u] ^= 0xA5u;
    crash_reopen();
    verify_anchor_state();
}

MDB_TEST(corrupt_active_page_header_crc_fails_boot) {
    microdb_core_t *core;
    seed_mixed_state();
    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
    core = microdb_core(&g_db);
    g_store.bytes[core->layout.active_bank == 0u ? core->layout.bank_a_offset + 24u : core->layout.bank_b_offset + 24u] ^= 0x11u;
    free(microdb_core(&g_db)->heap);
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_ERR_CORRUPT);
}

MDB_TEST(corrupt_active_page_payload_crc_fails_boot) {
    microdb_core_t *core;
    uint32_t kv_page_base;
    seed_mixed_state();
    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
    core = microdb_core(&g_db);
    kv_page_base = (core->layout.active_bank == 0u) ? core->layout.bank_a_offset : core->layout.bank_b_offset;
    g_store.bytes[kv_page_base + 32u] ^= 0x22u;
    free(microdb_core(&g_db)->heap);
    memset(&g_db, 0, sizeof(g_db));
    ASSERT_EQ(microdb_init(&g_db, &g_cfg), MICRODB_ERR_CORRUPT);
}

MDB_TEST(valid_old_bank_broken_new_bank_recovers_old) {
    uint8_t baseline[FI_CAPACITY];
    seed_mixed_state();
    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
    memcpy(baseline, g_store.bytes, sizeof(baseline));
    clean_reopen();
    g_store.fail_at_step = 2;
    (void)microdb_compact(&g_db);
    g_store.fail_at_step = -1;
    crash_reopen();
    verify_anchor_state();
    (void)baseline;
}

MDB_TEST(valid_new_bank_broken_old_bank_still_boots) {
    microdb_core_t *core;
    uint32_t old_bank;
    uint32_t old_kv_offset;

    seed_mixed_state();
    ASSERT_EQ(microdb_compact(&g_db), MICRODB_OK);
    core = microdb_core(&g_db);
    old_bank = (core->layout.active_bank == 0u) ? 1u : 0u;
    old_kv_offset = (old_bank == 0u) ? core->layout.bank_a_offset : core->layout.bank_b_offset;
    g_store.bytes[old_kv_offset + 0u] ^= 0xFFu;
    crash_reopen();
    verify_anchor_state();
}

MDB_TEST(txn_batch_without_commit_marker_is_discarded_on_replay) {
    uint8_t v = 44u;
    uint8_t out = 0u;

    ASSERT_EQ(microdb_persist_kv_set_txn(&g_db, "tkey", &v, 1u, 0u), MICRODB_OK);
    crash_reopen();
    ASSERT_EQ(microdb_kv_get(&g_db, "tkey", &out, 1u, NULL), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(txn_batch_with_commit_marker_replays_after_crash) {
    uint8_t v = 55u;
    uint8_t out = 0u;

    ASSERT_EQ(microdb_persist_kv_set_txn(&g_db, "tkey2", &v, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_persist_txn_commit(&g_db), MICRODB_OK);
    crash_reopen();
    ASSERT_EQ(microdb_kv_get(&g_db, "tkey2", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(out, 55u);
}

MDB_TEST(repeated_crash_reboot_cycles_mixed_workload) {
    microdb_table_t *table = NULL;
    uint32_t i;

    ASSERT_EQ(microdb_ts_register(&g_db, "mix", MICRODB_TS_U32, 0u), MICRODB_OK);
    create_rel_users(&g_db, &table);
    for (i = 0u; i < 24u; ++i) {
        uint8_t kv = (uint8_t)(i + 1u);
        uint32_t tsv = i;
        uint8_t row[64] = { 0 };
        uint32_t id = i + 1u;
        uint8_t age = (uint8_t)(20u + (i % 40u));
        char key[16];
        memset(key, 0, sizeof(key));
        key[0] = 'k';
        key[1] = (char)('0' + (char)(i % 10u));
        ASSERT_EQ(microdb_kv_set(&g_db, key, &kv, 1u, 0u), MICRODB_OK);
        ASSERT_EQ(microdb_ts_insert(&g_db, "mix", i + 1u, &tsv), MICRODB_OK);
        ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
        ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
        ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
        crash_reopen();
        ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    }
    {
        microdb_stats_t st;
        ASSERT_EQ(microdb_inspect(&g_db, &st), MICRODB_OK);
        ASSERT_GT(st.ts_samples_total, 0u);
        ASSERT_GT(st.rel_rows_total, 0u);
    }
}

int main(void) {
    MDB_RUN_TEST(init_fixture, fini_fixture, power_cut_after_each_compact_step_keeps_committed_state);
    MDB_RUN_TEST(init_fixture, fini_fixture, power_cut_after_superblock_write_before_sync_keeps_old_state);
    MDB_RUN_TEST(init_fixture, fini_fixture, corrupt_superblock_a_only_boots_from_other_copy);
    MDB_RUN_TEST(init_fixture, fini_fixture, corrupt_superblock_b_only_boots_from_other_copy);
    MDB_RUN_TEST(init_fixture, fini_fixture, corrupt_active_page_header_crc_fails_boot);
    MDB_RUN_TEST(init_fixture, fini_fixture, corrupt_active_page_payload_crc_fails_boot);
    MDB_RUN_TEST(init_fixture, fini_fixture, valid_old_bank_broken_new_bank_recovers_old);
    MDB_RUN_TEST(init_fixture, fini_fixture, valid_new_bank_broken_old_bank_still_boots);
    MDB_RUN_TEST(init_fixture, fini_fixture, txn_batch_without_commit_marker_is_discarded_on_replay);
    MDB_RUN_TEST(init_fixture, fini_fixture, txn_batch_with_commit_marker_replays_after_crash);
    MDB_RUN_TEST(init_fixture, fini_fixture, repeated_crash_reboot_cycles_mixed_workload);
    return MDB_RESULT();
}
