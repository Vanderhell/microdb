// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static char g_path[128];
static unsigned g_path_seq = 0u;

#define KV_MODEL_KEYS 24u
#define REL_MODEL_IDS 48u

typedef struct {
    bool present;
    uint32_t value;
} kv_model_entry_t;

typedef struct {
    kv_model_entry_t kv[KV_MODEL_KEYS];
    bool ts_has_sample;
    lox_timestamp_t ts_last_ts;
    uint32_t ts_last_value;
    bool rel_present[REL_MODEL_IDS];
    uint32_t rel_count;
} model_t;

static uint32_t g_rng = 0xC0FFEE11u;

static uint32_t rng_next(void) {
    g_rng = (g_rng * 1664525u) + 1013904223u;
    return g_rng;
}

static void make_kv_key(uint32_t idx, char *buf, size_t buf_len) {
    (void)snprintf(buf, buf_len, "k%02u", (unsigned)idx);
}

static void open_db(void) {
    lox_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 131072u), LOX_OK);

    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void setup_db(void) {
    g_path_seq++;
    (void)snprintf(g_path, sizeof(g_path), "resilience_test_%u.bin", g_path_seq);
    lox_port_posix_remove(g_path);
    g_rng = 0xC0FFEE11u;
    open_db();
}

static void teardown_db(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        lox_err_t rc = lox_deinit(&g_db);
        if (rc != LOX_OK && lox_core_const(&g_db)->magic == LOX_MAGIC) {
            free(lox_core(&g_db)->heap);
            memset(&g_db, 0, sizeof(g_db));
        }
    }
    lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(g_path);
}

static void reopen_clean(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        lox_err_t rc = lox_deinit(&g_db);
        ASSERT_EQ(rc, LOX_OK);
        if (rc != LOX_OK && lox_core_const(&g_db)->magic == LOX_MAGIC) {
            free(lox_core(&g_db)->heap);
            memset(&g_db, 0, sizeof(g_db));
        }
    }
    lox_port_posix_deinit(&g_storage);
    open_db();
}

static void reopen_power_loss(void) {
    lox_port_posix_simulate_power_loss(&g_storage);
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
        memset(&g_db, 0, sizeof(g_db));
    }
    lox_port_posix_deinit(&g_storage);
    open_db();
}

static void ensure_rel_table(lox_table_t **table_out) {
    lox_schema_t schema;
    lox_err_t rc;

    rc = lox_table_get(&g_db, "users", table_out);
    if (rc == LOX_OK) {
        return;
    }
    ASSERT_EQ(rc, LOX_ERR_NOT_FOUND);

    ASSERT_EQ(lox_schema_init(&schema, "users", REL_MODEL_IDS + 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "age", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    rc = lox_table_create(&g_db, &schema);
    ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_EXISTS), 1);
    ASSERT_EQ(lox_table_get(&g_db, "users", table_out), LOX_OK);
}

static void verify_state(const model_t *m) {
    uint32_t i;
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t rel_count = 0u;

    for (i = 0u; i < KV_MODEL_KEYS; ++i) {
        char key[16];
        uint32_t got = 0u;
        size_t out_len = 0u;
        make_kv_key(i, key, sizeof(key));
        if (m->kv[i].present) {
            ASSERT_EQ(lox_kv_exists(&g_db, key), LOX_OK);
            ASSERT_EQ(lox_kv_get(&g_db, key, &got, sizeof(got), &out_len), LOX_OK);
            ASSERT_EQ(out_len, (long long)sizeof(got));
            ASSERT_EQ(got, m->kv[i].value);
        } else {
            ASSERT_EQ(lox_kv_exists(&g_db, key), LOX_ERR_NOT_FOUND);
            ASSERT_EQ(lox_kv_get(&g_db, key, &got, sizeof(got), &out_len), LOX_ERR_NOT_FOUND);
        }
    }

    if (m->ts_has_sample) {
        lox_ts_sample_t sample;
        ASSERT_EQ(lox_ts_last(&g_db, "main_ts", &sample), LOX_OK);
        ASSERT_EQ(sample.ts, m->ts_last_ts);
        ASSERT_EQ(sample.v.u32, m->ts_last_value);
    } else {
        lox_ts_sample_t sample;
        ASSERT_EQ(lox_ts_last(&g_db, "main_ts", &sample), LOX_ERR_NOT_FOUND);
    }

    ensure_rel_table(&table);
    ASSERT_EQ(lox_rel_count(table, &rel_count), LOX_OK);
    ASSERT_EQ(rel_count, m->rel_count);

    for (i = 0u; i < REL_MODEL_IDS; ++i) {
        if (m->rel_present[i]) {
            ASSERT_EQ(lox_rel_find_by(&g_db, table, "id", &i, row), LOX_OK);
        }
    }
}

static void do_random_operation(model_t *m, uint32_t op_index) {
    uint32_t op = rng_next() % 6u;

    if (op == 0u || op == 1u || op == 2u) {
        uint32_t key_idx = rng_next() % KV_MODEL_KEYS;
        char key[16];
        make_kv_key(key_idx, key, sizeof(key));
        if (op == 0u) {
            uint32_t value = op_index * 3u + 17u;
            ASSERT_EQ(lox_kv_put(&g_db, key, &value, sizeof(value)), LOX_OK);
            m->kv[key_idx].present = true;
            m->kv[key_idx].value = value;
        } else if (op == 1u) {
            lox_err_t rc = lox_kv_del(&g_db, key);
            if (m->kv[key_idx].present) {
                ASSERT_EQ(rc, LOX_OK);
                m->kv[key_idx].present = false;
                m->kv[key_idx].value = 0u;
            } else {
                ASSERT_EQ(rc, LOX_ERR_NOT_FOUND);
            }
        } else {
            uint32_t got = 0u;
            size_t out_len = 0u;
            lox_err_t rc = lox_kv_get(&g_db, key, &got, sizeof(got), &out_len);
            if (m->kv[key_idx].present) {
                ASSERT_EQ(rc, LOX_OK);
                ASSERT_EQ(out_len, (long long)sizeof(got));
                ASSERT_EQ(got, m->kv[key_idx].value);
            } else {
                ASSERT_EQ(rc, LOX_ERR_NOT_FOUND);
            }
        }
        return;
    }

    if (op == 3u) {
        uint32_t ts_value = op_index ^ 0xA55Au;
        lox_timestamp_t ts = (lox_timestamp_t)(op_index + 1u);
        ASSERT_EQ(lox_ts_insert(&g_db, "main_ts", ts, &ts_value), LOX_OK);
        m->ts_has_sample = true;
        m->ts_last_ts = ts;
        m->ts_last_value = ts_value;
        return;
    }

    {
        lox_table_t *table = NULL;
        uint32_t id = rng_next() % REL_MODEL_IDS;
        uint8_t row[64] = { 0 };
        uint8_t age = (uint8_t)(20u + (id % 50u));
        uint32_t deleted = 0u;
        ensure_rel_table(&table);

        if (op == 4u) {
            if (!m->rel_present[id]) {
                ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
                ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
                ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
                m->rel_present[id] = true;
                m->rel_count++;
            } else {
                ASSERT_EQ(lox_rel_find_by(&g_db, table, "id", &id, row), LOX_OK);
            }
        } else {
            ASSERT_EQ(lox_rel_delete(&g_db, table, &id, &deleted), LOX_OK);
            if (m->rel_present[id]) {
                ASSERT_EQ(deleted, 1u);
                m->rel_present[id] = false;
                m->rel_count--;
            } else {
                ASSERT_EQ(deleted, 0u);
            }
        }
    }
}

static void run_mixed_workload(uint32_t ops, bool with_reopen, bool with_compact, bool with_power_loss) {
    model_t m;
    uint32_t i;
    lox_table_t *table = NULL;
    lox_stats_t st;

    memset(&m, 0, sizeof(m));
    ensure_rel_table(&table);
    ASSERT_EQ(lox_ts_register(&g_db, "main_ts", LOX_TS_U32, 0u), LOX_OK);

    for (i = 0u; i < ops; ++i) {
        do_random_operation(&m, i);

        if (((i + 1u) % 40u) == 0u) {
            ASSERT_EQ(lox_flush(&g_db), LOX_OK);
        }
        if (with_compact && ((i + 1u) % 120u) == 0u) {
            ASSERT_EQ(lox_compact(&g_db), LOX_OK);
            verify_state(&m);
        }
        if (with_reopen && ((i + 1u) % 100u) == 0u) {
            reopen_clean();
            verify_state(&m);
        }
        if (with_power_loss && ((i + 1u) % 100u) == 0u) {
            reopen_power_loss();
            verify_state(&m);
        }
    }

    verify_state(&m);
    ASSERT_EQ(lox_inspect(&g_db, &st), LOX_OK);
    ASSERT_EQ(st.rel_rows_total, m.rel_count);
}

MDB_TEST(contract_calls_before_init_are_invalid) {
    lox_t db;
    lox_stats_t st;
    uint8_t b = 1u;
    lox_ts_sample_t s;
    uint32_t deleted = 0u;
    lox_table_t *table = NULL;
    lox_schema_t schema;
    size_t out_len = 0u;

    memset(&db, 0, sizeof(db));
    memset(&schema, 0, sizeof(schema));
    ASSERT_EQ(lox_flush(&db), LOX_ERR_INVALID);
    ASSERT_EQ(lox_compact(&db), LOX_ERR_INVALID);
    ASSERT_EQ(lox_inspect(&db, &st), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_put(&db, "a", &b, 1u), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_get(&db, "a", &b, 1u, &out_len), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_del(&db, "a"), LOX_ERR_INVALID);
    ASSERT_EQ(lox_ts_register(&db, "s", LOX_TS_U32, 0u), LOX_ERR_INVALID);
    ASSERT_EQ(lox_ts_last(&db, "s", &s), LOX_ERR_INVALID);
    ASSERT_EQ(lox_table_get(&db, "users", &table), LOX_ERR_INVALID);
    ASSERT_EQ(lox_rel_insert(&db, table, &b), LOX_ERR_INVALID);
    ASSERT_EQ(lox_rel_delete(&db, table, &b, &deleted), LOX_ERR_INVALID);
    ASSERT_EQ(lox_table_create(&db, &schema), LOX_ERR_INVALID);
    ASSERT_EQ(lox_txn_begin(&db), LOX_ERR_INVALID);
}

MDB_TEST(contract_calls_after_deinit_are_invalid) {
    lox_t db;
    lox_cfg_t cfg;
    uint8_t b = 1u;
    lox_stats_t st;
    size_t out_len = 0u;

    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(&db, &cfg), LOX_OK);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
    ASSERT_EQ(lox_flush(&db), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_put(&db, "a", &b, 1u), LOX_ERR_INVALID);
    ASSERT_EQ(lox_kv_get(&db, "a", &b, 1u, &out_len), LOX_ERR_INVALID);
    ASSERT_EQ(lox_inspect(&db, &st), LOX_ERR_INVALID);
}

MDB_TEST(contract_double_deinit_second_call_invalid) {
    lox_t db;
    lox_cfg_t cfg;

    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(&db, &cfg), LOX_OK);
    ASSERT_EQ(lox_deinit(&db), LOX_OK);
    ASSERT_EQ(lox_deinit(&db), LOX_ERR_INVALID);
}

MDB_TEST(contract_init_null_args_invalid) {
    lox_cfg_t cfg;
    lox_t db;

    memset(&cfg, 0, sizeof(cfg));
    memset(&db, 0, sizeof(db));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;

    ASSERT_EQ(lox_init(NULL, &cfg), LOX_ERR_INVALID);
    ASSERT_EQ(lox_init(&db, NULL), LOX_ERR_INVALID);
}

MDB_TEST(contract_inspect_null_out_invalid) {
    ASSERT_EQ(lox_inspect(&g_db, NULL), LOX_ERR_INVALID);
}

MDB_TEST(contract_kv_get_null_buf_invalid) {
    uint8_t value = 3u;
    size_t out_len = 0u;

    ASSERT_EQ(lox_kv_put(&g_db, "a", &value, sizeof(value)), LOX_OK);
    ASSERT_EQ(lox_kv_get(&g_db, "a", NULL, sizeof(value), &out_len), LOX_ERR_INVALID);
}

MDB_TEST(contract_ts_query_buf_null_out_count_invalid) {
    lox_ts_sample_t sample;
    uint32_t v = 10u;

    ASSERT_EQ(lox_ts_register(&g_db, "q", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(lox_ts_insert(&g_db, "q", 1u, &v), LOX_OK);
    ASSERT_EQ(lox_ts_query_buf(&g_db, "q", 0u, 10u, &sample, 1u, NULL), LOX_OK);
}

MDB_TEST(contract_ts_count_null_out_count_invalid) {
    ASSERT_EQ(lox_ts_count(&g_db, "missing", 0u, 1u, NULL), LOX_ERR_INVALID);
}

MDB_TEST(contract_rel_count_null_out_invalid) {
    lox_table_t *table = NULL;
    ensure_rel_table(&table);
    ASSERT_EQ(lox_rel_count(table, NULL), LOX_ERR_INVALID);
}

MDB_TEST(stress_random_mixed_workload_with_reopen) {
    run_mixed_workload(360u, true, false, false);
}

MDB_TEST(stress_random_mixed_workload_with_compact) {
    run_mixed_workload(360u, false, true, false);
}

MDB_TEST(stress_random_mixed_workload_with_power_loss_recovery) {
    run_mixed_workload(360u, false, false, true);
}

MDB_TEST(stress_random_mixed_workload_with_reopen_and_compact) {
    run_mixed_workload(420u, true, true, false);
}

MDB_TEST(stress_random_mixed_workload_with_reopen_and_power_loss) {
    run_mixed_workload(420u, true, false, true);
}

MDB_TEST(stress_random_mixed_workload_long_run) {
    run_mixed_workload(720u, true, true, false);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, contract_calls_before_init_are_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_calls_after_deinit_are_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_double_deinit_second_call_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_init_null_args_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_inspect_null_out_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_kv_get_null_buf_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_ts_query_buf_null_out_count_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_ts_count_null_out_count_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_rel_count_null_out_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, stress_random_mixed_workload_with_reopen);
    MDB_RUN_TEST(setup_db, teardown_db, stress_random_mixed_workload_with_compact);
    MDB_RUN_TEST(setup_db, teardown_db, stress_random_mixed_workload_with_power_loss_recovery);
    MDB_RUN_TEST(setup_db, teardown_db, stress_random_mixed_workload_with_reopen_and_compact);
    MDB_RUN_TEST(setup_db, teardown_db, stress_random_mixed_workload_with_reopen_and_power_loss);
    MDB_RUN_TEST(setup_db, teardown_db, stress_random_mixed_workload_long_run);
    return MDB_RESULT();
}
