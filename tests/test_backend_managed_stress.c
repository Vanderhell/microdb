// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "microdb_backend_adapter.h"
#include "microdb_backend_open.h"
#include "../src/microdb_internal.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

int microdb_backend_nand_stub_register(void);

enum {
    MANAGED_CAPACITY = 131072u,
    MANAGED_ERASE_SIZE = 4096u,
    MODEL_KV_KEYS = 24u,
    MODEL_REL_IDS = 32u
};

typedef struct {
    uint8_t durable[MANAGED_CAPACITY];
    uint8_t working[MANAGED_CAPACITY];
} managed_mem_ctx_t;

typedef struct {
    uint8_t present;
    uint8_t value;
} kv_model_entry_t;

typedef struct {
    kv_model_entry_t kv[MODEL_KV_KEYS];
    uint8_t rel_present[MODEL_REL_IDS];
    uint32_t rel_count;
    uint8_t ts_has_sample;
    microdb_timestamp_t ts_last_ts;
    uint32_t ts_last_value;
} model_t;

static managed_mem_ctx_t g_media;
static microdb_storage_t g_raw_storage;
static microdb_storage_t *g_effective_storage = NULL;
static microdb_backend_open_session_t g_open_session;
static microdb_t g_db;
static uint32_t g_now = 1000u;
static uint32_t g_rng = 0x1234ABCDu;

static int fail_runtime_gate(const char *op, long max_ms, double elapsed_ms) {
    fprintf(stderr,
            "%s failed: EXIT_FAILURE (%d) - elapsed=%.2f ms > budget=%ld ms\n",
            op,
            EXIT_FAILURE,
            elapsed_ms,
            max_ms);
    return EXIT_FAILURE;
}

static microdb_timestamp_t mock_now(void) {
    return g_now++;
}

static uint32_t rng_next(void) {
    g_rng = (g_rng * 1664525u) + 1013904223u;
    return g_rng;
}

static microdb_err_t managed_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    managed_mem_ctx_t *m = (managed_mem_ctx_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > MANAGED_CAPACITY) {
        return MICRODB_ERR_STORAGE;
    }
    memcpy(buf, m->working + offset, len);
    return MICRODB_OK;
}

static microdb_err_t managed_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    managed_mem_ctx_t *m = (managed_mem_ctx_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > MANAGED_CAPACITY) {
        return MICRODB_ERR_STORAGE;
    }
    memcpy(m->working + offset, buf, len);
    return MICRODB_OK;
}

static microdb_err_t managed_erase(void *ctx, uint32_t offset) {
    managed_mem_ctx_t *m = (managed_mem_ctx_t *)ctx;
    uint32_t base;
    if (m == NULL || offset >= MANAGED_CAPACITY) {
        return MICRODB_ERR_STORAGE;
    }
    base = (offset / MANAGED_ERASE_SIZE) * MANAGED_ERASE_SIZE;
    memset(m->working + base, 0xFF, MANAGED_ERASE_SIZE);
    return MICRODB_OK;
}

static microdb_err_t managed_sync(void *ctx) {
    managed_mem_ctx_t *m = (managed_mem_ctx_t *)ctx;
    if (m == NULL) {
        return MICRODB_ERR_STORAGE;
    }
    memcpy(m->durable, m->working, MANAGED_CAPACITY);
    return MICRODB_OK;
}

static void power_loss_reset_to_durable(void) {
    memcpy(g_media.working, g_media.durable, MANAGED_CAPACITY);
}

static void create_rel_table_if_missing(microdb_table_t **out) {
    microdb_schema_t schema;
    microdb_err_t rc = microdb_table_get(&g_db, "users", out);
    if (rc == MICRODB_OK) {
        return;
    }
    ASSERT_EQ(rc, MICRODB_ERR_NOT_FOUND);
    ASSERT_EQ(microdb_schema_init(&schema, "users", MODEL_REL_IDS + 8u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    rc = microdb_table_create(&g_db, &schema);
    ASSERT_EQ((rc == MICRODB_OK || rc == MICRODB_ERR_EXISTS), 1);
    ASSERT_EQ(microdb_table_get(&g_db, "users", out), MICRODB_OK);
}

static void create_ts_stream_if_missing(void) {
    microdb_err_t rc = microdb_ts_register(&g_db, "main_ts", MICRODB_TS_U32, 0u);
    ASSERT_EQ((rc == MICRODB_OK || rc == MICRODB_ERR_EXISTS), 1);
}

static void managed_open_db(void) {
    microdb_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    g_effective_storage = NULL;
    ASSERT_EQ(microdb_backend_open_prepare("nand_stub", &g_raw_storage, 0u, 1u, &g_open_session, &g_effective_storage), MICRODB_OK);
    ASSERT_EQ(g_open_session.using_managed_adapter, 1u);
    ASSERT_EQ(g_effective_storage != NULL, 1);

    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = g_effective_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void managed_crash_reopen(void) {
    if (microdb_core_const(&g_db)->magic == MICRODB_MAGIC) {
        free(microdb_core(&g_db)->heap);
    }
    microdb_backend_open_release(&g_open_session);
    memset(&g_db, 0, sizeof(g_db));
    managed_open_db();
}

static void verify_model(const model_t *model) {
    uint32_t i;
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t rel_count = 0u;

    for (i = 0u; i < MODEL_KV_KEYS; ++i) {
        char key[16];
        uint8_t out = 0u;
        size_t out_len = 0u;
        (void)snprintf(key, sizeof(key), "k%02u", (unsigned)i);
        if (model->kv[i].present != 0u) {
            ASSERT_EQ(microdb_kv_get(&g_db, key, &out, sizeof(out), &out_len), MICRODB_OK);
            ASSERT_EQ(out_len, (long long)sizeof(out));
            ASSERT_EQ(out, model->kv[i].value);
        } else {
            ASSERT_EQ(microdb_kv_get(&g_db, key, &out, sizeof(out), &out_len), MICRODB_ERR_NOT_FOUND);
        }
    }

    if (model->ts_has_sample != 0u) {
        microdb_ts_sample_t sample;
        ASSERT_EQ(microdb_ts_last(&g_db, "main_ts", &sample), MICRODB_OK);
        ASSERT_EQ(sample.ts, model->ts_last_ts);
        ASSERT_EQ(sample.v.u32, model->ts_last_value);
    } else {
        microdb_ts_sample_t sample;
        ASSERT_EQ(microdb_ts_last(&g_db, "main_ts", &sample), MICRODB_ERR_NOT_FOUND);
    }

    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_count(table, &rel_count), MICRODB_OK);
    ASSERT_EQ(rel_count, model->rel_count);

    for (i = 0u; i < MODEL_REL_IDS; ++i) {
        if (model->rel_present[i] != 0u) {
            ASSERT_EQ(microdb_rel_find_by(&g_db, table, "id", &i, row), MICRODB_OK);
        }
    }
}

static void setup_fixture(void) {
    memset(&g_media, 0, sizeof(g_media));
    memset(g_media.durable, 0xFF, sizeof(g_media.durable));
    memcpy(g_media.working, g_media.durable, sizeof(g_media.working));
    memset(&g_raw_storage, 0, sizeof(g_raw_storage));
    memset(&g_open_session, 0, sizeof(g_open_session));
    g_now = 1000u;
    g_rng = 0x1234ABCDu;

    g_raw_storage.read = managed_read;
    g_raw_storage.write = managed_write;
    g_raw_storage.erase = managed_erase;
    g_raw_storage.sync = managed_sync;
    g_raw_storage.capacity = MANAGED_CAPACITY;
    g_raw_storage.erase_size = MANAGED_ERASE_SIZE;
    g_raw_storage.write_size = 1u;
    g_raw_storage.ctx = &g_media;

    microdb_backend_registry_reset();
    ASSERT_EQ(microdb_backend_nand_stub_register(), 0);
    managed_open_db();
}

static void teardown_fixture(void) {
    if (microdb_core_const(&g_db)->magic == MICRODB_MAGIC) {
        (void)microdb_deinit(&g_db);
    }
    microdb_backend_open_release(&g_open_session);
    microdb_backend_registry_reset();
}

static void run_managed_stress_workload(uint32_t iterations, uint32_t reopen_period) {
    model_t model;
    microdb_table_t *table = NULL;
    uint32_t i;

    memset(&model, 0, sizeof(model));
    create_ts_stream_if_missing();
    create_rel_table_if_missing(&table);

    for (i = 0u; i < iterations; ++i) {
        uint32_t op = rng_next() % 5u;
        if (op <= 1u) {
            uint32_t idx = rng_next() % MODEL_KV_KEYS;
            char key[16];
            (void)snprintf(key, sizeof(key), "k%02u", (unsigned)idx);
            if (op == 0u) {
                uint8_t value = (uint8_t)((i % 200u) + 1u);
                ASSERT_EQ(microdb_kv_set(&g_db, key, &value, sizeof(value), 0u), MICRODB_OK);
                model.kv[idx].present = 1u;
                model.kv[idx].value = value;
            } else {
                microdb_err_t rc = microdb_kv_del(&g_db, key);
                if (model.kv[idx].present != 0u) {
                    ASSERT_EQ(rc, MICRODB_OK);
                    model.kv[idx].present = 0u;
                    model.kv[idx].value = 0u;
                } else {
                    ASSERT_EQ(rc, MICRODB_ERR_NOT_FOUND);
                }
            }
        } else if (op == 2u) {
            uint32_t ts_value = i ^ 0xA55Au;
            microdb_timestamp_t ts = (microdb_timestamp_t)(1000u + i);
            ASSERT_EQ(microdb_ts_insert(&g_db, "main_ts", ts, &ts_value), MICRODB_OK);
            model.ts_has_sample = 1u;
            model.ts_last_ts = ts;
            model.ts_last_value = ts_value;
        } else {
            uint32_t id = rng_next() % MODEL_REL_IDS;
            uint8_t row[64] = { 0 };
            uint8_t age = (uint8_t)(20u + (id % 50u));
            if (op == 3u) {
                if (model.rel_present[id] == 0u) {
                    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
                    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
                    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
                    model.rel_present[id] = 1u;
                    model.rel_count++;
                }
            } else {
                uint32_t deleted = 0u;
                ASSERT_EQ(microdb_rel_delete(&g_db, table, &id, &deleted), MICRODB_OK);
                if (model.rel_present[id] != 0u) {
                    ASSERT_EQ(deleted, 1u);
                    model.rel_present[id] = 0u;
                    model.rel_count--;
                } else {
                    ASSERT_EQ(deleted, 0u);
                }
            }
        }

        if (reopen_period != 0u && ((i + 1u) % reopen_period) == 0u) {
            power_loss_reset_to_durable();
            managed_crash_reopen();
            ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
            verify_model(&model);
        }
    }

    verify_model(&model);
}

MDB_TEST(managed_stress_smoke_mixed_workload) {
    run_managed_stress_workload(320u, 32u);
}

MDB_TEST(managed_stress_long_mixed_workload) {
    run_managed_stress_workload(1600u, 40u);
}

int main(int argc, char **argv) {
    int run_long = 0;
    long max_ms = -1;
    int i;
    int rc;
    clock_t begin_ticks;
    double elapsed_ms;

    for (i = 1; i < argc; ++i) {
        if (argv != NULL && argv[i] != NULL && strcmp(argv[i], "--long") == 0) {
            run_long = 1;
        } else if (argv != NULL && argv[i] != NULL && strcmp(argv[i], "--max-ms") == 0 && (i + 1) < argc) {
            char *endptr = NULL;
            long parsed = strtol(argv[i + 1], &endptr, 10);
            if (endptr != NULL && *endptr == '\0' && parsed > 0) {
                max_ms = parsed;
            }
            i++;
        }
    }

    begin_ticks = clock();
    if (run_long != 0) {
        MDB_RUN_TEST(setup_fixture, teardown_fixture, managed_stress_long_mixed_workload);
    } else {
        MDB_RUN_TEST(setup_fixture, teardown_fixture, managed_stress_smoke_mixed_workload);
    }
    rc = MDB_RESULT();

    elapsed_ms = ((double)(clock() - begin_ticks) * 1000.0) / (double)CLOCKS_PER_SEC;
    if (max_ms > 0 && elapsed_ms > (double)max_ms) {
        (void)fail_runtime_gate("managed_stress_runtime_gate", max_ms, elapsed_ms);
        if (rc == EXIT_SUCCESS) {
            rc = EXIT_FAILURE;
        }
    }
    return rc;
}
