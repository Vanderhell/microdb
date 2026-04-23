// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_backend_adapter.h"
#include "lox_backend_open.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

int lox_backend_fs_stub_register(void);
int lox_backend_block_stub_register(void);

enum {
    FS_MATRIX_CAPACITY = 131072u,
    FS_MATRIX_ERASE_SIZE = 4096u,
    FS_MATRIX_REL_IDS = 40u
};

typedef struct {
    uint8_t durable[FS_MATRIX_CAPACITY];
    uint8_t working[FS_MATRIX_CAPACITY];
    uint32_t sync_calls;
    uint8_t write_through;
} fs_matrix_media_t;

static fs_matrix_media_t g_media;
static lox_storage_t g_raw_storage;
static lox_storage_t *g_effective_storage = NULL;
static lox_backend_open_session_t g_open_session;
static lox_t g_db;
static uint32_t g_now = 9000u;
static uint32_t g_rng = 0x9E3779B9u;

static int fail_runtime_gate(const char *op, long max_ms, double elapsed_ms) {
    fprintf(stderr,
            "%s failed: EXIT_FAILURE (%d) - elapsed=%.2f ms > budget=%ld ms\n",
            op,
            EXIT_FAILURE,
            elapsed_ms,
            max_ms);
    return EXIT_FAILURE;
}

static lox_timestamp_t mock_now(void) {
    return g_now++;
}

static uint32_t rng_next(void) {
    g_rng = (g_rng * 1664525u) + 1013904223u;
    return g_rng;
}

static lox_err_t medium_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    fs_matrix_media_t *m = (fs_matrix_media_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > FS_MATRIX_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    memcpy(buf, m->working + offset, len);
    return LOX_OK;
}

static lox_err_t medium_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    fs_matrix_media_t *m = (fs_matrix_media_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > FS_MATRIX_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    memcpy(m->working + offset, buf, len);
    if (m->write_through != 0u) {
        memcpy(m->durable + offset, buf, len);
    }
    return LOX_OK;
}

static lox_err_t medium_erase(void *ctx, uint32_t offset) {
    fs_matrix_media_t *m = (fs_matrix_media_t *)ctx;
    uint32_t base;
    if (m == NULL || offset >= FS_MATRIX_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    base = (offset / FS_MATRIX_ERASE_SIZE) * FS_MATRIX_ERASE_SIZE;
    memset(m->working + base, 0xFF, FS_MATRIX_ERASE_SIZE);
    if (m->write_through != 0u) {
        memset(m->durable + base, 0xFF, FS_MATRIX_ERASE_SIZE);
    }
    return LOX_OK;
}

static lox_err_t medium_sync(void *ctx) {
    fs_matrix_media_t *m = (fs_matrix_media_t *)ctx;
    if (m == NULL) {
        return LOX_ERR_STORAGE;
    }
    m->sync_calls++;
    memcpy(m->durable, m->working, FS_MATRIX_CAPACITY);
    return LOX_OK;
}

static void power_loss_reset_to_durable(void) {
    memcpy(g_media.working, g_media.durable, FS_MATRIX_CAPACITY);
}

static void open_db(const char *backend_name, uint8_t write_through) {
    lox_cfg_t cfg;

    g_media.write_through = write_through;
    memset(&g_db, 0, sizeof(g_db));
    g_effective_storage = NULL;
    ASSERT_EQ(lox_backend_open_prepare(backend_name, &g_raw_storage, 0u, 1u, &g_open_session, &g_effective_storage), LOX_OK);
    ASSERT_EQ(g_open_session.using_fs_adapter, 1u);
    ASSERT_EQ(g_effective_storage != NULL, 1);
    ASSERT_EQ(g_effective_storage->write_size, 1u);

    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = g_effective_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void close_db(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        (void)lox_deinit(&g_db);
    }
    lox_backend_open_release(&g_open_session);
    g_effective_storage = NULL;
    memset(&g_db, 0, sizeof(g_db));
}

static void crash_reopen(const char *backend_name, uint8_t write_through) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
    }
    lox_backend_open_release(&g_open_session);
    memset(&g_db, 0, sizeof(g_db));
    open_db(backend_name, write_through);
}

static void setup_fixture(void) {
    memset(&g_media, 0, sizeof(g_media));
    memset(g_media.durable, 0xFF, sizeof(g_media.durable));
    memcpy(g_media.working, g_media.durable, sizeof(g_media.working));
    memset(&g_raw_storage, 0, sizeof(g_raw_storage));
    memset(&g_open_session, 0, sizeof(g_open_session));
    g_effective_storage = NULL;
    g_now = 9000u;
    g_rng = 0x9E3779B9u;

    g_raw_storage.read = medium_read;
    g_raw_storage.write = medium_write;
    g_raw_storage.erase = medium_erase;
    g_raw_storage.sync = medium_sync;
    g_raw_storage.capacity = FS_MATRIX_CAPACITY;
    g_raw_storage.erase_size = FS_MATRIX_ERASE_SIZE;
    g_raw_storage.write_size = 1u;
    g_raw_storage.ctx = &g_media;

    lox_backend_registry_reset();
    ASSERT_EQ(lox_backend_fs_stub_register(), 0);
    ASSERT_EQ(lox_backend_block_stub_register(), 0);
}

static void teardown_fixture(void) {
    close_db();
    lox_backend_registry_reset();
}

static void run_near_full_lane(const char *backend_name, uint8_t write_through) {
    uint32_t i;
    uint8_t full_seen = 0u;
    lox_stats_t st;
    open_db(backend_name, write_through);

    for (i = 0u; i < 1800u; ++i) {
        char key[24];
        uint8_t payload[56];
        uint8_t out = 0u;
        size_t out_len = 0u;
        lox_err_t rc;

        memset(payload, (int)(i & 0xFFu), sizeof(payload));
        (void)snprintf(key, sizeof(key), "nf_%04u", (unsigned)i);
        rc = lox_kv_set(&g_db, key, payload, sizeof(payload), 0u);
        if (rc == LOX_ERR_FULL) {
            full_seen = 1u;
            break;
        }
        ASSERT_EQ(rc, LOX_OK);

        if (((i + 1u) % 160u) == 0u) {
            uint32_t health = i;
            uint32_t out = 0u;
            ASSERT_EQ(lox_kv_set(&g_db, "health", &i, sizeof(i), 0u), LOX_OK);
            ASSERT_EQ(lox_kv_get(&g_db, "health", &out, sizeof(out), &out_len), LOX_OK);
            ASSERT_EQ(out_len, (long long)sizeof(out));
            ASSERT_EQ(out, health);
        }
    }

    ASSERT_EQ(lox_inspect(&g_db, &st), LOX_OK);
    ASSERT_EQ((st.wal_fill_pct >= 70u || full_seen != 0u), 1);
    power_loss_reset_to_durable();
    crash_reopen(backend_name, write_through);
    ASSERT_EQ(lox_inspect(&g_db, &st), LOX_OK);
    close_db();
}

static void run_corruption_lane(const char *backend_name, uint8_t write_through) {
    lox_cfg_t cfg;
    lox_err_t rc;

    open_db(backend_name, write_through);
    {
        uint8_t value = 0xABu;
        ASSERT_EQ(lox_kv_set(&g_db, "stable", &value, sizeof(value), 0u), LOX_OK);
        if (write_through == 0u) {
            ASSERT_EQ(lox_flush(&g_db), LOX_OK);
        }
    }
    close_db();

    g_media.durable[32u] ^= 0x5Au;
    power_loss_reset_to_durable();

    g_effective_storage = NULL;
    ASSERT_EQ(lox_backend_open_prepare(backend_name, &g_raw_storage, 0u, 1u, &g_open_session, &g_effective_storage), LOX_OK);
    ASSERT_EQ(g_effective_storage != NULL, 1);
    memset(&cfg, 0, sizeof(cfg));
    memset(&g_db, 0, sizeof(g_db));
    cfg.storage = g_effective_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    rc = lox_init(&g_db, &cfg);
    ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_CORRUPT), 1);
    if (rc == LOX_OK) {
        ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    }
    lox_backend_open_release(&g_open_session);
    memset(&g_db, 0, sizeof(g_db));

    memset(g_media.durable, 0xFF, sizeof(g_media.durable));
    memcpy(g_media.working, g_media.durable, sizeof(g_media.working));
    open_db(backend_name, write_through);
    close_db();
}

static void ensure_rel_table(void) {
    lox_table_t *table = NULL;
    lox_schema_t schema;
    lox_err_t rc = lox_table_get(&g_db, "m", &table);
    if (rc == LOX_OK) {
        return;
    }
    ASSERT_EQ(rc, LOX_ERR_NOT_FOUND);
    ASSERT_EQ(lox_schema_init(&schema, "m", FS_MATRIX_REL_IDS + 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "v", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    rc = lox_table_create(&g_db, &schema);
    ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_EXISTS), 1);
}

static void run_latency_workload(const char *backend_name, uint8_t write_through, uint32_t iterations, uint32_t reopen_period) {
    uint32_t i;
    lox_err_t rc;
    open_db(backend_name, write_through);
    rc = lox_ts_register(&g_db, "s", LOX_TS_U32, 0u);
    ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_EXISTS), 1);
    ensure_rel_table();

    for (i = 0u; i < iterations; ++i) {
        uint32_t op = rng_next() % 5u;
        if (op <= 1u) {
            char key[20];
            uint32_t val = i ^ 0xA5A5u;
            lox_err_t rc;
            (void)snprintf(key, sizeof(key), "k%03u", (unsigned)(rng_next() % 128u));
            if (op == 0u) {
                rc = lox_kv_set(&g_db, key, &val, sizeof(val), 0u);
                ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_FULL), 1);
            } else {
                rc = lox_kv_del(&g_db, key);
                ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_NOT_FOUND), 1);
            }
        } else if (op == 2u) {
            uint32_t tsv = i;
            ASSERT_EQ(lox_ts_insert(&g_db, "s", (lox_timestamp_t)(1000u + i), &tsv), LOX_OK);
        } else {
            lox_table_t *table = NULL;
            uint8_t row[64] = { 0 };
            uint32_t id = rng_next() % FS_MATRIX_REL_IDS;
            uint8_t v = (uint8_t)(id & 0xFFu);
            ASSERT_EQ(lox_table_get(&g_db, "m", &table), LOX_OK);
            if (op == 3u) {
                ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
                ASSERT_EQ(lox_row_set(table, row, "v", &v), LOX_OK);
                {
                    lox_err_t rc = lox_rel_insert(&g_db, table, row);
                    ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_EXISTS || rc == LOX_ERR_FULL), 1);
                }
            } else {
                ASSERT_EQ((lox_rel_delete(&g_db, table, &id, NULL) == LOX_OK), 1);
            }
        }

        if (reopen_period != 0u && ((i + 1u) % reopen_period) == 0u) {
            if (write_through == 0u) {
                ASSERT_EQ(lox_flush(&g_db), LOX_OK);
            }
            power_loss_reset_to_durable();
            crash_reopen(backend_name, write_through);
            {
                lox_stats_t st;
                ASSERT_EQ(lox_inspect(&g_db, &st), LOX_OK);
            }
        }
    }
    close_db();
}

MDB_TEST(fs_matrix_near_full_fs_stub) {
    run_near_full_lane("fs_stub", 0u);
}

MDB_TEST(fs_matrix_near_full_block_stub) {
    run_near_full_lane("block_stub", 1u);
}

MDB_TEST(fs_matrix_corruption_contract_fs_stub) {
    run_corruption_lane("fs_stub", 0u);
}

MDB_TEST(fs_matrix_corruption_contract_block_stub) {
    run_corruption_lane("block_stub", 1u);
}

static int run_latency_suite(int run_long, long max_ms) {
    clock_t begin_ticks = clock();
    double elapsed_ms;

    if (run_long != 0) {
        run_latency_workload("fs_stub", 0u, 1400u, 35u);
        run_latency_workload("block_stub", 1u, 1400u, 35u);
    } else {
        run_latency_workload("fs_stub", 0u, 420u, 28u);
        run_latency_workload("block_stub", 1u, 420u, 28u);
    }

    elapsed_ms = ((double)(clock() - begin_ticks) * 1000.0) / (double)CLOCKS_PER_SEC;
    if (max_ms > 0 && elapsed_ms > (double)max_ms) {
        return fail_runtime_gate("fs_matrix_runtime_gate", max_ms, elapsed_ms);
    }
    return EXIT_SUCCESS;
}

int main(int argc, char **argv) {
    int run_long = 0;
    long max_ms = -1;
    int i;
    int rc;

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

    MDB_RUN_TEST(setup_fixture, teardown_fixture, fs_matrix_near_full_fs_stub);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, fs_matrix_near_full_block_stub);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, fs_matrix_corruption_contract_fs_stub);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, fs_matrix_corruption_contract_block_stub);
    rc = MDB_RESULT();
    if (rc != EXIT_SUCCESS) {
        return rc;
    }
    return run_latency_suite(run_long, max_ms);
}
