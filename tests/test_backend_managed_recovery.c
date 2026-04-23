// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_backend_adapter.h"
#include "lox_backend_open.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <string.h>

int lox_backend_nand_stub_register(void);

enum {
    MANAGED_CAPACITY = 131072u,
    MANAGED_ERASE_SIZE = 4096u
};

typedef struct {
    uint8_t durable[MANAGED_CAPACITY];
    uint8_t working[MANAGED_CAPACITY];
    uint32_t sync_calls;
    uint8_t fail_next_sync;
} managed_mem_ctx_t;

static managed_mem_ctx_t g_media;
static lox_storage_t g_raw_storage;
static lox_storage_t *g_effective_storage = NULL;
static lox_backend_open_session_t g_open_session;
static lox_t g_db;
static uint32_t g_now = 1000u;

static lox_timestamp_t mock_now(void) {
    return g_now++;
}

static lox_err_t managed_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    managed_mem_ctx_t *m = (managed_mem_ctx_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > MANAGED_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    memcpy(buf, m->working + offset, len);
    return LOX_OK;
}

static lox_err_t managed_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    managed_mem_ctx_t *m = (managed_mem_ctx_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > MANAGED_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    memcpy(m->working + offset, buf, len);
    return LOX_OK;
}

static lox_err_t managed_erase(void *ctx, uint32_t offset) {
    managed_mem_ctx_t *m = (managed_mem_ctx_t *)ctx;
    uint32_t base;
    if (m == NULL || offset >= MANAGED_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    base = (offset / MANAGED_ERASE_SIZE) * MANAGED_ERASE_SIZE;
    memset(m->working + base, 0xFF, MANAGED_ERASE_SIZE);
    return LOX_OK;
}

static lox_err_t managed_sync(void *ctx) {
    managed_mem_ctx_t *m = (managed_mem_ctx_t *)ctx;
    if (m == NULL) {
        return LOX_ERR_STORAGE;
    }
    m->sync_calls++;
    if (m->fail_next_sync != 0u) {
        m->fail_next_sync = 0u;
        return LOX_ERR_STORAGE;
    }
    memcpy(m->durable, m->working, MANAGED_CAPACITY);
    return LOX_OK;
}

static void power_loss_reset_to_durable(void) {
    memcpy(g_media.working, g_media.durable, MANAGED_CAPACITY);
}

static void managed_open_db(void) {
    lox_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    g_effective_storage = NULL;
    ASSERT_EQ(lox_backend_open_prepare("nand_stub", &g_raw_storage, 0u, 1u, &g_open_session, &g_effective_storage), LOX_OK);
    ASSERT_EQ(g_open_session.using_managed_adapter, 1u);
    ASSERT_EQ(g_effective_storage != NULL, 1);

    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = g_effective_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void managed_close_db_clean(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    }
    lox_backend_open_release(&g_open_session);
    g_effective_storage = NULL;
    memset(&g_db, 0, sizeof(g_db));
}

static void managed_crash_reopen(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
    }
    lox_backend_open_release(&g_open_session);
    memset(&g_db, 0, sizeof(g_db));
    managed_open_db();
}

static void setup_fixture(void) {
    memset(&g_media, 0, sizeof(g_media));
    memset(g_media.durable, 0xFF, sizeof(g_media.durable));
    memcpy(g_media.working, g_media.durable, sizeof(g_media.working));
    memset(&g_raw_storage, 0, sizeof(g_raw_storage));
    memset(&g_open_session, 0, sizeof(g_open_session));
    g_now = 1000u;

    g_raw_storage.read = managed_read;
    g_raw_storage.write = managed_write;
    g_raw_storage.erase = managed_erase;
    g_raw_storage.sync = managed_sync;
    g_raw_storage.capacity = MANAGED_CAPACITY;
    g_raw_storage.erase_size = MANAGED_ERASE_SIZE;
    g_raw_storage.write_size = 1u;
    g_raw_storage.ctx = &g_media;

    lox_backend_registry_reset();
    ASSERT_EQ(lox_backend_nand_stub_register(), 0);
    managed_open_db();
}

static void teardown_fixture(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        (void)lox_deinit(&g_db);
    }
    lox_backend_open_release(&g_open_session);
    lox_backend_registry_reset();
}

MDB_TEST(managed_recovery_wal_replays_after_power_loss) {
    uint8_t in = 77u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_set(&g_db, "k", &in, 1u, 0u), LOX_OK);
    power_loss_reset_to_durable();
    managed_crash_reopen();
    ASSERT_EQ(lox_kv_get(&g_db, "k", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 77u);
}

MDB_TEST(managed_recovery_sync_failure_does_not_commit_new_value) {
    uint8_t in = 11u;
    uint8_t out = 0u;

    g_media.fail_next_sync = 1u;
    ASSERT_EQ(lox_kv_set(&g_db, "volatile", &in, 1u, 0u), LOX_ERR_STORAGE);
    power_loss_reset_to_durable();
    managed_crash_reopen();
    ASSERT_EQ(lox_kv_get(&g_db, "volatile", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(managed_recovery_clean_reopen_preserves_data) {
    uint8_t in = 33u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_set(&g_db, "stable", &in, 1u, 0u), LOX_OK);
    managed_close_db_clean();
    managed_open_db();
    ASSERT_EQ(lox_kv_get(&g_db, "stable", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, 33u);
}

int main(void) {
    MDB_RUN_TEST(setup_fixture, teardown_fixture, managed_recovery_wal_replays_after_power_loss);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, managed_recovery_sync_failure_does_not_commit_new_value);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, managed_recovery_clean_reopen_preserves_data);
    return MDB_RESULT();
}
