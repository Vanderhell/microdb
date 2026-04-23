// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_backend_adapter.h"
#include "lox_backend_open.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <string.h>

int lox_backend_aligned_stub_register(void);

enum {
    ALIGNED_CAPACITY = 131072u,
    ALIGNED_ERASE_SIZE = 4096u,
    ALIGNED_WRITE_SIZE = 16u
};

typedef struct {
    uint8_t durable[ALIGNED_CAPACITY];
    uint8_t working[ALIGNED_CAPACITY];
    uint8_t sync_fail_once;
    uint32_t sync_calls;
} aligned_mem_ctx_t;

static aligned_mem_ctx_t g_media;
static lox_storage_t g_raw_storage;
static lox_storage_t *g_effective_storage = NULL;
static lox_backend_open_session_t g_open_session;
static lox_t g_db;
static uint32_t g_now = 5000u;

static lox_timestamp_t mock_now(void) {
    return g_now++;
}

static lox_err_t aligned_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    aligned_mem_ctx_t *m = (aligned_mem_ctx_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > ALIGNED_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    if ((offset % ALIGNED_WRITE_SIZE) != 0u || (len % ALIGNED_WRITE_SIZE) != 0u || len == 0u) {
        return LOX_ERR_STORAGE;
    }
    memcpy(buf, m->working + offset, len);
    return LOX_OK;
}

static lox_err_t aligned_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    aligned_mem_ctx_t *m = (aligned_mem_ctx_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > ALIGNED_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    if ((offset % ALIGNED_WRITE_SIZE) != 0u || (len % ALIGNED_WRITE_SIZE) != 0u || len == 0u) {
        return LOX_ERR_STORAGE;
    }
    memcpy(m->working + offset, buf, len);
    return LOX_OK;
}

static lox_err_t aligned_erase(void *ctx, uint32_t offset) {
    aligned_mem_ctx_t *m = (aligned_mem_ctx_t *)ctx;
    uint32_t base;
    if (m == NULL || offset >= ALIGNED_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    base = (offset / ALIGNED_ERASE_SIZE) * ALIGNED_ERASE_SIZE;
    memset(m->working + base, 0xFF, ALIGNED_ERASE_SIZE);
    return LOX_OK;
}

static lox_err_t aligned_sync(void *ctx) {
    aligned_mem_ctx_t *m = (aligned_mem_ctx_t *)ctx;
    if (m == NULL) {
        return LOX_ERR_STORAGE;
    }
    m->sync_calls++;
    if (m->sync_fail_once != 0u) {
        m->sync_fail_once = 0u;
        return LOX_ERR_STORAGE;
    }
    memcpy(m->durable, m->working, ALIGNED_CAPACITY);
    return LOX_OK;
}

static void power_loss_reset_to_durable(void) {
    memcpy(g_media.working, g_media.durable, ALIGNED_CAPACITY);
}

static void aligned_open_db(void) {
    lox_cfg_t cfg;

    memset(&g_db, 0, sizeof(g_db));
    g_effective_storage = NULL;
    ASSERT_EQ(lox_backend_open_prepare("aligned_stub", &g_raw_storage, 1u, 0u, &g_open_session, &g_effective_storage), LOX_OK);
    ASSERT_EQ(g_open_session.using_aligned_adapter, 1u);
    ASSERT_EQ(g_effective_storage != NULL, 1);
    ASSERT_EQ(g_effective_storage->write_size, 1u);

    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = g_effective_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void aligned_close_db_clean(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    }
    lox_backend_open_release(&g_open_session);
    g_effective_storage = NULL;
    memset(&g_db, 0, sizeof(g_db));
}

static void aligned_crash_reopen(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
    }
    lox_backend_open_release(&g_open_session);
    memset(&g_db, 0, sizeof(g_db));
    aligned_open_db();
}

static void setup_fixture(void) {
    memset(&g_media, 0, sizeof(g_media));
    memset(g_media.durable, 0xFF, sizeof(g_media.durable));
    memcpy(g_media.working, g_media.durable, sizeof(g_media.working));
    memset(&g_raw_storage, 0, sizeof(g_raw_storage));
    memset(&g_open_session, 0, sizeof(g_open_session));
    g_now = 5000u;

    g_raw_storage.read = aligned_read;
    g_raw_storage.write = aligned_write;
    g_raw_storage.erase = aligned_erase;
    g_raw_storage.sync = aligned_sync;
    g_raw_storage.capacity = ALIGNED_CAPACITY;
    g_raw_storage.erase_size = ALIGNED_ERASE_SIZE;
    g_raw_storage.write_size = ALIGNED_WRITE_SIZE;
    g_raw_storage.ctx = &g_media;

    lox_backend_registry_reset();
    ASSERT_EQ(lox_backend_aligned_stub_register(), 0);
    aligned_open_db();
}

static void teardown_fixture(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        (void)lox_deinit(&g_db);
    }
    lox_backend_open_release(&g_open_session);
    lox_backend_registry_reset();
}

MDB_TEST(aligned_recovery_wal_replays_after_power_loss) {
    uint8_t in = 71u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_set(&g_db, "a", &in, 1u, 0u), LOX_OK);
    power_loss_reset_to_durable();
    aligned_crash_reopen();
    ASSERT_EQ(lox_kv_get(&g_db, "a", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, in);
}

MDB_TEST(aligned_recovery_sync_failure_does_not_commit_value) {
    uint8_t in = 99u;
    uint8_t out = 0u;

    g_media.sync_fail_once = 1u;
    ASSERT_EQ(lox_kv_set(&g_db, "volatile", &in, 1u, 0u), LOX_ERR_STORAGE);
    power_loss_reset_to_durable();
    aligned_crash_reopen();
    ASSERT_EQ(lox_kv_get(&g_db, "volatile", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(aligned_recovery_clean_reopen_preserves_value) {
    uint8_t in = 33u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_set(&g_db, "stable", &in, 1u, 0u), LOX_OK);
    aligned_close_db_clean();
    aligned_open_db();
    ASSERT_EQ(lox_kv_get(&g_db, "stable", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, in);
}

int main(void) {
    MDB_RUN_TEST(setup_fixture, teardown_fixture, aligned_recovery_wal_replays_after_power_loss);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, aligned_recovery_sync_failure_does_not_commit_value);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, aligned_recovery_clean_reopen_preserves_value);
    return MDB_RESULT();
}
