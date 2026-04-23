// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_backend_adapter.h"
#include "lox_backend_open.h"
#include "../src/lox_internal.h"

#include <stdlib.h>
#include <string.h>

int lox_backend_fs_stub_register(void);
int lox_backend_block_stub_register(void);

enum {
    FS_CAPACITY = 131072u,
    FS_ERASE_SIZE = 4096u
};

typedef struct {
    uint8_t durable[FS_CAPACITY];
    uint8_t working[FS_CAPACITY];
    uint32_t sync_calls;
    uint8_t fail_next_sync;
    uint8_t write_through;
} fs_mem_ctx_t;

static fs_mem_ctx_t g_media;
static lox_storage_t g_raw_storage;
static lox_storage_t *g_effective_storage = NULL;
static lox_backend_open_session_t g_open_session;
static lox_t g_db;
static uint32_t g_now = 7000u;
static const char *g_backend_name = NULL;

static lox_timestamp_t mock_now(void) {
    return g_now++;
}

static lox_err_t fs_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    fs_mem_ctx_t *m = (fs_mem_ctx_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > FS_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    memcpy(buf, m->working + offset, len);
    return LOX_OK;
}

static lox_err_t fs_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    fs_mem_ctx_t *m = (fs_mem_ctx_t *)ctx;
    if (m == NULL || buf == NULL || ((size_t)offset + len) > FS_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    memcpy(m->working + offset, buf, len);
    if (m->write_through != 0u) {
        memcpy(m->durable + offset, buf, len);
    }
    return LOX_OK;
}

static lox_err_t fs_erase(void *ctx, uint32_t offset) {
    fs_mem_ctx_t *m = (fs_mem_ctx_t *)ctx;
    uint32_t base;
    if (m == NULL || offset >= FS_CAPACITY) {
        return LOX_ERR_STORAGE;
    }
    base = (offset / FS_ERASE_SIZE) * FS_ERASE_SIZE;
    memset(m->working + base, 0xFF, FS_ERASE_SIZE);
    if (m->write_through != 0u) {
        memset(m->durable + base, 0xFF, FS_ERASE_SIZE);
    }
    return LOX_OK;
}

static lox_err_t fs_sync(void *ctx) {
    fs_mem_ctx_t *m = (fs_mem_ctx_t *)ctx;
    if (m == NULL) {
        return LOX_ERR_STORAGE;
    }
    m->sync_calls++;
    if (m->fail_next_sync != 0u) {
        m->fail_next_sync = 0u;
        return LOX_ERR_STORAGE;
    }
    memcpy(m->durable, m->working, FS_CAPACITY);
    return LOX_OK;
}

static void power_loss_reset_to_durable(void) {
    memcpy(g_media.working, g_media.durable, FS_CAPACITY);
}

static void open_db(const char *backend_name, uint8_t write_through) {
    lox_cfg_t cfg;

    g_backend_name = backend_name;
    g_media.write_through = write_through;
    memset(&g_db, 0, sizeof(g_db));
    g_effective_storage = NULL;
    ASSERT_EQ(lox_backend_open_prepare(backend_name, &g_raw_storage, 0u, 1u, &g_open_session, &g_effective_storage), LOX_OK);
    ASSERT_EQ(g_open_session.using_fs_adapter, 1u);
    ASSERT_EQ(g_open_session.using_managed_adapter, 0u);
    ASSERT_EQ(g_effective_storage != NULL, 1);
    ASSERT_EQ(g_effective_storage->write_size, 1u);

    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = g_effective_storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void close_db_clean(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    }
    lox_backend_open_release(&g_open_session);
    g_effective_storage = NULL;
    memset(&g_db, 0, sizeof(g_db));
}

static void crash_reopen(void) {
    uint8_t write_through = g_media.write_through;
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        free(lox_core(&g_db)->heap);
    }
    lox_backend_open_release(&g_open_session);
    memset(&g_db, 0, sizeof(g_db));
    open_db(g_backend_name, write_through);
}

static void setup_fixture(void) {
    memset(&g_media, 0, sizeof(g_media));
    memset(g_media.durable, 0xFF, sizeof(g_media.durable));
    memcpy(g_media.working, g_media.durable, sizeof(g_media.working));
    memset(&g_raw_storage, 0, sizeof(g_raw_storage));
    memset(&g_open_session, 0, sizeof(g_open_session));
    g_effective_storage = NULL;
    g_backend_name = NULL;
    g_now = 7000u;

    g_raw_storage.read = fs_read;
    g_raw_storage.write = fs_write;
    g_raw_storage.erase = fs_erase;
    g_raw_storage.sync = fs_sync;
    g_raw_storage.capacity = FS_CAPACITY;
    g_raw_storage.erase_size = FS_ERASE_SIZE;
    g_raw_storage.write_size = 1u;
    g_raw_storage.ctx = &g_media;

    lox_backend_registry_reset();
    ASSERT_EQ(lox_backend_fs_stub_register(), 0);
    ASSERT_EQ(lox_backend_block_stub_register(), 0);
}

static void teardown_fixture(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        (void)lox_deinit(&g_db);
    }
    lox_backend_open_release(&g_open_session);
    lox_backend_registry_reset();
}

MDB_TEST(fs_recovery_wal_replays_after_power_loss) {
    uint8_t in = 57u;
    uint8_t out = 0u;

    open_db("fs_stub", 0u);
    ASSERT_EQ(lox_kv_set(&g_db, "fs-k", &in, 1u, 0u), LOX_OK);
    ASSERT_EQ(g_media.sync_calls > 0u, 1);
    power_loss_reset_to_durable();
    crash_reopen();
    ASSERT_EQ(lox_kv_get(&g_db, "fs-k", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, in);
}

MDB_TEST(fs_recovery_sync_failure_does_not_commit_value) {
    uint8_t in = 19u;
    uint8_t out = 0u;

    open_db("fs_stub", 0u);
    g_media.fail_next_sync = 1u;
    ASSERT_EQ(lox_kv_set(&g_db, "volatile", &in, 1u, 0u), LOX_ERR_STORAGE);
    power_loss_reset_to_durable();
    crash_reopen();
    ASSERT_EQ(lox_kv_get(&g_db, "volatile", &out, 1u, NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(block_recovery_reopen_preserves_value_without_raw_sync) {
    uint8_t in = 81u;
    uint8_t out = 0u;

    open_db("block_stub", 1u);
    ASSERT_EQ(g_media.sync_calls, 0u);
    ASSERT_EQ(lox_kv_set(&g_db, "blk-k", &in, 1u, 0u), LOX_OK);
    ASSERT_EQ(g_media.sync_calls, 0u);
    power_loss_reset_to_durable();
    crash_reopen();
    ASSERT_EQ(lox_kv_get(&g_db, "blk-k", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, in);
    ASSERT_EQ(g_media.sync_calls, 0u);
}

MDB_TEST(fs_recovery_clean_reopen_preserves_value) {
    uint8_t in = 43u;
    uint8_t out = 0u;

    open_db("fs_stub", 0u);
    ASSERT_EQ(lox_kv_set(&g_db, "stable", &in, 1u, 0u), LOX_OK);
    close_db_clean();
    open_db("fs_stub", 0u);
    ASSERT_EQ(lox_kv_get(&g_db, "stable", &out, 1u, NULL), LOX_OK);
    ASSERT_EQ(out, in);
}

int main(void) {
    MDB_RUN_TEST(setup_fixture, teardown_fixture, fs_recovery_wal_replays_after_power_loss);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, fs_recovery_sync_failure_does_not_commit_value);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, block_recovery_reopen_preserves_value_without_raw_sync);
    MDB_RUN_TEST(setup_fixture, teardown_fixture, fs_recovery_clean_reopen_preserves_value);
    return MDB_RESULT();
}
