// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox_backend_managed_adapter.h"

#include <string.h>

typedef struct {
    uint8_t mem[128];
    uint32_t write_calls;
    uint32_t erase_calls;
    uint32_t sync_calls;
    lox_err_t sync_status;
} raw_ctx_t;

static raw_ctx_t g_raw;
static lox_storage_t g_raw_storage;
static lox_storage_t g_adapted_storage;
static lox_backend_managed_adapter_ctx_t g_adapter_ctx;

static lox_err_t raw_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL || buf == NULL || ((size_t)offset + len) > sizeof(raw->mem)) {
        return LOX_ERR_STORAGE;
    }
    memcpy(buf, raw->mem + offset, len);
    return LOX_OK;
}

static lox_err_t raw_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL || buf == NULL || ((size_t)offset + len) > sizeof(raw->mem)) {
        return LOX_ERR_STORAGE;
    }
    memcpy(raw->mem + offset, buf, len);
    raw->write_calls++;
    return LOX_OK;
}

static lox_err_t raw_erase(void *ctx, uint32_t offset) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL || offset >= sizeof(raw->mem)) {
        return LOX_ERR_STORAGE;
    }
    memset(raw->mem + ((offset / 32u) * 32u), 0xFF, 32u);
    raw->erase_calls++;
    return LOX_OK;
}

static lox_err_t raw_sync(void *ctx) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL) {
        return LOX_ERR_STORAGE;
    }
    raw->sync_calls++;
    return raw->sync_status;
}

static void setup_storage(void) {
    memset(&g_raw, 0, sizeof(g_raw));
    memset(g_raw.mem, 0xFF, sizeof(g_raw.mem));
    g_raw.sync_status = LOX_OK;
    memset(&g_raw_storage, 0, sizeof(g_raw_storage));
    memset(&g_adapted_storage, 0, sizeof(g_adapted_storage));
    memset(&g_adapter_ctx, 0, sizeof(g_adapter_ctx));

    g_raw_storage.read = raw_read;
    g_raw_storage.write = raw_write;
    g_raw_storage.erase = raw_erase;
    g_raw_storage.sync = raw_sync;
    g_raw_storage.capacity = (uint32_t)sizeof(g_raw.mem);
    g_raw_storage.erase_size = 32u;
    g_raw_storage.write_size = 1u;
    g_raw_storage.ctx = &g_raw;
}

static void teardown_storage(void) {
    lox_backend_managed_adapter_deinit(&g_adapted_storage);
}

MDB_TEST(managed_adapter_passthrough_io) {
    static const uint8_t payload[4] = { 1u, 2u, 3u, 4u };
    ASSERT_EQ(lox_backend_managed_adapter_init(&g_adapted_storage, &g_adapter_ctx, &g_raw_storage), LOX_OK);
    ASSERT_EQ(g_adapted_storage.write_size, 1);

    ASSERT_EQ(g_adapted_storage.write(g_adapted_storage.ctx, 5u, payload, sizeof(payload)), LOX_OK);
    ASSERT_EQ(g_raw.write_calls, 1);
    ASSERT_EQ(g_raw.mem[5], 1);
    ASSERT_EQ(g_raw.mem[8], 4);

    ASSERT_EQ(g_adapted_storage.erase(g_adapted_storage.ctx, 40u), LOX_OK);
    ASSERT_EQ(g_raw.erase_calls, 1);

    ASSERT_EQ(g_adapted_storage.sync(g_adapted_storage.ctx), LOX_OK);
    ASSERT_EQ(g_raw.sync_calls, 2);
}

MDB_TEST(managed_adapter_rejects_non_byte_raw_write_size) {
    lox_storage_t invalid = g_raw_storage;
    invalid.write_size = 8u;
    ASSERT_EQ(lox_backend_managed_adapter_init(&g_adapted_storage, &g_adapter_ctx, &invalid), LOX_ERR_INVALID);
}

MDB_TEST(managed_adapter_mount_probe_fails_when_sync_fails) {
    g_raw.sync_status = LOX_ERR_STORAGE;
    ASSERT_EQ(lox_backend_managed_adapter_init(&g_adapted_storage, &g_adapter_ctx, &g_raw_storage), LOX_ERR_STORAGE);
    ASSERT_EQ(g_raw.sync_calls, 1);
}

MDB_TEST(managed_adapter_relaxed_expectations_allow_non_byte_write) {
    lox_storage_t invalid = g_raw_storage;
    lox_backend_managed_expectations_t relaxed;
    invalid.write_size = 8u;
    lox_backend_managed_expectations_default(&relaxed);
    relaxed.require_byte_write = 0u;
    relaxed.require_sync_probe_on_mount = 0u;
    ASSERT_EQ(lox_backend_managed_adapter_init_with_expectations(&g_adapted_storage, &g_adapter_ctx, &invalid, &relaxed),
              LOX_OK);
    ASSERT_EQ(g_raw.sync_calls, 0);
}

int main(void) {
    MDB_RUN_TEST(setup_storage, teardown_storage, managed_adapter_passthrough_io);
    MDB_RUN_TEST(setup_storage, teardown_storage, managed_adapter_rejects_non_byte_raw_write_size);
    MDB_RUN_TEST(setup_storage, teardown_storage, managed_adapter_mount_probe_fails_when_sync_fails);
    MDB_RUN_TEST(setup_storage, teardown_storage, managed_adapter_relaxed_expectations_allow_non_byte_write);
    return MDB_RESULT();
}
