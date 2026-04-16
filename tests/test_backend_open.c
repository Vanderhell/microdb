#include "microtest.h"
#include "microdb_backend_adapter.h"
#include "microdb_backend_open.h"

#include <string.h>

int microdb_backend_aligned_stub_register(void);

typedef struct {
    uint8_t mem[256];
    uint32_t write_calls;
} raw_ctx_t;

static raw_ctx_t g_raw;

static microdb_err_t raw_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL || buf == NULL || ((size_t)offset + len) > sizeof(raw->mem)) {
        return MICRODB_ERR_STORAGE;
    }
    memcpy(buf, raw->mem + offset, len);
    return MICRODB_OK;
}

static microdb_err_t raw_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL || buf == NULL || ((size_t)offset + len) > sizeof(raw->mem)) {
        return MICRODB_ERR_STORAGE;
    }
    if ((offset % 16u) != 0u || (len % 16u) != 0u || len == 0u) {
        return MICRODB_ERR_STORAGE;
    }
    memcpy(raw->mem + offset, buf, len);
    raw->write_calls++;
    return MICRODB_OK;
}

static microdb_err_t raw_erase(void *ctx, uint32_t offset) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL || offset >= sizeof(raw->mem)) {
        return MICRODB_ERR_STORAGE;
    }
    return MICRODB_OK;
}

static microdb_err_t raw_sync(void *ctx) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL) {
        return MICRODB_ERR_STORAGE;
    }
    return MICRODB_OK;
}

static microdb_storage_capability_t byte_capability(void) {
    microdb_storage_capability_t cap;
    memset(&cap, 0, sizeof(cap));
    cap.backend_class = MICRODB_BACKEND_CLASS_BYTE;
    cap.minimal_write_unit = 1u;
    cap.erase_granularity = 256u;
    cap.atomic_write_granularity = 1u;
    cap.sync_semantics = MICRODB_SYNC_SEMANTICS_DURABLE_SYNC;
    return cap;
}

static void setup_open(void) {
    memset(&g_raw, 0, sizeof(g_raw));
    memset(g_raw.mem, 0xFF, sizeof(g_raw.mem));
    microdb_backend_registry_reset();
}

static void teardown_open(void) {
    microdb_backend_registry_reset();
}

MDB_TEST(backend_open_unknown_backend_rejected) {
    microdb_storage_t raw = { raw_read, raw_write, raw_erase, raw_sync, (uint32_t)sizeof(g_raw.mem), 64u, 16u, &g_raw };
    microdb_backend_open_session_t session;
    microdb_storage_t *effective = NULL;
    microdb_err_t rc = microdb_backend_open_prepare("missing", &raw, 1u, 0u, &session, &effective);
    ASSERT_EQ(rc, MICRODB_ERR_INVALID);
    ASSERT_EQ(effective == NULL, 1);
    ASSERT_EQ(session.last_decision.reason, MICRODB_BACKEND_REASON_BACKEND_NOT_REGISTERED);
}

MDB_TEST(backend_open_byte_backend_direct_passthrough) {
    microdb_storage_t raw = { raw_read, raw_write, raw_erase, raw_sync, (uint32_t)sizeof(g_raw.mem), 64u, 1u, &g_raw };
    microdb_backend_open_session_t session;
    microdb_storage_t *effective = NULL;
    microdb_backend_adapter_t byte_adapter = { "byte_stub", { 0 } };
    byte_adapter.capability = byte_capability();

    ASSERT_EQ(microdb_backend_registry_register(&byte_adapter), 0);
    ASSERT_EQ(microdb_backend_open_prepare("byte_stub", &raw, 0u, 0u, &session, &effective), MICRODB_OK);
    ASSERT_EQ(effective == &raw, 1);
    ASSERT_EQ(session.using_aligned_adapter, 0);
    microdb_backend_open_release(&session);
}

MDB_TEST(backend_open_aligned_requires_adapter_flag) {
    microdb_storage_t raw = { raw_read, raw_write, raw_erase, raw_sync, (uint32_t)sizeof(g_raw.mem), 64u, 16u, &g_raw };
    microdb_backend_open_session_t session;
    microdb_storage_t *effective = NULL;

    ASSERT_EQ(microdb_backend_aligned_stub_register(), 0);
    ASSERT_EQ(microdb_backend_open_prepare("aligned_stub", &raw, 0u, 0u, &session, &effective), MICRODB_ERR_INVALID);
    ASSERT_EQ(session.last_decision.reason, MICRODB_BACKEND_REASON_MISSING_ALIGNED_ADAPTER);
}

MDB_TEST(backend_open_aligned_uses_adapter_shim) {
    static const uint8_t payload[3] = { 0xA1u, 0xA2u, 0xA3u };
    microdb_storage_t raw = { raw_read, raw_write, raw_erase, raw_sync, (uint32_t)sizeof(g_raw.mem), 64u, 16u, &g_raw };
    microdb_backend_open_session_t session;
    microdb_storage_t *effective = NULL;

    ASSERT_EQ(microdb_backend_aligned_stub_register(), 0);
    ASSERT_EQ(microdb_backend_open_prepare("aligned_stub", &raw, 1u, 0u, &session, &effective), MICRODB_OK);
    ASSERT_EQ(session.using_aligned_adapter, 1);
    ASSERT_EQ(effective != NULL, 1);
    ASSERT_EQ(effective->write_size, 1);

    ASSERT_EQ(effective->write(effective->ctx, 5u, payload, sizeof(payload)), MICRODB_OK);
    ASSERT_EQ(g_raw.write_calls, 1);
    ASSERT_EQ(g_raw.mem[5], 0xA1);
    ASSERT_EQ(g_raw.mem[7], 0xA3);

    microdb_backend_open_release(&session);
    ASSERT_EQ(session.using_aligned_adapter, 0);
}

int main(void) {
    MDB_RUN_TEST(setup_open, teardown_open, backend_open_unknown_backend_rejected);
    MDB_RUN_TEST(setup_open, teardown_open, backend_open_byte_backend_direct_passthrough);
    MDB_RUN_TEST(setup_open, teardown_open, backend_open_aligned_requires_adapter_flag);
    MDB_RUN_TEST(setup_open, teardown_open, backend_open_aligned_uses_adapter_shim);
    return MDB_RESULT();
}
