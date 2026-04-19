// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb_backend_aligned_adapter.h"

#include <string.h>

typedef struct {
    uint8_t mem[256];
    uint32_t write_calls;
    uint32_t erase_calls;
    uint32_t sync_calls;
} raw_ctx_t;

static raw_ctx_t g_raw;
static microdb_storage_t g_raw_storage;
static microdb_storage_t g_adapted_storage;
static microdb_backend_aligned_adapter_ctx_t g_adapter_ctx;

static microdb_err_t raw_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL || buf == NULL || ((size_t)offset + len) > sizeof(raw->mem)) {
        return MICRODB_ERR_STORAGE;
    }
    if ((offset % 16u) != 0u || (len % 16u) != 0u || len == 0u) {
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
    uint32_t block_start;
    if (raw == NULL || offset >= sizeof(raw->mem)) {
        return MICRODB_ERR_STORAGE;
    }
    block_start = (offset / 64u) * 64u;
    memset(raw->mem + block_start, 0xFF, 64u);
    raw->erase_calls++;
    return MICRODB_OK;
}

static microdb_err_t raw_sync(void *ctx) {
    raw_ctx_t *raw = (raw_ctx_t *)ctx;
    if (raw == NULL) {
        return MICRODB_ERR_STORAGE;
    }
    raw->sync_calls++;
    return MICRODB_OK;
}

static void setup_storage(void) {
    memset(&g_raw, 0, sizeof(g_raw));
    memset(&g_raw_storage, 0, sizeof(g_raw_storage));
    memset(&g_adapted_storage, 0, sizeof(g_adapted_storage));
    memset(&g_adapter_ctx, 0, sizeof(g_adapter_ctx));
    memset(g_raw.mem, 0xFF, sizeof(g_raw.mem));

    g_raw_storage.read = raw_read;
    g_raw_storage.write = raw_write;
    g_raw_storage.erase = raw_erase;
    g_raw_storage.sync = raw_sync;
    g_raw_storage.capacity = (uint32_t)sizeof(g_raw.mem);
    g_raw_storage.erase_size = 64u;
    g_raw_storage.write_size = 16u;
    g_raw_storage.ctx = &g_raw;
}

static void teardown_storage(void) {
    microdb_backend_aligned_adapter_deinit(&g_adapted_storage);
}

MDB_TEST(aligned_adapter_exposes_byte_write_to_core) {
    static const uint8_t payload[5] = { 0x11u, 0x22u, 0x33u, 0x44u, 0x55u };
    ASSERT_EQ(microdb_backend_aligned_adapter_init(&g_adapted_storage, &g_adapter_ctx, &g_raw_storage), MICRODB_OK);
    ASSERT_EQ(g_adapted_storage.write_size, 1);
    ASSERT_EQ(g_adapted_storage.erase_size, 64);

    ASSERT_EQ(g_adapted_storage.write(g_adapted_storage.ctx, 3u, payload, sizeof(payload)), MICRODB_OK);
    ASSERT_EQ(g_raw.write_calls, 1);
    ASSERT_EQ(g_raw.mem[2], 0xFF);
    ASSERT_EQ(g_raw.mem[3], 0x11);
    ASSERT_EQ(g_raw.mem[7], 0x55);
    ASSERT_EQ(g_raw.mem[8], 0xFF);
}

MDB_TEST(aligned_adapter_handles_cross_boundary_rmw) {
    static const uint8_t payload[4] = { 0xAAu, 0xBBu, 0xCCu, 0xDDu };
    ASSERT_EQ(microdb_backend_aligned_adapter_init(&g_adapted_storage, &g_adapter_ctx, &g_raw_storage), MICRODB_OK);

    ASSERT_EQ(g_adapted_storage.write(g_adapted_storage.ctx, 14u, payload, sizeof(payload)), MICRODB_OK);
    ASSERT_EQ(g_raw.write_calls, 2);
    ASSERT_EQ(g_raw.mem[13], 0xFF);
    ASSERT_EQ(g_raw.mem[14], 0xAA);
    ASSERT_EQ(g_raw.mem[15], 0xBB);
    ASSERT_EQ(g_raw.mem[16], 0xCC);
    ASSERT_EQ(g_raw.mem[17], 0xDD);
    ASSERT_EQ(g_raw.mem[18], 0xFF);
}

MDB_TEST(aligned_adapter_forwards_erase_and_sync) {
    ASSERT_EQ(microdb_backend_aligned_adapter_init(&g_adapted_storage, &g_adapter_ctx, &g_raw_storage), MICRODB_OK);
    g_raw.mem[70] = 0x42u;

    ASSERT_EQ(g_adapted_storage.erase(g_adapted_storage.ctx, 70u), MICRODB_OK);
    ASSERT_EQ(g_raw.erase_calls, 1);
    ASSERT_EQ(g_raw.mem[70], 0xFF);

    ASSERT_EQ(g_adapted_storage.sync(g_adapted_storage.ctx), MICRODB_OK);
    ASSERT_EQ(g_raw.sync_calls, 1);
}

MDB_TEST(aligned_adapter_rejects_invalid_raw_contract) {
    microdb_storage_t invalid = g_raw_storage;
    invalid.write_size = 1u;
    ASSERT_EQ(microdb_backend_aligned_adapter_init(&g_adapted_storage, &g_adapter_ctx, &invalid), MICRODB_ERR_INVALID);
}

MDB_TEST(aligned_adapter_supports_unaligned_read_via_bounce) {
    uint8_t out[5] = { 0 };
    uint8_t i;
    ASSERT_EQ(microdb_backend_aligned_adapter_init(&g_adapted_storage, &g_adapter_ctx, &g_raw_storage), MICRODB_OK);
    for (i = 0u; i < 32u; ++i) {
        g_raw.mem[i] = (uint8_t)(i + 1u);
    }

    ASSERT_EQ(g_adapted_storage.read(g_adapted_storage.ctx, 3u, out, sizeof(out)), MICRODB_OK);
    ASSERT_EQ(out[0], 4u);
    ASSERT_EQ(out[1], 5u);
    ASSERT_EQ(out[2], 6u);
    ASSERT_EQ(out[3], 7u);
    ASSERT_EQ(out[4], 8u);
}

MDB_TEST(aligned_adapter_rejects_write_out_of_range) {
    static const uint8_t payload[8] = { 0x10u, 0x11u, 0x12u, 0x13u, 0x14u, 0x15u, 0x16u, 0x17u };
    ASSERT_EQ(microdb_backend_aligned_adapter_init(&g_adapted_storage, &g_adapter_ctx, &g_raw_storage), MICRODB_OK);
    ASSERT_EQ(g_adapted_storage.write(g_adapted_storage.ctx, 252u, payload, sizeof(payload)), MICRODB_ERR_STORAGE);
}

MDB_TEST(aligned_adapter_rejects_capacity_not_write_aligned) {
    microdb_storage_t invalid = g_raw_storage;
    invalid.capacity = 255u;
    ASSERT_EQ(microdb_backend_aligned_adapter_init(&g_adapted_storage, &g_adapter_ctx, &invalid), MICRODB_ERR_INVALID);
}

MDB_TEST(aligned_adapter_rejects_erase_not_write_aligned) {
    microdb_storage_t invalid = g_raw_storage;
    invalid.erase_size = 18u;
    ASSERT_EQ(microdb_backend_aligned_adapter_init(&g_adapted_storage, &g_adapter_ctx, &invalid), MICRODB_ERR_INVALID);
}

int main(void) {
    MDB_RUN_TEST(setup_storage, teardown_storage, aligned_adapter_exposes_byte_write_to_core);
    MDB_RUN_TEST(setup_storage, teardown_storage, aligned_adapter_handles_cross_boundary_rmw);
    MDB_RUN_TEST(setup_storage, teardown_storage, aligned_adapter_forwards_erase_and_sync);
    MDB_RUN_TEST(setup_storage, teardown_storage, aligned_adapter_rejects_invalid_raw_contract);
    MDB_RUN_TEST(setup_storage, teardown_storage, aligned_adapter_supports_unaligned_read_via_bounce);
    MDB_RUN_TEST(setup_storage, teardown_storage, aligned_adapter_rejects_write_out_of_range);
    MDB_RUN_TEST(setup_storage, teardown_storage, aligned_adapter_rejects_capacity_not_write_aligned);
    MDB_RUN_TEST(setup_storage, teardown_storage, aligned_adapter_rejects_erase_not_write_aligned);
    return MDB_RESULT();
}
