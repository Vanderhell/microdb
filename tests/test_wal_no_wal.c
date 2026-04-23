// SPDX-License-Identifier: MIT
#define LOX_ENABLE_WAL 0
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"

#include <string.h>

static lox_t g_db;
static lox_storage_t g_storage;
static const char *g_path = "wal_no_wal.bin";

static void setup_db(void) {
    lox_cfg_t cfg;

    memset(&cfg, 0, sizeof(cfg));
    lox_port_posix_remove(g_path);
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 65536u), LOX_OK);
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    (void)lox_deinit(&g_db);
    lox_port_posix_deinit(&g_storage);
    lox_port_posix_remove(g_path);
}

MDB_TEST(wal_disabled_writes_direct_without_wal_header) {
    uint8_t value = 1u;
    uint8_t out = 0u;
    uint32_t magic = 0u;

    ASSERT_EQ(lox_kv_set(&g_db, "direct", &value, 1u, 0u), LOX_OK);
    ASSERT_EQ(g_storage.read(g_storage.ctx, 0u, &magic, sizeof(magic)), LOX_OK);
    ASSERT_EQ(magic == 0x4D44424Cu, 0);
    ASSERT_EQ(lox_deinit(&g_db), LOX_OK);
    lox_port_posix_deinit(&g_storage);
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_path, 65536u), LOX_OK);
    {
        lox_cfg_t cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.storage = &g_storage;
        cfg.ram_kb = 32u;
        cfg.now = NULL;
        ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
    }
    ASSERT_EQ(lox_kv_get(&g_db, "direct", &out, 1u, NULL), LOX_OK);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, wal_disabled_writes_direct_without_wal_header);
    return MDB_RESULT();
}
