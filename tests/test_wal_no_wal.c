#define MICRODB_ENABLE_WAL 0
#include "microtest.h"
#include "microdb.h"
#include "../port/posix/microdb_port_posix.h"

#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_storage;
static const char *g_path = "wal_no_wal.bin";

static void setup_db(void) {
    microdb_cfg_t cfg;

    memset(&cfg, 0, sizeof(cfg));
    microdb_port_posix_remove(g_path);
    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 65536u), MICRODB_OK);
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    (void)microdb_deinit(&g_db);
    microdb_port_posix_deinit(&g_storage);
    microdb_port_posix_remove(g_path);
}

MDB_TEST(wal_disabled_writes_direct_without_wal_header) {
    uint8_t value = 1u;
    uint8_t out = 0u;
    uint32_t magic = 0u;

    ASSERT_EQ(microdb_kv_set(&g_db, "direct", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(g_storage.read(g_storage.ctx, 0u, &magic, sizeof(magic)), MICRODB_OK);
    ASSERT_EQ(magic == 0x4D44424Cu, 0);
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_OK);
    microdb_port_posix_deinit(&g_storage);
    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 65536u), MICRODB_OK);
    {
        microdb_cfg_t cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.storage = &g_storage;
        cfg.ram_kb = 32u;
        cfg.now = NULL;
        ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
    }
    ASSERT_EQ(microdb_kv_get(&g_db, "direct", &out, 1u, NULL), MICRODB_OK);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, wal_disabled_writes_direct_without_wal_header);
    return MDB_RESULT();
}
