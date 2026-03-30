#define MICRODB_KV_MAX_KEYS 128
#include "microtest.h"
#include "microdb.h"
#include "../port/posix/microdb_port_posix.h"

#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_storage;
static const char *g_path = "wal_kv128.bin";

static void setup_db(void) {
    microdb_cfg_t cfg;

    memset(&cfg, 0, sizeof(cfg));
    microdb_port_posix_remove(g_path);
    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 131072u), MICRODB_OK);
    cfg.storage = &g_storage;
    cfg.ram_kb = 64u;
    cfg.now = NULL;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    (void)microdb_deinit(&g_db);
    microdb_port_posix_deinit(&g_storage);
    microdb_port_posix_remove(g_path);
}

MDB_TEST(wal_stress_100_kv_set_flush_reinit_survive) {
    char key[16];
    uint8_t value;
    uint8_t out = 0u;
    uint32_t i;

    for (i = 0u; i < 100u; ++i) {
        memset(key, 0, sizeof(key));
        key[0] = 'k';
        key[1] = (char)('0' + (char)((i / 100u) % 10u));
        key[2] = (char)('0' + (char)((i / 10u) % 10u));
        key[3] = (char)('0' + (char)(i % 10u));
        value = (uint8_t)i;
        ASSERT_EQ(microdb_kv_set(&g_db, key, &value, 1u, 0u), MICRODB_OK);
    }

    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_OK);
    microdb_port_posix_deinit(&g_storage);
    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 131072u), MICRODB_OK);
    {
        microdb_cfg_t cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.storage = &g_storage;
        cfg.ram_kb = 64u;
        cfg.now = NULL;
        ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
    }

    for (i = 0u; i < 100u; ++i) {
        memset(key, 0, sizeof(key));
        key[0] = 'k';
        key[1] = (char)('0' + (char)((i / 100u) % 10u));
        key[2] = (char)('0' + (char)((i / 10u) % 10u));
        key[3] = (char)('0' + (char)(i % 10u));
        ASSERT_EQ(microdb_kv_get(&g_db, key, &out, 1u, NULL), MICRODB_OK);
        ASSERT_EQ(out, (uint8_t)i);
    }
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, wal_stress_100_kv_set_flush_reinit_survive);
    return MDB_RESULT();
}
