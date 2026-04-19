// SPDX-License-Identifier: MIT
#include "microdb.h"
#include "microdb_port_posix.h"

#include <stdint.h>
#include <string.h>

int main(void) {
    microdb_storage_t storage;
    microdb_cfg_t cfg;
    microdb_t db;
    uint8_t in = 0x2Au;
    uint8_t out = 0u;
    size_t out_len = 0u;
    const char *path = "footprint_min_baseline.bin";
    int rc = 1;

    memset(&storage, 0, sizeof(storage));
    memset(&cfg, 0, sizeof(cfg));
    memset(&db, 0, sizeof(db));
    microdb_port_posix_remove(path);

    if (microdb_port_posix_init(&storage, path, 131072u) != MICRODB_OK) {
        goto cleanup;
    }
    cfg.storage = &storage;
    cfg.ram_kb = 0u;
    if (microdb_init(&db, &cfg) != MICRODB_OK) {
        goto cleanup;
    }
    if (microdb_kv_set(&db, "k", &in, 1u, 0u) != MICRODB_OK) {
        goto cleanup;
    }
    if (microdb_kv_get(&db, "k", &out, sizeof(out), &out_len) != MICRODB_OK || out_len != 1u || out != in) {
        goto cleanup;
    }
    if (microdb_deinit(&db) != MICRODB_OK) {
        memset(&db, 0, sizeof(db));
        goto cleanup;
    }
    memset(&db, 0, sizeof(db));

    if (microdb_init(&db, &cfg) != MICRODB_OK) {
        goto cleanup;
    }
    out = 0u;
    out_len = 0u;
    if (microdb_kv_get(&db, "k", &out, sizeof(out), &out_len) != MICRODB_OK || out_len != 1u || out != in) {
        goto cleanup;
    }

    rc = 0;

cleanup:
    (void)microdb_deinit(&db);
    microdb_port_posix_deinit(&storage);
    microdb_port_posix_remove(path);
    return rc;
}
