// SPDX-License-Identifier: MIT
#include "lox.h"
#include "lox_port_posix.h"

#include <stdint.h>
#include <string.h>

int main(void) {
    lox_storage_t storage;
    lox_cfg_t cfg;
    lox_t db;
    uint8_t in = 0x2Au;
    uint8_t out = 0u;
    size_t out_len = 0u;
    const char *path = "footprint_min_baseline.bin";
    int rc = 1;

    memset(&storage, 0, sizeof(storage));
    memset(&cfg, 0, sizeof(cfg));
    memset(&db, 0, sizeof(db));
    lox_port_posix_remove(path);

    if (lox_port_posix_init(&storage, path, 131072u) != LOX_OK) {
        goto cleanup;
    }
    cfg.storage = &storage;
    cfg.ram_kb = 0u;
    if (lox_init(&db, &cfg) != LOX_OK) {
        goto cleanup;
    }
    if (lox_kv_set(&db, "k", &in, 1u, 0u) != LOX_OK) {
        goto cleanup;
    }
    if (lox_kv_get(&db, "k", &out, sizeof(out), &out_len) != LOX_OK || out_len != 1u || out != in) {
        goto cleanup;
    }
    if (lox_deinit(&db) != LOX_OK) {
        memset(&db, 0, sizeof(db));
        goto cleanup;
    }
    memset(&db, 0, sizeof(db));

    if (lox_init(&db, &cfg) != LOX_OK) {
        goto cleanup;
    }
    out = 0u;
    out_len = 0u;
    if (lox_kv_get(&db, "k", &out, sizeof(out), &out_len) != LOX_OK || out_len != 1u || out != in) {
        goto cleanup;
    }

    rc = 0;

cleanup:
    (void)lox_deinit(&db);
    lox_port_posix_deinit(&storage);
    lox_port_posix_remove(path);
    return rc;
}
