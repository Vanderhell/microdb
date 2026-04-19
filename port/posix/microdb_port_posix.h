// SPDX-License-Identifier: MIT
#ifndef MICRODB_PORT_POSIX_H
#define MICRODB_PORT_POSIX_H

#include "microdb.h"

typedef struct {
    void *file;
    char path[260];
    uint32_t capacity;
    uint32_t erase_size;
} microdb_port_posix_ctx_t;

microdb_err_t microdb_port_posix_init(microdb_storage_t *storage, const char *path, uint32_t capacity);
void microdb_port_posix_deinit(microdb_storage_t *storage);
void microdb_port_posix_simulate_power_loss(microdb_storage_t *storage);
void microdb_port_posix_remove(const char *path);

#endif
