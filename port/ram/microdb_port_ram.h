// SPDX-License-Identifier: MIT
#ifndef MICRODB_PORT_RAM_H
#define MICRODB_PORT_RAM_H

#include "microdb.h"

typedef struct {
    uint8_t *buf;
    uint32_t capacity;
    uint32_t erase_size;
} microdb_port_ram_ctx_t;

microdb_err_t microdb_port_ram_init(microdb_storage_t *storage, uint32_t capacity);
void microdb_port_ram_deinit(microdb_storage_t *storage);

#endif
