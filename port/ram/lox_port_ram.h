// SPDX-License-Identifier: MIT
#ifndef LOX_PORT_RAM_H
#define LOX_PORT_RAM_H

#include "lox.h"

typedef struct {
    uint8_t *buf;
    uint32_t capacity;
    uint32_t erase_size;
} lox_port_ram_ctx_t;

lox_err_t lox_port_ram_init(lox_storage_t *storage, uint32_t capacity);
void lox_port_ram_deinit(lox_storage_t *storage);

#endif
