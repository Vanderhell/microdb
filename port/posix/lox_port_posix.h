// SPDX-License-Identifier: MIT
#ifndef LOX_PORT_POSIX_H
#define LOX_PORT_POSIX_H

#include "lox.h"

typedef struct {
    void *file;
    char path[260];
    uint32_t capacity;
    uint32_t erase_size;
} lox_port_posix_ctx_t;

lox_err_t lox_port_posix_init(lox_storage_t *storage, const char *path, uint32_t capacity);
void lox_port_posix_deinit(lox_storage_t *storage);
void lox_port_posix_simulate_power_loss(lox_storage_t *storage);
void lox_port_posix_remove(const char *path);

#endif
