// SPDX-License-Identifier: MIT
#ifndef LOX_PORT_ESP32_H
#define LOX_PORT_ESP32_H

#include "lox.h"

#if defined(IDF_TARGET)
#include "esp_partition.h"
#endif

typedef struct {
#if defined(IDF_TARGET)
    const esp_partition_t *partition;
#else
    const void *partition;
#endif
} lox_esp32_ctx_t;

lox_err_t lox_port_esp32_init(lox_storage_t *storage, const char *partition_label);
void lox_port_esp32_deinit(lox_storage_t *storage);

#endif
