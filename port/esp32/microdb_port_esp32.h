// SPDX-License-Identifier: MIT
#ifndef MICRODB_PORT_ESP32_H
#define MICRODB_PORT_ESP32_H

#include "microdb.h"

#if defined(IDF_TARGET)
#include "esp_partition.h"
#endif

typedef struct {
#if defined(IDF_TARGET)
    const esp_partition_t *partition;
#else
    const void *partition;
#endif
} microdb_esp32_ctx_t;

microdb_err_t microdb_port_esp32_init(microdb_storage_t *storage, const char *partition_label);
void microdb_port_esp32_deinit(microdb_storage_t *storage);

#endif
