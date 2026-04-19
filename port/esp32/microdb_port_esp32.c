// SPDX-License-Identifier: MIT
#include "microdb_port_esp32.h"

#include <stddef.h>
#include <string.h>

#if defined(IDF_TARGET)

static microdb_esp32_ctx_t g_microdb_esp32_ctx;

static microdb_err_t microdb_port_esp32_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    microdb_esp32_ctx_t *esp32 = (microdb_esp32_ctx_t *)ctx;

    if (esp32 == NULL || esp32->partition == NULL || buf == NULL) {
        return MICRODB_ERR_INVALID;
    }

    return (esp_partition_read(esp32->partition, offset, buf, len) == ESP_OK) ? MICRODB_OK : MICRODB_ERR_STORAGE;
}

static microdb_err_t microdb_port_esp32_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    microdb_esp32_ctx_t *esp32 = (microdb_esp32_ctx_t *)ctx;

    if (esp32 == NULL || esp32->partition == NULL || buf == NULL) {
        return MICRODB_ERR_INVALID;
    }

    return (esp_partition_write(esp32->partition, offset, buf, len) == ESP_OK) ? MICRODB_OK : MICRODB_ERR_STORAGE;
}

static microdb_err_t microdb_port_esp32_erase(void *ctx, uint32_t offset) {
    microdb_esp32_ctx_t *esp32 = (microdb_esp32_ctx_t *)ctx;

    if (esp32 == NULL || esp32->partition == NULL) {
        return MICRODB_ERR_INVALID;
    }

    return (esp_partition_erase_range(esp32->partition, offset, esp32->partition->erase_size) == ESP_OK)
               ? MICRODB_OK
               : MICRODB_ERR_STORAGE;
}

static microdb_err_t microdb_port_esp32_sync(void *ctx) {
    (void)ctx;
    return MICRODB_OK;
}

microdb_err_t microdb_port_esp32_init(microdb_storage_t *storage, const char *partition_label) {
    if (storage == NULL || partition_label == NULL || partition_label[0] == '\0') {
        return MICRODB_ERR_INVALID;
    }

    memset(storage, 0, sizeof(*storage));
    memset(&g_microdb_esp32_ctx, 0, sizeof(g_microdb_esp32_ctx));
    g_microdb_esp32_ctx.partition =
        esp_partition_find_first(ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_ANY, partition_label);
    if (g_microdb_esp32_ctx.partition == NULL) {
        return MICRODB_ERR_STORAGE;
    }

    storage->read = microdb_port_esp32_read;
    storage->write = microdb_port_esp32_write;
    storage->erase = microdb_port_esp32_erase;
    storage->sync = microdb_port_esp32_sync;
    storage->capacity = g_microdb_esp32_ctx.partition->size;
    storage->erase_size = g_microdb_esp32_ctx.partition->erase_size;
    storage->write_size = 1u;
    storage->ctx = &g_microdb_esp32_ctx;
    return MICRODB_OK;
}

void microdb_port_esp32_deinit(microdb_storage_t *storage) {
    if (storage != NULL) {
        memset(storage, 0, sizeof(*storage));
    }
    memset(&g_microdb_esp32_ctx, 0, sizeof(g_microdb_esp32_ctx));
}

#else

microdb_err_t microdb_port_esp32_init(microdb_storage_t *storage, const char *partition_label) {
    (void)storage;
    (void)partition_label;
    return MICRODB_ERR_DISABLED;
}

void microdb_port_esp32_deinit(microdb_storage_t *storage) {
    (void)storage;
}

#endif
