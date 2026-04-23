// SPDX-License-Identifier: MIT
#include "lox_port_esp32.h"

#include <stddef.h>
#include <string.h>

#if defined(IDF_TARGET)

static lox_esp32_ctx_t g_lox_esp32_ctx;

static lox_err_t lox_port_esp32_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    lox_esp32_ctx_t *esp32 = (lox_esp32_ctx_t *)ctx;

    if (esp32 == NULL || esp32->partition == NULL || buf == NULL) {
        return LOX_ERR_INVALID;
    }

    /* Port mapping: storage.read -> esp_partition_read */
    return (esp_partition_read(esp32->partition, offset, buf, len) == ESP_OK) ? LOX_OK : LOX_ERR_STORAGE;
}

static lox_err_t lox_port_esp32_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    lox_esp32_ctx_t *esp32 = (lox_esp32_ctx_t *)ctx;

    if (esp32 == NULL || esp32->partition == NULL || buf == NULL) {
        return LOX_ERR_INVALID;
    }

    /* Port mapping: storage.write -> esp_partition_write */
    return (esp_partition_write(esp32->partition, offset, buf, len) == ESP_OK) ? LOX_OK : LOX_ERR_STORAGE;
}

static lox_err_t lox_port_esp32_erase(void *ctx, uint32_t offset) {
    lox_esp32_ctx_t *esp32 = (lox_esp32_ctx_t *)ctx;

    if (esp32 == NULL || esp32->partition == NULL) {
        return LOX_ERR_INVALID;
    }

    /* Erase contract: one erase block per call at the requested offset. */
    return (esp_partition_erase_range(esp32->partition, offset, esp32->partition->erase_size) == ESP_OK)
               ? LOX_OK
               : LOX_ERR_STORAGE;
}

static lox_err_t lox_port_esp32_sync(void *ctx) {
    (void)ctx;
    /* ESP32 partition API path here is synchronous enough for this port contract. */
    return LOX_OK;
}

lox_err_t lox_port_esp32_init(lox_storage_t *storage, const char *partition_label) {
    if (storage == NULL || partition_label == NULL || partition_label[0] == '\0') {
        return LOX_ERR_INVALID;
    }

    memset(storage, 0, sizeof(*storage));
    memset(&g_lox_esp32_ctx, 0, sizeof(g_lox_esp32_ctx));
    g_lox_esp32_ctx.partition =
        esp_partition_find_first(ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_ANY, partition_label);
    if (g_lox_esp32_ctx.partition == NULL) {
        return LOX_ERR_STORAGE;
    }

    storage->read = lox_port_esp32_read;
    storage->write = lox_port_esp32_write;
    storage->erase = lox_port_esp32_erase;
    storage->sync = lox_port_esp32_sync;
    storage->capacity = g_lox_esp32_ctx.partition->size;
    storage->erase_size = g_lox_esp32_ctx.partition->erase_size;
    /* Core contract today requires byte-write semantics. */
    storage->write_size = 1u;
    storage->ctx = &g_lox_esp32_ctx;
    return LOX_OK;
}

void lox_port_esp32_deinit(lox_storage_t *storage) {
    if (storage != NULL) {
        memset(storage, 0, sizeof(*storage));
    }
    memset(&g_lox_esp32_ctx, 0, sizeof(g_lox_esp32_ctx));
}

#else

lox_err_t lox_port_esp32_init(lox_storage_t *storage, const char *partition_label) {
    (void)storage;
    (void)partition_label;
    return LOX_ERR_DISABLED;
}

void lox_port_esp32_deinit(lox_storage_t *storage) {
    (void)storage;
}

#endif
