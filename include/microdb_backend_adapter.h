// SPDX-License-Identifier: MIT
#ifndef MICRODB_BACKEND_ADAPTER_H
#define MICRODB_BACKEND_ADAPTER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    MICRODB_BACKEND_CLASS_BYTE = 0,
    MICRODB_BACKEND_CLASS_ALIGNED = 1,
    MICRODB_BACKEND_CLASS_MANAGED = 2
} microdb_backend_class_t;

typedef enum {
    MICRODB_SYNC_SEMANTICS_NONE = 0,
    MICRODB_SYNC_SEMANTICS_FLUSH_ONLY = 1,
    MICRODB_SYNC_SEMANTICS_DURABLE_SYNC = 2
} microdb_sync_semantics_t;

typedef struct {
    microdb_backend_class_t backend_class;
    uint32_t minimal_write_unit;
    uint32_t erase_granularity;
    uint32_t atomic_write_granularity;
    microdb_sync_semantics_t sync_semantics;
    uint8_t is_managed;
} microdb_storage_capability_t;

typedef struct {
    const char *name;
    microdb_storage_capability_t capability;
} microdb_backend_adapter_t;

typedef enum {
    MICRODB_BACKEND_REGISTRY_OK = 0,
    MICRODB_BACKEND_REGISTRY_ERR_INVALID = -1,
    MICRODB_BACKEND_REGISTRY_ERR_FULL = -2
} microdb_backend_registry_status_t;

void microdb_backend_registry_reset(void);
microdb_backend_registry_status_t microdb_backend_registry_register(const microdb_backend_adapter_t *adapter);
size_t microdb_backend_registry_count(void);
const microdb_backend_adapter_t *microdb_backend_registry_get(size_t index);
const microdb_backend_adapter_t *microdb_backend_registry_find(const char *name);

#ifdef __cplusplus
}
#endif

#endif
