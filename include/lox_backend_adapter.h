// SPDX-License-Identifier: MIT
#ifndef LOX_BACKEND_ADAPTER_H
#define LOX_BACKEND_ADAPTER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    LOX_BACKEND_CLASS_BYTE = 0,
    LOX_BACKEND_CLASS_ALIGNED = 1,
    LOX_BACKEND_CLASS_MANAGED = 2
} lox_backend_class_t;

typedef enum {
    LOX_SYNC_SEMANTICS_NONE = 0,
    LOX_SYNC_SEMANTICS_FLUSH_ONLY = 1,
    LOX_SYNC_SEMANTICS_DURABLE_SYNC = 2
} lox_sync_semantics_t;

typedef struct {
    lox_backend_class_t backend_class;
    uint32_t minimal_write_unit;
    uint32_t erase_granularity;
    uint32_t atomic_write_granularity;
    lox_sync_semantics_t sync_semantics;
    uint8_t is_managed;
} lox_storage_capability_t;

typedef struct {
    const char *name;
    lox_storage_capability_t capability;
} lox_backend_adapter_t;

typedef enum {
    LOX_BACKEND_REGISTRY_OK = 0,
    LOX_BACKEND_REGISTRY_ERR_INVALID = -1,
    LOX_BACKEND_REGISTRY_ERR_FULL = -2
} lox_backend_registry_status_t;

void lox_backend_registry_reset(void);
lox_backend_registry_status_t lox_backend_registry_register(const lox_backend_adapter_t *adapter);
size_t lox_backend_registry_count(void);
const lox_backend_adapter_t *lox_backend_registry_get(size_t index);
const lox_backend_adapter_t *lox_backend_registry_find(const char *name);

#ifdef __cplusplus
}
#endif

#endif
