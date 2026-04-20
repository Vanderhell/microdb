// SPDX-License-Identifier: MIT
#include "microdb_backend_adapter.h"

#include <string.h>

#define MICRODB_BACKEND_REGISTRY_MAX 16u

static const microdb_backend_adapter_t *g_registry[MICRODB_BACKEND_REGISTRY_MAX];
static size_t g_registry_count = 0u;

void microdb_backend_registry_reset(void) {
    size_t i;
    for (i = 0u; i < MICRODB_BACKEND_REGISTRY_MAX; ++i) {
        g_registry[i] = NULL;
    }
    g_registry_count = 0u;
}

microdb_backend_registry_status_t microdb_backend_registry_register(const microdb_backend_adapter_t *adapter) {
    size_t i;

    if (adapter == NULL || adapter->name == NULL || adapter->name[0] == '\0') {
        return MICRODB_BACKEND_REGISTRY_ERR_INVALID;
    }

    for (i = 0u; i < g_registry_count; ++i) {
        if (strcmp(g_registry[i]->name, adapter->name) == 0) {
            return MICRODB_BACKEND_REGISTRY_OK;
        }
    }

    if (g_registry_count >= MICRODB_BACKEND_REGISTRY_MAX) {
        return MICRODB_BACKEND_REGISTRY_ERR_FULL;
    }

    g_registry[g_registry_count++] = adapter;
    return MICRODB_BACKEND_REGISTRY_OK;
}

size_t microdb_backend_registry_count(void) {
    return g_registry_count;
}

const microdb_backend_adapter_t *microdb_backend_registry_get(size_t index) {
    if (index >= g_registry_count) {
        return NULL;
    }
    return g_registry[index];
}

const microdb_backend_adapter_t *microdb_backend_registry_find(const char *name) {
    size_t i;

    if (name == NULL || name[0] == '\0') {
        return NULL;
    }

    for (i = 0u; i < g_registry_count; ++i) {
        if (strcmp(g_registry[i]->name, name) == 0) {
            return g_registry[i];
        }
    }
    return NULL;
}
