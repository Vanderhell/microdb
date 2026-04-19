// SPDX-License-Identifier: MIT
#include <stdint.h>

#include "microdb_backend_adapter.h"

static const microdb_backend_adapter_t g_block_adapter = {
    "block_stub",
    {
        MICRODB_BACKEND_CLASS_MANAGED,
        1u,
        512u,
        1u,
        MICRODB_SYNC_SEMANTICS_NONE,
        0u
    }
};

const char *microdb_backend_block_stub_id(void) {
    return "microdb_backend_block_stub";
}

int microdb_backend_block_stub_marker(void) {
    return (int)(uint8_t)0x42u;
}

const microdb_backend_adapter_t *microdb_backend_block_stub_adapter(void) {
    return &g_block_adapter;
}

int microdb_backend_block_stub_register(void) {
    return microdb_backend_registry_register(&g_block_adapter);
}
