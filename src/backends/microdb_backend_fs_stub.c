// SPDX-License-Identifier: MIT
#include <stdint.h>

#include "microdb_backend_adapter.h"

static const microdb_backend_adapter_t g_fs_adapter = {
    "fs_stub",
    {
        MICRODB_BACKEND_CLASS_MANAGED,
        1u,
        4096u,
        1u,
        MICRODB_SYNC_SEMANTICS_FLUSH_ONLY,
        1u
    }
};

const char *microdb_backend_fs_stub_id(void) {
    return "microdb_backend_fs_stub";
}

int microdb_backend_fs_stub_marker(void) {
    return (int)(uint8_t)0x46u;
}

const microdb_backend_adapter_t *microdb_backend_fs_stub_adapter(void) {
    return &g_fs_adapter;
}

int microdb_backend_fs_stub_register(void) {
    return microdb_backend_registry_register(&g_fs_adapter);
}
