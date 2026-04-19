// SPDX-License-Identifier: MIT
#include <stdint.h>
#include "microdb_backend_adapter.h"

static const microdb_backend_adapter_t g_sd_adapter = {
    "sd_stub",
    {
        MICRODB_BACKEND_CLASS_MANAGED,
        1u,
        512u,
        1u,
        MICRODB_SYNC_SEMANTICS_DURABLE_SYNC,
        1u
    }
};

const char *microdb_backend_sd_stub_id(void) {
    return "microdb_backend_sd_stub";
}

int microdb_backend_sd_stub_marker(void) {
    return (int)(uint8_t)0x53u;
}

const microdb_backend_adapter_t *microdb_backend_sd_stub_adapter(void) {
    return &g_sd_adapter;
}

int microdb_backend_sd_stub_register(void) {
    return microdb_backend_registry_register(&g_sd_adapter);
}
