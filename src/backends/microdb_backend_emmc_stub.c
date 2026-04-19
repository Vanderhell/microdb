// SPDX-License-Identifier: MIT
#include <stdint.h>
#include "microdb_backend_adapter.h"

static const microdb_backend_adapter_t g_emmc_adapter = {
    "emmc_stub",
    {
        MICRODB_BACKEND_CLASS_MANAGED,
        1u,
        4096u,
        1u,
        MICRODB_SYNC_SEMANTICS_DURABLE_SYNC,
        1u
    }
};

const char *microdb_backend_emmc_stub_id(void) {
    return "microdb_backend_emmc_stub";
}

int microdb_backend_emmc_stub_marker(void) {
    return (int)(uint8_t)0x45u;
}

const microdb_backend_adapter_t *microdb_backend_emmc_stub_adapter(void) {
    return &g_emmc_adapter;
}

int microdb_backend_emmc_stub_register(void) {
    return microdb_backend_registry_register(&g_emmc_adapter);
}
