// SPDX-License-Identifier: MIT
#include <stdint.h>
#include "microdb_backend_adapter.h"

static const microdb_backend_adapter_t g_nand_adapter = {
    "nand_stub",
    {
        MICRODB_BACKEND_CLASS_MANAGED,
        1u,
        4096u,
        1u,
        MICRODB_SYNC_SEMANTICS_DURABLE_SYNC,
        1u
    }
};

const char *microdb_backend_nand_stub_id(void) {
    return "microdb_backend_nand_stub";
}

int microdb_backend_nand_stub_marker(void) {
    return (int)(uint8_t)0x4Eu;
}

const microdb_backend_adapter_t *microdb_backend_nand_stub_adapter(void) {
    return &g_nand_adapter;
}

int microdb_backend_nand_stub_register(void) {
    return microdb_backend_registry_register(&g_nand_adapter);
}
