// SPDX-License-Identifier: MIT
#include <stdint.h>

#include "lox_backend_adapter.h"

static const lox_backend_adapter_t g_block_adapter = {
    "block_stub",
    {
        LOX_BACKEND_CLASS_MANAGED,
        1u,
        512u,
        1u,
        LOX_SYNC_SEMANTICS_NONE,
        0u
    }
};

const char *lox_backend_block_stub_id(void) {
    return "lox_backend_block_stub";
}

int lox_backend_block_stub_marker(void) {
    return (int)(uint8_t)0x42u;
}

const lox_backend_adapter_t *lox_backend_block_stub_adapter(void) {
    return &g_block_adapter;
}

int lox_backend_block_stub_register(void) {
    return lox_backend_registry_register(&g_block_adapter);
}
