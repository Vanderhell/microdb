// SPDX-License-Identifier: MIT
#include <stdint.h>

#include "lox_backend_adapter.h"

static const lox_backend_adapter_t g_aligned_adapter = {
    "aligned_stub",
    {
        LOX_BACKEND_CLASS_ALIGNED,
        16u,
        4096u,
        16u,
        LOX_SYNC_SEMANTICS_DURABLE_SYNC,
        0u
    }
};

const char *lox_backend_aligned_stub_id(void) {
    return "lox_backend_aligned_stub";
}

int lox_backend_aligned_stub_marker(void) {
    return (int)(uint8_t)0x41u;
}

const lox_backend_adapter_t *lox_backend_aligned_stub_adapter(void) {
    return &g_aligned_adapter;
}

int lox_backend_aligned_stub_register(void) {
    return lox_backend_registry_register(&g_aligned_adapter);
}
