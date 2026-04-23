// SPDX-License-Identifier: MIT
#include <stdint.h>

#include "lox_backend_adapter.h"

static const lox_backend_adapter_t g_fs_adapter = {
    "fs_stub",
    {
        LOX_BACKEND_CLASS_MANAGED,
        1u,
        4096u,
        1u,
        LOX_SYNC_SEMANTICS_FLUSH_ONLY,
        1u
    }
};

const char *lox_backend_fs_stub_id(void) {
    return "lox_backend_fs_stub";
}

int lox_backend_fs_stub_marker(void) {
    return (int)(uint8_t)0x46u;
}

const lox_backend_adapter_t *lox_backend_fs_stub_adapter(void) {
    return &g_fs_adapter;
}

int lox_backend_fs_stub_register(void) {
    return lox_backend_registry_register(&g_fs_adapter);
}
