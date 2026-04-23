// SPDX-License-Identifier: MIT
#include <stdint.h>
#include "lox_backend_adapter.h"

static const lox_backend_adapter_t g_sd_adapter = {
    "sd_stub",
    {
        LOX_BACKEND_CLASS_MANAGED,
        1u,
        512u,
        1u,
        LOX_SYNC_SEMANTICS_DURABLE_SYNC,
        1u
    }
};

const char *lox_backend_sd_stub_id(void) {
    return "lox_backend_sd_stub";
}

int lox_backend_sd_stub_marker(void) {
    return (int)(uint8_t)0x53u;
}

const lox_backend_adapter_t *lox_backend_sd_stub_adapter(void) {
    return &g_sd_adapter;
}

int lox_backend_sd_stub_register(void) {
    return lox_backend_registry_register(&g_sd_adapter);
}
