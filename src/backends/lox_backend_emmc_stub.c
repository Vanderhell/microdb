// SPDX-License-Identifier: MIT
#include <stdint.h>
#include "lox_backend_adapter.h"

static const lox_backend_adapter_t g_emmc_adapter = {
    "emmc_stub",
    {
        LOX_BACKEND_CLASS_MANAGED,
        1u,
        4096u,
        1u,
        LOX_SYNC_SEMANTICS_DURABLE_SYNC,
        1u
    }
};

const char *lox_backend_emmc_stub_id(void) {
    return "lox_backend_emmc_stub";
}

int lox_backend_emmc_stub_marker(void) {
    return (int)(uint8_t)0x45u;
}

const lox_backend_adapter_t *lox_backend_emmc_stub_adapter(void) {
    return &g_emmc_adapter;
}

int lox_backend_emmc_stub_register(void) {
    return lox_backend_registry_register(&g_emmc_adapter);
}
