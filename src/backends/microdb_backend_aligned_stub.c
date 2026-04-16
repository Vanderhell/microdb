#include <stdint.h>

#include "microdb_backend_adapter.h"

static const microdb_backend_adapter_t g_aligned_adapter = {
    "aligned_stub",
    {
        MICRODB_BACKEND_CLASS_ALIGNED,
        16u,
        4096u,
        16u,
        MICRODB_SYNC_SEMANTICS_DURABLE_SYNC,
        0u
    }
};

const char *microdb_backend_aligned_stub_id(void) {
    return "microdb_backend_aligned_stub";
}

int microdb_backend_aligned_stub_marker(void) {
    return (int)(uint8_t)0x41u;
}

const microdb_backend_adapter_t *microdb_backend_aligned_stub_adapter(void) {
    return &g_aligned_adapter;
}

int microdb_backend_aligned_stub_register(void) {
    return microdb_backend_registry_register(&g_aligned_adapter);
}
