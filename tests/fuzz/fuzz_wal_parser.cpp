// SPDX-License-Identifier: MIT
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

// We reuse the WAL inspector from the offline verifier to fuzz WAL parsing.
// Rename verifier's main() so the harness can link.
#define main lox_verify_main
#include "../../tools/lox_verify.c"
#undef main

static FILE *mem_to_tmpfile(const uint8_t *data, size_t size) {
    FILE *fp = tmpfile();
    if (!fp) {
        return nullptr;
    }
    if (size > 0) {
        (void)fwrite(data, 1, size, fp);
    }
    (void)fflush(fp);
    (void)fseek(fp, 0, SEEK_SET);
    return fp;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    // Keep runtime bounded and avoid pathological allocations/IO.
    if (!data || size == 0) {
        return 0;
    }
    if (size > (128u * 1024u)) {
        size = 128u * 1024u;
    }

    FILE *fp = mem_to_tmpfile(data, size);
    if (!fp) {
        return 0;
    }

    verify_layout_t layout;
    memset(&layout, 0, sizeof(layout));
    layout.wal_offset = 0u;
    layout.wal_size = (uint32_t)size;

    // Fuzz WAL header + entry parsing. We ignore the returned verdict; crashes/UB are what matter.
    (void)inspect_wal(fp, &layout);

    (void)fclose(fp);
    return 0;
}

