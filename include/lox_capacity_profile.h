// SPDX-License-Identifier: MIT
#ifndef LOX_CAPACITY_PROFILE_H
#define LOX_CAPACITY_PROFILE_H

#include <stdint.h>

typedef enum {
    LOX_STORAGE_PROFILE_2_MIB = 0,
    LOX_STORAGE_PROFILE_4_MIB = 1,
    LOX_STORAGE_PROFILE_8_MIB = 2,
    LOX_STORAGE_PROFILE_16_MIB = 3,
    LOX_STORAGE_PROFILE_32_MIB = 4
} lox_storage_profile_t;

#define LOX_MIB_BYTES(value_mib) ((uint32_t)(value_mib) * 1024u * 1024u)

static inline uint32_t lox_storage_profile_capacity_bytes(lox_storage_profile_t profile) {
    switch (profile) {
        case LOX_STORAGE_PROFILE_2_MIB:
            return LOX_MIB_BYTES(2u);
        case LOX_STORAGE_PROFILE_4_MIB:
            return LOX_MIB_BYTES(4u);
        case LOX_STORAGE_PROFILE_8_MIB:
            return LOX_MIB_BYTES(8u);
        case LOX_STORAGE_PROFILE_16_MIB:
            return LOX_MIB_BYTES(16u);
        case LOX_STORAGE_PROFILE_32_MIB:
            return LOX_MIB_BYTES(32u);
        default:
            return 0u;
    }
}

static inline const char *lox_storage_profile_name(lox_storage_profile_t profile) {
    switch (profile) {
        case LOX_STORAGE_PROFILE_2_MIB:
            return "2 MiB";
        case LOX_STORAGE_PROFILE_4_MIB:
            return "4 MiB";
        case LOX_STORAGE_PROFILE_8_MIB:
            return "8 MiB";
        case LOX_STORAGE_PROFILE_16_MIB:
            return "16 MiB";
        case LOX_STORAGE_PROFILE_32_MIB:
            return "32 MiB";
        default:
            return "unknown";
    }
}

#endif
