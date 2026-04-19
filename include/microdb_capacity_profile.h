// SPDX-License-Identifier: MIT
#ifndef MICRODB_CAPACITY_PROFILE_H
#define MICRODB_CAPACITY_PROFILE_H

#include <stdint.h>

typedef enum {
    MICRODB_STORAGE_PROFILE_2_MIB = 0,
    MICRODB_STORAGE_PROFILE_4_MIB = 1,
    MICRODB_STORAGE_PROFILE_8_MIB = 2,
    MICRODB_STORAGE_PROFILE_16_MIB = 3,
    MICRODB_STORAGE_PROFILE_32_MIB = 4
} microdb_storage_profile_t;

#define MICRODB_MIB_BYTES(value_mib) ((uint32_t)(value_mib) * 1024u * 1024u)

static inline uint32_t microdb_storage_profile_capacity_bytes(microdb_storage_profile_t profile) {
    switch (profile) {
        case MICRODB_STORAGE_PROFILE_2_MIB:
            return MICRODB_MIB_BYTES(2u);
        case MICRODB_STORAGE_PROFILE_4_MIB:
            return MICRODB_MIB_BYTES(4u);
        case MICRODB_STORAGE_PROFILE_8_MIB:
            return MICRODB_MIB_BYTES(8u);
        case MICRODB_STORAGE_PROFILE_16_MIB:
            return MICRODB_MIB_BYTES(16u);
        case MICRODB_STORAGE_PROFILE_32_MIB:
            return MICRODB_MIB_BYTES(32u);
        default:
            return 0u;
    }
}

static inline const char *microdb_storage_profile_name(microdb_storage_profile_t profile) {
    switch (profile) {
        case MICRODB_STORAGE_PROFILE_2_MIB:
            return "2 MiB";
        case MICRODB_STORAGE_PROFILE_4_MIB:
            return "4 MiB";
        case MICRODB_STORAGE_PROFILE_8_MIB:
            return "8 MiB";
        case MICRODB_STORAGE_PROFILE_16_MIB:
            return "16 MiB";
        case MICRODB_STORAGE_PROFILE_32_MIB:
            return "32 MiB";
        default:
            return "unknown";
    }
}

#endif
