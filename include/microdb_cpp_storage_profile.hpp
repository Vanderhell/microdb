// SPDX-License-Identifier: MIT
#ifndef MICRODB_CPP_STORAGE_PROFILE_HPP
#define MICRODB_CPP_STORAGE_PROFILE_HPP

#include <cstdint>

extern "C" {
#include "microdb_capacity_profile.h"
}

namespace microdb {
namespace storage_profile {

enum class Profile : std::uint8_t {
    MiB2 = static_cast<std::uint8_t>(MICRODB_STORAGE_PROFILE_2_MIB),
    MiB4 = static_cast<std::uint8_t>(MICRODB_STORAGE_PROFILE_4_MIB),
    MiB8 = static_cast<std::uint8_t>(MICRODB_STORAGE_PROFILE_8_MIB),
    MiB16 = static_cast<std::uint8_t>(MICRODB_STORAGE_PROFILE_16_MIB),
    MiB32 = static_cast<std::uint8_t>(MICRODB_STORAGE_PROFILE_32_MIB)
};

inline std::uint32_t capacity_bytes(Profile profile) {
    return microdb_storage_profile_capacity_bytes(static_cast<microdb_storage_profile_t>(profile));
}

inline const char *name(Profile profile) {
    return microdb_storage_profile_name(static_cast<microdb_storage_profile_t>(profile));
}

}  // namespace storage_profile
}  // namespace microdb

#endif
