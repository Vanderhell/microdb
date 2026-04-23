// SPDX-License-Identifier: MIT
#ifndef LOX_CPP_STORAGE_PROFILE_HPP
#define LOX_CPP_STORAGE_PROFILE_HPP

#include <cstdint>

extern "C" {
#include "lox_capacity_profile.h"
}

namespace loxdb {
namespace storage_profile {

enum class Profile : std::uint8_t {
    MiB2 = static_cast<std::uint8_t>(LOX_STORAGE_PROFILE_2_MIB),
    MiB4 = static_cast<std::uint8_t>(LOX_STORAGE_PROFILE_4_MIB),
    MiB8 = static_cast<std::uint8_t>(LOX_STORAGE_PROFILE_8_MIB),
    MiB16 = static_cast<std::uint8_t>(LOX_STORAGE_PROFILE_16_MIB),
    MiB32 = static_cast<std::uint8_t>(LOX_STORAGE_PROFILE_32_MIB)
};

inline std::uint32_t capacity_bytes(Profile profile) {
    return lox_storage_profile_capacity_bytes(static_cast<lox_storage_profile_t>(profile));
}

inline const char *name(Profile profile) {
    return lox_storage_profile_name(static_cast<lox_storage_profile_t>(profile));
}

}  // namespace storage_profile
}  // namespace loxdb

#endif
