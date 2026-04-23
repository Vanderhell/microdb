// SPDX-License-Identifier: MIT
#ifndef LOX_CRC_H
#define LOX_CRC_H

#include <stddef.h>
#include <stdint.h>

uint32_t lox_crc32(uint32_t crc, const void *data, size_t len);

#define LOX_CRC32(data, len) lox_crc32(0xFFFFFFFFu, (data), (len))

#endif
