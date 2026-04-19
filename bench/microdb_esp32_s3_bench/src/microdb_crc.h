// SPDX-License-Identifier: MIT
#ifndef MICRODB_CRC_H
#define MICRODB_CRC_H

#include <stddef.h>
#include <stdint.h>

uint32_t microdb_crc32(uint32_t crc, const void *data, size_t len);

#define MICRODB_CRC32(data, len) microdb_crc32(0xFFFFFFFFu, (data), (len))

#endif
