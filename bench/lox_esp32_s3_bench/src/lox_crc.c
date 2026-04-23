// SPDX-License-Identifier: MIT
#include "lox_crc.h"

uint32_t lox_crc32(uint32_t crc, const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    size_t i;

    crc = ~crc;
    while (len-- != 0u) {
        crc ^= *p++;
        for (i = 0; i < 8u; ++i) {
            if ((crc & 1u) != 0u) {
                crc = 0xEDB88320u ^ (crc >> 1u);
            } else {
                crc >>= 1u;
            }
        }
    }

    return ~crc;
}
