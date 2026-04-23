// SPDX-License-Identifier: MIT
#ifndef LOX_WCET_H
#define LOX_WCET_H

#include "lox.h"

/*
 * NOTE:
 * lox_kv bucket count is next_pow2(ceil(KV_MAX_KEYS * 4 / 3)).
 * A portable preprocessor-only exact next_pow2 is awkward; this macro is a
 * conservative compile-time bound valid for all supported configs.
 */
#define LOX_WCET_KV_PROBE_MAX ((((LOX_KV_MAX_KEYS * 4u) + 2u) / 3u) * 2u)

/* header(16) + aligned payload(1 + key + 4 + val + 4) */
#define LOX_WCET_WAL_KV_SET_MAX \
    (16u + (((1u + LOX_KV_KEY_MAX_LEN + 4u + LOX_KV_VAL_MAX_LEN + 4u + 3u) & ~3u)))

/*
 * Conservative memmove upper bound in bytes for KV growth overwrite path.
 * Value store is a subset of kv_arena; this remains a safe upper bound.
 */
#define LOX_WCET_KV_MEMMOVE_MAX (((LOX_RAM_KB * 1024u) * LOX_RAM_KV_PCT) / 100u)

/* header(16) + aligned payload(1 + stream_name + ts(u32) + value(u32) + raw_len(u32)) */
#define LOX_WCET_WAL_TS_F32_INSERT_MAX \
    (16u + (((1u + LOX_TS_STREAM_NAME_LEN + 4u + 4u + 4u + 3u) & ~3u)))

/* worst RAW payload uses LOX_TS_RAW_MAX value bytes */
#define LOX_WCET_WAL_TS_RAW_INSERT_MAX \
    (16u + (((1u + LOX_TS_STREAM_NAME_LEN + 4u + LOX_TS_RAW_MAX + 4u + 3u) & ~3u)))

#define LOX_WCET_REL_INDEX_SHIFTS_MAX(max_rows) (max_rows)

#define LOX_WCET_WAL_REL_INSERT_MAX(row_size) \
    (16u + (((1u + LOX_REL_TABLE_NAME_LEN + 4u + (row_size) + 3u) & ~3u)))

#endif
