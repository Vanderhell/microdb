// SPDX-License-Identifier: MIT
#ifndef LOX_IMPORT_EXPORT_H
#define LOX_IMPORT_EXPORT_H

#include "lox.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    /* When 0, existing keys are skipped during import. */
    uint8_t overwrite_existing;
    /* When 1, malformed items are skipped instead of aborting import. */
    uint8_t skip_invalid_items;
} lox_ie_options_t;

typedef struct {
    const char *name;
    lox_ts_type_t type;
    size_t raw_size;
} lox_ie_ts_stream_desc_t;

typedef struct {
    const char *name;
    size_t row_size;
} lox_ie_rel_table_desc_t;

lox_ie_options_t lox_ie_default_options(void);

/* Data fidelity contract:
 * - KV/TS/REL payloads are exported as hex-encoded bytes (`value_hex` / `row_hex`),
 *   so import/export is byte-exact on the same architecture/ABI.
 * - KV TTL in JSON is remaining TTL seconds (not absolute expires_at).
 *   Non-expiring keys are represented as ttl=0.
 * - Cross-architecture transfer may require explicit normalization
 *   (endianness/ABI-sensitive binary layouts, especially raw REL rows).
 */

/* Exports selected KV keys into JSON:
 * {"format":"loxdb.kv.v1","items":[{"key":"...","ttl":N,"value_hex":"..."}]}
 */
lox_err_t lox_ie_export_kv_json(lox_t *db,
                                        const char *const *keys,
                                        size_t key_count,
                                        char *out_json,
                                        size_t out_json_len,
                                        size_t *out_used,
                                        uint32_t *out_exported);

/* Imports KV items from the same format produced by lox_ie_export_kv_json. */
lox_err_t lox_ie_import_kv_json(lox_t *db,
                                        const char *json,
                                        const lox_ie_options_t *options,
                                        uint32_t *out_imported,
                                        uint32_t *out_skipped);

/* Exports selected TS streams:
 * {"format":"loxdb.ts.v1","items":[{"stream":"...","type":"u32","ts":1,"value_hex":"..."}]}
 */
lox_err_t lox_ie_export_ts_json(lox_t *db,
                                        const lox_ie_ts_stream_desc_t *streams,
                                        size_t stream_count,
                                        lox_timestamp_t from,
                                        lox_timestamp_t to,
                                        char *out_json,
                                        size_t out_json_len,
                                        size_t *out_used,
                                        uint32_t *out_exported);

lox_err_t lox_ie_import_ts_json(lox_t *db,
                                        const char *json,
                                        const lox_ie_ts_stream_desc_t *streams,
                                        size_t stream_count,
                                        const lox_ie_options_t *options,
                                        uint32_t *out_imported,
                                        uint32_t *out_skipped);

/* Exports selected REL tables:
 * {"format":"loxdb.rel.v1","items":[{"table":"...","row_hex":"..."}]}
 */
lox_err_t lox_ie_export_rel_json(lox_t *db,
                                         const lox_ie_rel_table_desc_t *tables,
                                         size_t table_count,
                                         char *out_json,
                                         size_t out_json_len,
                                         size_t *out_used,
                                         uint32_t *out_exported);

lox_err_t lox_ie_import_rel_json(lox_t *db,
                                         const char *json,
                                         const lox_ie_rel_table_desc_t *tables,
                                         size_t table_count,
                                         const lox_ie_options_t *options,
                                         uint32_t *out_imported,
                                         uint32_t *out_skipped);

#ifdef __cplusplus
}
#endif

#endif
