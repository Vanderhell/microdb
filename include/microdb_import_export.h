// SPDX-License-Identifier: MIT
#ifndef MICRODB_IMPORT_EXPORT_H
#define MICRODB_IMPORT_EXPORT_H

#include "microdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    /* When 0, existing keys are skipped during import. */
    uint8_t overwrite_existing;
    /* When 1, malformed items are skipped instead of aborting import. */
    uint8_t skip_invalid_items;
} microdb_ie_options_t;

typedef struct {
    const char *name;
    microdb_ts_type_t type;
    size_t raw_size;
} microdb_ie_ts_stream_desc_t;

typedef struct {
    const char *name;
    size_t row_size;
} microdb_ie_rel_table_desc_t;

microdb_ie_options_t microdb_ie_default_options(void);

/* Data fidelity contract:
 * - KV/TS/REL payloads are exported as hex-encoded bytes (`value_hex` / `row_hex`),
 *   so import/export is byte-exact on the same architecture/ABI.
 * - KV TTL in JSON is remaining TTL seconds (not absolute expires_at).
 *   Non-expiring keys are represented as ttl=0.
 * - Cross-architecture transfer may require explicit normalization
 *   (endianness/ABI-sensitive binary layouts, especially raw REL rows).
 */

/* Exports selected KV keys into JSON:
 * {"format":"microdb.kv.v1","items":[{"key":"...","ttl":N,"value_hex":"..."}]}
 */
microdb_err_t microdb_ie_export_kv_json(microdb_t *db,
                                        const char *const *keys,
                                        size_t key_count,
                                        char *out_json,
                                        size_t out_json_len,
                                        size_t *out_used,
                                        uint32_t *out_exported);

/* Imports KV items from the same format produced by microdb_ie_export_kv_json. */
microdb_err_t microdb_ie_import_kv_json(microdb_t *db,
                                        const char *json,
                                        const microdb_ie_options_t *options,
                                        uint32_t *out_imported,
                                        uint32_t *out_skipped);

/* Exports selected TS streams:
 * {"format":"microdb.ts.v1","items":[{"stream":"...","type":"u32","ts":1,"value_hex":"..."}]}
 */
microdb_err_t microdb_ie_export_ts_json(microdb_t *db,
                                        const microdb_ie_ts_stream_desc_t *streams,
                                        size_t stream_count,
                                        microdb_timestamp_t from,
                                        microdb_timestamp_t to,
                                        char *out_json,
                                        size_t out_json_len,
                                        size_t *out_used,
                                        uint32_t *out_exported);

microdb_err_t microdb_ie_import_ts_json(microdb_t *db,
                                        const char *json,
                                        const microdb_ie_ts_stream_desc_t *streams,
                                        size_t stream_count,
                                        const microdb_ie_options_t *options,
                                        uint32_t *out_imported,
                                        uint32_t *out_skipped);

/* Exports selected REL tables:
 * {"format":"microdb.rel.v1","items":[{"table":"...","row_hex":"..."}]}
 */
microdb_err_t microdb_ie_export_rel_json(microdb_t *db,
                                         const microdb_ie_rel_table_desc_t *tables,
                                         size_t table_count,
                                         char *out_json,
                                         size_t out_json_len,
                                         size_t *out_used,
                                         uint32_t *out_exported);

microdb_err_t microdb_ie_import_rel_json(microdb_t *db,
                                         const char *json,
                                         const microdb_ie_rel_table_desc_t *tables,
                                         size_t table_count,
                                         const microdb_ie_options_t *options,
                                         uint32_t *out_imported,
                                         uint32_t *out_skipped);

#ifdef __cplusplus
}
#endif

#endif
