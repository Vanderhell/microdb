// SPDX-License-Identifier: MIT
#ifndef LOX_JSON_WRAPPER_H
#define LOX_JSON_WRAPPER_H

#include "lox.h"

#ifdef __cplusplus
extern "C" {
#endif

lox_err_t lox_json_kv_set_u32(lox_t *db, const char *key, uint32_t value, uint32_t ttl);
lox_err_t lox_json_kv_get_u32(lox_t *db, const char *key, uint32_t *out_value);

lox_err_t lox_json_kv_set_i32(lox_t *db, const char *key, int32_t value, uint32_t ttl);
lox_err_t lox_json_kv_get_i32(lox_t *db, const char *key, int32_t *out_value);

lox_err_t lox_json_kv_set_bool(lox_t *db, const char *key, bool value, uint32_t ttl);
lox_err_t lox_json_kv_get_bool(lox_t *db, const char *key, bool *out_value);

lox_err_t lox_json_kv_set_cstr(lox_t *db, const char *key, const char *value, uint32_t ttl);
lox_err_t lox_json_kv_get_cstr(lox_t *db, const char *key, char *out_buf, size_t out_buf_len, size_t *out_len);

/* Encodes a record as:
 * {"key":"...","ttl":123,"value_hex":"A1B2..."}
 */
lox_err_t lox_json_encode_kv_record(const char *key,
                                            const void *value,
                                            size_t value_len,
                                            uint32_t ttl,
                                            char *out_json,
                                            size_t out_json_len,
                                            size_t *out_used);

/* Decodes the same schema produced by lox_json_encode_kv_record. */
lox_err_t lox_json_decode_kv_record(const char *json,
                                            char *key_out,
                                            size_t key_out_len,
                                            uint8_t *value_out,
                                            size_t value_out_len,
                                            size_t *value_len_out,
                                            uint32_t *ttl_out);

#ifdef __cplusplus
}
#endif

#endif
