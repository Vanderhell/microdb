// SPDX-License-Identifier: MIT
#include "lox_json_wrapper.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int json_is_hex_char(char c) {
    return ((c >= '0' && c <= '9') ||
            (c >= 'a' && c <= 'f') ||
            (c >= 'A' && c <= 'F'));
}

static uint8_t json_hex_val(char c) {
    if (c >= '0' && c <= '9') return (uint8_t)(c - '0');
    if (c >= 'a' && c <= 'f') return (uint8_t)(10 + (c - 'a'));
    return (uint8_t)(10 + (c - 'A'));
}

static lox_err_t json_append_char(char *out, size_t out_len, size_t *pos, char c) {
    if (*pos >= out_len) return LOX_ERR_OVERFLOW;
    out[*pos] = c;
    (*pos)++;
    return LOX_OK;
}

static lox_err_t json_append_str(char *out, size_t out_len, size_t *pos, const char *s) {
    size_t n = strlen(s);
    if (*pos + n > out_len) return LOX_ERR_OVERFLOW;
    memcpy(out + *pos, s, n);
    *pos += n;
    return LOX_OK;
}

static lox_err_t json_append_escaped(char *out, size_t out_len, size_t *pos, const char *s) {
    static const char hx[] = "0123456789ABCDEF";
    while (*s != '\0') {
        unsigned char c = (unsigned char)*s++;
        lox_err_t rc;
        if (c == '"' || c == '\\') {
            rc = json_append_char(out, out_len, pos, '\\');
            if (rc != LOX_OK) return rc;
            rc = json_append_char(out, out_len, pos, (char)c);
            if (rc != LOX_OK) return rc;
        } else if (c < 0x20u) {
            rc = json_append_str(out, out_len, pos, "\\u00");
            if (rc != LOX_OK) return rc;
            rc = json_append_char(out, out_len, pos, hx[(c >> 4) & 0x0Fu]);
            if (rc != LOX_OK) return rc;
            rc = json_append_char(out, out_len, pos, hx[c & 0x0Fu]);
            if (rc != LOX_OK) return rc;
        } else {
            rc = json_append_char(out, out_len, pos, (char)c);
            if (rc != LOX_OK) return rc;
        }
    }
    return LOX_OK;
}

static lox_err_t json_append_hex(char *out, size_t out_len, size_t *pos, const uint8_t *buf, size_t len) {
    static const char hx[] = "0123456789ABCDEF";
    size_t i;
    if (*pos + (len * 2u) > out_len) return LOX_ERR_OVERFLOW;
    for (i = 0u; i < len; ++i) {
        out[*pos + i * 2u] = hx[(buf[i] >> 4) & 0x0Fu];
        out[*pos + i * 2u + 1u] = hx[buf[i] & 0x0Fu];
    }
    *pos += len * 2u;
    return LOX_OK;
}

static void json_skip_ws(const char **p) {
    while (**p != '\0' && isspace((unsigned char)**p)) (*p)++;
}

static lox_err_t json_parse_u32(const char **p, uint32_t *out) {
    unsigned long v;
    char *end = NULL;
    json_skip_ws(p);
    if (**p == '\0' || !isdigit((unsigned char)**p)) return LOX_ERR_INVALID;
    v = strtoul(*p, &end, 10);
    if (end == NULL || end == *p || v > 0xFFFFFFFFul) return LOX_ERR_INVALID;
    *p = end;
    *out = (uint32_t)v;
    return LOX_OK;
}

static lox_err_t json_parse_string(const char **p, char *out, size_t out_len) {
    size_t pos = 0u;
    json_skip_ws(p);
    if (**p != '"') return LOX_ERR_INVALID;
    (*p)++;
    while (**p != '\0') {
        char c = **p;
        if (c == '"') {
            (*p)++;
            if (pos >= out_len) return LOX_ERR_OVERFLOW;
            out[pos] = '\0';
            return LOX_OK;
        }
        if (c == '\\') {
            (*p)++;
            c = **p;
            if (c == '\0') return LOX_ERR_INVALID;
            switch (c) {
                case '"':
                case '\\':
                case '/':
                    break;
                case 'b':
                    c = '\b';
                    break;
                case 'f':
                    c = '\f';
                    break;
                case 'n':
                    c = '\n';
                    break;
                case 'r':
                    c = '\r';
                    break;
                case 't':
                    c = '\t';
                    break;
                case 'u':
                    if ((*p)[1] == '0' && (*p)[2] == '0' &&
                        json_is_hex_char((*p)[3]) && json_is_hex_char((*p)[4])) {
                        c = (char)((json_hex_val((*p)[3]) << 4) | json_hex_val((*p)[4]));
                        (*p) += 4;
                    } else {
                        return LOX_ERR_INVALID;
                    }
                    break;
                default:
                    return LOX_ERR_INVALID;
            }
        }
        if (pos + 1u >= out_len) return LOX_ERR_OVERFLOW;
        out[pos++] = c;
        (*p)++;
    }
    return LOX_ERR_INVALID;
}

static lox_err_t json_parse_hex_string(const char **p, uint8_t *out, size_t out_len, size_t *out_len_used) {
    size_t n = 0u;
    json_skip_ws(p);
    if (**p != '"') return LOX_ERR_INVALID;
    (*p)++;
    while (**p != '\0' && **p != '"') {
        if (!json_is_hex_char((*p)[0]) || !json_is_hex_char((*p)[1])) return LOX_ERR_INVALID;
        if (n >= out_len) return LOX_ERR_OVERFLOW;
        out[n++] = (uint8_t)((json_hex_val((*p)[0]) << 4) | json_hex_val((*p)[1]));
        (*p) += 2;
    }
    if (**p != '"') return LOX_ERR_INVALID;
    (*p)++;
    *out_len_used = n;
    return LOX_OK;
}

lox_err_t lox_json_kv_set_u32(lox_t *db, const char *key, uint32_t value, uint32_t ttl) {
    char buf[16];
    int n = snprintf(buf, sizeof(buf), "%u", (unsigned)value);
    if (n <= 0 || (size_t)n >= sizeof(buf)) return LOX_ERR_INVALID;
    return lox_kv_set(db, key, buf, (size_t)n, ttl);
}

lox_err_t lox_json_kv_get_u32(lox_t *db, const char *key, uint32_t *out_value) {
    char buf[16];
    size_t out_len = 0u;
    char *end = NULL;
    unsigned long v;
    lox_err_t rc;
    if (out_value == NULL) return LOX_ERR_INVALID;
    rc = lox_kv_get(db, key, buf, sizeof(buf) - 1u, &out_len);
    if (rc != LOX_OK) return rc;
    buf[out_len] = '\0';
    v = strtoul(buf, &end, 10);
    if (end == NULL || *end != '\0' || v > 0xFFFFFFFFul) return LOX_ERR_INVALID;
    *out_value = (uint32_t)v;
    return LOX_OK;
}

lox_err_t lox_json_kv_set_i32(lox_t *db, const char *key, int32_t value, uint32_t ttl) {
    char buf[16];
    int n = snprintf(buf, sizeof(buf), "%d", (int)value);
    if (n <= 0 || (size_t)n >= sizeof(buf)) return LOX_ERR_INVALID;
    return lox_kv_set(db, key, buf, (size_t)n, ttl);
}

lox_err_t lox_json_kv_get_i32(lox_t *db, const char *key, int32_t *out_value) {
    char buf[16];
    size_t out_len = 0u;
    char *end = NULL;
    long v;
    lox_err_t rc;
    if (out_value == NULL) return LOX_ERR_INVALID;
    rc = lox_kv_get(db, key, buf, sizeof(buf) - 1u, &out_len);
    if (rc != LOX_OK) return rc;
    buf[out_len] = '\0';
    v = strtol(buf, &end, 10);
    if (end == NULL || *end != '\0' || v < (-2147483647L - 1L) || v > 2147483647L) return LOX_ERR_INVALID;
    *out_value = (int32_t)v;
    return LOX_OK;
}

lox_err_t lox_json_kv_set_bool(lox_t *db, const char *key, bool value, uint32_t ttl) {
    const char *s = value ? "true" : "false";
    return lox_kv_set(db, key, s, strlen(s), ttl);
}

lox_err_t lox_json_kv_get_bool(lox_t *db, const char *key, bool *out_value) {
    char buf[8];
    size_t out_len = 0u;
    lox_err_t rc;
    if (out_value == NULL) return LOX_ERR_INVALID;
    rc = lox_kv_get(db, key, buf, sizeof(buf) - 1u, &out_len);
    if (rc != LOX_OK) return rc;
    buf[out_len] = '\0';
    if (strcmp(buf, "true") == 0) {
        *out_value = true;
        return LOX_OK;
    }
    if (strcmp(buf, "false") == 0) {
        *out_value = false;
        return LOX_OK;
    }
    return LOX_ERR_INVALID;
}

lox_err_t lox_json_kv_set_cstr(lox_t *db, const char *key, const char *value, uint32_t ttl) {
    if (value == NULL) return LOX_ERR_INVALID;
    return lox_kv_set(db, key, value, strlen(value), ttl);
}

lox_err_t lox_json_kv_get_cstr(lox_t *db, const char *key, char *out_buf, size_t out_buf_len, size_t *out_len) {
    lox_err_t rc;
    size_t n = 0u;
    if (out_buf == NULL || out_buf_len == 0u) return LOX_ERR_INVALID;
    rc = lox_kv_get(db, key, out_buf, out_buf_len - 1u, &n);
    if (rc != LOX_OK) return rc;
    out_buf[n] = '\0';
    if (out_len != NULL) *out_len = n;
    return LOX_OK;
}

lox_err_t lox_json_encode_kv_record(const char *key,
                                            const void *value,
                                            size_t value_len,
                                            uint32_t ttl,
                                            char *out_json,
                                            size_t out_json_len,
                                            size_t *out_used) {
    size_t pos = 0u;
    lox_err_t rc;
    if (key == NULL || value == NULL || out_json == NULL || out_json_len == 0u || out_used == NULL) return LOX_ERR_INVALID;
    rc = json_append_char(out_json, out_json_len, &pos, '{');
    if (rc != LOX_OK) return rc;
    rc = json_append_str(out_json, out_json_len, &pos, "\"key\":\"");
    if (rc != LOX_OK) return rc;
    rc = json_append_escaped(out_json, out_json_len, &pos, key);
    if (rc != LOX_OK) return rc;
    rc = json_append_str(out_json, out_json_len, &pos, "\",\"ttl\":");
    if (rc != LOX_OK) return rc;
    {
        char num[16];
        int n = snprintf(num, sizeof(num), "%u", (unsigned)ttl);
        if (n <= 0 || (size_t)n >= sizeof(num)) return LOX_ERR_INVALID;
        rc = json_append_str(out_json, out_json_len, &pos, num);
        if (rc != LOX_OK) return rc;
    }
    rc = json_append_str(out_json, out_json_len, &pos, ",\"value_hex\":\"");
    if (rc != LOX_OK) return rc;
    rc = json_append_hex(out_json, out_json_len, &pos, (const uint8_t *)value, value_len);
    if (rc != LOX_OK) return rc;
    rc = json_append_str(out_json, out_json_len, &pos, "\"}");
    if (rc != LOX_OK) return rc;
    if (pos >= out_json_len) return LOX_ERR_OVERFLOW;
    out_json[pos] = '\0';
    *out_used = pos;
    return LOX_OK;
}

lox_err_t lox_json_decode_kv_record(const char *json,
                                            char *key_out,
                                            size_t key_out_len,
                                            uint8_t *value_out,
                                            size_t value_out_len,
                                            size_t *value_len_out,
                                            uint32_t *ttl_out) {
    const char *p = json;
    char field[32];
    uint8_t seen_key = 0u;
    uint8_t seen_ttl = 0u;
    uint8_t seen_val = 0u;
    if (json == NULL || key_out == NULL || key_out_len == 0u ||
        value_out == NULL || value_len_out == NULL || ttl_out == NULL) {
        return LOX_ERR_INVALID;
    }
    json_skip_ws(&p);
    if (*p != '{') return LOX_ERR_INVALID;
    p++;
    for (;;) {
        lox_err_t rc;
        json_skip_ws(&p);
        if (*p == '}') {
            p++;
            break;
        }
        rc = json_parse_string(&p, field, sizeof(field));
        if (rc != LOX_OK) return rc;
        json_skip_ws(&p);
        if (*p != ':') return LOX_ERR_INVALID;
        p++;
        if (strcmp(field, "key") == 0) {
            rc = json_parse_string(&p, key_out, key_out_len);
            if (rc != LOX_OK) return rc;
            seen_key = 1u;
        } else if (strcmp(field, "ttl") == 0) {
            rc = json_parse_u32(&p, ttl_out);
            if (rc != LOX_OK) return rc;
            seen_ttl = 1u;
        } else if (strcmp(field, "value_hex") == 0) {
            rc = json_parse_hex_string(&p, value_out, value_out_len, value_len_out);
            if (rc != LOX_OK) return rc;
            seen_val = 1u;
        } else {
            return LOX_ERR_INVALID;
        }
        json_skip_ws(&p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == '}') {
            p++;
            break;
        }
        return LOX_ERR_INVALID;
    }
    json_skip_ws(&p);
    if (*p != '\0') return LOX_ERR_INVALID;
    if (!(seen_key && seen_ttl && seen_val)) return LOX_ERR_INVALID;
    return LOX_OK;
}
