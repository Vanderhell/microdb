// SPDX-License-Identifier: MIT
#include "lox_import_export.h"
#include "lox_json_wrapper.h"

#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

typedef struct {
    const char *target_key;
    uint32_t ttl;
    uint8_t found;
} ie_ttl_lookup_t;

typedef struct {
    char *out;
    size_t out_len;
    size_t *pos;
    const lox_ie_ts_stream_desc_t *desc;
    uint32_t *exported;
    lox_err_t rc;
} ie_ts_export_ctx_t;

typedef struct {
    char *out;
    size_t out_len;
    size_t *pos;
    const char *table_name;
    size_t row_size;
    uint32_t *exported;
    lox_err_t rc;
} ie_rel_export_ctx_t;

static bool ie_find_ttl_cb(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    ie_ttl_lookup_t *q = (ie_ttl_lookup_t *)ctx;
    (void)val;
    (void)val_len;
    if (strcmp(key, q->target_key) == 0) {
        q->ttl = ttl_remaining;
        q->found = 1u;
        return false;
    }
    return true;
}

static lox_err_t ie_lookup_ttl(lox_t *db, const char *key, uint32_t *out_ttl) {
    ie_ttl_lookup_t q;
    lox_err_t rc;
    q.target_key = key;
    q.ttl = 0u;
    q.found = 0u;
    rc = lox_kv_iter(db, ie_find_ttl_cb, &q);
    if (rc != LOX_OK) return rc;
    if (!q.found) return LOX_ERR_NOT_FOUND;
    *out_ttl = q.ttl;
    return LOX_OK;
}

static lox_err_t ie_append(char *out, size_t out_len, size_t *pos, const char *s) {
    size_t n = strlen(s);
    if (*pos + n > out_len) return LOX_ERR_OVERFLOW;
    memcpy(out + *pos, s, n);
    *pos += n;
    return LOX_OK;
}

static lox_err_t ie_append_char(char *out, size_t out_len, size_t *pos, char c) {
    if (*pos >= out_len) return LOX_ERR_OVERFLOW;
    out[*pos] = c;
    (*pos)++;
    return LOX_OK;
}

static lox_err_t ie_append_u32(char *out, size_t out_len, size_t *pos, uint32_t v) {
    char buf[16];
    int n = snprintf(buf, sizeof(buf), "%u", (unsigned)v);
    if (n <= 0 || (size_t)n >= sizeof(buf)) return LOX_ERR_INVALID;
    return ie_append(out, out_len, pos, buf);
}

static lox_err_t ie_append_hex(char *out, size_t out_len, size_t *pos, const uint8_t *buf, size_t len) {
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

static void ie_skip_ws(const char **p) {
    while (**p != '\0' && isspace((unsigned char)**p)) (*p)++;
}

static const char *ie_find_items_array(const char *json) {
    const char *items = strstr(json, "\"items\"");
    if (items == NULL) return NULL;
    items = strchr(items, '[');
    return items;
}

static const char *ie_find_obj_end(const char *p) {
    int depth = 0;
    int in_string = 0;
    int esc = 0;
    while (*p != '\0') {
        char c = *p;
        if (in_string) {
            if (esc) {
                esc = 0;
            } else if (c == '\\') {
                esc = 1;
            } else if (c == '"') {
                in_string = 0;
            }
        } else {
            if (c == '"') {
                in_string = 1;
            } else if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) return p;
            }
        }
        p++;
    }
    return NULL;
}

static int ie_is_hex_char(char c) {
    return ((c >= '0' && c <= '9') ||
            (c >= 'a' && c <= 'f') ||
            (c >= 'A' && c <= 'F'));
}

static uint8_t ie_hex_val(char c) {
    if (c >= '0' && c <= '9') return (uint8_t)(c - '0');
    if (c >= 'a' && c <= 'f') return (uint8_t)(10 + (c - 'a'));
    return (uint8_t)(10 + (c - 'A'));
}

static lox_err_t ie_parse_json_string(const char **p, char *out, size_t out_len) {
    size_t pos = 0u;
    ie_skip_ws(p);
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
                        ie_is_hex_char((*p)[3]) && ie_is_hex_char((*p)[4])) {
                        c = (char)((ie_hex_val((*p)[3]) << 4) | ie_hex_val((*p)[4]));
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

static lox_err_t ie_parse_json_u32(const char **p, uint32_t *out) {
    unsigned long v;
    char *end = NULL;
    ie_skip_ws(p);
    if (**p == '\0' || !isdigit((unsigned char)**p)) return LOX_ERR_INVALID;
    v = strtoul(*p, &end, 10);
    if (end == NULL || end == *p || v > 0xFFFFFFFFul) return LOX_ERR_INVALID;
    *p = end;
    *out = (uint32_t)v;
    return LOX_OK;
}

static lox_err_t ie_parse_json_hex_string(const char **p, uint8_t *out, size_t out_len, size_t *out_used) {
    size_t n = 0u;
    ie_skip_ws(p);
    if (**p != '"') return LOX_ERR_INVALID;
    (*p)++;
    while (**p != '\0' && **p != '"') {
        if (!ie_is_hex_char((*p)[0]) || !ie_is_hex_char((*p)[1])) return LOX_ERR_INVALID;
        if (n >= out_len) return LOX_ERR_OVERFLOW;
        out[n++] = (uint8_t)((ie_hex_val((*p)[0]) << 4) | ie_hex_val((*p)[1]));
        (*p) += 2;
    }
    if (**p != '"') return LOX_ERR_INVALID;
    (*p)++;
    *out_used = n;
    return LOX_OK;
}

static lox_err_t ie_parse_object_field_string(const char *obj, const char *field_name, char *out, size_t out_len) {
    char needle[64];
    const char *p;
    if (strlen(field_name) + 4u >= sizeof(needle)) return LOX_ERR_INVALID;
    snprintf(needle, sizeof(needle), "\"%s\"", field_name);
    p = strstr(obj, needle);
    if (p == NULL) return LOX_ERR_NOT_FOUND;
    p += strlen(needle);
    ie_skip_ws(&p);
    if (*p != ':') return LOX_ERR_INVALID;
    p++;
    return ie_parse_json_string(&p, out, out_len);
}

static lox_err_t ie_parse_object_field_u32(const char *obj, const char *field_name, uint32_t *out) {
    char needle[64];
    const char *p;
    if (strlen(field_name) + 4u >= sizeof(needle)) return LOX_ERR_INVALID;
    snprintf(needle, sizeof(needle), "\"%s\"", field_name);
    p = strstr(obj, needle);
    if (p == NULL) return LOX_ERR_NOT_FOUND;
    p += strlen(needle);
    ie_skip_ws(&p);
    if (*p != ':') return LOX_ERR_INVALID;
    p++;
    return ie_parse_json_u32(&p, out);
}

static lox_err_t ie_parse_object_field_hex(const char *obj, const char *field_name, uint8_t *out, size_t out_len, size_t *out_used) {
    char needle[64];
    const char *p;
    if (strlen(field_name) + 4u >= sizeof(needle)) return LOX_ERR_INVALID;
    snprintf(needle, sizeof(needle), "\"%s\"", field_name);
    p = strstr(obj, needle);
    if (p == NULL) return LOX_ERR_NOT_FOUND;
    p += strlen(needle);
    ie_skip_ws(&p);
    if (*p != ':') return LOX_ERR_INVALID;
    p++;
    return ie_parse_json_hex_string(&p, out, out_len, out_used);
}

static const lox_ie_ts_stream_desc_t *ie_find_ts_desc(const lox_ie_ts_stream_desc_t *streams,
                                                           size_t stream_count,
                                                           const char *name) {
    size_t i;
    for (i = 0u; i < stream_count; ++i) {
        if (streams[i].name != NULL && strcmp(streams[i].name, name) == 0) return &streams[i];
    }
    return NULL;
}

static const lox_ie_rel_table_desc_t *ie_find_rel_desc(const lox_ie_rel_table_desc_t *tables,
                                                            size_t table_count,
                                                            const char *name) {
    size_t i;
    for (i = 0u; i < table_count; ++i) {
        if (tables[i].name != NULL && strcmp(tables[i].name, name) == 0) return &tables[i];
    }
    return NULL;
}

static const char *ie_ts_type_to_str(lox_ts_type_t type) {
    switch (type) {
        case LOX_TS_F32: return "f32";
        case LOX_TS_I32: return "i32";
        case LOX_TS_U32: return "u32";
        case LOX_TS_RAW: return "raw";
        default: return NULL;
    }
}

static lox_err_t ie_ts_type_from_str(const char *s, lox_ts_type_t *out) {
    if (strcmp(s, "f32") == 0) {
        *out = LOX_TS_F32;
        return LOX_OK;
    }
    if (strcmp(s, "i32") == 0) {
        *out = LOX_TS_I32;
        return LOX_OK;
    }
    if (strcmp(s, "u32") == 0) {
        *out = LOX_TS_U32;
        return LOX_OK;
    }
    if (strcmp(s, "raw") == 0) {
        *out = LOX_TS_RAW;
        return LOX_OK;
    }
    return LOX_ERR_INVALID;
}

static lox_err_t ie_ts_value_size(const lox_ie_ts_stream_desc_t *desc, size_t *out_size) {
    if (desc == NULL || out_size == NULL) return LOX_ERR_INVALID;
    switch (desc->type) {
        case LOX_TS_F32:
        case LOX_TS_I32:
        case LOX_TS_U32:
            *out_size = sizeof(uint32_t);
            return LOX_OK;
        case LOX_TS_RAW:
            if (desc->raw_size == 0u || desc->raw_size > LOX_TS_RAW_MAX) return LOX_ERR_INVALID;
            *out_size = desc->raw_size;
            return LOX_OK;
        default:
            return LOX_ERR_INVALID;
    }
}

static bool ie_ts_export_cb(const lox_ts_sample_t *sample, void *ctx) {
    ie_ts_export_ctx_t *x = (ie_ts_export_ctx_t *)ctx;
    uint8_t bytes[LOX_TS_RAW_MAX];
    size_t value_size = 0u;
    const char *type_name;
    lox_err_t rc;

    if (x->rc != LOX_OK) return false;
    rc = ie_ts_value_size(x->desc, &value_size);
    if (rc != LOX_OK) {
        x->rc = rc;
        return false;
    }

    switch (x->desc->type) {
        case LOX_TS_F32:
            memcpy(bytes, &sample->v.f32, sizeof(float));
            break;
        case LOX_TS_I32:
            memcpy(bytes, &sample->v.i32, sizeof(int32_t));
            break;
        case LOX_TS_U32:
            memcpy(bytes, &sample->v.u32, sizeof(uint32_t));
            break;
        case LOX_TS_RAW:
            memcpy(bytes, sample->v.raw, value_size);
            break;
        default:
            x->rc = LOX_ERR_INVALID;
            return false;
    }

    if (*(x->exported) > 0u) {
        rc = ie_append_char(x->out, x->out_len, x->pos, ',');
        if (rc != LOX_OK) {
            x->rc = rc;
            return false;
        }
    }

    type_name = ie_ts_type_to_str(x->desc->type);
    if (type_name == NULL) {
        x->rc = LOX_ERR_INVALID;
        return false;
    }

    rc = ie_append(x->out, x->out_len, x->pos, "{\"stream\":\"");
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, x->desc->name);
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, "\",\"type\":\"");
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, type_name);
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, "\",\"ts\":");
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append_u32(x->out, x->out_len, x->pos, (uint32_t)sample->ts);
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, ",\"value_hex\":\"");
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append_hex(x->out, x->out_len, x->pos, bytes, value_size);
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, "\"}");
    if (rc != LOX_OK) { x->rc = rc; return false; }

    (*(x->exported))++;
    return true;
}

static bool ie_rel_export_cb(const void *row_buf, void *ctx) {
    ie_rel_export_ctx_t *x = (ie_rel_export_ctx_t *)ctx;
    lox_err_t rc;
    if (x->rc != LOX_OK) return false;

    if (*(x->exported) > 0u) {
        rc = ie_append_char(x->out, x->out_len, x->pos, ',');
        if (rc != LOX_OK) { x->rc = rc; return false; }
    }

    rc = ie_append(x->out, x->out_len, x->pos, "{\"table\":\"");
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, x->table_name);
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, "\",\"row_hex\":\"");
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append_hex(x->out, x->out_len, x->pos, (const uint8_t *)row_buf, x->row_size);
    if (rc != LOX_OK) { x->rc = rc; return false; }
    rc = ie_append(x->out, x->out_len, x->pos, "\"}");
    if (rc != LOX_OK) { x->rc = rc; return false; }

    (*(x->exported))++;
    return true;
}

lox_ie_options_t lox_ie_default_options(void) {
    lox_ie_options_t o;
    o.overwrite_existing = 0u;
    o.skip_invalid_items = 0u;
    return o;
}

lox_err_t lox_ie_export_kv_json(lox_t *db,
                                        const char *const *keys,
                                        size_t key_count,
                                        char *out_json,
                                        size_t out_json_len,
                                        size_t *out_used,
                                        uint32_t *out_exported) {
    size_t pos = 0u;
    size_t i;
    uint32_t exported = 0u;
    lox_err_t rc;
    if (db == NULL || keys == NULL || out_json == NULL || out_json_len == 0u || out_used == NULL || out_exported == NULL) {
        return LOX_ERR_INVALID;
    }

    rc = ie_append(out_json, out_json_len, &pos, "{\"format\":\"loxdb.kv.v1\",\"items\":[");
    if (rc != LOX_OK) return rc;

    for (i = 0u; i < key_count; ++i) {
        const char *key = keys[i];
        uint8_t value_buf[LOX_KV_VAL_MAX_LEN];
        size_t value_len = 0u;
        uint32_t ttl = 0u;
        char rec[1024];
        size_t rec_used = 0u;

        if (key == NULL || key[0] == '\0') return LOX_ERR_INVALID;
        rc = lox_kv_get(db, key, value_buf, sizeof(value_buf), &value_len);
        if (rc != LOX_OK) return rc;
        rc = ie_lookup_ttl(db, key, &ttl);
        if (rc != LOX_OK) return rc;
        if (ttl == UINT32_MAX) {
            /* kv_iter uses UINT32_MAX as "no expiry" sentinel; JSON IE uses ttl=0 for persistent keys. */
            ttl = 0u;
        }

        rc = lox_json_encode_kv_record(key, value_buf, value_len, ttl, rec, sizeof(rec), &rec_used);
        if (rc != LOX_OK) return rc;
        if (exported > 0u) {
            rc = ie_append(out_json, out_json_len, &pos, ",");
            if (rc != LOX_OK) return rc;
        }
        rc = ie_append(out_json, out_json_len, &pos, rec);
        if (rc != LOX_OK) return rc;
        exported++;
    }

    rc = ie_append(out_json, out_json_len, &pos, "]}");
    if (rc != LOX_OK) return rc;
    if (pos >= out_json_len) return LOX_ERR_OVERFLOW;
    out_json[pos] = '\0';
    *out_used = pos;
    *out_exported = exported;
    return LOX_OK;
}

lox_err_t lox_ie_import_kv_json(lox_t *db,
                                        const char *json,
                                        const lox_ie_options_t *options,
                                        uint32_t *out_imported,
                                        uint32_t *out_skipped) {
    const lox_ie_options_t default_opts = lox_ie_default_options();
    const lox_ie_options_t *opts = options != NULL ? options : &default_opts;
    const char *p;
    uint32_t imported = 0u;
    uint32_t skipped = 0u;

    if (db == NULL || json == NULL || out_imported == NULL || out_skipped == NULL) return LOX_ERR_INVALID;
    p = ie_find_items_array(json);
    if (p == NULL || *p != '[') return LOX_ERR_INVALID;
    p++;

    for (;;) {
        lox_err_t rc;
        char obj[1024];
        const char *obj_end;
        size_t obj_len;
        char key[LOX_KV_KEY_MAX_LEN];
        uint8_t value[LOX_KV_VAL_MAX_LEN];
        size_t value_len = 0u;
        uint32_t ttl = 0u;
        lox_err_t exists_rc;

        ie_skip_ws(&p);
        if (*p == ']') {
            p++;
            break;
        }
        if (*p != '{') return LOX_ERR_INVALID;
        obj_end = ie_find_obj_end(p);
        if (obj_end == NULL) return LOX_ERR_INVALID;
        obj_len = (size_t)(obj_end - p + 1);
        if (obj_len >= sizeof(obj)) return LOX_ERR_OVERFLOW;
        memcpy(obj, p, obj_len);
        obj[obj_len] = '\0';
        p = obj_end + 1;

        rc = lox_json_decode_kv_record(obj, key, sizeof(key), value, sizeof(value), &value_len, &ttl);
        if (rc != LOX_OK) {
            if (opts->skip_invalid_items) {
                skipped++;
            } else {
                return rc;
            }
        } else {
            exists_rc = lox_kv_exists(db, key);
            if (!opts->overwrite_existing && exists_rc == LOX_OK) {
                skipped++;
            } else {
                rc = lox_kv_set(db, key, value, value_len, ttl);
                if (rc != LOX_OK) {
                    if (opts->skip_invalid_items) {
                        skipped++;
                    } else {
                        return rc;
                    }
                } else {
                    imported++;
                }
            }
        }

        ie_skip_ws(&p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') {
            p++;
            break;
        }
        return LOX_ERR_INVALID;
    }

    ie_skip_ws(&p);
    if (*p == '}') {
        p++;
        ie_skip_ws(&p);
    }
    if (*p != '\0') return LOX_ERR_INVALID;
    *out_imported = imported;
    *out_skipped = skipped;
    return LOX_OK;
}

lox_err_t lox_ie_export_ts_json(lox_t *db,
                                        const lox_ie_ts_stream_desc_t *streams,
                                        size_t stream_count,
                                        lox_timestamp_t from,
                                        lox_timestamp_t to,
                                        char *out_json,
                                        size_t out_json_len,
                                        size_t *out_used,
                                        uint32_t *out_exported) {
    size_t pos = 0u;
    size_t i;
    uint32_t exported = 0u;
    lox_err_t rc;

    if (db == NULL || streams == NULL || out_json == NULL || out_json_len == 0u || out_used == NULL || out_exported == NULL) {
        return LOX_ERR_INVALID;
    }

    rc = ie_append(out_json, out_json_len, &pos, "{\"format\":\"loxdb.ts.v1\",\"items\":[");
    if (rc != LOX_OK) return rc;

    for (i = 0u; i < stream_count; ++i) {
        ie_ts_export_ctx_t ctx;
        if (streams[i].name == NULL || streams[i].name[0] == '\0') return LOX_ERR_INVALID;
        rc = ie_ts_value_size(&streams[i], &ctx.out_len);
        if (rc != LOX_OK) return rc;

        ctx.out = out_json;
        ctx.out_len = out_json_len;
        ctx.pos = &pos;
        ctx.desc = &streams[i];
        ctx.exported = &exported;
        ctx.rc = LOX_OK;

        rc = lox_ts_query(db, streams[i].name, from, to, ie_ts_export_cb, &ctx);
        if (rc != LOX_OK) return rc;
        if (ctx.rc != LOX_OK) return ctx.rc;
    }

    rc = ie_append(out_json, out_json_len, &pos, "]}");
    if (rc != LOX_OK) return rc;
    if (pos >= out_json_len) return LOX_ERR_OVERFLOW;
    out_json[pos] = '\0';
    *out_used = pos;
    *out_exported = exported;
    return LOX_OK;
}

lox_err_t lox_ie_import_ts_json(lox_t *db,
                                        const char *json,
                                        const lox_ie_ts_stream_desc_t *streams,
                                        size_t stream_count,
                                        const lox_ie_options_t *options,
                                        uint32_t *out_imported,
                                        uint32_t *out_skipped) {
    const lox_ie_options_t default_opts = lox_ie_default_options();
    const lox_ie_options_t *opts = options != NULL ? options : &default_opts;
    const char *p;
    uint32_t imported = 0u;
    uint32_t skipped = 0u;

    if (db == NULL || json == NULL || streams == NULL || out_imported == NULL || out_skipped == NULL) return LOX_ERR_INVALID;
    p = ie_find_items_array(json);
    if (p == NULL || *p != '[') return LOX_ERR_INVALID;
    p++;

    for (;;) {
        char obj[1024];
        const char *obj_end;
        size_t obj_len;
        char stream_name[LOX_TS_STREAM_NAME_LEN + 1u];
        char type_name[16];
        uint32_t ts = 0u;
        uint8_t bytes[LOX_TS_RAW_MAX];
        size_t bytes_len = 0u;
        lox_ts_type_t item_type;
        const lox_ie_ts_stream_desc_t *desc;
        size_t expected_len = 0u;
        lox_err_t rc;

        ie_skip_ws(&p);
        if (*p == ']') {
            p++;
            break;
        }
        if (*p != '{') return LOX_ERR_INVALID;
        obj_end = ie_find_obj_end(p);
        if (obj_end == NULL) return LOX_ERR_INVALID;
        obj_len = (size_t)(obj_end - p + 1);
        if (obj_len >= sizeof(obj)) return LOX_ERR_OVERFLOW;
        memcpy(obj, p, obj_len);
        obj[obj_len] = '\0';
        p = obj_end + 1;

        rc = ie_parse_object_field_string(obj, "stream", stream_name, sizeof(stream_name));
        if (rc != LOX_OK) goto ts_item_error;
        rc = ie_parse_object_field_string(obj, "type", type_name, sizeof(type_name));
        if (rc != LOX_OK) goto ts_item_error;
        rc = ie_parse_object_field_u32(obj, "ts", &ts);
        if (rc != LOX_OK) goto ts_item_error;
        rc = ie_parse_object_field_hex(obj, "value_hex", bytes, sizeof(bytes), &bytes_len);
        if (rc != LOX_OK) goto ts_item_error;

        rc = ie_ts_type_from_str(type_name, &item_type);
        if (rc != LOX_OK) goto ts_item_error;
        desc = ie_find_ts_desc(streams, stream_count, stream_name);
        if (desc == NULL) {
            rc = LOX_ERR_NOT_FOUND;
            goto ts_item_error;
        }
        if (desc->type != item_type) {
            rc = LOX_ERR_SCHEMA;
            goto ts_item_error;
        }
        rc = ie_ts_value_size(desc, &expected_len);
        if (rc != LOX_OK) goto ts_item_error;
        if (bytes_len != expected_len) {
            rc = LOX_ERR_INVALID;
            goto ts_item_error;
        }

        rc = lox_ts_insert(db, stream_name, (lox_timestamp_t)ts, bytes);
        if (rc != LOX_OK) goto ts_item_error;
        imported++;
        goto ts_item_next;

    ts_item_error:
        if (opts->skip_invalid_items) {
            skipped++;
        } else {
            return rc;
        }

    ts_item_next:
        ie_skip_ws(&p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') {
            p++;
            break;
        }
        return LOX_ERR_INVALID;
    }

    ie_skip_ws(&p);
    if (*p == '}') {
        p++;
        ie_skip_ws(&p);
    }
    if (*p != '\0') return LOX_ERR_INVALID;
    *out_imported = imported;
    *out_skipped = skipped;
    return LOX_OK;
}

lox_err_t lox_ie_export_rel_json(lox_t *db,
                                         const lox_ie_rel_table_desc_t *tables,
                                         size_t table_count,
                                         char *out_json,
                                         size_t out_json_len,
                                         size_t *out_used,
                                         uint32_t *out_exported) {
    size_t pos = 0u;
    size_t i;
    uint32_t exported = 0u;
    lox_err_t rc;

    if (db == NULL || tables == NULL || out_json == NULL || out_json_len == 0u || out_used == NULL || out_exported == NULL) {
        return LOX_ERR_INVALID;
    }

    rc = ie_append(out_json, out_json_len, &pos, "{\"format\":\"loxdb.rel.v1\",\"items\":[");
    if (rc != LOX_OK) return rc;

    for (i = 0u; i < table_count; ++i) {
        lox_table_t *table = NULL;
        size_t actual_row_size;
        ie_rel_export_ctx_t ctx;

        if (tables[i].name == NULL || tables[i].name[0] == '\0') return LOX_ERR_INVALID;
        rc = lox_table_get(db, tables[i].name, &table);
        if (rc != LOX_OK) return rc;
        actual_row_size = lox_table_row_size(table);
        if (actual_row_size == 0u || actual_row_size > 1024u) return LOX_ERR_INVALID;
        if (tables[i].row_size != 0u && tables[i].row_size != actual_row_size) return LOX_ERR_SCHEMA;

        ctx.out = out_json;
        ctx.out_len = out_json_len;
        ctx.pos = &pos;
        ctx.table_name = tables[i].name;
        ctx.row_size = actual_row_size;
        ctx.exported = &exported;
        ctx.rc = LOX_OK;

        rc = lox_rel_iter(db, table, ie_rel_export_cb, &ctx);
        if (rc != LOX_OK) return rc;
        if (ctx.rc != LOX_OK) return ctx.rc;
    }

    rc = ie_append(out_json, out_json_len, &pos, "]}");
    if (rc != LOX_OK) return rc;
    if (pos >= out_json_len) return LOX_ERR_OVERFLOW;
    out_json[pos] = '\0';
    *out_used = pos;
    *out_exported = exported;
    return LOX_OK;
}

lox_err_t lox_ie_import_rel_json(lox_t *db,
                                         const char *json,
                                         const lox_ie_rel_table_desc_t *tables,
                                         size_t table_count,
                                         const lox_ie_options_t *options,
                                         uint32_t *out_imported,
                                         uint32_t *out_skipped) {
    const lox_ie_options_t default_opts = lox_ie_default_options();
    const lox_ie_options_t *opts = options != NULL ? options : &default_opts;
    const char *p;
    uint32_t imported = 0u;
    uint32_t skipped = 0u;

    if (db == NULL || json == NULL || tables == NULL || out_imported == NULL || out_skipped == NULL) return LOX_ERR_INVALID;
    p = ie_find_items_array(json);
    if (p == NULL || *p != '[') return LOX_ERR_INVALID;
    p++;

    for (;;) {
        char obj[2048];
        const char *obj_end;
        size_t obj_len;
        char table_name[LOX_REL_TABLE_NAME_LEN + 1u];
        uint8_t row[1024];
        size_t row_len = 0u;
        const lox_ie_rel_table_desc_t *desc;
        lox_table_t *table = NULL;
        size_t actual_row_size;
        lox_err_t rc;

        ie_skip_ws(&p);
        if (*p == ']') {
            p++;
            break;
        }
        if (*p != '{') return LOX_ERR_INVALID;
        obj_end = ie_find_obj_end(p);
        if (obj_end == NULL) return LOX_ERR_INVALID;
        obj_len = (size_t)(obj_end - p + 1);
        if (obj_len >= sizeof(obj)) return LOX_ERR_OVERFLOW;
        memcpy(obj, p, obj_len);
        obj[obj_len] = '\0';
        p = obj_end + 1;

        rc = ie_parse_object_field_string(obj, "table", table_name, sizeof(table_name));
        if (rc != LOX_OK) goto rel_item_error;
        rc = ie_parse_object_field_hex(obj, "row_hex", row, sizeof(row), &row_len);
        if (rc != LOX_OK) goto rel_item_error;

        desc = ie_find_rel_desc(tables, table_count, table_name);
        if (desc == NULL) {
            rc = LOX_ERR_NOT_FOUND;
            goto rel_item_error;
        }

        rc = lox_table_get(db, table_name, &table);
        if (rc != LOX_OK) goto rel_item_error;
        actual_row_size = lox_table_row_size(table);
        if (desc->row_size != 0u && desc->row_size != actual_row_size) {
            rc = LOX_ERR_SCHEMA;
            goto rel_item_error;
        }
        if (row_len != actual_row_size) {
            rc = LOX_ERR_INVALID;
            goto rel_item_error;
        }

        rc = lox_rel_insert(db, table, row);
        if (rc == LOX_ERR_EXISTS) {
            skipped++;
            goto rel_item_next;
        }
        if (rc != LOX_OK) goto rel_item_error;
        imported++;
        goto rel_item_next;

    rel_item_error:
        if (opts->skip_invalid_items) {
            skipped++;
        } else {
            return rc;
        }

    rel_item_next:
        ie_skip_ws(&p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') {
            p++;
            break;
        }
        return LOX_ERR_INVALID;
    }

    ie_skip_ws(&p);
    if (*p == '}') {
        p++;
        ie_skip_ws(&p);
    }
    if (*p != '\0') return LOX_ERR_INVALID;
    *out_imported = imported;
    *out_skipped = skipped;
    return LOX_OK;
}
