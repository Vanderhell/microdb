// SPDX-License-Identifier: MIT
#include "lox.h"
#include "lox_crc.h"

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    VERIFY_OK = 0,
    VERIFY_USAGE = 1,
    VERIFY_IO_ERROR = 2,
    VERIFY_INVALID_CONFIG = 3,
    VERIFY_CORRUPT = 4,
    VERIFY_UNINITIALIZED = 5
};

enum {
    LOX_WAL_MAGIC = 0x4D44424Cu,
    LOX_WAL_VERSION = 0x00010000u,
    LOX_SNAPSHOT_FORMAT_VERSION = 0x00020000u,
    LOX_WAL_ENTRY_MAGIC = 0x454E5452u,
    LOX_KV_PAGE_MAGIC = 0x4B565047u,
    LOX_TS_PAGE_MAGIC = 0x54535047u,
    LOX_REL_PAGE_MAGIC = 0x524C5047u,
    LOX_SUPER_MAGIC = 0x53555052u,
    LOX_WAL_ENGINE_KV = 0u,
    LOX_WAL_ENGINE_TS = 1u,
    LOX_WAL_ENGINE_REL = 2u,
    LOX_WAL_ENGINE_TXN_KV = 3u,
    LOX_WAL_ENGINE_META = 0xFFu,
    LOX_WAL_OP_SET_INSERT = 0u,
    LOX_WAL_OP_DEL = 1u,
    LOX_WAL_OP_CLEAR = 2u,
    LOX_WAL_OP_TXN_COMMIT = 5u,
    LOX_WAL_OP_TS_REGISTER = 6u,
    LOX_WAL_OP_REL_TABLE_CREATE = 7u
};

#define LOX_WAL_HEADER_SIZE 32u
#define LOX_PAGE_HEADER_SIZE 32u
#define LOX_SUPERBLOCK_SIZE 32u
#define VERIFY_WARN_MAX 64u
#define VERIFY_WARN_LINE 192u

typedef struct {
    const char *image_path;
    uint32_t ram_kb;
    uint32_t kv_pct;
    uint32_t ts_pct;
    uint32_t rel_pct;
    uint32_t erase_size;
    bool json;
    bool check;
} verify_cfg_t;

typedef struct {
    uint32_t wal_offset;
    uint32_t wal_size;
    uint32_t super_a_offset;
    uint32_t super_b_offset;
    uint32_t super_size;
    uint32_t bank_a_offset;
    uint32_t bank_b_offset;
    uint32_t bank_size;
    uint32_t kv_size;
    uint32_t ts_size;
    uint32_t rel_size;
    uint32_t total_size;
} verify_layout_t;

typedef struct {
    bool valid;
    uint32_t generation;
    uint32_t active_bank;
} super_info_t;

typedef struct {
    bool header_valid;
    bool payload_crc_valid;
    uint32_t generation;
    uint32_t payload_len;
    uint32_t entry_count;
    uint32_t payload_crc;
    uint32_t payload_offset;
    const char *reason;
} page_info_t;

typedef struct {
    bool valid;
    uint32_t generation;
    const char *reason;
    page_info_t kv;
    page_info_t ts;
    page_info_t rel;
} bank_info_t;

typedef struct {
    uint32_t kv_set;
    uint32_t kv_del;
    uint32_t kv_clear;
    uint32_t ts_insert;
    uint32_t ts_register;
    uint32_t ts_clear;
    uint32_t rel_insert;
    uint32_t rel_del;
    uint32_t rel_clear;
    uint32_t rel_create;
    uint32_t txn_kv;
    uint32_t txn_committed;
    uint32_t txn_orphaned;
} wal_semantic_t;

typedef struct {
    bool header_valid;
    bool entries_valid;
    uint32_t entry_count;
    uint32_t sequence;
    uint32_t used_bytes;
    wal_semantic_t semantic;
} wal_info_t;

typedef struct {
    char lines[VERIFY_WARN_MAX][VERIFY_WARN_LINE];
    uint32_t count;
} warn_log_t;

typedef struct {
    uint32_t live_keys;
    uint32_t tombstones;
    uint32_t value_bytes_used;
    uint32_t overlaps_detected;
} kv_decode_t;

typedef struct {
    uint32_t stream_count;
    uint32_t retained_samples;
    uint32_t ring_anomalies;
} ts_decode_t;

typedef struct {
    uint32_t table_count;
    uint32_t live_rows;
    uint32_t bitmap_mismatches;
} rel_decode_t;

static uint32_t align_u32(uint32_t value, uint32_t align) {
    return (value + (align - 1u)) & ~(align - 1u);
}

static uint32_t get_u32(const uint8_t *src) {
    uint32_t v = 0u;
    memcpy(&v, src, sizeof(v));
    return v;
}

static uint16_t get_u16(const uint8_t *src) {
    uint16_t v = 0u;
    memcpy(&v, src, sizeof(v));
    return v;
}

static void print_usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s --image <path> [--ram-kb N] [--kv-pct N --ts-pct N --rel-pct N] [--erase-size N] [--json] [--check]\n",
            prog);
}

static bool parse_u32(const char *s, uint32_t *out) {
    char *end = NULL;
    unsigned long v;
    if (s == NULL || out == NULL || s[0] == '\0') {
        return false;
    }
    v = strtoul(s, &end, 10);
    if (end == NULL || *end != '\0' || v > 0xFFFFFFFFul) {
        return false;
    }
    *out = (uint32_t)v;
    return true;
}

static bool read_at(FILE *fp, uint32_t off, void *buf, size_t len) {
    if (fseek(fp, (long)off, SEEK_SET) != 0) {
        return false;
    }
    return fread(buf, 1u, len, fp) == len;
}

static bool file_size_u32(FILE *fp, uint32_t *out_size) {
    long pos;
    long end;
    if (fp == NULL || out_size == NULL) {
        return false;
    }
    pos = ftell(fp);
    if (pos < 0) {
        pos = 0;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        return false;
    }
    end = ftell(fp);
    if (end < 0 || (unsigned long)end > 0xFFFFFFFFul) {
        return false;
    }
    if (fseek(fp, pos, SEEK_SET) != 0) {
        return false;
    }
    *out_size = (uint32_t)end;
    return true;
}

static bool region_all_ff(FILE *fp, uint32_t off, uint32_t len) {
    uint8_t buf[128];
    uint32_t pos = 0u;
    while (pos < len) {
        uint32_t take = len - pos;
        uint32_t i;
        if (take > (uint32_t)sizeof(buf)) {
            take = (uint32_t)sizeof(buf);
        }
        if (!read_at(fp, off + pos, buf, take)) {
            return false;
        }
        for (i = 0u; i < take; ++i) {
            if (buf[i] != 0xFFu) {
                return false;
            }
        }
        pos += take;
    }
    return true;
}

static bool crc_region(FILE *fp, uint32_t off, uint32_t len, uint32_t *out_crc) {
    uint8_t buf[256];
    uint32_t pos = 0u;
    uint32_t crc = 0xFFFFFFFFu;
    if (out_crc == NULL) {
        return false;
    }
    while (pos < len) {
        uint32_t take = len - pos;
        if (take > (uint32_t)sizeof(buf)) {
            take = (uint32_t)sizeof(buf);
        }
        if (!read_at(fp, off + pos, buf, take)) {
            return false;
        }
        crc = lox_crc32(crc, buf, take);
        pos += take;
    }
    *out_crc = crc;
    return true;
}

static void add_warn(warn_log_t *log, const char *fmt, ...) {
    va_list ap;
    if (log == NULL || log->count >= VERIFY_WARN_MAX) {
        return;
    }
    va_start(ap, fmt);
    (void)vsnprintf(log->lines[log->count], VERIFY_WARN_LINE, fmt, ap);
    va_end(ap);
    log->count++;
}

static bool compute_layout(uint32_t storage_capacity, const verify_cfg_t *cfg, verify_layout_t *layout) {
    uint32_t total_bytes;
    uint32_t kv_bytes;
    uint32_t ts_bytes;
    uint32_t rel_bytes;
    uint32_t max_key_len;
    uint32_t per_entry;
    uint32_t max_entries;
    uint32_t kv_payload_max;
    uint32_t wal_target;
    uint32_t wal_min;
    uint32_t fixed_bytes;
    uint32_t need_without_wal;
    uint32_t max_wal;
    uint32_t max_wal_aligned;

    if (cfg == NULL || layout == NULL) {
        return false;
    }
    if (cfg->erase_size == 0u || cfg->kv_pct + cfg->ts_pct + cfg->rel_pct != 100u) {
        return false;
    }

    total_bytes = cfg->ram_kb * 1024u;
    kv_bytes = (total_bytes * cfg->kv_pct) / 100u;
    ts_bytes = (total_bytes * cfg->ts_pct) / 100u;
    rel_bytes = total_bytes - kv_bytes - ts_bytes;

    max_key_len = (LOX_KV_KEY_MAX_LEN > 0u) ? (LOX_KV_KEY_MAX_LEN - 1u) : 0u;
    per_entry = 1u + max_key_len + 4u + LOX_KV_VAL_MAX_LEN + 4u;
    max_entries = (LOX_KV_MAX_KEYS > LOX_TXN_STAGE_KEYS) ? (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS) : 0u;
    kv_payload_max = max_entries * per_entry;

    memset(layout, 0, sizeof(*layout));
    layout->wal_offset = 0u;
    layout->super_size = cfg->erase_size;
    layout->kv_size = align_u32(kv_payload_max + LOX_PAGE_HEADER_SIZE, cfg->erase_size);
    layout->ts_size = align_u32(ts_bytes + LOX_PAGE_HEADER_SIZE, cfg->erase_size);
    layout->rel_size = align_u32(rel_bytes + LOX_PAGE_HEADER_SIZE, cfg->erase_size);
    layout->bank_size = layout->kv_size + layout->ts_size + layout->rel_size;

    wal_target = cfg->erase_size * 8u;
    wal_min = cfg->erase_size * 2u;
    fixed_bytes = layout->super_size * 2u;
    need_without_wal = fixed_bytes + (layout->bank_size * 2u);

    if (storage_capacity < need_without_wal + wal_min) {
        return false;
    }
    max_wal = storage_capacity - need_without_wal;
    max_wal_aligned = (max_wal / cfg->erase_size) * cfg->erase_size;
    if (max_wal_aligned < wal_min) {
        return false;
    }

    layout->wal_size = wal_target;
    if (layout->wal_size > max_wal_aligned) {
        layout->wal_size = max_wal_aligned;
    }
    if (layout->wal_size < wal_min) {
        layout->wal_size = wal_min;
    }

    layout->super_a_offset = layout->wal_offset + layout->wal_size;
    layout->super_b_offset = layout->super_a_offset + layout->super_size;
    layout->bank_a_offset = layout->super_b_offset + layout->super_size;
    layout->bank_b_offset = layout->bank_a_offset + layout->bank_size;
    layout->total_size = layout->bank_b_offset + layout->bank_size;
    return storage_capacity >= layout->total_size;
}

static bool validate_page_header(const uint8_t *header,
                                 uint32_t expected_magic,
                                 uint32_t max_payload_len,
                                 uint32_t *out_generation,
                                 uint32_t *out_payload_len,
                                 uint32_t *out_entry_count,
                                 uint32_t *out_payload_crc) {
    uint32_t payload_len = get_u32(header + 12u);
    uint32_t header_crc = get_u32(header + 24u);
    if (get_u32(header + 0u) != expected_magic) {
        return false;
    }
    if (get_u32(header + 4u) != LOX_SNAPSHOT_FORMAT_VERSION) {
        return false;
    }
    if (LOX_CRC32(header, 24u) != header_crc) {
        return false;
    }
    if (payload_len > max_payload_len) {
        return false;
    }
    *out_generation = get_u32(header + 8u);
    *out_payload_len = payload_len;
    *out_entry_count = get_u32(header + 16u);
    *out_payload_crc = get_u32(header + 20u);
    return true;
}

static bool validate_superblock(const uint8_t *super, uint32_t *out_generation, uint32_t *out_active_bank) {
    uint32_t header_crc = get_u32(super + 20u);
    if (get_u32(super + 0u) != LOX_SUPER_MAGIC) {
        return false;
    }
    if (get_u32(super + 4u) != LOX_SNAPSHOT_FORMAT_VERSION) {
        return false;
    }
    if (LOX_CRC32(super, 20u) != header_crc) {
        return false;
    }
    if (get_u32(super + 16u) > 1u) {
        return false;
    }
    *out_generation = get_u32(super + 12u);
    *out_active_bank = get_u32(super + 16u);
    return true;
}

static page_info_t inspect_page(FILE *fp, uint32_t page_offset, uint32_t expected_magic, uint32_t max_payload_len) {
    page_info_t info;
    uint8_t header[LOX_PAGE_HEADER_SIZE];
    uint32_t calc_crc = 0u;
    memset(&info, 0, sizeof(info));
    info.reason = "ok";
    info.payload_offset = page_offset + LOX_PAGE_HEADER_SIZE;

    if (!read_at(fp, page_offset, header, sizeof(header))) {
        info.reason = "header_read_error";
        return info;
    }
    if (!validate_page_header(header,
                              expected_magic,
                              max_payload_len,
                              &info.generation,
                              &info.payload_len,
                              &info.entry_count,
                              &info.payload_crc)) {
        info.reason = "header_invalid";
        return info;
    }
    info.header_valid = true;
    if (!crc_region(fp, info.payload_offset, info.payload_len, &calc_crc)) {
        info.reason = "payload_read_error";
        return info;
    }
    if (calc_crc != info.payload_crc) {
        info.reason = "payload_crc_mismatch";
        return info;
    }
    info.payload_crc_valid = true;
    return info;
}

static bank_info_t verify_bank(FILE *fp, const verify_layout_t *layout, uint32_t bank) {
    bank_info_t info;
    uint32_t bank_base = (bank == 0u) ? layout->bank_a_offset : layout->bank_b_offset;
    memset(&info, 0, sizeof(info));
    info.reason = "ok";

    info.kv = inspect_page(fp, bank_base, LOX_KV_PAGE_MAGIC, layout->kv_size - LOX_PAGE_HEADER_SIZE);
    if (!info.kv.header_valid || !info.kv.payload_crc_valid) {
        info.reason = info.kv.reason;
        return info;
    }

    info.ts = inspect_page(fp,
                           bank_base + layout->kv_size,
                           LOX_TS_PAGE_MAGIC,
                           layout->ts_size - LOX_PAGE_HEADER_SIZE);
    if (!info.ts.header_valid || !info.ts.payload_crc_valid) {
        info.reason = info.ts.reason;
        return info;
    }
    if (info.ts.generation != info.kv.generation) {
        info.reason = "generation_mismatch_kv_ts";
        return info;
    }

    info.rel = inspect_page(fp,
                            bank_base + layout->kv_size + layout->ts_size,
                            LOX_REL_PAGE_MAGIC,
                            layout->rel_size - LOX_PAGE_HEADER_SIZE);
    if (!info.rel.header_valid || !info.rel.payload_crc_valid) {
        info.reason = info.rel.reason;
        return info;
    }
    if (info.rel.generation != info.kv.generation) {
        info.reason = "generation_mismatch_kv_rel";
        return info;
    }

    info.generation = info.kv.generation;
    info.valid = true;
    return info;
}

static bool load_payload(FILE *fp, const page_info_t *page, uint8_t **out_buf) {
    uint8_t *buf;
    if (out_buf == NULL || page == NULL) {
        return false;
    }
    *out_buf = NULL;
    if (page->payload_len == 0u) {
        return true;
    }
    buf = (uint8_t *)malloc(page->payload_len);
    if (buf == NULL) {
        return false;
    }
    if (!read_at(fp, page->payload_offset, buf, page->payload_len)) {
        free(buf);
        return false;
    }
    *out_buf = buf;
    return true;
}

static void decode_kv_payload(const uint8_t *payload,
                              uint32_t payload_len,
                              uint32_t entry_count,
                              uint32_t value_store_region_size,
                              kv_decode_t *out,
                              warn_log_t *warns) {
    uint32_t i;
    uint32_t off = 0u;
    uint64_t value_off = 0u;

    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    for (i = 0u; i < entry_count; ++i) {
        uint8_t key_len;
        uint32_t val_len;
        uint64_t next_value_off;
        if (off + 1u > payload_len) {
            add_warn(warns, "WARN: kv decode truncated before key_len at entry %u", i);
            break;
        }
        key_len = payload[off++];
        if (off + key_len + 4u > payload_len) {
            add_warn(warns, "WARN: kv decode truncated in key/val_len at entry %u", i);
            break;
        }
        off += key_len;
        val_len = get_u32(payload + off);
        off += 4u;

        next_value_off = value_off + (uint64_t)val_len;
        if (next_value_off > (uint64_t)value_store_region_size) {
            out->overlaps_detected++;
            add_warn(warns, "WARN: kv decode value range exceeds store region at entry %u", i);
        }
        if (off + val_len + 4u > payload_len) {
            out->overlaps_detected++;
            add_warn(warns, "WARN: kv decode truncated in value/expires at entry %u", i);
            break;
        }

        out->live_keys++;
        out->value_bytes_used += val_len;
        off += val_len; /* value bytes */
        off += 4u;      /* expires */
        value_off = next_value_off;
    }
    if (i != entry_count) {
        add_warn(warns, "WARN: kv decode parsed %u/%u entries", i, entry_count);
    }
}

static void decode_ts_payload(const uint8_t *payload,
                              uint32_t payload_len,
                              uint32_t entry_count,
                              ts_decode_t *out,
                              warn_log_t *warns) {
    uint32_t i;
    uint32_t off = 0u;
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    for (i = 0u; i < entry_count; ++i) {
        uint8_t name_len;
        uint8_t type_byte;
        uint32_t raw_size;
        uint32_t count;
        uint32_t val_len;
        uint32_t j;

        if (off + 1u > payload_len) {
            out->ring_anomalies++;
            add_warn(warns, "WARN: ts decode truncated before stream name_len at stream %u", i);
            break;
        }
        name_len = payload[off++];
        if (off + name_len + 1u + 4u + 4u > payload_len) {
            out->ring_anomalies++;
            add_warn(warns, "WARN: ts decode truncated in stream header at stream %u", i);
            break;
        }
        off += name_len;
        type_byte = payload[off++];
        raw_size = get_u32(payload + off);
        off += 4u;
        count = get_u32(payload + off);
        off += 4u;

        if (type_byte > (uint8_t)LOX_TS_RAW) {
            out->ring_anomalies++;
            add_warn(warns, "WARN: ts decode invalid stream type %u at stream %u", (uint32_t)type_byte, i);
        }
        val_len = (type_byte == (uint8_t)LOX_TS_RAW) ? raw_size : 4u;
        for (j = 0u; j < count; ++j) {
            if (off + 8u + val_len > payload_len) {
                out->ring_anomalies++;
                add_warn(warns, "WARN: ts decode truncated in sample data at stream %u", i);
                j = count;
                break;
            }
            off += 8u + val_len;
            out->retained_samples++;
        }
        out->stream_count++;
    }
    if (i != entry_count) {
        out->ring_anomalies++;
    }
}

static void decode_rel_payload(const uint8_t *payload,
                               uint32_t payload_len,
                               uint32_t entry_count,
                               rel_decode_t *out,
                               warn_log_t *warns) {
    uint32_t i;
    uint32_t off = 0u;
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    for (i = 0u; i < entry_count; ++i) {
        uint8_t name_len;
        uint32_t row_size;
        uint32_t col_count;
        uint32_t live_count;
        uint32_t c;
        uint64_t rows_bytes;

        if (off + 1u > payload_len) {
            out->bitmap_mismatches++;
            add_warn(warns, "WARN: rel decode truncated before table name_len at table %u", i);
            break;
        }
        name_len = payload[off++];
        if (off + name_len + 2u + 4u + 4u + 4u + 4u + 4u > payload_len) {
            out->bitmap_mismatches++;
            add_warn(warns, "WARN: rel decode truncated in table header at table %u", i);
            break;
        }
        off += name_len;     /* table name */
        off += 2u;           /* schema_version */
        off += 4u;           /* max_rows */
        row_size = get_u32(payload + off);
        off += 4u;
        col_count = get_u32(payload + off);
        off += 4u;
        off += 4u;           /* index_col */
        live_count = get_u32(payload + off);
        off += 4u;

        if (col_count > LOX_REL_MAX_COLS) {
            out->bitmap_mismatches++;
            add_warn(warns, "WARN: rel decode col_count %u exceeds max at table %u", col_count, i);
        }
        for (c = 0u; c < col_count; ++c) {
            uint8_t col_name_len;
            if (off + 1u > payload_len) {
                out->bitmap_mismatches++;
                add_warn(warns, "WARN: rel decode truncated before column name_len at table %u", i);
                c = col_count;
                break;
            }
            col_name_len = payload[off++];
            if (off + col_name_len + 2u + 4u > payload_len) {
                out->bitmap_mismatches++;
                add_warn(warns, "WARN: rel decode truncated in column metadata at table %u", i);
                c = col_count;
                break;
            }
            off += col_name_len + 2u + 4u;
        }

        rows_bytes = (uint64_t)live_count * (uint64_t)row_size;
        if ((uint64_t)off + rows_bytes > (uint64_t)payload_len) {
            out->bitmap_mismatches++;
            add_warn(warns, "WARN: rel decode live_count/row_size exceeds payload at table %u", i);
            break;
        }
        off += (uint32_t)rows_bytes;
        out->table_count++;
        out->live_rows += live_count;
    }
    if (i != entry_count) {
        out->bitmap_mismatches++;
    }
}

static wal_info_t inspect_wal(FILE *fp, const verify_layout_t *layout) {
    wal_info_t info;
    uint8_t header[LOX_WAL_HEADER_SIZE];
    uint32_t i;
    uint32_t offset = layout->wal_offset + LOX_WAL_HEADER_SIZE;
    uint32_t pending_txn_kv = 0u;
    memset(&info, 0, sizeof(info));
    info.used_bytes = LOX_WAL_HEADER_SIZE;

    if (!read_at(fp, layout->wal_offset, header, sizeof(header))) {
        return info;
    }
    if (get_u32(header + 0u) != LOX_WAL_MAGIC) {
        return info;
    }
    if (LOX_CRC32(header, 16u) != get_u32(header + 16u)) {
        return info;
    }
    info.header_valid = true;
    info.entry_count = get_u32(header + 8u);
    info.sequence = get_u32(header + 12u);
    info.entries_valid = true;

    for (i = 0u; i < info.entry_count; ++i) {
        uint8_t entry_header[16];
        uint8_t payload[1536];
        uint16_t data_len;
        uint32_t aligned_len;
        uint32_t entry_crc;
        uint32_t crc;
        uint8_t engine;
        uint8_t op;

        if (!read_at(fp, offset, entry_header, sizeof(entry_header))) {
            info.entries_valid = false;
            break;
        }
        if (get_u32(entry_header + 0u) != LOX_WAL_ENTRY_MAGIC) {
            info.entries_valid = false;
            break;
        }
        data_len = get_u16(entry_header + 10u);
        aligned_len = align_u32((uint32_t)data_len, 4u);
        if (data_len > sizeof(payload) || offset + 16u + aligned_len > layout->wal_offset + layout->wal_size) {
            info.entries_valid = false;
            break;
        }
        if (!read_at(fp, offset + 16u, payload, aligned_len)) {
            info.entries_valid = false;
            break;
        }
        entry_crc = get_u32(entry_header + 12u);
        crc = LOX_CRC32(entry_header, 12u);
        crc = lox_crc32(crc, payload, data_len);
        if (crc != entry_crc) {
            info.entries_valid = false;
            break;
        }

        engine = entry_header[8];
        op = entry_header[9];
        if (engine == LOX_WAL_ENGINE_KV) {
            if (op == LOX_WAL_OP_SET_INSERT) {
                info.semantic.kv_set++;
            } else if (op == LOX_WAL_OP_DEL) {
                info.semantic.kv_del++;
            } else if (op == LOX_WAL_OP_CLEAR) {
                info.semantic.kv_clear++;
            }
        } else if (engine == LOX_WAL_ENGINE_TS) {
            if (op == LOX_WAL_OP_SET_INSERT) {
                info.semantic.ts_insert++;
            } else if (op == LOX_WAL_OP_TS_REGISTER) {
                info.semantic.ts_register++;
            } else if (op == LOX_WAL_OP_CLEAR) {
                info.semantic.ts_clear++;
            }
        } else if (engine == LOX_WAL_ENGINE_REL) {
            if (op == LOX_WAL_OP_SET_INSERT) {
                info.semantic.rel_insert++;
            } else if (op == LOX_WAL_OP_DEL) {
                info.semantic.rel_del++;
            } else if (op == LOX_WAL_OP_CLEAR) {
                info.semantic.rel_clear++;
            } else if (op == LOX_WAL_OP_REL_TABLE_CREATE) {
                info.semantic.rel_create++;
            }
        } else if (engine == LOX_WAL_ENGINE_TXN_KV) {
            info.semantic.txn_kv++;
            pending_txn_kv++;
        } else if (engine == LOX_WAL_ENGINE_META && op == LOX_WAL_OP_TXN_COMMIT) {
            info.semantic.txn_committed++;
            pending_txn_kv = 0u;
        }

        offset += 16u + aligned_len;
    }
    info.semantic.txn_orphaned = pending_txn_kv;
    info.used_bytes = offset - layout->wal_offset;
    return info;
}

static int parse_args(int argc, char **argv, verify_cfg_t *cfg) {
    int i;
    if (cfg == NULL) {
        return VERIFY_USAGE;
    }
    memset(cfg, 0, sizeof(*cfg));
    cfg->ram_kb = LOX_RAM_KB;
    cfg->kv_pct = LOX_RAM_KV_PCT;
    cfg->ts_pct = LOX_RAM_TS_PCT;
    cfg->rel_pct = LOX_RAM_REL_PCT;
    cfg->erase_size = 4096u;

    for (i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return VERIFY_USAGE;
        }
        if (strcmp(argv[i], "--image") == 0 && i + 1 < argc) {
            cfg->image_path = argv[++i];
        } else if (strcmp(argv[i], "--ram-kb") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &cfg->ram_kb)) {
                return VERIFY_USAGE;
            }
        } else if (strcmp(argv[i], "--kv-pct") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &cfg->kv_pct)) {
                return VERIFY_USAGE;
            }
        } else if (strcmp(argv[i], "--ts-pct") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &cfg->ts_pct)) {
                return VERIFY_USAGE;
            }
        } else if (strcmp(argv[i], "--rel-pct") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &cfg->rel_pct)) {
                return VERIFY_USAGE;
            }
        } else if (strcmp(argv[i], "--erase-size") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &cfg->erase_size)) {
                return VERIFY_USAGE;
            }
        } else if (strcmp(argv[i], "--json") == 0) {
            cfg->json = true;
        } else if (strcmp(argv[i], "--check") == 0) {
            cfg->check = true;
        } else {
            return VERIFY_USAGE;
        }
    }

    if (cfg->image_path == NULL) {
        return VERIFY_USAGE;
    }
    return VERIFY_OK;
}

static const char *verdict_upper(const char *verdict) {
    if (strcmp(verdict, "ok") == 0) {
        return "OK";
    }
    if (strcmp(verdict, "recoverable") == 0) {
        return "RECOVERABLE";
    }
    if (strcmp(verdict, "uninitialized") == 0) {
        return "UNINITIALIZED";
    }
    return "CORRUPT";
}

int main(int argc, char **argv) {
    verify_cfg_t cfg;
    verify_layout_t layout;
    FILE *fp = NULL;
    uint32_t cap = 0u;
    uint8_t super_a[LOX_SUPERBLOCK_SIZE];
    uint8_t super_b[LOX_SUPERBLOCK_SIZE];
    super_info_t sa;
    super_info_t sb;
    bank_info_t ba;
    bank_info_t bb;
    wal_info_t wal;
    warn_log_t warns;
    kv_decode_t kv_decode;
    ts_decode_t ts_decode;
    rel_decode_t rel_decode;
    const char *recovery_reason = "none";
    const char *verdict = "ok";
    uint32_t selected_bank = 0u;
    uint32_t selected_gen = 0u;
    bool have_selected = false;
    bool selected_from_super = false;
    bool super_region_ff = false;
    int rc;
    uint8_t *payload = NULL;
    const bank_info_t *selected = NULL;
    uint32_t kv_usable;
    uint32_t i;

    memset(&sa, 0, sizeof(sa));
    memset(&sb, 0, sizeof(sb));
    memset(&warns, 0, sizeof(warns));
    memset(&kv_decode, 0, sizeof(kv_decode));
    memset(&ts_decode, 0, sizeof(ts_decode));
    memset(&rel_decode, 0, sizeof(rel_decode));

    rc = parse_args(argc, argv, &cfg);
    if (rc != VERIFY_OK) {
        if (rc == VERIFY_USAGE) {
            print_usage((argc > 0) ? argv[0] : "lox_verify");
        }
        return rc;
    }

    fp = fopen(cfg.image_path, "rb");
    if (fp == NULL) {
        return VERIFY_IO_ERROR;
    }
    if (!file_size_u32(fp, &cap)) {
        fclose(fp);
        return VERIFY_IO_ERROR;
    }
    if (!compute_layout(cap, &cfg, &layout)) {
        fclose(fp);
        return VERIFY_INVALID_CONFIG;
    }
    if (!read_at(fp, layout.super_a_offset, super_a, sizeof(super_a)) ||
        !read_at(fp, layout.super_b_offset, super_b, sizeof(super_b))) {
        fclose(fp);
        return VERIFY_IO_ERROR;
    }

    sa.valid = validate_superblock(super_a, &sa.generation, &sa.active_bank);
    sb.valid = validate_superblock(super_b, &sb.generation, &sb.active_bank);
    ba = verify_bank(fp, &layout, 0u);
    bb = verify_bank(fp, &layout, 1u);
    wal = inspect_wal(fp, &layout);

    if (sa.valid || sb.valid) {
        if (sa.valid && (!sb.valid || sa.generation >= sb.generation)) {
            selected_bank = sa.active_bank;
            selected_gen = sa.generation;
            recovery_reason = "superblock_a";
        } else {
            selected_bank = sb.active_bank;
            selected_gen = sb.generation;
            recovery_reason = "superblock_b";
        }
        have_selected = true;
        selected_from_super = true;
    } else if (ba.valid || bb.valid) {
        if (ba.valid && (!bb.valid || ba.generation >= bb.generation)) {
            selected_bank = 0u;
            selected_gen = ba.generation;
        } else {
            selected_bank = 1u;
            selected_gen = bb.generation;
        }
        have_selected = true;
        recovery_reason = "bank_scan";
    } else {
        super_region_ff = region_all_ff(fp, layout.super_a_offset, layout.super_size) &&
                          region_all_ff(fp, layout.super_b_offset, layout.super_size);
        if (super_region_ff) {
            verdict = "uninitialized";
            recovery_reason = "cold_start";
        } else {
            verdict = "corrupt";
            recovery_reason = "no_valid_superblock_or_bank";
        }
    }

    if (have_selected) {
        selected = (selected_bank == 0u) ? &ba : &bb;
        if (!selected->valid || selected->generation != selected_gen) {
            verdict = "corrupt";
            recovery_reason = selected_from_super ? "selected_superblock_bank_invalid" : "selected_bank_invalid";
        } else if (!wal.header_valid) {
            verdict = "recoverable";
            recovery_reason = "wal_header_reset";
        } else if (!wal.entries_valid) {
            verdict = "recoverable";
            recovery_reason = "wal_tail_truncation";
        } else {
            verdict = "ok";
        }
    }

    if (selected != NULL && selected->valid) {
        if (load_payload(fp, &selected->kv, &payload)) {
            decode_kv_payload(payload,
                              selected->kv.payload_len,
                              selected->kv.entry_count,
                              layout.kv_size - LOX_PAGE_HEADER_SIZE,
                              &kv_decode,
                              &warns);
            free(payload);
            payload = NULL;
        } else {
            add_warn(&warns, "WARN: unable to load KV payload for decode");
        }

        if (load_payload(fp, &selected->ts, &payload)) {
            decode_ts_payload(payload, selected->ts.payload_len, selected->ts.entry_count, &ts_decode, &warns);
            free(payload);
            payload = NULL;
        } else {
            add_warn(&warns, "WARN: unable to load TS payload for decode");
        }

        if (load_payload(fp, &selected->rel, &payload)) {
            decode_rel_payload(payload, selected->rel.payload_len, selected->rel.entry_count, &rel_decode, &warns);
            free(payload);
            payload = NULL;
        } else {
            add_warn(&warns, "WARN: unable to load REL payload for decode");
        }
    }

    if (strcmp(verdict, "ok") == 0 && warns.count > 0u) {
        verdict = "recoverable";
        recovery_reason = "decode_warnings";
    }

    kv_usable = (LOX_KV_MAX_KEYS > LOX_TXN_STAGE_KEYS) ? (LOX_KV_MAX_KEYS - LOX_TXN_STAGE_KEYS) : 0u;

    if (cfg.check) {
        printf("verdict: %s\n", verdict);
        for (i = 0u; i < warns.count; ++i) {
            printf("%s\n", warns.lines[i]);
        }
        if (strcmp(verdict, "uninitialized") == 0) {
            fclose(fp);
            return VERIFY_UNINITIALIZED;
        }
        if (strcmp(verdict, "ok") != 0 || warns.count != 0u) {
            fclose(fp);
            return VERIFY_CORRUPT;
        }
        fclose(fp);
        return VERIFY_OK;
    }

    if (cfg.json) {
        printf("{\n");
        printf("  \"verdict\": \"%s\",\n", verdict);
        printf("  \"recovery_reason\": \"%s\",\n", recovery_reason);
        printf("  \"image\": \"%s\",\n", cfg.image_path);
        printf("  \"layout\": {\"total_size\": %u, \"wal_size\": %u, \"super_a_offset\": %u, \"super_b_offset\": %u, \"bank_a_offset\": %u, \"bank_b_offset\": %u},\n",
               layout.total_size,
               layout.wal_size,
               layout.super_a_offset,
               layout.super_b_offset,
               layout.bank_a_offset,
               layout.bank_b_offset);
        printf("  \"super\": {\"a_valid\": %s, \"a_generation\": %u, \"a_bank\": %u, \"b_valid\": %s, \"b_generation\": %u, \"b_bank\": %u},\n",
               sa.valid ? "true" : "false",
               sa.generation,
               sa.active_bank,
               sb.valid ? "true" : "false",
               sb.generation,
               sb.active_bank);
        printf("  \"banks\": {\"a_valid\": %s, \"a_generation\": %u, \"a_reason\": \"%s\", \"b_valid\": %s, \"b_generation\": %u, \"b_reason\": \"%s\"},\n",
               ba.valid ? "true" : "false",
               ba.generation,
               ba.reason,
               bb.valid ? "true" : "false",
               bb.generation,
               bb.reason);
        printf("  \"selected\": {\"has_selected\": %s, \"bank\": %u, \"generation\": %u},\n",
               have_selected ? "true" : "false",
               selected_bank,
               selected_gen);
        printf("  \"wal\": {\"header_valid\": %s, \"entries_valid\": %s, \"entry_count\": %u, \"sequence\": %u, \"used_bytes\": %u},\n",
               wal.header_valid ? "true" : "false",
               wal.entries_valid ? "true" : "false",
               wal.entry_count,
               wal.sequence,
               wal.used_bytes);
        printf("  \"kv_decode\": {\"live_keys\": %u, \"tombstones\": %u, \"value_bytes_used\": %u, \"overlaps_detected\": %u},\n",
               kv_decode.live_keys,
               kv_decode.tombstones,
               kv_decode.value_bytes_used,
               kv_decode.overlaps_detected);
        printf("  \"ts_decode\": {\"stream_count\": %u, \"retained_samples\": %u, \"ring_anomalies\": %u},\n",
               ts_decode.stream_count,
               ts_decode.retained_samples,
               ts_decode.ring_anomalies);
        printf("  \"rel_decode\": {\"table_count\": %u, \"live_rows\": %u, \"bitmap_mismatches\": %u},\n",
               rel_decode.table_count,
               rel_decode.live_rows,
               rel_decode.bitmap_mismatches);
        printf("  \"wal_semantic\": {\"kv_set\": %u, \"kv_del\": %u, \"kv_clear\": %u, \"ts_insert\": %u, \"ts_register\": %u, \"rel_insert\": %u, \"rel_del\": %u, \"txn_committed\": %u, \"txn_orphaned\": %u},\n",
               wal.semantic.kv_set,
               wal.semantic.kv_del,
               wal.semantic.kv_clear,
               wal.semantic.ts_insert,
               wal.semantic.ts_register,
               wal.semantic.rel_insert,
               wal.semantic.rel_del,
               wal.semantic.txn_committed,
               wal.semantic.txn_orphaned);
        printf("  \"warnings\": [");
        for (i = 0u; i < warns.count; ++i) {
            printf("%s\"%s\"", (i == 0u) ? "" : ", ", warns.lines[i]);
        }
        printf("]\n");
        printf("}\n");
    } else {
        printf("loxdb-verify  v1.0\n");
        printf("image: %s  (%u bytes)\n", cfg.image_path, cap);
        printf("layout: ram=%uKB  kv=%u%%  ts=%u%%  rel=%u%%  erase=%u\n",
               cfg.ram_kb,
               cfg.kv_pct,
               cfg.ts_pct,
               cfg.rel_pct,
               cfg.erase_size);
        printf("-----------------------------------------------------\n");
        printf("Superblock A:    %s   gen=%u  bank=%u\n", sa.valid ? "VALID" : "INVALID", sa.generation, sa.active_bank);
        printf("Superblock B:    %s   gen=%u  bank=%u\n", sb.valid ? "VALID" : "INVALID", sb.generation, sb.active_bank);
        if (have_selected) {
            printf("Active bank:     %u  (generation %u)\n", selected_bank, selected_gen);
        } else {
            printf("Active bank:     none\n");
        }
        printf("-----------------------------------------------------\n");
        printf("KV page  [bank %u]:  %s\n", selected_bank, (selected != NULL && selected->kv.payload_crc_valid) ? "VALID" : "INVALID");
        printf("  live keys:        %u / %u usable\n", kv_decode.live_keys, kv_usable);
        printf("  tombstones:       %u\n", kv_decode.tombstones);
        printf("  value store:      %u / %u bytes used\n",
               kv_decode.value_bytes_used,
               layout.kv_size - LOX_PAGE_HEADER_SIZE);
        if (kv_decode.overlaps_detected == 0u) {
            printf("  overlaps:         none\n");
        } else {
            printf("  overlaps:         %u\n", kv_decode.overlaps_detected);
        }
        printf("\nTS page  [bank %u]:  %s\n", selected_bank, (selected != NULL && selected->ts.payload_crc_valid) ? "VALID" : "INVALID");
        printf("  streams:          %u\n", ts_decode.stream_count);
        printf("  retained samples: %u\n", ts_decode.retained_samples);
        if (ts_decode.ring_anomalies == 0u) {
            printf("  ring anomalies:   none\n");
        } else {
            printf("  ring anomalies:   %u\n", ts_decode.ring_anomalies);
        }
        printf("\nREL page [bank %u]:  %s\n", selected_bank, (selected != NULL && selected->rel.payload_crc_valid) ? "VALID" : "INVALID");
        printf("  tables:           %u\n", rel_decode.table_count);
        printf("  live rows:        %u\n", rel_decode.live_rows);
        if (rel_decode.bitmap_mismatches == 0u) {
            printf("  bitmap issues:    none\n");
        } else {
            printf("  bitmap issues:    %u\n", rel_decode.bitmap_mismatches);
        }
        printf("-----------------------------------------------------\n");
        printf("WAL:              %s\n", (wal.header_valid && wal.entries_valid) ? "VALID" : "RECOVERABLE");
        printf("  entries:          %u\n", wal.entry_count);
        printf("  KV ops:           %u SET / %u DEL / %u CLEAR\n", wal.semantic.kv_set, wal.semantic.kv_del, wal.semantic.kv_clear);
        printf("  TS ops:           %u INSERT / %u REGISTER\n", wal.semantic.ts_insert, wal.semantic.ts_register);
        printf("  REL ops:          %u INSERT / %u DEL / %u CREATE\n",
               wal.semantic.rel_insert,
               wal.semantic.rel_del,
               wal.semantic.rel_create);
        printf("  TXN:              %u committed / %u orphaned\n", wal.semantic.txn_committed, wal.semantic.txn_orphaned);
        printf("-----------------------------------------------------\n");
        printf("Overall verdict:  %s\n", verdict_upper(verdict));
        printf("Recovery reason:  %s\n", recovery_reason);
        for (i = 0u; i < warns.count; ++i) {
            printf("%s\n", warns.lines[i]);
        }
    }

    fclose(fp);
    if (strcmp(verdict, "uninitialized") == 0) {
        return VERIFY_UNINITIALIZED;
    }
    if (strcmp(verdict, "corrupt") == 0) {
        return VERIFY_CORRUPT;
    }
    return VERIFY_OK;
}
