// SPDX-License-Identifier: MIT
#include "microdb.h"
#include "microdb_crc.h"

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
    MICRODB_WAL_MAGIC = 0x4D44424Cu,
    MICRODB_WAL_VERSION = 0x00010000u,
    MICRODB_SNAPSHOT_FORMAT_VERSION = 0x00020000u,
    MICRODB_WAL_ENTRY_MAGIC = 0x454E5452u,
    MICRODB_KV_PAGE_MAGIC = 0x4B565047u,
    MICRODB_TS_PAGE_MAGIC = 0x54535047u,
    MICRODB_REL_PAGE_MAGIC = 0x524C5047u,
    MICRODB_SUPER_MAGIC = 0x53555052u
};

#define MICRODB_WAL_HEADER_SIZE 32u
#define MICRODB_PAGE_HEADER_SIZE 32u
#define MICRODB_SUPERBLOCK_SIZE 32u

typedef struct {
    const char *image_path;
    uint32_t ram_kb;
    uint32_t kv_pct;
    uint32_t ts_pct;
    uint32_t rel_pct;
    uint32_t erase_size;
    bool json;
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
    bool valid;
    uint32_t generation;
    uint32_t kv_entries;
    uint32_t ts_entries;
    uint32_t rel_entries;
    const char *reason;
} bank_info_t;

typedef struct {
    bool header_valid;
    bool entries_valid;
    uint32_t entry_count;
    uint32_t sequence;
    uint32_t used_bytes;
} wal_info_t;

static const char *verify_code_to_string(int code) {
    switch (code) {
        case VERIFY_OK:
            return "VERIFY_OK";
        case VERIFY_USAGE:
            return "VERIFY_USAGE";
        case VERIFY_IO_ERROR:
            return "VERIFY_IO_ERROR";
        case VERIFY_INVALID_CONFIG:
            return "VERIFY_INVALID_CONFIG";
        case VERIFY_CORRUPT:
            return "VERIFY_CORRUPT";
        case VERIFY_UNINITIALIZED:
            return "VERIFY_UNINITIALIZED";
        default:
            return "VERIFY_UNKNOWN";
    }
}

static int fail_verify(const char *op, const char *detail, int code) {
    fprintf(stderr, "%s failed: %s (%d)", op, verify_code_to_string(code), code);
    if (detail != NULL && detail[0] != '\0') {
        fprintf(stderr, " - %s", detail);
    }
    fprintf(stderr, "\n");
    return code;
}

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
    if (end < 0) {
        return false;
    }
    if ((unsigned long)end > 0xFFFFFFFFul) {
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
        crc = microdb_crc32(crc, buf, take);
        pos += take;
    }
    *out_crc = crc;
    return true;
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
    if (get_u32(header + 4u) != MICRODB_SNAPSHOT_FORMAT_VERSION) {
        return false;
    }
    if (MICRODB_CRC32(header, 24u) != header_crc) {
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
    if (get_u32(super + 0u) != MICRODB_SUPER_MAGIC) {
        return false;
    }
    if (get_u32(super + 4u) != MICRODB_SNAPSHOT_FORMAT_VERSION) {
        return false;
    }
    if (MICRODB_CRC32(super, 20u) != header_crc) {
        return false;
    }
    if (get_u32(super + 16u) > 1u) {
        return false;
    }
    *out_generation = get_u32(super + 12u);
    *out_active_bank = get_u32(super + 16u);
    return true;
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
    if (cfg->erase_size == 0u) {
        return false;
    }
    if (cfg->kv_pct == 0u || cfg->ts_pct == 0u || cfg->rel_pct == 0u) {
        return false;
    }
    if (cfg->kv_pct + cfg->ts_pct + cfg->rel_pct != 100u) {
        return false;
    }

    total_bytes = cfg->ram_kb * 1024u;
    kv_bytes = (total_bytes * cfg->kv_pct) / 100u;
    ts_bytes = (total_bytes * cfg->ts_pct) / 100u;
    rel_bytes = total_bytes - kv_bytes - ts_bytes;

    max_key_len = (MICRODB_KV_KEY_MAX_LEN > 0u) ? (MICRODB_KV_KEY_MAX_LEN - 1u) : 0u;
    per_entry = 1u + max_key_len + 4u + MICRODB_KV_VAL_MAX_LEN + 4u;
    max_entries = (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS) ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS) : 0u;
    kv_payload_max = max_entries * per_entry;

    memset(layout, 0, sizeof(*layout));
    layout->wal_offset = 0u;
    layout->super_size = cfg->erase_size;
    layout->kv_size = align_u32(kv_payload_max + MICRODB_PAGE_HEADER_SIZE, cfg->erase_size);
    layout->ts_size = align_u32(ts_bytes + MICRODB_PAGE_HEADER_SIZE, cfg->erase_size);
    layout->rel_size = align_u32(rel_bytes + MICRODB_PAGE_HEADER_SIZE, cfg->erase_size);
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

static bank_info_t verify_bank(FILE *fp, const verify_layout_t *layout, uint32_t bank) {
    bank_info_t info;
    uint8_t header[MICRODB_PAGE_HEADER_SIZE];
    uint32_t bank_base = (bank == 0u) ? layout->bank_a_offset : layout->bank_b_offset;
    uint32_t gen_kv = 0u;
    uint32_t gen_ts = 0u;
    uint32_t gen_rel = 0u;
    uint32_t len = 0u;
    uint32_t count = 0u;
    uint32_t stored_crc = 0u;
    uint32_t calc_crc = 0u;

    memset(&info, 0, sizeof(info));
    info.reason = "ok";

    if (!read_at(fp, bank_base, header, sizeof(header))) {
        info.reason = "kv_header_read_error";
        return info;
    }
    if (!validate_page_header(header, MICRODB_KV_PAGE_MAGIC, layout->kv_size - MICRODB_PAGE_HEADER_SIZE, &gen_kv, &len, &count, &stored_crc)) {
        info.reason = "kv_header_invalid";
        return info;
    }
    if (!crc_region(fp, bank_base + MICRODB_PAGE_HEADER_SIZE, len, &calc_crc) || calc_crc != stored_crc) {
        info.reason = "kv_payload_crc_mismatch";
        return info;
    }
    info.kv_entries = count;

    if (!read_at(fp, bank_base + layout->kv_size, header, sizeof(header))) {
        info.reason = "ts_header_read_error";
        return info;
    }
    if (!validate_page_header(header, MICRODB_TS_PAGE_MAGIC, layout->ts_size - MICRODB_PAGE_HEADER_SIZE, &gen_ts, &len, &count, &stored_crc)) {
        info.reason = "ts_header_invalid";
        return info;
    }
    if (gen_ts != gen_kv) {
        info.reason = "generation_mismatch_kv_ts";
        return info;
    }
    if (!crc_region(fp, bank_base + layout->kv_size + MICRODB_PAGE_HEADER_SIZE, len, &calc_crc) || calc_crc != stored_crc) {
        info.reason = "ts_payload_crc_mismatch";
        return info;
    }
    info.ts_entries = count;

    if (!read_at(fp, bank_base + layout->kv_size + layout->ts_size, header, sizeof(header))) {
        info.reason = "rel_header_read_error";
        return info;
    }
    if (!validate_page_header(header, MICRODB_REL_PAGE_MAGIC, layout->rel_size - MICRODB_PAGE_HEADER_SIZE, &gen_rel, &len, &count, &stored_crc)) {
        info.reason = "rel_header_invalid";
        return info;
    }
    if (gen_rel != gen_kv) {
        info.reason = "generation_mismatch_kv_rel";
        return info;
    }
    if (!crc_region(fp, bank_base + layout->kv_size + layout->ts_size + MICRODB_PAGE_HEADER_SIZE, len, &calc_crc) ||
        calc_crc != stored_crc) {
        info.reason = "rel_payload_crc_mismatch";
        return info;
    }
    info.rel_entries = count;

    info.valid = true;
    info.generation = gen_kv;
    return info;
}

static wal_info_t inspect_wal(FILE *fp, const verify_layout_t *layout) {
    wal_info_t info;
    uint8_t header[MICRODB_WAL_HEADER_SIZE];
    uint32_t i;
    uint32_t offset;
    memset(&info, 0, sizeof(info));
    info.used_bytes = MICRODB_WAL_HEADER_SIZE;

    if (!read_at(fp, layout->wal_offset, header, sizeof(header))) {
        return info;
    }
    if (get_u32(header + 0u) != MICRODB_WAL_MAGIC) {
        return info;
    }
    if (MICRODB_CRC32(header, 16u) != get_u32(header + 16u)) {
        return info;
    }
    info.header_valid = true;
    info.entry_count = get_u32(header + 8u);
    info.sequence = get_u32(header + 12u);
    info.entries_valid = true;

    offset = layout->wal_offset + MICRODB_WAL_HEADER_SIZE;
    for (i = 0u; i < info.entry_count; ++i) {
        uint8_t entry_header[16];
        uint8_t payload[1536];
        uint16_t data_len;
        uint32_t aligned_len;
        uint32_t entry_crc;
        uint32_t crc;

        if (!read_at(fp, offset, entry_header, sizeof(entry_header))) {
            info.entries_valid = false;
            break;
        }
        if (get_u32(entry_header + 0u) != MICRODB_WAL_ENTRY_MAGIC) {
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
        crc = MICRODB_CRC32(entry_header, 12u);
        crc = microdb_crc32(crc, payload, data_len);
        if (crc != entry_crc) {
            info.entries_valid = false;
            break;
        }
        offset += 16u + aligned_len;
    }

    info.used_bytes = offset - layout->wal_offset;
    return info;
}

static void print_usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s --image <path> [--ram-kb N] [--kv-pct N --ts-pct N --rel-pct N] [--erase-size N] [--json]\n",
            prog);
}

static int parse_args(int argc, char **argv, verify_cfg_t *cfg) {
    int i;
    if (cfg == NULL) {
        return VERIFY_USAGE;
    }
    memset(cfg, 0, sizeof(*cfg));
    cfg->ram_kb = MICRODB_RAM_KB;
    cfg->kv_pct = MICRODB_RAM_KV_PCT;
    cfg->ts_pct = MICRODB_RAM_TS_PCT;
    cfg->rel_pct = MICRODB_RAM_REL_PCT;
    cfg->erase_size = 4096u;
    cfg->json = false;

    for (i = 1; i < argc; ++i) {
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
        } else {
            return VERIFY_USAGE;
        }
    }

    if (cfg->image_path == NULL) {
        return VERIFY_USAGE;
    }
    return VERIFY_OK;
}

int main(int argc, char **argv) {
    verify_cfg_t cfg;
    verify_layout_t layout;
    FILE *fp = NULL;
    uint32_t cap = 0u;
    uint8_t super_a[MICRODB_SUPERBLOCK_SIZE];
    uint8_t super_b[MICRODB_SUPERBLOCK_SIZE];
    super_info_t sa;
    super_info_t sb;
    bank_info_t ba;
    bank_info_t bb;
    wal_info_t wal;
    const char *recovery_reason = "none";
    const char *verdict = "ok";
    uint32_t selected_bank = 0u;
    uint32_t selected_gen = 0u;
    bool have_selected = false;
    bool selected_from_super = false;
    bool super_region_ff = false;
    int rc;

    memset(&sa, 0, sizeof(sa));
    memset(&sb, 0, sizeof(sb));

    rc = parse_args(argc, argv, &cfg);
    if (rc != VERIFY_OK) {
        (void)fail_verify("parse_args", "invalid arguments", rc);
        print_usage(argv[0]);
        return rc;
    }

    fp = fopen(cfg.image_path, "rb");
    if (fp == NULL) {
        char detail[256];
        (void)snprintf(detail, sizeof(detail), "cannot open image '%s'", cfg.image_path);
        return fail_verify("fopen(image)", detail, VERIFY_IO_ERROR);
    }
    if (!file_size_u32(fp, &cap)) {
        fclose(fp);
        return fail_verify("file_size_u32", "cannot read image size", VERIFY_IO_ERROR);
    }
    if (!compute_layout(cap, &cfg, &layout)) {
        fclose(fp);
        return fail_verify("compute_layout",
                           "invalid config/layout (ram/split/erase mismatch vs image size)",
                           VERIFY_INVALID_CONFIG);
    }

    if (!read_at(fp, layout.super_a_offset, super_a, sizeof(super_a)) ||
        !read_at(fp, layout.super_b_offset, super_b, sizeof(super_b))) {
        fclose(fp);
        return fail_verify("read_at(superblocks)", "cannot read superblocks", VERIFY_IO_ERROR);
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
        const bank_info_t *selected = (selected_bank == 0u) ? &ba : &bb;
        if (!selected->valid || selected->generation != selected_gen) {
            verdict = "corrupt";
            recovery_reason = selected_from_super ? "selected_superblock_bank_invalid" : "selected_bank_invalid";
        } else if (!wal.header_valid) {
            verdict = "ok_with_wal_header_reset";
            recovery_reason = "wal_header_reset";
        } else if (!wal.entries_valid) {
            verdict = "ok_with_wal_tail_truncation";
            recovery_reason = "wal_tail_truncated";
        } else {
            verdict = "ok";
        }
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
        printf("  \"wal\": {\"header_valid\": %s, \"entries_valid\": %s, \"entry_count\": %u, \"sequence\": %u, \"used_bytes\": %u}\n",
               wal.header_valid ? "true" : "false",
               wal.entries_valid ? "true" : "false",
               wal.entry_count,
               wal.sequence,
               wal.used_bytes);
        printf("}\n");
    } else {
        printf("verdict: %s\n", verdict);
        printf("recovery_reason: %s\n", recovery_reason);
        printf("selected: %s bank=%u generation=%u\n", have_selected ? "yes" : "no", selected_bank, selected_gen);
        printf("super: a(valid=%u gen=%u bank=%u) b(valid=%u gen=%u bank=%u)\n",
               sa.valid ? 1u : 0u,
               sa.generation,
               sa.active_bank,
               sb.valid ? 1u : 0u,
               sb.generation,
               sb.active_bank);
        printf("bank_a: valid=%u gen=%u reason=%s\n", ba.valid ? 1u : 0u, ba.generation, ba.reason);
        printf("bank_b: valid=%u gen=%u reason=%s\n", bb.valid ? 1u : 0u, bb.generation, bb.reason);
        printf("wal: header_valid=%u entries_valid=%u entries=%u seq=%u used=%u/%u\n",
               wal.header_valid ? 1u : 0u,
               wal.entries_valid ? 1u : 0u,
               wal.entry_count,
               wal.sequence,
               wal.used_bytes,
               layout.wal_size);
    }

    fclose(fp);
    if (strcmp(verdict, "ok") == 0 || strcmp(verdict, "ok_with_wal_header_reset") == 0 ||
        strcmp(verdict, "ok_with_wal_tail_truncation") == 0) {
        return VERIFY_OK;
    }
    if (strcmp(verdict, "uninitialized") == 0) {
        return VERIFY_UNINITIALIZED;
    }
    return VERIFY_CORRUPT;
}
