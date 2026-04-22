// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "../port/posix/microdb_port_posix.h"
#include "../src/microdb_crc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <process.h>
#else
#include <sys/wait.h>
#endif

enum {
    MICRODB_WAL_ENTRY_MAGIC = 0x454E5452u,
    MICRODB_WAL_ENGINE_TXN_KV = 3u,
    MICRODB_WAL_OP_SET_INSERT = 0u
};

static char g_verify_tool[512];
static unsigned g_seq = 0u;

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
} verifier_layout_t;

static int normalize_system_exit(int rc) {
#ifdef _WIN32
    return rc;
#else
    if (rc < 0) {
        return rc;
    }
    if (WIFEXITED(rc)) {
        return WEXITSTATUS(rc);
    }
    return rc;
#endif
}

static void put_u32(uint8_t *dst, uint32_t value) {
    memcpy(dst, &value, sizeof(value));
}

static void put_u16(uint8_t *dst, uint16_t value) {
    memcpy(dst, &value, sizeof(value));
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

static uint32_t align_u32(uint32_t value, uint32_t align) {
    return (value + (align - 1u)) & ~(align - 1u);
}

static int contains_text(const char *haystack, const char *needle) {
    return (haystack != NULL && needle != NULL && strstr(haystack, needle) != NULL) ? 1 : 0;
}

static int read_file_to_buffer(const char *path, char *out, size_t out_sz) {
    FILE *fp;
    size_t n;
    if (path == NULL || out == NULL || out_sz == 0u) {
        return 0;
    }
    fp = fopen(path, "rb");
    if (fp == NULL) {
        return 0;
    }
    n = fread(out, 1u, out_sz - 1u, fp);
    out[n] = '\0';
    fclose(fp);
    return 1;
}

static void strip_outer_quotes(char *s) {
    size_t r = 0u;
    size_t w = 0u;
    if (s == NULL) {
        return;
    }
    while (s[r] != '\0') {
        if (s[r] != '"' && s[r] != '\'') {
            s[w++] = s[r];
        }
        r++;
    }
    s[w] = '\0';
}

static void set_verify_tool_path(const char *argv0) {
#ifdef MICRODB_VERIFY_TOOL_PATH
    (void)argv0;
    memset(g_verify_tool, 0, sizeof(g_verify_tool));
    (void)snprintf(g_verify_tool, sizeof(g_verify_tool), "%s", MICRODB_VERIFY_TOOL_PATH);
    strip_outer_quotes(g_verify_tool);
#else
    const char *slash = NULL;
    size_t dir_len = 0u;
    memset(g_verify_tool, 0, sizeof(g_verify_tool));
    if (argv0 == NULL || argv0[0] == '\0') {
#ifdef _WIN32
        (void)snprintf(g_verify_tool, sizeof(g_verify_tool), ".\\microdb_verify.exe");
#else
        (void)snprintf(g_verify_tool, sizeof(g_verify_tool), "./microdb_verify");
#endif
        return;
    }
    slash = strrchr(argv0, '/');
#ifdef _WIN32
    {
        const char *backslash = strrchr(argv0, '\\');
        if (backslash != NULL && (slash == NULL || backslash > slash)) {
            slash = backslash;
        }
    }
#endif
    if (slash != NULL) {
        dir_len = (size_t)(slash - argv0);
    }
    if (dir_len == 0u) {
        dir_len = 1u;
        g_verify_tool[0] = '.';
    } else if (dir_len < sizeof(g_verify_tool)) {
        memcpy(g_verify_tool, argv0, dir_len);
    } else {
        dir_len = sizeof(g_verify_tool) - 1u;
        memcpy(g_verify_tool, argv0, dir_len);
    }
#ifdef _WIN32
    (void)snprintf(g_verify_tool + dir_len, sizeof(g_verify_tool) - dir_len, "\\microdb_verify.exe");
#else
    (void)snprintf(g_verify_tool + dir_len, sizeof(g_verify_tool) - dir_len, "/microdb_verify");
#endif
    strip_outer_quotes(g_verify_tool);
#endif
}

static void compute_layout_32kb(verifier_layout_t *out) {
    uint32_t erase_size = 256u;
    uint32_t total_bytes = 32u * 1024u;
    uint32_t kv_bytes = (total_bytes * 40u) / 100u;
    uint32_t ts_bytes = (total_bytes * 40u) / 100u;
    uint32_t rel_bytes = total_bytes - kv_bytes - ts_bytes;
    uint32_t max_key_len = (MICRODB_KV_KEY_MAX_LEN > 0u) ? (MICRODB_KV_KEY_MAX_LEN - 1u) : 0u;
    uint32_t per_entry = 1u + max_key_len + 4u + MICRODB_KV_VAL_MAX_LEN + 4u;
    uint32_t max_entries = (MICRODB_KV_MAX_KEYS > MICRODB_TXN_STAGE_KEYS) ? (MICRODB_KV_MAX_KEYS - MICRODB_TXN_STAGE_KEYS) : 0u;
    uint32_t kv_payload_max = max_entries * per_entry;

    memset(out, 0, sizeof(*out));
    out->wal_offset = 0u;
    out->super_size = erase_size;
    out->kv_size = align_u32(kv_payload_max + 32u, erase_size);
    out->ts_size = align_u32(ts_bytes + 32u, erase_size);
    out->rel_size = align_u32(rel_bytes + 32u, erase_size);
    out->bank_size = out->kv_size + out->ts_size + out->rel_size;
    out->wal_size = erase_size * 8u;
    out->super_a_offset = out->wal_size;
    out->super_b_offset = out->super_a_offset + out->super_size;
    out->bank_a_offset = out->super_b_offset + out->super_size;
    out->bank_b_offset = out->bank_a_offset + out->bank_size;
    out->total_size = out->bank_b_offset + out->bank_size;
}

static int read_at(FILE *fp, uint32_t off, void *buf, size_t len) {
    if (fseek(fp, (long)off, SEEK_SET) != 0) {
        return 0;
    }
    return fread(buf, 1u, len, fp) == len;
}

static int write_at(FILE *fp, uint32_t off, const void *buf, size_t len) {
    if (fseek(fp, (long)off, SEEK_SET) != 0) {
        return 0;
    }
    return fwrite(buf, 1u, len, fp) == len;
}

static int patch_page_crc(FILE *fp, uint32_t page_offset) {
    uint8_t header[32];
    uint8_t *payload = NULL;
    uint32_t payload_len;
    uint32_t payload_crc = 0xFFFFFFFFu;
    uint32_t header_crc;
    if (!read_at(fp, page_offset, header, sizeof(header))) {
        return 0;
    }
    payload_len = get_u32(header + 12u);
    if (payload_len > 0u) {
        payload = (uint8_t *)malloc(payload_len);
        if (payload == NULL) {
            return 0;
        }
        if (!read_at(fp, page_offset + 32u, payload, payload_len)) {
            free(payload);
            return 0;
        }
        payload_crc = microdb_crc32(payload_crc, payload, payload_len);
        free(payload);
    }
    put_u32(header + 20u, payload_crc);
    header_crc = MICRODB_CRC32(header, 24u);
    put_u32(header + 24u, header_crc);
    return write_at(fp, page_offset, header, sizeof(header));
}

static int run_verify_capture(const char *image_path, const char *extra_args, char *output, size_t output_sz) {
    char out_path[128];
    char cmd[1600];
    int rc;
    if (output != NULL && output_sz > 0u) {
        output[0] = '\0';
    }
    (void)snprintf(out_path, sizeof(out_path), "verify_out_%u.txt", g_seq++);
    if (extra_args == NULL) {
        extra_args = "";
    }
#ifdef _WIN32
    (void)snprintf(cmd,
                   sizeof(cmd),
                   "cmd /c \"\"%s\" --image \"%s\" --ram-kb 32 --kv-pct 40 --ts-pct 40 --rel-pct 20 --erase-size 256 %s > \"%s\" 2>&1\"",
                   g_verify_tool,
                   image_path,
                   extra_args,
                   out_path);
#else
    (void)snprintf(cmd,
                   sizeof(cmd),
                   "\"%s\" --image \"%s\" --ram-kb 32 --kv-pct 40 --ts-pct 40 --rel-pct 20 --erase-size 256 %s > \"%s\" 2>&1",
                   g_verify_tool,
                   image_path,
                   extra_args,
                   out_path);
#endif
    rc = normalize_system_exit(system(cmd));
    if (output != NULL && output_sz > 0u) {
        (void)read_file_to_buffer(out_path, output, output_sz);
    }
    remove(out_path);
    return rc;
}

static int run_json_tool_on_file(const char *json_path) {
    char cmd[512];
    int rc;
#ifdef _WIN32
    (void)snprintf(cmd, sizeof(cmd), "python -m json.tool \"%s\" > NUL 2>&1", json_path);
    rc = normalize_system_exit(system(cmd));
    if (rc == 0) {
        return rc;
    }
    (void)snprintf(cmd, sizeof(cmd), "py -3 -m json.tool \"%s\" > NUL 2>&1", json_path);
    return normalize_system_exit(system(cmd));
#else
    (void)snprintf(cmd, sizeof(cmd), "python3 -m json.tool \"%s\" > /dev/null 2>&1", json_path);
    rc = normalize_system_exit(system(cmd));
    if (rc == 0) {
        return rc;
    }
    (void)snprintf(cmd, sizeof(cmd), "python -m json.tool \"%s\" > /dev/null 2>&1", json_path);
    return normalize_system_exit(system(cmd));
#endif
}

static void create_valid_image(const char *path, uint32_t capacity) {
    microdb_storage_t storage;
    microdb_cfg_t cfg;
    microdb_t db;
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint32_t i;
    uint32_t u;
    uint8_t row[64];

    memset(&storage, 0, sizeof(storage));
    memset(&cfg, 0, sizeof(cfg));
    memset(&db, 0, sizeof(db));
    memset(&schema, 0, sizeof(schema));
    memset(row, 0, sizeof(row));
    microdb_port_posix_remove(path);
    ASSERT_EQ(microdb_port_posix_init(&storage, path, capacity), MICRODB_OK);

    cfg.storage = &storage;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_OK);

    ASSERT_EQ(microdb_kv_set(&db, "k1", "aa", 2u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&db, "k2", "bb", 2u, 0u), MICRODB_OK);

    ASSERT_EQ(microdb_ts_register(&db, "s1", MICRODB_TS_U32, 0u), MICRODB_OK);
    for (i = 0u; i < 4u; ++i) {
        u = i + 1u;
        ASSERT_EQ(microdb_ts_insert(&db, "s1", i, &u), MICRODB_OK);
    }

    ASSERT_EQ(microdb_schema_init(&schema, "users", 4u), MICRODB_OK);
    schema.schema_version = 1u;
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&db, &schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_get(&db, "users", &table), MICRODB_OK);
    u = 10u;
    ASSERT_EQ(microdb_row_set(table, row, "id", &u), MICRODB_OK);
    row[4] = 33u;
    ASSERT_EQ(microdb_rel_insert(&db, table, row), MICRODB_OK);

    ASSERT_EQ(microdb_deinit(&db), MICRODB_OK);
    microdb_port_posix_deinit(&storage);
}

static void corrupt_kv_overlap(const char *path) {
    verifier_layout_t l;
    FILE *fp;
    uint8_t payload[64];
    uint32_t banks[2];
    uint32_t i;
    compute_layout_32kb(&l);
    banks[0] = l.bank_a_offset;
    banks[1] = l.bank_b_offset;

    fp = fopen(path, "rb+");
    ASSERT_EQ(fp != NULL, 1);
    for (i = 0u; i < 2u; ++i) {
        uint32_t page = banks[i];
        uint8_t key_len;
        uint32_t val_len_off;
        ASSERT_EQ(read_at(fp, page + 32u, payload, sizeof(payload)), 1);
        key_len = payload[0];
        if (key_len > 48u) {
            /* Keep mutation deterministic under ASan/UBSan even on sparse payloads. */
            payload[0] = 2u;
            payload[1] = 'k';
            payload[2] = 'x';
            key_len = 2u;
        }
        val_len_off = 1u + (uint32_t)key_len;
        if (val_len_off + 4u > sizeof(payload)) {
            val_len_off = 3u;
        }
        put_u32(payload + val_len_off, 0xFFFFFFF0u);
        ASSERT_EQ(write_at(fp, page + 32u, payload, sizeof(payload)), 1);
        ASSERT_EQ(patch_page_crc(fp, page), 1);
    }
    ASSERT_EQ(fclose(fp), 0);
}

static void corrupt_ts_count_inconsistent(const char *path) {
    verifier_layout_t l;
    FILE *fp;
    uint8_t payload[64];
    uint32_t banks[2];
    uint32_t i;
    compute_layout_32kb(&l);
    banks[0] = l.bank_a_offset + l.kv_size;
    banks[1] = l.bank_b_offset + l.kv_size;

    fp = fopen(path, "rb+");
    ASSERT_EQ(fp != NULL, 1);
    for (i = 0u; i < 2u; ++i) {
        uint8_t name_len;
        uint32_t count_off;
        ASSERT_EQ(read_at(fp, banks[i] + 32u, payload, sizeof(payload)), 1);
        name_len = payload[0];
        if (name_len > 48u) {
            payload[0] = 2u;
            payload[1] = 's';
            payload[2] = '1';
            name_len = 2u;
        }
        count_off = 1u + (uint32_t)name_len + 1u + 4u;
        if (count_off + 4u > sizeof(payload)) {
            count_off = 8u;
        }
        put_u32(payload + count_off, 0x0000FFFFu);
        ASSERT_EQ(write_at(fp, banks[i] + 32u, payload, sizeof(payload)), 1);
        ASSERT_EQ(patch_page_crc(fp, banks[i]), 1);
    }
    ASSERT_EQ(fclose(fp), 0);
}

static void corrupt_rel_live_count_mismatch(const char *path) {
    verifier_layout_t l;
    FILE *fp;
    uint8_t payload[80];
    uint32_t banks[2];
    uint32_t i;
    compute_layout_32kb(&l);
    banks[0] = l.bank_a_offset + l.kv_size + l.ts_size;
    banks[1] = l.bank_b_offset + l.kv_size + l.ts_size;

    fp = fopen(path, "rb+");
    ASSERT_EQ(fp != NULL, 1);
    for (i = 0u; i < 2u; ++i) {
        uint8_t name_len;
        uint32_t live_count_off;
        ASSERT_EQ(read_at(fp, banks[i] + 32u, payload, sizeof(payload)), 1);
        name_len = payload[0];
        if (name_len > 56u) {
            payload[0] = 3u;
            payload[1] = 'r';
            payload[2] = 'e';
            payload[3] = 'l';
            name_len = 3u;
        }
        live_count_off = 1u + (uint32_t)name_len + 2u + 4u + 4u + 4u + 4u;
        if (live_count_off + 4u > sizeof(payload)) {
            live_count_off = 26u;
        }
        put_u32(payload + live_count_off, 0x0000FFFFu);
        ASSERT_EQ(write_at(fp, banks[i] + 32u, payload, sizeof(payload)), 1);
        ASSERT_EQ(patch_page_crc(fp, banks[i]), 1);
    }
    ASSERT_EQ(fclose(fp), 0);
}

static void append_orphaned_txn_kv(const char *path) {
    verifier_layout_t l;
    FILE *fp;
    uint8_t wal_header[32];
    uint32_t i;
    uint32_t off;
    uint32_t entry_count;
    uint16_t payload_len;
    uint8_t payload[32];
    uint8_t entry[48];
    uint32_t crc;
    compute_layout_32kb(&l);

    fp = fopen(path, "rb+");
    ASSERT_EQ(fp != NULL, 1);
    ASSERT_EQ(read_at(fp, l.wal_offset, wal_header, sizeof(wal_header)), 1);
    entry_count = get_u32(wal_header + 8u);
    off = l.wal_offset + 32u;
    for (i = 0u; i < entry_count; ++i) {
        uint8_t h[16];
        uint16_t len;
        ASSERT_EQ(read_at(fp, off, h, sizeof(h)), 1);
        len = get_u16(h + 10u);
        off += 16u + align_u32((uint32_t)len, 4u);
    }

    memset(payload, 0, sizeof(payload));
    payload[0] = 3u;
    memcpy(payload + 1u, "otx", 3u);
    put_u32(payload + 4u, 1u);
    payload[8] = 7u;
    put_u32(payload + 9u, 0u);
    payload_len = 13u;

    memset(entry, 0, sizeof(entry));
    put_u32(entry + 0u, MICRODB_WAL_ENTRY_MAGIC);
    put_u32(entry + 4u, entry_count + 1u);
    entry[8] = MICRODB_WAL_ENGINE_TXN_KV;
    entry[9] = MICRODB_WAL_OP_SET_INSERT;
    put_u16(entry + 10u, (uint16_t)payload_len);
    memcpy(entry + 16u, payload, payload_len);
    put_u32(entry + 12u, MICRODB_CRC32(entry, 12u));
    crc = get_u32(entry + 12u);
    crc = microdb_crc32(crc, payload, payload_len);
    put_u32(entry + 12u, crc);

    ASSERT_EQ(write_at(fp, off, entry, 16u + align_u32((uint32_t)payload_len, 4u)), 1);
    put_u32(wal_header + 8u, entry_count + 1u);
    put_u32(wal_header + 12u, entry_count + 1u);
    put_u32(wal_header + 16u, MICRODB_CRC32(wal_header, 16u));
    ASSERT_EQ(write_at(fp, l.wal_offset, wal_header, sizeof(wal_header)), 1);
    ASSERT_EQ(fclose(fp), 0);
}

MDB_TEST(verify_kv_page_overlap_detected) {
    char path[128];
    char output[4096];
    int rc;
    (void)snprintf(path, sizeof(path), "verify_kv_overlap_%u.bin", g_seq++);
    create_valid_image(path, 262144u);
    corrupt_kv_overlap(path);
    rc = run_verify_capture(path, "--json", output, sizeof(output));
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(contains_text(output, "\"overlaps_detected\": "), 1);
    ASSERT_EQ(contains_text(output, "WARN: kv decode"), 1);
    microdb_port_posix_remove(path);
}

MDB_TEST(verify_ts_ring_inconsistent_count) {
    char path[128];
    char output[4096];
    int rc;
    (void)snprintf(path, sizeof(path), "verify_ts_ring_%u.bin", g_seq++);
    create_valid_image(path, 262144u);
    corrupt_ts_count_inconsistent(path);
    rc = run_verify_capture(path, "--json", output, sizeof(output));
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(contains_text(output, "\"ring_anomalies\": "), 1);
    ASSERT_EQ(contains_text(output, "WARN: ts decode"), 1);
    microdb_port_posix_remove(path);
}

MDB_TEST(verify_rel_bitmap_mismatch) {
    char path[128];
    char output[4096];
    int rc;
    (void)snprintf(path, sizeof(path), "verify_rel_mismatch_%u.bin", g_seq++);
    create_valid_image(path, 262144u);
    corrupt_rel_live_count_mismatch(path);
    rc = run_verify_capture(path, "--json", output, sizeof(output));
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(contains_text(output, "\"bitmap_mismatches\": "), 1);
    ASSERT_EQ(contains_text(output, "WARN: rel decode"), 1);
    microdb_port_posix_remove(path);
}

MDB_TEST(verify_wal_orphaned_txn_detected) {
    char path[128];
    char output[4096];
    int rc;
    (void)snprintf(path, sizeof(path), "verify_wal_orphan_%u.bin", g_seq++);
    create_valid_image(path, 262144u);
    append_orphaned_txn_kv(path);
    rc = run_verify_capture(path, "--json", output, sizeof(output));
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(contains_text(output, "\"txn_orphaned\": 1"), 1);
    microdb_port_posix_remove(path);
}

MDB_TEST(verify_check_flag_exits_nonzero_on_warn) {
    char path[128];
    char output[4096];
    int rc;
    (void)snprintf(path, sizeof(path), "verify_check_warn_%u.bin", g_seq++);
    create_valid_image(path, 262144u);
    corrupt_kv_overlap(path);
    rc = run_verify_capture(path, "--check", output, sizeof(output));
    ASSERT_EQ(rc, 4);
    ASSERT_EQ(contains_text(output, "verdict:"), 1);
    ASSERT_EQ(contains_text(output, "WARN:"), 1);
    microdb_port_posix_remove(path);
}

MDB_TEST(verify_json_is_valid) {
    char path[128];
    char out_path[128];
    char output[4096];
    FILE *fp;
    int rc;
    (void)snprintf(path, sizeof(path), "verify_json_valid_%u.bin", g_seq++);
    (void)snprintf(out_path, sizeof(out_path), "verify_json_out_%u.json", g_seq++);
    create_valid_image(path, 262144u);
    rc = run_verify_capture(path, "--json", output, sizeof(output));
    ASSERT_EQ(rc, 0);
    fp = fopen(out_path, "wb");
    ASSERT_EQ(fp != NULL, 1);
    ASSERT_EQ(fwrite(output, 1u, strlen(output), fp), strlen(output));
    ASSERT_EQ(fclose(fp), 0);
    rc = run_json_tool_on_file(out_path);
    ASSERT_EQ(rc, 0);
    remove(out_path);
    microdb_port_posix_remove(path);
}

static void setup_noop(void) {}
static void teardown_noop(void) {}

int main(int argc, char **argv) {
    (void)argc;
    set_verify_tool_path((argv != NULL) ? argv[0] : NULL);
    MDB_RUN_TEST(setup_noop, teardown_noop, verify_kv_page_overlap_detected);
    MDB_RUN_TEST(setup_noop, teardown_noop, verify_ts_ring_inconsistent_count);
    MDB_RUN_TEST(setup_noop, teardown_noop, verify_rel_bitmap_mismatch);
    MDB_RUN_TEST(setup_noop, teardown_noop, verify_wal_orphaned_txn_detected);
    MDB_RUN_TEST(setup_noop, teardown_noop, verify_check_flag_exits_nonzero_on_warn);
    MDB_RUN_TEST(setup_noop, teardown_noop, verify_json_is_valid);
    return MDB_RESULT();
}
