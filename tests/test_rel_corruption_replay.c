// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/posix/lox_port_posix.h"
#include "../src/lox_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef LOX_REL_CORPUS_DIR
#define LOX_REL_CORPUS_DIR "tests/corpus/rel"
#endif

enum {
    REL_PAGE_HEADER_SIZE = 32u
};

typedef struct {
    char name[64];
    char mutation[64];
    char expect[64];
} rel_fixture_t;

typedef struct {
    uint32_t rel_page_offset_a;
    uint32_t rel_page_offset_b;
    uint32_t rel_payload_offset_a;
    uint32_t rel_payload_offset_b;
} rel_page_offsets_t;

static lox_t g_db;
static lox_storage_t g_storage;
static char g_db_path[128];
static unsigned g_seq = 0u;

static void close_db_if_open(void) {
    if (lox_core_const(&g_db)->magic == LOX_MAGIC) {
        (void)lox_deinit(&g_db);
    }
    lox_port_posix_deinit(&g_storage);
}

static void reset_and_open_db(void) {
    lox_cfg_t cfg;
    uint32_t db_bytes = 131072u;

    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_db_path, db_bytes), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void setup_fixture(void) {
    g_seq++;
    (void)snprintf(g_db_path, sizeof(g_db_path), "rel_corruption_replay_%u.bin", g_seq);
    lox_port_posix_remove(g_db_path);
    reset_and_open_db();
}

static void teardown_fixture(void) {
    close_db_if_open();
    lox_port_posix_remove(g_db_path);
}

static void write_u32_le(FILE *fp, uint32_t off, uint32_t v) {
    uint8_t b[4];
    b[0] = (uint8_t)(v & 0xFFu);
    b[1] = (uint8_t)((v >> 8u) & 0xFFu);
    b[2] = (uint8_t)((v >> 16u) & 0xFFu);
    b[3] = (uint8_t)((v >> 24u) & 0xFFu);
    ASSERT_EQ(fseek(fp, (long)off, SEEK_SET), 0);
    ASSERT_EQ(fwrite(b, 1u, sizeof(b), fp), sizeof(b));
}

static uint32_t read_u32_le(FILE *fp, uint32_t off) {
    uint8_t b[4];
    if (fseek(fp, (long)off, SEEK_SET) != 0) {
        return 0u;
    }
    if (fread(b, 1u, sizeof(b), fp) != sizeof(b)) {
        return 0u;
    }
    return ((uint32_t)b[0]) |
           ((uint32_t)b[1] << 8u) |
           ((uint32_t)b[2] << 16u) |
           ((uint32_t)b[3] << 24u);
}

static uint8_t read_u8(FILE *fp, uint32_t off) {
    uint8_t v = 0u;
    if (fseek(fp, (long)off, SEEK_SET) != 0) {
        return 0u;
    }
    if (fread(&v, 1u, 1u, fp) != 1u) {
        return 0u;
    }
    return v;
}

static void seed_rel_payload(rel_page_offsets_t *out) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[64];
    uint32_t id;
    uint8_t age;
    const lox_core_t *core;

    ASSERT_EQ(lox_schema_init(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "age", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);

    memset(row, 0, sizeof(row));
    id = 1u;
    age = 21u;
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);

    memset(row, 0, sizeof(row));
    id = 2u;
    age = 34u;
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);

    ASSERT_EQ(lox_flush(&g_db), LOX_OK);
    core = lox_core_const(&g_db);
    out->rel_page_offset_a = core->layout.bank_a_offset + core->layout.kv_size + core->layout.ts_size;
    out->rel_page_offset_b = core->layout.bank_b_offset + core->layout.kv_size + core->layout.ts_size;
    out->rel_payload_offset_a = out->rel_page_offset_a + REL_PAGE_HEADER_SIZE;
    out->rel_payload_offset_b = out->rel_page_offset_b + REL_PAGE_HEADER_SIZE;
}

static void trim_ws(char *s) {
    size_t len;
    char *start = s;
    while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n') {
        start++;
    }
    if (start != s) {
        memmove(s, start, strlen(start) + 1u);
    }
    len = strlen(s);
    while (len > 0u && (s[len - 1u] == ' ' || s[len - 1u] == '\t' || s[len - 1u] == '\r' || s[len - 1u] == '\n')) {
        s[len - 1u] = '\0';
        len--;
    }
}

static void load_fixture_file(const char *path, rel_fixture_t *fx) {
    FILE *fp;
    char line[256];

    memset(fx, 0, sizeof(*fx));
    fp = fopen(path, "rb");
    ASSERT_EQ(fp != NULL, 1);
    while (fgets(line, (int)sizeof(line), fp) != NULL) {
        char *eq = strchr(line, '=');
        if (eq == NULL) {
            continue;
        }
        *eq = '\0';
        trim_ws(line);
        trim_ws(eq + 1);
        if (strcmp(line, "name") == 0) {
            strncpy(fx->name, eq + 1, sizeof(fx->name) - 1u);
        } else if (strcmp(line, "mutation") == 0) {
            strncpy(fx->mutation, eq + 1, sizeof(fx->mutation) - 1u);
        } else if (strcmp(line, "expect") == 0) {
            strncpy(fx->expect, eq + 1, sizeof(fx->expect) - 1u);
        }
    }
    fclose(fp);

    ASSERT_EQ(fx->name[0] != '\0', 1);
    ASSERT_EQ(fx->mutation[0] != '\0', 1);
    ASSERT_EQ(fx->expect[0] != '\0', 1);
}

static lox_err_t parse_expect_code(const char *s) {
    if (strcmp(s, "LOX_OK") == 0) {
        return LOX_OK;
    }
    if (strcmp(s, "LOX_ERR_CORRUPT") == 0) {
        return LOX_ERR_CORRUPT;
    }
    if (strcmp(s, "LOX_OK_OR_CORRUPT") == 0) {
        return (lox_err_t)777;
    }
    return LOX_ERR_INVALID;
}

static void apply_rel_mutation(const char *db_path, const rel_page_offsets_t *off, const char *mutation) {
    uint32_t page_offsets[2];
    uint32_t payload_offsets[2];
    uint32_t i;
    FILE *fp = fopen(db_path, "rb+");
    ASSERT_EQ(fp != NULL, 1);
    page_offsets[0] = off->rel_page_offset_a;
    page_offsets[1] = off->rel_page_offset_b;
    payload_offsets[0] = off->rel_payload_offset_a;
    payload_offsets[1] = off->rel_payload_offset_b;

    if (strcmp(mutation, "flip_payload_crc") == 0) {
        for (i = 0u; i < 2u; ++i) {
            uint8_t v = read_u8(fp, page_offsets[i] + 20u);
            v ^= 0xA5u;
            ASSERT_EQ(fseek(fp, (long)(page_offsets[i] + 20u), SEEK_SET), 0);
            ASSERT_EQ(fwrite(&v, 1u, 1u, fp), 1u);
        }
    } else if (strcmp(mutation, "overflow_table_name_len") == 0) {
        for (i = 0u; i < 2u; ++i) {
            uint8_t bad = (uint8_t)LOX_REL_TABLE_NAME_LEN;
            ASSERT_EQ(fseek(fp, (long)payload_offsets[i], SEEK_SET), 0);
            ASSERT_EQ(fwrite(&bad, 1u, 1u, fp), 1u);
        }
    } else if (strcmp(mutation, "overflow_col_count") == 0) {
        for (i = 0u; i < 2u; ++i) {
            uint8_t name_len = read_u8(fp, payload_offsets[i]);
            uint32_t col_count_off = payload_offsets[i] + 1u + (uint32_t)name_len + 2u + 4u + 4u;
            write_u32_le(fp, col_count_off, (uint32_t)LOX_REL_MAX_COLS + 1u);
        }
    } else if (strcmp(mutation, "overflow_row_count") == 0) {
        for (i = 0u; i < 2u; ++i) {
            uint8_t name_len = read_u8(fp, payload_offsets[i]);
            uint32_t max_rows_off = payload_offsets[i] + 1u + (uint32_t)name_len + 2u;
            uint32_t row_count_off = payload_offsets[i] + 1u + (uint32_t)name_len + 2u + 4u + 4u + 4u + 4u;
            uint32_t max_rows = read_u32_le(fp, max_rows_off);
            write_u32_le(fp, row_count_off, max_rows + 1u);
        }
    } else {
        ASSERT_EQ(0, 1);
    }

    fclose(fp);
}

static void run_fixture_case(const char *fixture_name) {
    char fixture_path[256];
    rel_fixture_t fx;
    rel_page_offsets_t off;
    lox_cfg_t cfg;
    lox_err_t expected;
    lox_err_t rc;

    (void)snprintf(fixture_path, sizeof(fixture_path), "%s/%s", LOX_REL_CORPUS_DIR, fixture_name);
    load_fixture_file(fixture_path, &fx);
    expected = parse_expect_code(fx.expect);
    ASSERT_EQ(expected == LOX_ERR_INVALID, 0);

    close_db_if_open();
    lox_port_posix_remove(g_db_path);
    reset_and_open_db();
    seed_rel_payload(&off);
    close_db_if_open();
    apply_rel_mutation(g_db_path, &off, fx.mutation);

    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    ASSERT_EQ(lox_port_posix_init(&g_storage, g_db_path, 131072u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    rc = lox_init(&g_db, &cfg);
    if ((int)expected == 777) {
        ASSERT_EQ((rc == LOX_OK || rc == LOX_ERR_CORRUPT), 1);
    } else {
        ASSERT_EQ(rc, expected);
    }
}

MDB_TEST(rel_corruption_fixture_replay_contract) {
    static const char *fixtures[] = {
        "flip_payload_crc.fixture",
        "overflow_table_name_len.fixture",
        "overflow_col_count.fixture",
        "overflow_row_count.fixture"
    };
    size_t i;

    for (i = 0u; i < (sizeof(fixtures) / sizeof(fixtures[0])); ++i) {
        run_fixture_case(fixtures[i]);
        close_db_if_open();
    }
}

int main(void) {
    MDB_RUN_TEST(setup_fixture, teardown_fixture, rel_corruption_fixture_replay_contract);
    return MDB_RESULT();
}
