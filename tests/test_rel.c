// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "../port/ram/lox_port_ram.h"
#include "../src/lox_internal.h"

#include <string.h>

static lox_t g_db;
static lox_storage_t g_ram_storage;
static bool g_rel_mutate_once = false;

typedef struct {
    uint32_t count;
    uint32_t ids[32];
} rel_iter_ctx_t;

static const lox_schema_impl_t *schema_impl(const lox_schema_t *schema) {
    return (const lox_schema_impl_t *)&schema->_opaque[0];
}

static void setup_db(void) {
    lox_cfg_t cfg;

    ASSERT_EQ(lox_port_ram_init(&g_ram_storage, 65536u), LOX_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_ram_storage;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(lox_init(&g_db, &cfg), LOX_OK);
}

static void teardown_db(void) {
    lox_deinit(&g_db);
    lox_port_ram_deinit(&g_ram_storage);
}

static lox_err_t make_indexed_schema(lox_schema_t *schema, const char *name, uint32_t max_rows) {
    lox_err_t err;

    err = lox_schema_init(schema, name, max_rows);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(schema, "id", LOX_COL_U32, sizeof(uint32_t), true);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(schema, "age", LOX_COL_U8, sizeof(uint8_t), false);
    if (err != LOX_OK) {
        return err;
    }
    return lox_schema_seal(schema);
}

static lox_err_t make_table(lox_schema_t *schema, lox_table_t **out_table) {
    lox_err_t err;

    err = lox_table_create(&g_db, schema);
    if (err != LOX_OK) {
        return err;
    }
    return lox_table_get(&g_db, schema_impl(schema)->name, out_table);
}

static bool rel_collect_id_cb(const void *row_buf, void *ctx) {
    rel_iter_ctx_t *iter = (rel_iter_ctx_t *)ctx;
    const uint8_t *row = (const uint8_t *)row_buf;
    uint32_t id = 0u;

    memcpy(&id, row, sizeof(id));
    iter->ids[iter->count++] = id;
    return true;
}

static bool rel_collect_id_stop_after_two(const void *row_buf, void *ctx) {
    rel_iter_ctx_t *iter = (rel_iter_ctx_t *)ctx;
    const uint8_t *row = (const uint8_t *)row_buf;
    uint32_t id = 0u;

    memcpy(&id, row, sizeof(id));
    iter->ids[iter->count++] = id;
    return iter->count < 2u;
}

typedef struct {
    lox_table_t *table;
    uint32_t id;
} rel_find_mutate_ctx_t;

static bool rel_find_mutating_cb(const void *row_buf, void *ctx) {
    rel_find_mutate_ctx_t *mctx = (rel_find_mutate_ctx_t *)ctx;
    (void)row_buf;
    if (!g_rel_mutate_once) {
        g_rel_mutate_once = true;
        (void)lox_rel_delete(&g_db, mctx->table, &mctx->id, NULL);
    }
    return true;
}

MDB_TEST(rel_schema_init_ok) {
    lox_schema_t schema;
    ASSERT_EQ(lox_schema_init(&schema, "users", 16u), LOX_OK);
}

MDB_TEST(rel_schema_add_ok) {
    lox_schema_t schema;
    ASSERT_EQ(lox_schema_init(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
}

MDB_TEST(rel_schema_add_after_seal_returns_sealed) {
    lox_schema_t schema;
    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "extra", LOX_COL_U8, sizeof(uint8_t), false), LOX_ERR_SEALED);
}

MDB_TEST(rel_schema_second_index_invalid) {
    lox_schema_t schema;
    ASSERT_EQ(lox_schema_init(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "serial", LOX_COL_U32, sizeof(uint32_t), true), LOX_ERR_INVALID);
}

MDB_TEST(rel_schema_seal_empty_invalid) {
    lox_schema_t schema;
    ASSERT_EQ(lox_schema_init(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_ERR_INVALID);
}

MDB_TEST(rel_schema_seal_alignment_correct) {
    lox_schema_t schema;
    const lox_schema_impl_t *impl;

    ASSERT_EQ(lox_schema_init(&schema, "align", 8u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "a", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "b", LOX_COL_U32, sizeof(uint32_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "c", LOX_COL_U16, sizeof(uint16_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    impl = schema_impl(&schema);
    ASSERT_EQ(impl->cols[0].offset, 0u);
    ASSERT_EQ(impl->cols[1].offset, 4u);
    ASSERT_EQ(impl->cols[2].offset, 8u);
    ASSERT_EQ(impl->row_size, 12u);
}

MDB_TEST(rel_schema_seal_idempotent) {
    lox_schema_t schema;
    size_t row_size_before;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    row_size_before = schema_impl(&schema)->row_size;
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(schema_impl(&schema)->row_size, row_size_before);
}

MDB_TEST(rel_schema_seal_rejects_oversized_row) {
    lox_schema_t schema;
    ASSERT_EQ(lox_schema_init(&schema, "wide", 1u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "payload", LOX_COL_BLOB, 2048u, false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_ERR_OVERFLOW);
}

MDB_TEST(rel_schema_add_beyond_max_cols_full) {
    lox_schema_t schema;
    char name[16];
    uint32_t i;

    ASSERT_EQ(lox_schema_init(&schema, "cols", 8u), LOX_OK);
    for (i = 0; i < LOX_REL_MAX_COLS; ++i) {
        memset(name, 0, sizeof(name));
        name[0] = 'c';
        name[1] = (char)('a' + (char)(i % 26u));
        name[2] = (char)('0' + (char)(i / 26u));
        ASSERT_EQ(lox_schema_add(&schema, name, LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    }
    ASSERT_EQ(lox_schema_add(&schema, "overflow", LOX_COL_U8, sizeof(uint8_t), false), LOX_ERR_FULL);
}

MDB_TEST(rel_schema_scalar_size_mismatch_invalid) {
    lox_schema_t schema;
    ASSERT_EQ(lox_schema_init(&schema, "bad", 8u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint16_t), false), LOX_ERR_INVALID);
}

MDB_TEST(rel_table_create_sealed_ok) {
    lox_schema_t schema;
    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
}

MDB_TEST(rel_table_create_unsealed_invalid) {
    lox_schema_t schema;
    ASSERT_EQ(lox_schema_init(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_ERR_INVALID);
}

MDB_TEST(rel_table_create_duplicate_ok) {
    lox_schema_t schema;
    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
}

MDB_TEST(rel_table_create_schema_version_changed_after_seal_returns_err_schema) {
    lox_schema_t schema;
    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    schema.schema_version = 7u;
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_ERR_SCHEMA);
}

MDB_TEST(rel_table_create_beyond_max_tables_full) {
    lox_schema_t schema;
    char name[16];
    uint32_t i;

    for (i = 0; i < LOX_REL_MAX_TABLES; ++i) {
        memset(name, 0, sizeof(name));
        name[0] = 't';
        name[1] = (char)('0' + (char)i);
        ASSERT_EQ(make_indexed_schema(&schema, name, 4u), LOX_OK);
        ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    }

    ASSERT_EQ(make_indexed_schema(&schema, "extra", 4u), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_ERR_FULL);
}

MDB_TEST(rel_table_get_existing_ok) {
    lox_schema_t schema;
    lox_table_t *table = NULL;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(lox_table_create(&g_db, &schema), LOX_OK);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
    ASSERT_EQ(table != NULL, 1);
}

MDB_TEST(rel_table_get_unknown_not_found) {
    lox_table_t *table = NULL;
    ASSERT_EQ(lox_table_get(&g_db, "missing", &table), LOX_ERR_NOT_FOUND);
}

MDB_TEST(rel_table_row_size_matches_schema) {
    lox_schema_t schema;
    lox_table_t *table = NULL;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_table_row_size(table), schema_impl(&schema)->row_size);
}

MDB_TEST(rel_row_set_unknown_column_not_found) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "missing", &id), LOX_ERR_NOT_FOUND);
}

MDB_TEST(rel_row_get_unknown_column_not_found) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t out = 0u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "missing", &out, NULL), LOX_ERR_NOT_FOUND);
}

MDB_TEST(rel_row_roundtrip_unsigned_scalars) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint8_t u8 = 1u, ou8 = 0u;
    uint16_t u16 = 2u, ou16 = 0u;
    uint32_t u32 = 3u, ou32 = 0u;
    uint64_t u64 = 4u, ou64 = 0u;

    ASSERT_EQ(lox_schema_init(&schema, "u", 4u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "u8", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "u16", LOX_COL_U16, sizeof(uint16_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "u32", LOX_COL_U32, sizeof(uint32_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "u64", LOX_COL_U64, sizeof(uint64_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "u8", &u8), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "u16", &u16), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "u32", &u32), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "u64", &u64), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "u8", &ou8, NULL), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "u16", &ou16, NULL), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "u32", &ou32, NULL), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "u64", &ou64, NULL), LOX_OK);
    ASSERT_EQ(ou8, 1u);
    ASSERT_EQ(ou16, 2u);
    ASSERT_EQ(ou32, 3u);
    ASSERT_EQ(ou64, 4u);
}

MDB_TEST(rel_row_roundtrip_signed_scalars) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    int8_t i8 = -1, oi8 = 0;
    int16_t i16 = -2, oi16 = 0;
    int32_t i32 = -3, oi32 = 0;
    int64_t i64 = -4, oi64 = 0;

    ASSERT_EQ(lox_schema_init(&schema, "i", 4u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "i8", LOX_COL_I8, sizeof(int8_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "i16", LOX_COL_I16, sizeof(int16_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "i32", LOX_COL_I32, sizeof(int32_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "i64", LOX_COL_I64, sizeof(int64_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "i8", &i8), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "i16", &i16), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "i32", &i32), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "i64", &i64), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "i8", &oi8, NULL), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "i16", &oi16, NULL), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "i32", &oi32, NULL), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "i64", &oi64, NULL), LOX_OK);
    ASSERT_EQ(oi8, -1);
    ASSERT_EQ(oi16, -2);
    ASSERT_EQ(oi32, -3);
    ASSERT_EQ(oi64, -4);
}

MDB_TEST(rel_row_roundtrip_float_scalars) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    float f32 = 1.5f, of32 = 0.0f;
    double f64 = 2.5, of64 = 0.0;

    ASSERT_EQ(lox_schema_init(&schema, "f", 4u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "f32", LOX_COL_F32, sizeof(float), false), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "f64", LOX_COL_F64, sizeof(double), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "f32", &f32), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "f64", &f64), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "f32", &of32, NULL), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "f64", &of64, NULL), LOX_OK);
    ASSERT_EQ(of32 == f32, 1);
    ASSERT_EQ(of64 == f64, 1);
}

MDB_TEST(rel_row_roundtrip_bool) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    bool value = true;
    bool out = false;

    ASSERT_EQ(lox_schema_init(&schema, "b", 4u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "flag", LOX_COL_BOOL, sizeof(bool), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "flag", &value), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "flag", &out, NULL), LOX_OK);
    ASSERT_EQ(out, 1);
}

MDB_TEST(rel_row_roundtrip_str) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    char out[16] = { 0 };

    ASSERT_EQ(lox_schema_init(&schema, "s", 4u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "name", LOX_COL_STR, 8u, false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "name", "bob"), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "name", out, NULL), LOX_OK);
    ASSERT_EQ(strcmp(out, "bob"), 0);
}

MDB_TEST(rel_row_roundtrip_blob) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint8_t blob[4] = { 1u, 2u, 3u, 4u };
    uint8_t out[4] = { 0 };

    ASSERT_EQ(lox_schema_init(&schema, "bl", 4u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "blob", LOX_COL_BLOB, 4u, false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "blob", blob), LOX_OK);
    ASSERT_EQ(lox_row_get(table, row, "blob", out, NULL), LOX_OK);
    ASSERT_MEM_EQ(out, blob, 4u);
}

MDB_TEST(rel_str_overflow_schema) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };

    ASSERT_EQ(lox_schema_init(&schema, "s", 4u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "name", LOX_COL_STR, 4u, false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "name", "toolong"), LOX_ERR_SCHEMA);
}

MDB_TEST(rel_insert_ok) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 7u;
    uint8_t age = 9u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
}

MDB_TEST(rel_insert_beyond_max_rows_full) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 1u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 1u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    id = 2u;
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_ERR_FULL);
}

MDB_TEST(rel_insert_null_row_buf_invalid) {
    lox_schema_t schema;
    lox_table_t *table = NULL;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, NULL), LOX_ERR_INVALID);
}

MDB_TEST(rel_find_by_index_returns_rows) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 42u;
    uint8_t age = 3u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 3u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    ASSERT_EQ(lox_rel_find(&g_db, table, &id, rel_collect_id_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 1u);
    ASSERT_EQ(ctx.ids[0], 42u);
}

MDB_TEST(rel_find_no_match_cb_never_called) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint32_t id = 99u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_rel_find(&g_db, table, &id, rel_collect_id_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 0u);
}

MDB_TEST(rel_find_mutation_during_callback_returns_modified) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 42u;
    uint8_t age = 3u;
    rel_find_mutate_ctx_t ctx;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 4u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    age = 4u;
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);

    g_rel_mutate_once = false;
    ctx.table = table;
    ctx.id = id;
    ASSERT_EQ(lox_rel_find(&g_db, table, &id, rel_find_mutating_cb, &ctx), LOX_ERR_MODIFIED);
}

MDB_TEST(rel_find_without_index_invalid) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint32_t id = 1u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(lox_schema_init(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), false), LOX_OK);
    ASSERT_EQ(lox_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_rel_find(&g_db, table, &id, rel_collect_id_cb, &ctx), LOX_ERR_INVALID);
}

MDB_TEST(rel_find_by_non_index_correct) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint8_t out[128] = { 0 };
    uint32_t id = 2u;
    uint8_t age = 55u;
    uint32_t out_id = 0u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    ASSERT_EQ(lox_rel_find_by(&g_db, table, "age", &age, out), LOX_OK);
    ASSERT_EQ(lox_row_get(table, out, "id", &out_id, NULL), LOX_OK);
    ASSERT_EQ(out_id, 2u);
}

MDB_TEST(rel_find_by_no_match) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t out[128] = { 0 };
    uint8_t age = 77u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_rel_find_by(&g_db, table, "age", &age, out), LOX_ERR_NOT_FOUND);
}

MDB_TEST(rel_delete_by_index_removes_rows) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 3u;
    uint8_t age = 9u;
    uint32_t deleted = 0u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    ASSERT_EQ(lox_rel_delete(&g_db, table, &id, &deleted), LOX_OK);
    ASSERT_EQ(deleted, 1u);
    ASSERT_EQ(lox_rel_find(&g_db, table, &id, rel_collect_id_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 0u);
}

MDB_TEST(rel_delete_no_match_zero) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint32_t id = 4u;
    uint32_t deleted = 99u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_rel_delete(&g_db, table, &id, &deleted), LOX_OK);
    ASSERT_EQ(deleted, 0u);
}

MDB_TEST(rel_iter_visits_rows_in_insertion_order) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id;
    uint8_t age = 1u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 3u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    for (id = 1u; id <= 3u; ++id) {
        ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
        ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
        ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    }
    ASSERT_EQ(lox_rel_iter(&g_db, table, rel_collect_id_cb, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 3u);
    ASSERT_EQ(ctx.ids[0], 1u);
    ASSERT_EQ(ctx.ids[1], 2u);
    ASSERT_EQ(ctx.ids[2], 3u);
}

MDB_TEST(rel_iter_callback_false_stops_early) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id;
    uint8_t age = 1u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 3u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    for (id = 1u; id <= 3u; ++id) {
        ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
        ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
        ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    }
    ASSERT_EQ(lox_rel_iter(&g_db, table, rel_collect_id_stop_after_two, &ctx), LOX_OK);
    ASSERT_EQ(ctx.count, 2u);
}

MDB_TEST(rel_iter_detects_concurrent_mutation) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 1u;
    rel_find_mutate_ctx_t ctx;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 3u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    id = 2u;
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);

    g_rel_mutate_once = false;
    ctx.table = table;
    ctx.id = id;
    ASSERT_EQ(lox_rel_iter(&g_db, table, rel_find_mutating_cb, &ctx), LOX_ERR_MODIFIED);
}

MDB_TEST(rel_count_after_insert_delete) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 1u;
    uint32_t count = 0u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    ASSERT_EQ(lox_rel_count(table, &count), LOX_OK);
    ASSERT_EQ(count, 1u);
    ASSERT_EQ(lox_rel_delete(&g_db, table, &id, NULL), LOX_OK);
    ASSERT_EQ(lox_rel_count(table, &count), LOX_OK);
    ASSERT_EQ(count, 0u);
}

MDB_TEST(rel_clear_preserves_table) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 1u;
    uint32_t count = 99u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    ASSERT_EQ(lox_rel_clear(&g_db, table), LOX_OK);
    ASSERT_EQ(lox_rel_count(table, &count), LOX_OK);
    ASSERT_EQ(count, 0u);
    ASSERT_EQ(lox_table_get(&g_db, "users", &table), LOX_OK);
}

MDB_TEST(rel_delete_updates_bitmap_and_index) {
    lox_schema_t schema;
    lox_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 5u;
    uint8_t age = 2u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), LOX_OK);
    ASSERT_EQ(make_table(&schema, &table), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(lox_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(lox_rel_insert(&g_db, table, row), LOX_OK);
    ASSERT_EQ(lox_rel_delete(&g_db, table, &id, NULL), LOX_OK);
    ASSERT_EQ(table->live_count, 0u);
    ASSERT_EQ(table->index_count, 0u);
    ASSERT_EQ(((table->alive_bitmap[0] & 1u) == 0u), 1);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_init_ok);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_add_ok);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_add_after_seal_returns_sealed);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_second_index_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_seal_empty_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_seal_alignment_correct);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_seal_idempotent);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_seal_rejects_oversized_row);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_add_beyond_max_cols_full);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_scalar_size_mismatch_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_sealed_ok);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_unsealed_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_duplicate_ok);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_schema_version_changed_after_seal_returns_err_schema);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_beyond_max_tables_full);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_get_existing_ok);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_get_unknown_not_found);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_row_size_matches_schema);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_set_unknown_column_not_found);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_get_unknown_column_not_found);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_roundtrip_unsigned_scalars);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_roundtrip_signed_scalars);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_roundtrip_float_scalars);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_roundtrip_bool);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_roundtrip_str);
    MDB_RUN_TEST(setup_db, teardown_db, rel_row_roundtrip_blob);
    MDB_RUN_TEST(setup_db, teardown_db, rel_str_overflow_schema);
    MDB_RUN_TEST(setup_db, teardown_db, rel_insert_ok);
    MDB_RUN_TEST(setup_db, teardown_db, rel_insert_beyond_max_rows_full);
    MDB_RUN_TEST(setup_db, teardown_db, rel_insert_null_row_buf_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_index_returns_rows);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_no_match_cb_never_called);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_mutation_during_callback_returns_modified);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_without_index_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_non_index_correct);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_no_match);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_by_index_removes_rows);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_no_match_zero);
    MDB_RUN_TEST(setup_db, teardown_db, rel_iter_visits_rows_in_insertion_order);
    MDB_RUN_TEST(setup_db, teardown_db, rel_iter_callback_false_stops_early);
    MDB_RUN_TEST(setup_db, teardown_db, rel_iter_detects_concurrent_mutation);
    MDB_RUN_TEST(setup_db, teardown_db, rel_count_after_insert_delete);
    MDB_RUN_TEST(setup_db, teardown_db, rel_clear_preserves_table);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_updates_bitmap_and_index);
    return MDB_RESULT();
}
