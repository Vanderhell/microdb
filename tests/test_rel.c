#include "microtest.h"
#include "microdb.h"
#include "../port/ram/microdb_port_ram.h"
#include "../src/microdb_internal.h"

#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_ram_storage;

typedef struct {
    uint32_t count;
    uint32_t ids[32];
} rel_iter_ctx_t;

static const microdb_schema_impl_t *schema_impl(const microdb_schema_t *schema) {
    return (const microdb_schema_impl_t *)&schema->_opaque[0];
}

static void setup_db(void) {
    microdb_cfg_t cfg;

    ASSERT_EQ(microdb_port_ram_init(&g_ram_storage, 65536u), MICRODB_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_ram_storage;
    cfg.ram_kb = 32u;
    cfg.now = NULL;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    microdb_deinit(&g_db);
    microdb_port_ram_deinit(&g_ram_storage);
}

static microdb_err_t make_indexed_schema(microdb_schema_t *schema, const char *name, uint32_t max_rows) {
    microdb_err_t err;

    err = microdb_schema_init(schema, name, max_rows);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false);
    if (err != MICRODB_OK) {
        return err;
    }
    return microdb_schema_seal(schema);
}

static microdb_err_t make_table(microdb_schema_t *schema, microdb_table_t **out_table) {
    microdb_err_t err;

    err = microdb_table_create(&g_db, schema);
    if (err != MICRODB_OK) {
        return err;
    }
    return microdb_table_get(&g_db, schema_impl(schema)->name, out_table);
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

MDB_TEST(rel_schema_init_ok) {
    microdb_schema_t schema;
    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
}

MDB_TEST(rel_schema_add_ok) {
    microdb_schema_t schema;
    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
}

MDB_TEST(rel_schema_add_after_seal_returns_sealed) {
    microdb_schema_t schema;
    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "extra", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_ERR_SEALED);
}

MDB_TEST(rel_schema_second_index_invalid) {
    microdb_schema_t schema;
    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "serial", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_ERR_INVALID);
}

MDB_TEST(rel_schema_seal_empty_invalid) {
    microdb_schema_t schema;
    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_ERR_INVALID);
}

MDB_TEST(rel_schema_seal_alignment_correct) {
    microdb_schema_t schema;
    const microdb_schema_impl_t *impl;

    ASSERT_EQ(microdb_schema_init(&schema, "align", 8u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "a", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "b", MICRODB_COL_U32, sizeof(uint32_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "c", MICRODB_COL_U16, sizeof(uint16_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    impl = schema_impl(&schema);
    ASSERT_EQ(impl->cols[0].offset, 0u);
    ASSERT_EQ(impl->cols[1].offset, 4u);
    ASSERT_EQ(impl->cols[2].offset, 8u);
    ASSERT_EQ(impl->row_size, 12u);
}

MDB_TEST(rel_schema_seal_idempotent) {
    microdb_schema_t schema;
    size_t row_size_before;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), MICRODB_OK);
    row_size_before = schema_impl(&schema)->row_size;
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(schema_impl(&schema)->row_size, row_size_before);
}

MDB_TEST(rel_schema_add_beyond_max_cols_full) {
    microdb_schema_t schema;
    char name[16];
    uint32_t i;

    ASSERT_EQ(microdb_schema_init(&schema, "cols", 8u), MICRODB_OK);
    for (i = 0; i < MICRODB_REL_MAX_COLS; ++i) {
        memset(name, 0, sizeof(name));
        name[0] = 'c';
        name[1] = (char)('a' + (char)(i % 26u));
        name[2] = (char)('0' + (char)(i / 26u));
        ASSERT_EQ(microdb_schema_add(&schema, name, MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    }
    ASSERT_EQ(microdb_schema_add(&schema, "overflow", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_ERR_FULL);
}

MDB_TEST(rel_schema_scalar_size_mismatch_invalid) {
    microdb_schema_t schema;
    ASSERT_EQ(microdb_schema_init(&schema, "bad", 8u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint16_t), false), MICRODB_ERR_INVALID);
}

MDB_TEST(rel_table_create_sealed_ok) {
    microdb_schema_t schema;
    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_OK);
}

MDB_TEST(rel_table_create_unsealed_invalid) {
    microdb_schema_t schema;
    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_ERR_INVALID);
}

MDB_TEST(rel_table_create_duplicate_exists) {
    microdb_schema_t schema;
    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_ERR_EXISTS);
}

MDB_TEST(rel_table_create_beyond_max_tables_full) {
    microdb_schema_t schema;
    char name[16];
    uint32_t i;

    for (i = 0; i < MICRODB_REL_MAX_TABLES; ++i) {
        memset(name, 0, sizeof(name));
        name[0] = 't';
        name[1] = (char)('0' + (char)i);
        ASSERT_EQ(make_indexed_schema(&schema, name, 4u), MICRODB_OK);
        ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_OK);
    }

    ASSERT_EQ(make_indexed_schema(&schema, "extra", 4u), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_ERR_FULL);
}

MDB_TEST(rel_table_get_existing_ok) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(table != NULL, 1);
}

MDB_TEST(rel_table_get_unknown_not_found) {
    microdb_table_t *table = NULL;
    ASSERT_EQ(microdb_table_get(&g_db, "missing", &table), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(rel_table_row_size_matches_schema) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_table_row_size(table), schema_impl(&schema)->row_size);
}

MDB_TEST(rel_row_set_unknown_column_not_found) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "missing", &id), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(rel_row_get_unknown_column_not_found) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t out = 0u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "missing", &out, NULL), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(rel_row_roundtrip_unsigned_scalars) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint8_t u8 = 1u, ou8 = 0u;
    uint16_t u16 = 2u, ou16 = 0u;
    uint32_t u32 = 3u, ou32 = 0u;
    uint64_t u64 = 4u, ou64 = 0u;

    ASSERT_EQ(microdb_schema_init(&schema, "u", 4u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "u8", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "u16", MICRODB_COL_U16, sizeof(uint16_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "u32", MICRODB_COL_U32, sizeof(uint32_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "u64", MICRODB_COL_U64, sizeof(uint64_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "u8", &u8), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "u16", &u16), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "u32", &u32), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "u64", &u64), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "u8", &ou8, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "u16", &ou16, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "u32", &ou32, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "u64", &ou64, NULL), MICRODB_OK);
    ASSERT_EQ(ou8, 1u);
    ASSERT_EQ(ou16, 2u);
    ASSERT_EQ(ou32, 3u);
    ASSERT_EQ(ou64, 4u);
}

MDB_TEST(rel_row_roundtrip_signed_scalars) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    int8_t i8 = -1, oi8 = 0;
    int16_t i16 = -2, oi16 = 0;
    int32_t i32 = -3, oi32 = 0;
    int64_t i64 = -4, oi64 = 0;

    ASSERT_EQ(microdb_schema_init(&schema, "i", 4u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "i8", MICRODB_COL_I8, sizeof(int8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "i16", MICRODB_COL_I16, sizeof(int16_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "i32", MICRODB_COL_I32, sizeof(int32_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "i64", MICRODB_COL_I64, sizeof(int64_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "i8", &i8), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "i16", &i16), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "i32", &i32), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "i64", &i64), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "i8", &oi8, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "i16", &oi16, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "i32", &oi32, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "i64", &oi64, NULL), MICRODB_OK);
    ASSERT_EQ(oi8, -1);
    ASSERT_EQ(oi16, -2);
    ASSERT_EQ(oi32, -3);
    ASSERT_EQ(oi64, -4);
}

MDB_TEST(rel_row_roundtrip_float_scalars) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    float f32 = 1.5f, of32 = 0.0f;
    double f64 = 2.5, of64 = 0.0;

    ASSERT_EQ(microdb_schema_init(&schema, "f", 4u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "f32", MICRODB_COL_F32, sizeof(float), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "f64", MICRODB_COL_F64, sizeof(double), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "f32", &f32), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "f64", &f64), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "f32", &of32, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "f64", &of64, NULL), MICRODB_OK);
    ASSERT_EQ(of32 == f32, 1);
    ASSERT_EQ(of64 == f64, 1);
}

MDB_TEST(rel_row_roundtrip_bool) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    bool value = true;
    bool out = false;

    ASSERT_EQ(microdb_schema_init(&schema, "b", 4u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "flag", MICRODB_COL_BOOL, sizeof(bool), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "flag", &value), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "flag", &out, NULL), MICRODB_OK);
    ASSERT_EQ(out, 1);
}

MDB_TEST(rel_row_roundtrip_str) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    char out[16] = { 0 };

    ASSERT_EQ(microdb_schema_init(&schema, "s", 4u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "name", MICRODB_COL_STR, 8u, false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "name", "bob"), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "name", out, NULL), MICRODB_OK);
    ASSERT_EQ(strcmp(out, "bob"), 0);
}

MDB_TEST(rel_row_roundtrip_blob) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint8_t blob[4] = { 1u, 2u, 3u, 4u };
    uint8_t out[4] = { 0 };

    ASSERT_EQ(microdb_schema_init(&schema, "bl", 4u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "blob", MICRODB_COL_BLOB, 4u, false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "blob", blob), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, row, "blob", out, NULL), MICRODB_OK);
    ASSERT_MEM_EQ(out, blob, 4u);
}

MDB_TEST(rel_str_overflow_schema) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };

    ASSERT_EQ(microdb_schema_init(&schema, "s", 4u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "name", MICRODB_COL_STR, 4u, false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "name", "toolong"), MICRODB_ERR_SCHEMA);
}

MDB_TEST(rel_insert_ok) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 7u;
    uint8_t age = 9u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
}

MDB_TEST(rel_insert_beyond_max_rows_full) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 1u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 1u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    id = 2u;
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_ERR_FULL);
}

MDB_TEST(rel_insert_null_row_buf_invalid) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, NULL), MICRODB_ERR_INVALID);
}

MDB_TEST(rel_find_by_index_returns_rows) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 42u;
    uint8_t age = 3u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 3u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find(&g_db, table, &id, rel_collect_id_cb, &ctx), MICRODB_OK);
    ASSERT_EQ(ctx.count, 1u);
    ASSERT_EQ(ctx.ids[0], 42u);
}

MDB_TEST(rel_find_no_match_cb_never_called) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint32_t id = 99u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find(&g_db, table, &id, rel_collect_id_cb, &ctx), MICRODB_OK);
    ASSERT_EQ(ctx.count, 0u);
}

MDB_TEST(rel_find_without_index_invalid) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint32_t id = 1u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(microdb_schema_init(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find(&g_db, table, &id, rel_collect_id_cb, &ctx), MICRODB_ERR_INVALID);
}

MDB_TEST(rel_find_by_non_index_correct) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint8_t out[128] = { 0 };
    uint32_t id = 2u;
    uint8_t age = 55u;
    uint32_t out_id = 0u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find_by(&g_db, table, "age", &age, out), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(table, out, "id", &out_id, NULL), MICRODB_OK);
    ASSERT_EQ(out_id, 2u);
}

MDB_TEST(rel_find_by_no_match) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t out[128] = { 0 };
    uint8_t age = 77u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find_by(&g_db, table, "age", &age, out), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(rel_delete_by_index_removes_rows) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 3u;
    uint8_t age = 9u;
    uint32_t deleted = 0u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    ASSERT_EQ(microdb_rel_delete(&g_db, table, &id, &deleted), MICRODB_OK);
    ASSERT_EQ(deleted, 1u);
    ASSERT_EQ(microdb_rel_find(&g_db, table, &id, rel_collect_id_cb, &ctx), MICRODB_OK);
    ASSERT_EQ(ctx.count, 0u);
}

MDB_TEST(rel_delete_no_match_zero) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint32_t id = 4u;
    uint32_t deleted = 99u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_delete(&g_db, table, &id, &deleted), MICRODB_OK);
    ASSERT_EQ(deleted, 0u);
}

MDB_TEST(rel_iter_visits_rows_in_insertion_order) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id;
    uint8_t age = 1u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 3u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    for (id = 1u; id <= 3u; ++id) {
        ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
        ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
        ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    }
    ASSERT_EQ(microdb_rel_iter(&g_db, table, rel_collect_id_cb, &ctx), MICRODB_OK);
    ASSERT_EQ(ctx.count, 3u);
    ASSERT_EQ(ctx.ids[0], 1u);
    ASSERT_EQ(ctx.ids[1], 2u);
    ASSERT_EQ(ctx.ids[2], 3u);
}

MDB_TEST(rel_iter_callback_false_stops_early) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id;
    uint8_t age = 1u;
    rel_iter_ctx_t ctx = { 0 };

    ASSERT_EQ(make_indexed_schema(&schema, "users", 3u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    for (id = 1u; id <= 3u; ++id) {
        ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
        ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
        ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    }
    ASSERT_EQ(microdb_rel_iter(&g_db, table, rel_collect_id_stop_after_two, &ctx), MICRODB_OK);
    ASSERT_EQ(ctx.count, 2u);
}

MDB_TEST(rel_count_after_insert_delete) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 1u;
    uint32_t count = 0u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    ASSERT_EQ(microdb_rel_count(table, &count), MICRODB_OK);
    ASSERT_EQ(count, 1u);
    ASSERT_EQ(microdb_rel_delete(&g_db, table, &id, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_rel_count(table, &count), MICRODB_OK);
    ASSERT_EQ(count, 0u);
}

MDB_TEST(rel_clear_preserves_table) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;
    uint8_t age = 1u;
    uint32_t count = 99u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    ASSERT_EQ(microdb_rel_clear(&g_db, table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_count(table, &count), MICRODB_OK);
    ASSERT_EQ(count, 0u);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
}

MDB_TEST(rel_delete_updates_bitmap_and_index) {
    microdb_schema_t schema;
    microdb_table_t *table = NULL;
    uint8_t row[128] = { 0 };
    uint32_t id = 5u;
    uint8_t age = 2u;

    ASSERT_EQ(make_indexed_schema(&schema, "users", 2u), MICRODB_OK);
    ASSERT_EQ(make_table(&schema, &table), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    ASSERT_EQ(microdb_rel_delete(&g_db, table, &id, NULL), MICRODB_OK);
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
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_add_beyond_max_cols_full);
    MDB_RUN_TEST(setup_db, teardown_db, rel_schema_scalar_size_mismatch_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_sealed_ok);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_unsealed_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_table_create_duplicate_exists);
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
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_without_index_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_non_index_correct);
    MDB_RUN_TEST(setup_db, teardown_db, rel_find_by_no_match);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_by_index_removes_rows);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_no_match_zero);
    MDB_RUN_TEST(setup_db, teardown_db, rel_iter_visits_rows_in_insertion_order);
    MDB_RUN_TEST(setup_db, teardown_db, rel_iter_callback_false_stops_early);
    MDB_RUN_TEST(setup_db, teardown_db, rel_count_after_insert_delete);
    MDB_RUN_TEST(setup_db, teardown_db, rel_clear_preserves_table);
    MDB_RUN_TEST(setup_db, teardown_db, rel_delete_updates_bitmap_and_index);
    return MDB_RESULT();
}
