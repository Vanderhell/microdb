#include "microtest.h"
#include "microdb.h"
#include "../src/microdb_internal.h"

#include <string.h>

static void rel_make_table_from_schema(const microdb_schema_t *schema, microdb_table_t *table) {
    const microdb_schema_impl_t *impl = (const microdb_schema_impl_t *)&schema->_opaque[0];

    memset(table, 0, sizeof(*table));
    memcpy(table->name, impl->name, sizeof(table->name));
    memcpy(table->cols, impl->cols, sizeof(impl->cols));
    table->col_count = impl->col_count;
    table->max_rows = impl->max_rows;
    table->row_size = impl->row_size;
    table->index_col = impl->index_col;
}

MDB_TEST(rel_schema_init_ok) {
    microdb_schema_t schema;
    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
}

MDB_TEST(rel_schema_add_and_seal_ok) {
    microdb_schema_t schema;
    const microdb_schema_impl_t *impl;

    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    impl = (const microdb_schema_impl_t *)&schema._opaque[0];
    ASSERT_EQ(impl->sealed, 1);
    ASSERT_EQ(impl->row_size >= 5u, 1);
}

MDB_TEST(rel_add_after_seal_returns_sealed) {
    microdb_schema_t schema;

    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_ERR_SEALED);
}

MDB_TEST(rel_second_index_column_invalid) {
    microdb_schema_t schema;

    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "serial", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_ERR_INVALID);
}

MDB_TEST(rel_seal_empty_schema_invalid) {
    microdb_schema_t schema;

    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_ERR_INVALID);
}

MDB_TEST(rel_row_set_and_get_ok) {
    microdb_schema_t schema;
    microdb_table_t table;
    uint8_t row[64];
    uint32_t id = 77u;
    uint32_t out_id = 0u;
    size_t out_len = 0u;

    memset(row, 0, sizeof(row));
    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    rel_make_table_from_schema(&schema, &table);
    ASSERT_EQ(microdb_row_set(&table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_get(&table, row, "id", &out_id, &out_len), MICRODB_OK);
    ASSERT_EQ(out_len, sizeof(uint32_t));
    ASSERT_EQ(out_id, 77u);
}

MDB_TEST(rel_row_unknown_column_not_found) {
    microdb_schema_t schema;
    microdb_table_t table;
    uint8_t row[64];
    uint32_t id = 1u;

    memset(row, 0, sizeof(row));
    ASSERT_EQ(microdb_schema_init(&schema, "users", 16u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    rel_make_table_from_schema(&schema, &table);
    ASSERT_EQ(microdb_row_set(&table, row, "missing", &id), MICRODB_ERR_NOT_FOUND);
}

int main(void) {
    rel_schema_init_ok();
    rel_schema_add_and_seal_ok();
    rel_add_after_seal_returns_sealed();
    rel_second_index_column_invalid();
    rel_seal_empty_schema_invalid();
    rel_row_set_and_get_ok();
    rel_row_unknown_column_not_found();
    return MDB_RESULT();
}
