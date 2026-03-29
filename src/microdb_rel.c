#include "microdb_internal.h"
#include "microdb_arena.h"

#include <string.h>

static size_t microdb_rel_type_size(microdb_col_type_t type) {
    if (type == MICRODB_COL_U8 || type == MICRODB_COL_I8 || type == MICRODB_COL_BOOL) {
        return 1u;
    }
    if (type == MICRODB_COL_U16 || type == MICRODB_COL_I16) {
        return 2u;
    }
    if (type == MICRODB_COL_U32 || type == MICRODB_COL_I32 || type == MICRODB_COL_F32) {
        return 4u;
    }
    if (type == MICRODB_COL_U64 || type == MICRODB_COL_I64 || type == MICRODB_COL_F64) {
        return 8u;
    }
    return 0u;
}

static microdb_err_t microdb_rel_validate_name(const char *name, size_t max_len) {
    size_t len;

    if (name == NULL || name[0] == '\0') {
        return MICRODB_ERR_INVALID;
    }

    len = strlen(name);
    if (len >= max_len) {
        return MICRODB_ERR_INVALID;
    }

    return MICRODB_OK;
}

static microdb_col_desc_t *microdb_rel_find_col(microdb_col_desc_t *cols, uint32_t col_count, const char *name) {
    uint32_t i;

    for (i = 0; i < col_count; ++i) {
        if (strcmp(cols[i].name, name) == 0) {
            return &cols[i];
        }
    }

    return NULL;
}

static const microdb_col_desc_t *microdb_rel_find_col_const(const microdb_col_desc_t *cols, uint32_t col_count, const char *name) {
    uint32_t i;

    for (i = 0; i < col_count; ++i) {
        if (strcmp(cols[i].name, name) == 0) {
            return &cols[i];
        }
    }

    return NULL;
}

static size_t microdb_rel_align_for_size(size_t size) {
    if (size >= 8u) {
        return 8u;
    }
    if (size >= 4u) {
        return 4u;
    }
    if (size >= 2u) {
        return 2u;
    }
    return 1u;
}

static bool rel_is_alive(const uint8_t *bitmap, uint32_t row_idx) {
    return ((bitmap[row_idx >> 3u] >> (row_idx & 7u)) & 1u) != 0u;
}

static void rel_set_alive(uint8_t *bitmap, uint32_t row_idx, bool alive) {
    if (alive) {
        bitmap[row_idx >> 3u] |= (uint8_t)(1u << (row_idx & 7u));
    } else {
        bitmap[row_idx >> 3u] &= (uint8_t)~(1u << (row_idx & 7u));
    }
}

static const void *rel_row_ptr(const microdb_table_t *table, uint32_t row_idx) {
    return table->rows + ((size_t)row_idx * table->row_size);
}

static void *rel_row_ptr_mut(microdb_table_t *table, uint32_t row_idx) {
    return table->rows + ((size_t)row_idx * table->row_size);
}

static int rel_key_cmp(const void *a, const void *b, size_t size) {
    return memcmp(a, b, size);
}

static uint32_t rel_index_find_first(const microdb_table_t *table, const void *key_bytes) {
    int32_t lo = 0;
    int32_t hi = (int32_t)table->index_count - 1;
    int32_t result = -1;

    while (lo <= hi) {
        int32_t mid = lo + (hi - lo) / 2;
        int cmp = rel_key_cmp(table->index[mid].key_bytes, key_bytes, table->index_key_size);
        if (cmp == 0) {
            result = mid;
            hi = mid - 1;
        } else if (cmp < 0) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }

    return (result >= 0) ? (uint32_t)result : UINT32_MAX;
}

static void rel_index_insert(microdb_table_t *table, uint32_t row_idx, const void *key_bytes) {
    int32_t lo = 0;
    int32_t hi = (int32_t)table->index_count - 1;
    int32_t pos = (int32_t)table->index_count;

    while (lo <= hi) {
        int32_t mid = lo + (hi - lo) / 2;
        int cmp = rel_key_cmp(table->index[mid].key_bytes, key_bytes, table->index_key_size);
        if (cmp < 0) {
            lo = mid + 1;
        } else {
            pos = mid;
            hi = mid - 1;
        }
    }

    memmove(&table->index[pos + 1],
            &table->index[pos],
            (table->index_count - (uint32_t)pos) * sizeof(microdb_index_entry_t));
    memcpy(table->index[pos].key_bytes, key_bytes, table->index_key_size);
    table->index[pos].row_idx = row_idx;
    table->index_count++;
}

static void rel_index_remove_row(microdb_table_t *table, uint32_t row_idx) {
    uint32_t i;

    for (i = 0; i < table->index_count; ++i) {
        if (table->index[i].row_idx == row_idx) {
            memmove(&table->index[i],
                    &table->index[i + 1u],
                    (table->index_count - i - 1u) * sizeof(microdb_index_entry_t));
            table->index_count--;
            return;
        }
    }
}

static void rel_order_remove_row(microdb_table_t *table, uint32_t row_idx) {
    uint32_t i;

    for (i = 0; i < table->order_count; ++i) {
        if (table->order[i] == row_idx) {
            memmove(&table->order[i],
                    &table->order[i + 1u],
                    (table->order_count - i - 1u) * sizeof(uint32_t));
            table->order_count--;
            return;
        }
    }
}

static uint32_t rel_find_free_row(const microdb_table_t *table) {
    uint32_t i;

    for (i = 0; i < table->max_rows; ++i) {
        if (!rel_is_alive(table->alive_bitmap, i)) {
            return i;
        }
    }

    return UINT32_MAX;
}

static microdb_table_t *rel_find_table(microdb_core_t *core, const char *name) {
    uint32_t i;

    for (i = 0; i < MICRODB_REL_MAX_TABLES; ++i) {
        microdb_table_t *table = &core->rel.tables[i];
        if (table->registered && strcmp(table->name, name) == 0) {
            return table;
        }
    }

    return NULL;
}

static const microdb_col_desc_t *rel_index_col(const microdb_table_t *table) {
    if (table->index_col == UINT32_MAX) {
        return NULL;
    }
    return &table->cols[table->index_col];
}

static const void *rel_index_key_ptr(const microdb_table_t *table, const void *row_buf) {
    const microdb_col_desc_t *col = rel_index_col(table);
    if (col == NULL) {
        return NULL;
    }
    return (const uint8_t *)row_buf + col->offset;
}

static void rel_copy_column_to_index(uint8_t *dst, const microdb_col_desc_t *col, const void *row_buf) {
    memset(dst, 0, MICRODB_REL_INDEX_KEY_MAX);
    memcpy(dst, (const uint8_t *)row_buf + col->offset, col->size);
}

static microdb_err_t rel_validate_str_value(const char *str, size_t max_size) {
    size_t i;

    for (i = 0; i < max_size; ++i) {
        if (str[i] == '\0') {
            return MICRODB_OK;
        }
    }

    return MICRODB_ERR_SCHEMA;
}

static microdb_err_t rel_validate_table_and_handle(microdb_t *db, microdb_table_t *table) {
    if (db == NULL || table == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (microdb_core(db)->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }
    if (!table->registered) {
        return MICRODB_ERR_INVALID;
    }
    return MICRODB_OK;
}

#if MICRODB_ENABLE_REL
microdb_err_t microdb_schema_init(microdb_schema_t *schema, const char *name, uint32_t max_rows) {
    microdb_schema_impl_t *impl;
    microdb_err_t err;

    if (schema == NULL || max_rows == 0u) {
        return MICRODB_ERR_INVALID;
    }

    err = microdb_rel_validate_name(name, MICRODB_REL_TABLE_NAME_LEN);
    if (err != MICRODB_OK) {
        return err;
    }

    memset(schema, 0, sizeof(*schema));
    impl = (microdb_schema_impl_t *)&schema->_opaque[0];
    memcpy(impl->name, name, strlen(name) + 1u);
    impl->max_rows = max_rows;
    impl->index_col = UINT32_MAX;
    return MICRODB_OK;
}

microdb_err_t microdb_schema_add(microdb_schema_t *schema,
                                 const char *col_name,
                                 microdb_col_type_t type,
                                 size_t size,
                                 bool is_index) {
    microdb_schema_impl_t *impl;
    microdb_col_desc_t *col;
    microdb_err_t err;
    size_t fixed_size;

    if (schema == NULL) {
        return MICRODB_ERR_INVALID;
    }

    err = microdb_rel_validate_name(col_name, MICRODB_REL_COL_NAME_LEN);
    if (err != MICRODB_OK) {
        return err;
    }

    impl = (microdb_schema_impl_t *)&schema->_opaque[0];
    if (impl->sealed) {
        return MICRODB_ERR_SEALED;
    }
    if (impl->col_count >= MICRODB_REL_MAX_COLS) {
        return MICRODB_ERR_FULL;
    }
    if (microdb_rel_find_col(impl->cols, impl->col_count, col_name) != NULL) {
        return MICRODB_ERR_INVALID;
    }

    fixed_size = microdb_rel_type_size(type);
    if (fixed_size != 0u) {
        if (size != fixed_size) {
            return MICRODB_ERR_INVALID;
        }
    } else if ((type == MICRODB_COL_STR || type == MICRODB_COL_BLOB) && size != 0u) {
        if (size > MICRODB_REL_INDEX_KEY_MAX && is_index) {
            return MICRODB_ERR_INVALID;
        }
    } else {
        return MICRODB_ERR_INVALID;
    }

    if (is_index && impl->index_col != UINT32_MAX) {
        return MICRODB_ERR_INVALID;
    }

    col = &impl->cols[impl->col_count];
    memset(col, 0, sizeof(*col));
    memcpy(col->name, col_name, strlen(col_name) + 1u);
    col->type = type;
    col->size = size;
    col->is_index = is_index;
    if (is_index) {
        impl->index_col = impl->col_count;
    }
    impl->col_count++;
    return MICRODB_OK;
}

microdb_err_t microdb_schema_seal(microdb_schema_t *schema) {
    microdb_schema_impl_t *impl;
    size_t offset = 0u;
    uint32_t i;

    if (schema == NULL) {
        return MICRODB_ERR_INVALID;
    }

    impl = (microdb_schema_impl_t *)&schema->_opaque[0];
    if (impl->col_count == 0u) {
        return MICRODB_ERR_INVALID;
    }
    if (impl->sealed) {
        return MICRODB_OK;
    }

    for (i = 0; i < impl->col_count; ++i) {
        microdb_col_desc_t *col = &impl->cols[i];
        size_t align = microdb_rel_align_for_size(col->size);
        offset = (offset + (align - 1u)) & ~(align - 1u);
        col->offset = offset;
        offset += col->size;
    }

    impl->row_size = (offset + 3u) & ~3u;
    impl->sealed = true;
    return MICRODB_OK;
}

microdb_err_t microdb_table_create(microdb_t *db, microdb_schema_t *schema) {
    microdb_core_t *core;
    microdb_schema_impl_t *impl;
    microdb_table_t *table;
    uint32_t alive_bytes;
    uint32_t i;

    if (db == NULL || schema == NULL) {
        return MICRODB_ERR_INVALID;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    impl = (microdb_schema_impl_t *)&schema->_opaque[0];
    if (!impl->sealed) {
        return MICRODB_ERR_INVALID;
    }
    if (rel_find_table(core, impl->name) != NULL) {
        return MICRODB_ERR_EXISTS;
    }
    if (core->rel.registered_tables >= MICRODB_REL_MAX_TABLES) {
        return MICRODB_ERR_FULL;
    }

    for (i = 0; i < MICRODB_REL_MAX_TABLES; ++i) {
        table = &core->rel.tables[i];
        if (!table->registered) {
            memset(table, 0, sizeof(*table));
            memcpy(table->name, impl->name, sizeof(table->name));
            memcpy(table->cols, impl->cols, sizeof(impl->cols));
            table->col_count = impl->col_count;
            table->max_rows = impl->max_rows;
            table->row_size = impl->row_size;
            table->index_col = impl->index_col;
            if (impl->index_col != UINT32_MAX) {
                table->index_key_size = impl->cols[impl->index_col].size;
            }

            table->rows = (uint8_t *)microdb_arena_alloc(&core->rel_arena,
                                                         (size_t)table->max_rows * table->row_size,
                                                         8u);
            alive_bytes = (table->max_rows + 7u) / 8u;
            table->alive_bitmap = (uint8_t *)microdb_arena_alloc(&core->rel_arena, alive_bytes, 1u);
            table->order = (uint32_t *)microdb_arena_alloc(&core->rel_arena,
                                                           (size_t)table->max_rows * sizeof(uint32_t),
                                                           4u);
            if (table->index_key_size != 0u) {
                table->index = (microdb_index_entry_t *)microdb_arena_alloc(&core->rel_arena,
                                                                            (size_t)table->max_rows * sizeof(microdb_index_entry_t),
                                                                            4u);
            }

            if (table->rows == NULL || table->alive_bitmap == NULL || table->order == NULL ||
                (table->index_key_size != 0u && table->index == NULL)) {
                memset(table, 0, sizeof(*table));
                return MICRODB_ERR_NO_MEM;
            }

            memset(table->rows, 0, (size_t)table->max_rows * table->row_size);
            memset(table->alive_bitmap, 0, alive_bytes);
            if (table->index != NULL) {
                memset(table->index, 0, (size_t)table->max_rows * sizeof(microdb_index_entry_t));
            }
            memset(table->order, 0, (size_t)table->max_rows * sizeof(uint32_t));
            table->registered = true;
            core->rel.registered_tables++;
            return MICRODB_OK;
        }
    }

    return MICRODB_ERR_FULL;
}

microdb_err_t microdb_table_get(microdb_t *db, const char *name, microdb_table_t **out_table) {
    microdb_core_t *core;
    microdb_table_t *table;
    microdb_err_t err;

    if (db == NULL || out_table == NULL) {
        return MICRODB_ERR_INVALID;
    }

    err = microdb_rel_validate_name(name, MICRODB_REL_TABLE_NAME_LEN);
    if (err != MICRODB_OK) {
        return err;
    }

    core = microdb_core(db);
    if (core->magic != MICRODB_MAGIC) {
        return MICRODB_ERR_INVALID;
    }

    table = rel_find_table(core, name);
    if (table == NULL) {
        return MICRODB_ERR_NOT_FOUND;
    }

    *out_table = table;
    return MICRODB_OK;
}

size_t microdb_table_row_size(const microdb_table_t *table) {
    if (table == NULL) {
        return 0u;
    }
    return table->row_size;
}

microdb_err_t microdb_row_set(const microdb_table_t *table, void *row_buf, const char *col_name, const void *val) {
    const microdb_col_desc_t *col;

    if (table == NULL || row_buf == NULL || col_name == NULL || val == NULL) {
        return MICRODB_ERR_INVALID;
    }

    col = microdb_rel_find_col_const(table->cols, table->col_count, col_name);
    if (col == NULL) {
        return MICRODB_ERR_NOT_FOUND;
    }

    if (col->type == MICRODB_COL_STR) {
        if (rel_validate_str_value((const char *)val, col->size) != MICRODB_OK) {
            return MICRODB_ERR_SCHEMA;
        }
        memset((uint8_t *)row_buf + col->offset, 0, col->size);
        memcpy((uint8_t *)row_buf + col->offset, val, strlen((const char *)val) + 1u);
        return MICRODB_OK;
    }

    memcpy((uint8_t *)row_buf + col->offset, val, col->size);
    return MICRODB_OK;
}

microdb_err_t microdb_row_get(const microdb_table_t *table,
                              const void *row_buf,
                              const char *col_name,
                              void *out,
                              size_t *out_len) {
    const microdb_col_desc_t *col;

    if (table == NULL || row_buf == NULL || col_name == NULL || out == NULL) {
        return MICRODB_ERR_INVALID;
    }

    col = microdb_rel_find_col_const(table->cols, table->col_count, col_name);
    if (col == NULL) {
        return MICRODB_ERR_NOT_FOUND;
    }

    memcpy(out, (const uint8_t *)row_buf + col->offset, col->size);
    if (out_len != NULL) {
        *out_len = col->size;
    }
    return MICRODB_OK;
}

microdb_err_t microdb_rel_insert(microdb_t *db, microdb_table_t *table, const void *row_buf) {
    microdb_err_t err;
    uint32_t row_idx;
    const microdb_col_desc_t *idx_col;

    err = rel_validate_table_and_handle(db, table);
    if (err != MICRODB_OK) {
        return err;
    }
    if (row_buf == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (table->live_count >= table->max_rows) {
        return MICRODB_ERR_FULL;
    }

    row_idx = rel_find_free_row(table);
    if (row_idx == UINT32_MAX) {
        return MICRODB_ERR_FULL;
    }

    memcpy(rel_row_ptr_mut(table, row_idx), row_buf, table->row_size);
    rel_set_alive(table->alive_bitmap, row_idx, true);
    table->order[table->order_count++] = row_idx;
    table->live_count++;

    idx_col = rel_index_col(table);
    if (idx_col != NULL) {
        uint8_t key_bytes[MICRODB_REL_INDEX_KEY_MAX];
        rel_copy_column_to_index(key_bytes, idx_col, row_buf);
        rel_index_insert(table, row_idx, key_bytes);
    }

    return MICRODB_OK;
}

microdb_err_t microdb_rel_find(microdb_t *db,
                               microdb_table_t *table,
                               const void *search_val,
                               microdb_rel_iter_cb_t cb,
                               void *ctx) {
    uint32_t idx;
    microdb_err_t err;

    err = rel_validate_table_and_handle(db, table);
    if (err != MICRODB_OK) {
        return err;
    }
    if (search_val == NULL || cb == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (table->index_col == UINT32_MAX) {
        return MICRODB_ERR_INVALID;
    }

    idx = rel_index_find_first(table, search_val);
    if (idx == UINT32_MAX) {
        return MICRODB_OK;
    }

    while (idx < table->index_count &&
           rel_key_cmp(table->index[idx].key_bytes, search_val, table->index_key_size) == 0) {
        const void *row = rel_row_ptr(table, table->index[idx].row_idx);
        if (!cb(row, ctx)) {
            break;
        }
        idx++;
    }

    return MICRODB_OK;
}

microdb_err_t microdb_rel_find_by(microdb_t *db,
                                  microdb_table_t *table,
                                  const char *col_name,
                                  const void *search_val,
                                  void *out_buf) {
    const microdb_col_desc_t *col;
    uint32_t i;
    microdb_err_t err;

    err = rel_validate_table_and_handle(db, table);
    if (err != MICRODB_OK) {
        return err;
    }
    if (col_name == NULL || search_val == NULL || out_buf == NULL) {
        return MICRODB_ERR_INVALID;
    }

    col = microdb_rel_find_col_const(table->cols, table->col_count, col_name);
    if (col == NULL) {
        return MICRODB_ERR_NOT_FOUND;
    }

    for (i = 0; i < table->order_count; ++i) {
        uint32_t row_idx = table->order[i];
        const uint8_t *row = (const uint8_t *)rel_row_ptr(table, row_idx);
        bool match = false;

        if (!rel_is_alive(table->alive_bitmap, row_idx)) {
            continue;
        }

        if (col->type == MICRODB_COL_STR) {
            match = strncmp((const char *)(row + col->offset), (const char *)search_val, col->size) == 0;
        } else {
            match = memcmp(row + col->offset, search_val, col->size) == 0;
        }

        if (match) {
            memcpy(out_buf, row, table->row_size);
            return MICRODB_OK;
        }
    }

    return MICRODB_ERR_NOT_FOUND;
}

microdb_err_t microdb_rel_delete(microdb_t *db, microdb_table_t *table, const void *search_val, uint32_t *out_deleted) {
    uint32_t deleted = 0u;
    uint32_t idx;
    microdb_err_t err;

    err = rel_validate_table_and_handle(db, table);
    if (err != MICRODB_OK) {
        return err;
    }
    if (search_val == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (table->index_col == UINT32_MAX) {
        return MICRODB_ERR_INVALID;
    }

    idx = rel_index_find_first(table, search_val);
    while (idx != UINT32_MAX &&
           idx < table->index_count &&
           rel_key_cmp(table->index[idx].key_bytes, search_val, table->index_key_size) == 0) {
        uint32_t row_idx = table->index[idx].row_idx;
        rel_set_alive(table->alive_bitmap, row_idx, false);
        rel_index_remove_row(table, row_idx);
        rel_order_remove_row(table, row_idx);
        table->live_count--;
        deleted++;
        idx = rel_index_find_first(table, search_val);
    }

    if (out_deleted != NULL) {
        *out_deleted = deleted;
    }
    return MICRODB_OK;
}

microdb_err_t microdb_rel_iter(microdb_t *db, microdb_table_t *table, microdb_rel_iter_cb_t cb, void *ctx) {
    uint32_t i;
    microdb_err_t err;

    err = rel_validate_table_and_handle(db, table);
    if (err != MICRODB_OK) {
        return err;
    }
    if (cb == NULL) {
        return MICRODB_ERR_INVALID;
    }

    for (i = 0; i < table->order_count; ++i) {
        uint32_t row_idx = table->order[i];
        if (rel_is_alive(table->alive_bitmap, row_idx)) {
            if (!cb(rel_row_ptr(table, row_idx), ctx)) {
                break;
            }
        }
    }

    return MICRODB_OK;
}

microdb_err_t microdb_rel_count(const microdb_table_t *table, uint32_t *out_count) {
    if (table == NULL || out_count == NULL) {
        return MICRODB_ERR_INVALID;
    }
    if (!table->registered) {
        return MICRODB_ERR_INVALID;
    }
    *out_count = table->live_count;
    return MICRODB_OK;
}

microdb_err_t microdb_rel_clear(microdb_t *db, microdb_table_t *table) {
    uint32_t alive_bytes;
    microdb_err_t err;

    err = rel_validate_table_and_handle(db, table);
    if (err != MICRODB_OK) {
        return err;
    }

    alive_bytes = (table->max_rows + 7u) / 8u;
    memset(table->alive_bitmap, 0, alive_bytes);
    if (table->index != NULL) {
        memset(table->index, 0, (size_t)table->max_rows * sizeof(microdb_index_entry_t));
    }
    memset(table->order, 0, (size_t)table->max_rows * sizeof(uint32_t));
    table->live_count = 0u;
    table->index_count = 0u;
    table->order_count = 0u;
    return MICRODB_OK;
}
#endif
