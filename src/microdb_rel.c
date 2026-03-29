#include "microdb_internal.h"

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

static microdb_err_t microdb_rel_unavailable(const void *ptr) {
    if (ptr == NULL) {
        return MICRODB_ERR_INVALID;
    }
    return MICRODB_ERR_DISABLED;
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
    } else if (type == MICRODB_COL_STR || type == MICRODB_COL_BLOB) {
        if (size == 0u) {
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

    impl->row_size = (uint32_t)((offset + 3u) & ~3u);
    impl->sealed = true;
    return MICRODB_OK;
}

microdb_err_t microdb_table_create(microdb_t *db, microdb_schema_t *schema) {
    (void)schema;
    return microdb_rel_unavailable(db);
}

microdb_err_t microdb_table_get(microdb_t *db, const char *name, microdb_table_t **out_table) {
    (void)name;
    (void)out_table;
    return microdb_rel_unavailable(db);
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

    col = microdb_rel_find_col((microdb_col_desc_t *)table->cols, table->col_count, col_name);
    if (col == NULL) {
        return MICRODB_ERR_NOT_FOUND;
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

    col = microdb_rel_find_col((microdb_col_desc_t *)table->cols, table->col_count, col_name);
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
    (void)table;
    (void)row_buf;
    return microdb_rel_unavailable(db);
}

microdb_err_t microdb_rel_find(microdb_t *db,
                               microdb_table_t *table,
                               const void *search_val,
                               microdb_rel_iter_cb_t cb,
                               void *ctx) {
    (void)table;
    (void)search_val;
    (void)cb;
    (void)ctx;
    return microdb_rel_unavailable(db);
}

microdb_err_t microdb_rel_find_by(microdb_t *db,
                                  microdb_table_t *table,
                                  const char *col_name,
                                  const void *search_val,
                                  void *out_buf) {
    (void)table;
    (void)col_name;
    (void)search_val;
    (void)out_buf;
    return microdb_rel_unavailable(db);
}

microdb_err_t microdb_rel_delete(microdb_t *db, microdb_table_t *table, const void *search_val, uint32_t *out_deleted) {
    (void)table;
    (void)search_val;
    (void)out_deleted;
    return microdb_rel_unavailable(db);
}

microdb_err_t microdb_rel_iter(microdb_t *db, microdb_table_t *table, microdb_rel_iter_cb_t cb, void *ctx) {
    (void)table;
    (void)cb;
    (void)ctx;
    return microdb_rel_unavailable(db);
}

microdb_err_t microdb_rel_count(const microdb_table_t *table, uint32_t *out_count) {
    (void)out_count;
    return microdb_rel_unavailable(table);
}

microdb_err_t microdb_rel_clear(microdb_t *db, microdb_table_t *table) {
    (void)table;
    return microdb_rel_unavailable(db);
}
#endif
