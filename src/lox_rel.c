// SPDX-License-Identifier: MIT
#include "lox_internal.h"
#include "lox_lock.h"
#include "lox_arena.h"

#include <string.h>
#if defined(_MSC_VER)
#include <intrin.h>
#endif

#define LOX_REL_ROW_SCRATCH_MAX 1024u

static size_t lox_rel_type_size(lox_col_type_t type) {
    if (type == LOX_COL_U8 || type == LOX_COL_I8 || type == LOX_COL_BOOL) {
        return 1u;
    }
    if (type == LOX_COL_U16 || type == LOX_COL_I16) {
        return 2u;
    }
    if (type == LOX_COL_U32 || type == LOX_COL_I32 || type == LOX_COL_F32) {
        return 4u;
    }
    if (type == LOX_COL_U64 || type == LOX_COL_I64 || type == LOX_COL_F64) {
        return 8u;
    }
    return 0u;
}

static lox_err_t lox_rel_validate_name(const char *name, size_t max_len) {
    size_t len;

    if (name == NULL || name[0] == '\0') {
        return LOX_ERR_INVALID;
    }

    len = strlen(name);
    if (len >= max_len) {
        return LOX_ERR_INVALID;
    }

    return LOX_OK;
}

static lox_col_desc_t *lox_rel_find_col(lox_col_desc_t *cols, uint32_t col_count, const char *name) {
    uint32_t i;

    for (i = 0; i < col_count; ++i) {
        if (strcmp(cols[i].name, name) == 0) {
            return &cols[i];
        }
    }

    return NULL;
}

static const lox_col_desc_t *lox_rel_find_col_const(const lox_col_desc_t *cols, uint32_t col_count, const char *name) {
    uint32_t i;

    for (i = 0; i < col_count; ++i) {
        if (strcmp(cols[i].name, name) == 0) {
            return &cols[i];
        }
    }

    return NULL;
}

static size_t lox_rel_align_for_size(size_t size) {
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

static const void *rel_row_ptr(const lox_table_t *table, uint32_t row_idx) {
    return table->rows + ((size_t)row_idx * table->row_size);
}

static void *rel_row_ptr_mut(lox_table_t *table, uint32_t row_idx) {
    return table->rows + ((size_t)row_idx * table->row_size);
}

static int rel_key_cmp(const void *a, const void *b, size_t size) {
    return memcmp(a, b, size);
}

static uint32_t rel_ctz_u32(uint32_t value) {
#if defined(_MSC_VER)
    unsigned long idx;
    _BitScanForward(&idx, value);
    return (uint32_t)idx;
#elif defined(__GNUC__) || defined(__clang__)
    return (uint32_t)__builtin_ctz(value);
#else
    uint32_t idx = 0u;
    while ((value & 1u) == 0u) {
        value >>= 1u;
        idx++;
    }
    return idx;
#endif
}

static const lox_col_desc_t *rel_index_col(const lox_table_t *table);
static void rel_copy_column_to_index(uint8_t *dst, const lox_col_desc_t *col, const void *row_buf);

static uint32_t rel_index_find_first(const lox_table_t *table, const void *key_bytes) {
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

static void rel_index_insert(lox_table_t *table, uint32_t row_idx, const void *key_bytes) {
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
            (table->index_count - (uint32_t)pos) * sizeof(lox_index_entry_t));
    memcpy(table->index[pos].key_bytes, key_bytes, table->index_key_size);
    table->index[pos].row_idx = row_idx;
    table->index_count++;
}

static void rel_index_remove_row(lox_table_t *table, uint32_t row_idx) {
    uint32_t i;

    for (i = 0; i < table->index_count; ++i) {
        if (table->index[i].row_idx == row_idx) {
            memmove(&table->index[i],
                    &table->index[i + 1u],
                    (table->index_count - i - 1u) * sizeof(lox_index_entry_t));
            table->index_count--;
            return;
        }
    }
}

static void rel_order_remove_row(lox_table_t *table, uint32_t row_idx) {
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

static void rel_apply_insert_row(lox_table_t *table, uint32_t row_idx, const void *row_buf) {
    const lox_col_desc_t *idx_col;

    memcpy(rel_row_ptr_mut(table, row_idx), row_buf, table->row_size);
    rel_set_alive(table->alive_bitmap, row_idx, true);
    table->order[table->order_count++] = row_idx;
    table->live_count++;

    idx_col = rel_index_col(table);
    if (idx_col != NULL) {
        uint8_t key_bytes[LOX_REL_INDEX_KEY_MAX];
        rel_copy_column_to_index(key_bytes, idx_col, row_buf);
        rel_index_insert(table, row_idx, key_bytes);
    }
    table->mutation_seq++;
}

static void rel_apply_delete_row(lox_table_t *table, uint32_t row_idx) {
    rel_set_alive(table->alive_bitmap, row_idx, false);
    rel_index_remove_row(table, row_idx);
    rel_order_remove_row(table, row_idx);
    if (table->live_count != 0u) {
        table->live_count--;
    }
    table->mutation_seq++;
}

static bool rel_wal_mode(const lox_core_t *core) {
    return core->wal_enabled && core->storage != NULL && !core->storage_loading && !core->wal_replaying;
}

static bool rel_has_arena_space_for_table(const lox_core_t *core, const lox_schema_impl_t *impl) {
    size_t need_rows = (size_t)impl->max_rows * impl->row_size;
    size_t need_alive = (size_t)(impl->max_rows + 7u) / 8u;
    size_t need_order = (size_t)impl->max_rows * sizeof(uint32_t);
    size_t need_index = (impl->index_col != UINT32_MAX) ? ((size_t)impl->max_rows * sizeof(lox_index_entry_t)) : 0u;
    size_t need_total = need_rows + need_alive + need_order + need_index + 32u;
    return lox_arena_remaining((lox_arena_t *)&core->rel_arena) >= need_total;
}

static uint32_t rel_find_free_row(const lox_table_t *table) {
    uint32_t byte_idx;
    uint32_t alive_bytes = (table->max_rows + 7u) / 8u;

    for (byte_idx = 0u; byte_idx < alive_bytes; ++byte_idx) {
        uint32_t row_base = byte_idx * 8u;
        uint8_t effective = table->alive_bitmap[byte_idx];
        if (row_base + 8u > table->max_rows) {
            uint32_t valid_bits = table->max_rows - row_base;
            uint8_t valid_mask = (uint8_t)((1u << valid_bits) - 1u);
            effective |= (uint8_t)(~valid_mask);
        }
        if (effective == 0xFFu) {
            continue;
        }
        return row_base + rel_ctz_u32((uint32_t)(uint8_t)(~effective));
    }

    return UINT32_MAX;
}

static lox_table_t *rel_find_table(lox_core_t *core, const char *name) {
    uint32_t i;

    for (i = 0; i < LOX_REL_MAX_TABLES; ++i) {
        lox_table_t *table = &core->rel.tables[i];
        if (table->registered && strcmp(table->name, name) == 0) {
            return table;
        }
    }

    return NULL;
}

static const lox_col_desc_t *rel_index_col(const lox_table_t *table) {
    if (table->index_col == UINT32_MAX) {
        return NULL;
    }
    return &table->cols[table->index_col];
}

static const void *rel_index_key_ptr(const lox_table_t *table, const void *row_buf) {
    const lox_col_desc_t *col = rel_index_col(table);
    if (col == NULL) {
        return NULL;
    }
    return (const uint8_t *)row_buf + col->offset;
}

static void rel_copy_column_to_index(uint8_t *dst, const lox_col_desc_t *col, const void *row_buf) {
    memset(dst, 0, LOX_REL_INDEX_KEY_MAX);
    memcpy(dst, (const uint8_t *)row_buf + col->offset, col->size);
}

static lox_err_t rel_validate_str_value(const char *str, size_t max_size) {
    size_t i;

    for (i = 0; i < max_size; ++i) {
        if (str[i] == '\0') {
            return LOX_OK;
        }
    }

    return LOX_ERR_SCHEMA;
}

static lox_err_t rel_validate_table_and_handle(lox_t *db, lox_table_t *table) {
    if (db == NULL || table == NULL) {
        return LOX_ERR_INVALID;
    }
    if (lox_core(db)->magic != LOX_MAGIC) {
        return LOX_ERR_INVALID;
    }
    if (!table->registered) {
        return LOX_ERR_INVALID;
    }
    return LOX_OK;
}

#if LOX_ENABLE_REL
lox_err_t lox_schema_init(lox_schema_t *schema, const char *name, uint32_t max_rows) {
    lox_schema_impl_t *impl;
    lox_err_t err;

    if (schema == NULL || max_rows == 0u) {
        return LOX_ERR_INVALID;
    }

    err = lox_rel_validate_name(name, LOX_REL_TABLE_NAME_LEN);
    if (err != LOX_OK) {
        return err;
    }

    memset(schema, 0, sizeof(*schema));
    schema->schema_version = 0u;
    impl = (lox_schema_impl_t *)&schema->_opaque[0];
    memcpy(impl->name, name, strlen(name) + 1u);
    impl->schema_version = schema->schema_version;
    impl->max_rows = max_rows;
    impl->index_col = UINT32_MAX;
    return LOX_OK;
}

lox_err_t lox_schema_add(lox_schema_t *schema,
                                 const char *col_name,
                                 lox_col_type_t type,
                                 size_t size,
                                 bool is_index) {
    lox_schema_impl_t *impl;
    lox_col_desc_t *col;
    lox_err_t err;
    size_t fixed_size;

    if (schema == NULL) {
        return LOX_ERR_INVALID;
    }

    err = lox_rel_validate_name(col_name, LOX_REL_COL_NAME_LEN);
    if (err != LOX_OK) {
        return err;
    }

    impl = (lox_schema_impl_t *)&schema->_opaque[0];
    if (impl->sealed) {
        return LOX_ERR_SEALED;
    }
    if (impl->col_count >= LOX_REL_MAX_COLS) {
        return LOX_ERR_FULL;
    }
    if (lox_rel_find_col(impl->cols, impl->col_count, col_name) != NULL) {
        return LOX_ERR_INVALID;
    }

    fixed_size = lox_rel_type_size(type);
    if (fixed_size != 0u) {
        if (size != fixed_size) {
            return LOX_ERR_INVALID;
        }
    } else if ((type == LOX_COL_STR || type == LOX_COL_BLOB) && size != 0u) {
        if (size > LOX_REL_INDEX_KEY_MAX && is_index) {
            return LOX_ERR_INVALID;
        }
    } else {
        return LOX_ERR_INVALID;
    }

    if (is_index && impl->index_col != UINT32_MAX) {
        return LOX_ERR_INVALID;
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
    return LOX_OK;
}

lox_err_t lox_schema_seal(lox_schema_t *schema) {
    lox_schema_impl_t *impl;
    size_t offset = 0u;
    uint32_t i;

    if (schema == NULL) {
        return LOX_ERR_INVALID;
    }

    impl = (lox_schema_impl_t *)&schema->_opaque[0];
    if (impl->col_count == 0u) {
        return LOX_ERR_INVALID;
    }
    if (impl->sealed) {
        return LOX_OK;
    }

    for (i = 0; i < impl->col_count; ++i) {
        lox_col_desc_t *col = &impl->cols[i];
        size_t align = lox_rel_align_for_size(col->size);
        offset = (offset + (align - 1u)) & ~(align - 1u);
        col->offset = offset;
        offset += col->size;
    }

    impl->row_size = (offset + 3u) & ~3u;
    if (impl->row_size > LOX_REL_ROW_SCRATCH_MAX) {
        return LOX_ERR_OVERFLOW;
    }
    /* schema_version is captured at seal-time and treated as immutable afterwards.
     * Any post-seal mutation of schema->schema_version is rejected in table_create.
     */
    impl->schema_version = schema->schema_version;
    impl->sealed = true;
    return LOX_OK;
}

lox_err_t lox_table_create(lox_t *db, lox_schema_t *schema) {
    lox_core_t *core;
    lox_schema_impl_t *impl;
    lox_table_t *table;
    lox_table_t *existing;
    uint32_t alive_bytes;
    uint32_t i;
    lox_err_t rc = LOX_OK;
    bool need_migrate_cb = false;
    uint16_t migrate_old = 0u;
    uint16_t migrate_new = 0u;
    char migrate_name[LOX_REL_TABLE_NAME_LEN];
    bool wal_mode;

    if (db == NULL || schema == NULL) {
        return LOX_ERR_INVALID;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    impl = (lox_schema_impl_t *)&schema->_opaque[0];
    if (!impl->sealed) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    /* Defensive contract check: callers must set schema_version before seal. */
    if (schema->schema_version != impl->schema_version) {
        rc = LOX_ERR_SCHEMA;
        goto unlock;
    }
    wal_mode = rel_wal_mode(core);
    existing = rel_find_table(core, impl->name);
    if (existing != NULL) {
        if (existing->schema_version == impl->schema_version) {
            rc = LOX_OK;
            goto unlock;
        }
        if (core->on_migrate == NULL) {
            rc = LOX_ERR_SCHEMA;
            goto unlock;
        }
        if (core->migration_in_progress) {
            rc = LOX_ERR_SCHEMA;
            goto unlock;
        }
        core->migration_in_progress = true;
        need_migrate_cb = true;
        migrate_old = existing->schema_version;
        migrate_new = impl->schema_version;
        memset(migrate_name, 0, sizeof(migrate_name));
        memcpy(migrate_name, impl->name, strlen(impl->name) + 1u);
        goto unlock;
    }
    if (core->rel.registered_tables >= LOX_REL_MAX_TABLES) {
        rc = LOX_ERR_FULL;
        goto unlock;
    }
    if (!rel_has_arena_space_for_table(core, impl)) {
        rc = LOX_ERR_NO_MEM;
        goto unlock;
    }
    if (wal_mode) {
        rc = lox_persist_rel_table_create(db, schema);
        if (rc != LOX_OK) {
            goto unlock;
        }
    }

    for (i = 0; i < LOX_REL_MAX_TABLES; ++i) {
        table = &core->rel.tables[i];
        if (!table->registered) {
            memset(table, 0, sizeof(*table));
            memcpy(table->name, impl->name, sizeof(table->name));
            memcpy(table->cols, impl->cols, sizeof(impl->cols));
            table->col_count = impl->col_count;
            table->max_rows = impl->max_rows;
            table->row_size = impl->row_size;
            table->index_col = impl->index_col;
            table->schema_version = impl->schema_version;
            if (impl->index_col != UINT32_MAX) {
                table->index_key_size = impl->cols[impl->index_col].size;
            }

            table->rows = (uint8_t *)lox_arena_alloc(&core->rel_arena,
                                                         (size_t)table->max_rows * table->row_size,
                                                         8u);
            alive_bytes = (table->max_rows + 7u) / 8u;
            table->alive_bitmap = (uint8_t *)lox_arena_alloc(&core->rel_arena, alive_bytes, 1u);
            table->order = (uint32_t *)lox_arena_alloc(&core->rel_arena,
                                                           (size_t)table->max_rows * sizeof(uint32_t),
                                                           4u);
            if (table->index_key_size != 0u) {
                table->index = (lox_index_entry_t *)lox_arena_alloc(&core->rel_arena,
                                                                            (size_t)table->max_rows * sizeof(lox_index_entry_t),
                                                                            4u);
            }

            if (table->rows == NULL || table->alive_bitmap == NULL || table->order == NULL ||
                (table->index_key_size != 0u && table->index == NULL)) {
                memset(table, 0, sizeof(*table));
                rc = LOX_ERR_NO_MEM;
                goto unlock;
            }

            memset(table->rows, 0, (size_t)table->max_rows * table->row_size);
            memset(table->alive_bitmap, 0, alive_bytes);
            if (table->index != NULL) {
                memset(table->index, 0, (size_t)table->max_rows * sizeof(lox_index_entry_t));
            }
            memset(table->order, 0, (size_t)table->max_rows * sizeof(uint32_t));
            table->registered = true;
            core->rel.registered_tables++;
            if (!wal_mode) {
                rc = lox_storage_flush(db);
            } else {
                rc = LOX_OK;
            }
            goto unlock;
        }
    }

    rc = LOX_ERR_FULL;

unlock:
    LOX_UNLOCK(db);
    if (need_migrate_cb) {
        rc = core->on_migrate(db, migrate_name, migrate_old, migrate_new);
        LOX_LOCK(db);
        core = lox_core(db);
        if (core->magic != LOX_MAGIC) {
            LOX_UNLOCK(db);
            return LOX_ERR_INVALID;
        }
        core->migration_in_progress = false;
        if (rc != LOX_OK) {
            LOX_UNLOCK(db);
            return rc;
        }
        existing = rel_find_table(core, migrate_name);
        if (existing == NULL) {
            LOX_UNLOCK(db);
            return LOX_ERR_NOT_FOUND;
        }
        if (rel_wal_mode(core)) {
            rc = lox_persist_rel_table_create(db, schema);
            if (rc != LOX_OK) {
                LOX_UNLOCK(db);
                return rc;
            }
        }
        existing->schema_version = migrate_new;
        if (!rel_wal_mode(core)) {
            rc = lox_storage_flush(db);
        } else {
            rc = LOX_OK;
        }
        LOX_UNLOCK(db);
        return rc;
    }
    return rc;
}

lox_err_t lox_table_get(lox_t *db, const char *name, lox_table_t **out_table) {
    lox_core_t *core;
    lox_table_t *table;
    lox_err_t err;
    lox_err_t rc = LOX_OK;

    if (db == NULL || out_table == NULL) {
        return LOX_ERR_INVALID;
    }

    err = lox_rel_validate_name(name, LOX_REL_TABLE_NAME_LEN);
    if (err != LOX_OK) {
        return err;
    }

    LOX_LOCK(db);
    core = lox_core(db);
    if (core->magic != LOX_MAGIC) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    table = rel_find_table(core, name);
    if (table == NULL) {
        rc = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    *out_table = table;
    rc = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

size_t lox_table_row_size(const lox_table_t *table) {
    if (table == NULL) {
        return 0u;
    }
    return table->row_size;
}

lox_err_t lox_row_set(const lox_table_t *table, void *row_buf, const char *col_name, const void *val) {
    const lox_col_desc_t *col;

    if (table == NULL || row_buf == NULL || col_name == NULL || val == NULL) {
        return LOX_ERR_INVALID;
    }

    col = lox_rel_find_col_const(table->cols, table->col_count, col_name);
    if (col == NULL) {
        return LOX_ERR_NOT_FOUND;
    }

    if (col->type == LOX_COL_STR) {
        if (rel_validate_str_value((const char *)val, col->size) != LOX_OK) {
            return LOX_ERR_SCHEMA;
        }
        memset((uint8_t *)row_buf + col->offset, 0, col->size);
        memcpy((uint8_t *)row_buf + col->offset, val, strlen((const char *)val) + 1u);
        return LOX_OK;
    }

    memcpy((uint8_t *)row_buf + col->offset, val, col->size);
    return LOX_OK;
}

lox_err_t lox_row_get(const lox_table_t *table,
                              const void *row_buf,
                              const char *col_name,
                              void *out,
                              size_t *out_len) {
    const lox_col_desc_t *col;

    if (table == NULL || row_buf == NULL || col_name == NULL || out == NULL) {
        return LOX_ERR_INVALID;
    }

    col = lox_rel_find_col_const(table->cols, table->col_count, col_name);
    if (col == NULL) {
        return LOX_ERR_NOT_FOUND;
    }

    memcpy(out, (const uint8_t *)row_buf + col->offset, col->size);
    if (out_len != NULL) {
        *out_len = col->size;
    }
    return LOX_OK;
}

lox_err_t lox_rel_insert(lox_t *db, lox_table_t *table, const void *row_buf) {
    lox_err_t err;
    uint32_t row_idx;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }
    LOX_LOCK(db);

    err = rel_validate_table_and_handle(db, table);
    if (err != LOX_OK) {
        rc = err;
        goto unlock;
    }
    if (row_buf == NULL) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    if (table->live_count >= table->max_rows) {
        rc = LOX_ERR_FULL;
        goto unlock;
    }

    row_idx = rel_find_free_row(table);
    if (row_idx == UINT32_MAX) {
        rc = LOX_ERR_FULL;
        goto unlock;
    }

    rc = lox_persist_rel_insert(db, table, row_buf);
    if (rc == LOX_OK) {
        rel_apply_insert_row(table, row_idx, row_buf);
        lox__maybe_compact(db);
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_rel_find(lox_t *db,
                               lox_table_t *table,
                               const void *search_val,
                               lox_rel_iter_cb_t cb,
                               void *ctx) {
    uint32_t idx;
    uint32_t snapshot_mutation_seq;
    lox_err_t err;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }
    LOX_LOCK(db);
    err = rel_validate_table_and_handle(db, table);
    if (err != LOX_OK) {
        rc = err;
        goto unlock;
    }
    if (search_val == NULL || cb == NULL) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    if (table->index_col == UINT32_MAX) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    idx = rel_index_find_first(table, search_val);
    snapshot_mutation_seq = table->mutation_seq;
    if (idx == UINT32_MAX) {
        rc = LOX_OK;
        goto unlock;
    }

    while (idx < table->index_count &&
           rel_key_cmp(table->index[idx].key_bytes, search_val, table->index_key_size) == 0) {
        uint8_t row_copy[LOX_REL_ROW_SCRATCH_MAX];
        uint32_t row_idx = table->index[idx].row_idx;
        if (table->row_size > sizeof(row_copy)) {
            rc = LOX_ERR_OVERFLOW;
            goto unlock;
        }
        memcpy(row_copy, rel_row_ptr(table, row_idx), table->row_size);
        idx++;
        LOX_UNLOCK(db);
        if (!cb(row_copy, ctx)) {
            return LOX_OK;
        }
        LOX_LOCK(db);
        err = rel_validate_table_and_handle(db, table);
        if (err != LOX_OK) {
            rc = err;
            goto unlock;
        }
        if (table->mutation_seq != snapshot_mutation_seq) {
            rc = LOX_ERR_MODIFIED;
            goto unlock;
        }
    }

    rc = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_rel_find_by(lox_t *db,
                                  lox_table_t *table,
                                  const char *col_name,
                                  const void *search_val,
                                  void *out_buf) {
    const lox_col_desc_t *col;
    uint32_t i;
    lox_err_t err;

    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }
    LOX_LOCK(db);
    err = rel_validate_table_and_handle(db, table);
    if (err != LOX_OK) {
        rc = err;
        goto unlock;
    }
    if (col_name == NULL || search_val == NULL || out_buf == NULL) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    col = lox_rel_find_col_const(table->cols, table->col_count, col_name);
    if (col == NULL) {
        rc = LOX_ERR_NOT_FOUND;
        goto unlock;
    }

    for (i = 0; i < table->order_count; ++i) {
        uint32_t row_idx = table->order[i];
        const uint8_t *row = (const uint8_t *)rel_row_ptr(table, row_idx);
        bool match = false;

        if (!rel_is_alive(table->alive_bitmap, row_idx)) {
            continue;
        }

        if (col->type == LOX_COL_STR) {
            match = strncmp((const char *)(row + col->offset), (const char *)search_val, col->size) == 0;
        } else {
            match = memcmp(row + col->offset, search_val, col->size) == 0;
        }

        if (match) {
            memcpy(out_buf, row, table->row_size);
            rc = LOX_OK;
            goto unlock;
        }
    }

    rc = LOX_ERR_NOT_FOUND;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_rel_delete(lox_t *db, lox_table_t *table, const void *search_val, uint32_t *out_deleted) {
    uint32_t deleted = 0u;
    uint32_t idx;
    uint32_t match_count = 0u;
    uint32_t m;
    lox_err_t err;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }
    LOX_LOCK(db);

    err = rel_validate_table_and_handle(db, table);
    if (err != LOX_OK) {
        rc = err;
        goto unlock;
    }
    if (search_val == NULL) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    if (table->index_col == UINT32_MAX) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }

    idx = rel_index_find_first(table, search_val);
    while (idx != UINT32_MAX &&
           idx < table->index_count &&
           rel_key_cmp(table->index[idx].key_bytes, search_val, table->index_key_size) == 0) {
        match_count++;
        idx++;
    }

    if (match_count == 0u) {
        if (out_deleted != NULL) {
            *out_deleted = 0u;
        }
        rc = LOX_OK;
        goto unlock;
    }

    rc = lox_persist_rel_delete(db, table, search_val);
    if (rc != LOX_OK) {
        goto unlock;
    }

    for (m = 0u; m < match_count; ++m) {
        idx = rel_index_find_first(table, search_val);
        if (idx == UINT32_MAX) {
            break;
        }
        rel_apply_delete_row(table, table->index[idx].row_idx);
        deleted++;
    }
    if (out_deleted != NULL) {
        *out_deleted = deleted;
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_rel_iter(lox_t *db, lox_table_t *table, lox_rel_iter_cb_t cb, void *ctx) {
    uint32_t i;
    uint32_t snapshot_mutation_seq;
    lox_err_t err;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }
    LOX_LOCK(db);
    err = rel_validate_table_and_handle(db, table);
    if (err != LOX_OK) {
        rc = err;
        goto unlock;
    }
    if (cb == NULL) {
        rc = LOX_ERR_INVALID;
        goto unlock;
    }
    snapshot_mutation_seq = table->mutation_seq;

    for (i = 0; i < table->order_count; ++i) {
        uint32_t row_idx = table->order[i];
        if (rel_is_alive(table->alive_bitmap, row_idx)) {
            uint8_t row_copy[LOX_REL_ROW_SCRATCH_MAX];
            if (table->row_size > sizeof(row_copy)) {
                rc = LOX_ERR_OVERFLOW;
                goto unlock;
            }
            memcpy(row_copy, rel_row_ptr(table, row_idx), table->row_size);
            LOX_UNLOCK(db);
            if (!cb(row_copy, ctx)) {
                return LOX_OK;
            }
            LOX_LOCK(db);
            err = rel_validate_table_and_handle(db, table);
            if (err != LOX_OK) {
                rc = err;
                goto unlock;
            }
            if (table->mutation_seq != snapshot_mutation_seq) {
                rc = LOX_ERR_MODIFIED;
                goto unlock;
            }
        }
    }

    rc = LOX_OK;

unlock:
    LOX_UNLOCK(db);
    return rc;
}

lox_err_t lox_rel_count(const lox_table_t *table, uint32_t *out_count) {
    if (table == NULL || out_count == NULL) {
        return LOX_ERR_INVALID;
    }
    if (!table->registered) {
        return LOX_ERR_INVALID;
    }
    *out_count = table->live_count;
    return LOX_OK;
}

lox_err_t lox_rel_clear(lox_t *db, lox_table_t *table) {
    uint32_t alive_bytes;
    lox_err_t err;
    lox_err_t rc = LOX_OK;

    if (db == NULL) {
        return LOX_ERR_INVALID;
    }
    LOX_LOCK(db);
    err = rel_validate_table_and_handle(db, table);
    if (err != LOX_OK) {
        rc = err;
        goto unlock;
    }
    if (rel_wal_mode(lox_core(db))) {
        rc = lox_persist_rel_clear(db, table);
        if (rc != LOX_OK) {
            goto unlock;
        }
    }

    alive_bytes = (table->max_rows + 7u) / 8u;
    memset(table->alive_bitmap, 0, alive_bytes);
    if (table->index != NULL) {
        memset(table->index, 0, (size_t)table->max_rows * sizeof(lox_index_entry_t));
    }
    memset(table->order, 0, (size_t)table->max_rows * sizeof(uint32_t));
    table->live_count = 0u;
    table->index_count = 0u;
    table->order_count = 0u;
    table->mutation_seq++;
    if (!rel_wal_mode(lox_core(db))) {
        rc = lox_storage_flush(db);
    } else {
        rc = LOX_OK;
    }

unlock:
    LOX_UNLOCK(db);
    return rc;
}
#else
lox_err_t lox_schema_init(lox_schema_t *schema, const char *name, uint32_t max_rows) {
    (void)schema;
    (void)name;
    (void)max_rows;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_schema_add(lox_schema_t *schema,
                                 const char *col_name,
                                 lox_col_type_t type,
                                 size_t size,
                                 bool is_index) {
    (void)schema;
    (void)col_name;
    (void)type;
    (void)size;
    (void)is_index;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_schema_seal(lox_schema_t *schema) {
    (void)schema;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_table_create(lox_t *db, lox_schema_t *schema) {
    (void)db;
    (void)schema;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_table_get(lox_t *db, const char *name, lox_table_t **out_table) {
    (void)db;
    (void)name;
    (void)out_table;
    return LOX_ERR_DISABLED;
}

size_t lox_table_row_size(const lox_table_t *table) {
    (void)table;
    return 0u;
}

lox_err_t lox_row_set(const lox_table_t *table, void *row_buf, const char *col_name, const void *val) {
    (void)table;
    (void)row_buf;
    (void)col_name;
    (void)val;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_row_get(const lox_table_t *table,
                              const void *row_buf,
                              const char *col_name,
                              void *out,
                              size_t *out_len) {
    (void)table;
    (void)row_buf;
    (void)col_name;
    (void)out;
    (void)out_len;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_rel_insert(lox_t *db, lox_table_t *table, const void *row_buf) {
    (void)db;
    (void)table;
    (void)row_buf;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_rel_find(lox_t *db,
                               lox_table_t *table,
                               const void *search_val,
                               lox_rel_iter_cb_t cb,
                               void *ctx) {
    (void)db;
    (void)table;
    (void)search_val;
    (void)cb;
    (void)ctx;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_rel_find_by(lox_t *db,
                                  lox_table_t *table,
                                  const char *col_name,
                                  const void *search_val,
                                  void *out_buf) {
    (void)db;
    (void)table;
    (void)col_name;
    (void)search_val;
    (void)out_buf;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_rel_delete(lox_t *db, lox_table_t *table, const void *search_val, uint32_t *out_deleted) {
    (void)db;
    (void)table;
    (void)search_val;
    (void)out_deleted;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_rel_iter(lox_t *db, lox_table_t *table, lox_rel_iter_cb_t cb, void *ctx) {
    (void)db;
    (void)table;
    (void)cb;
    (void)ctx;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_rel_count(const lox_table_t *table, uint32_t *out_count) {
    (void)table;
    (void)out_count;
    return LOX_ERR_DISABLED;
}

lox_err_t lox_rel_clear(lox_t *db, lox_table_t *table) {
    (void)db;
    (void)table;
    return LOX_ERR_DISABLED;
}
#endif
