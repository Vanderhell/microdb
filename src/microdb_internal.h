// SPDX-License-Identifier: MIT
#ifndef MICRODB_INTERNAL_H
#define MICRODB_INTERNAL_H

#include "microdb.h"

#define MICRODB_MAGIC 0x4D444230u

typedef struct {
    uint8_t *base;
    size_t used;
    size_t capacity;
} microdb_arena_t;

typedef struct {
    uint8_t state;
    char key[MICRODB_KV_KEY_MAX_LEN];
    uint32_t val_offset;
    uint32_t val_len;
    uint32_t expires_at;
    uint32_t last_access;
} microdb_kv_bucket_t;

typedef struct {
    microdb_kv_bucket_t *buckets;
    uint32_t bucket_count;
    uint32_t entry_count;
    uint32_t collision_count;
    uint32_t eviction_count;
    uint8_t *value_store;
    uint32_t value_capacity;
    uint32_t value_used;
    uint32_t access_clock;
} microdb_kv_state_t;

typedef struct {
    char key[MICRODB_KV_KEY_MAX_LEN];
    void *val_ptr;
    size_t val_len;
    uint32_t expires_at;
    uint8_t op;
    uint8_t val_buf[MICRODB_KV_VAL_MAX_LEN];
} microdb_txn_stage_entry_t;

typedef struct {
    char name[MICRODB_TS_STREAM_NAME_LEN];
    microdb_ts_type_t type;
    size_t raw_size;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
    uint32_t capacity;
    microdb_ts_sample_t *buf;
    bool registered;
} microdb_ts_stream_t;

typedef struct {
    microdb_ts_stream_t streams[MICRODB_TS_MAX_STREAMS];
    uint32_t registered_streams;
} microdb_ts_state_t;

typedef struct {
    char name[MICRODB_REL_COL_NAME_LEN];
    microdb_col_type_t type;
    size_t size;
    size_t offset;
    bool is_index;
} microdb_col_desc_t;

typedef struct {
    uint8_t key_bytes[MICRODB_REL_INDEX_KEY_MAX];
    uint32_t row_idx;
} microdb_index_entry_t;

struct microdb_table_s {
    char name[MICRODB_REL_TABLE_NAME_LEN];
    uint16_t schema_version;
    microdb_col_desc_t cols[MICRODB_REL_MAX_COLS];
    uint32_t col_count;
    uint32_t max_rows;
    size_t row_size;
    uint32_t index_col;
    size_t index_key_size;
    uint8_t *rows;
    uint8_t *alive_bitmap;
    microdb_index_entry_t *index;
    uint32_t *order;
    uint32_t live_count;
    uint32_t index_count;
    uint32_t order_count;
    bool registered;
};

typedef struct {
    struct microdb_table_s tables[MICRODB_REL_MAX_TABLES];
    uint32_t registered_tables;
} microdb_rel_state_t;

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
    uint32_t active_bank;
    uint32_t active_generation;
} microdb_storage_layout_t;

typedef struct {
    uint32_t magic;
    uint8_t *heap;
    size_t heap_size;
    size_t live_bytes;
    microdb_storage_t *storage;
    microdb_timestamp_t (*now)(void);
    void (*lock)(void *hdl);
    void (*unlock)(void *hdl);
    void (*lock_destroy)(void *hdl);
    void *lock_handle;
    uint32_t storage_bytes_written;
    uint32_t compact_count;
    uint32_t reopen_count;
    uint32_t recovery_count;
    microdb_err_t last_runtime_error;
    microdb_err_t last_recovery_status;
    bool wal_enabled;
    microdb_arena_t arena;
    microdb_arena_t kv_arena;
    microdb_arena_t ts_arena;
    microdb_arena_t rel_arena;
    microdb_kv_state_t kv;
    microdb_txn_stage_entry_t *txn_stage;
    uint8_t txn_active;
    uint32_t txn_stage_count;
    microdb_ts_state_t ts;
    microdb_rel_state_t rel;
    microdb_storage_layout_t layout;
    uint32_t wal_sequence;
    uint32_t wal_entry_count;
    uint32_t wal_used;
    uint8_t wal_compact_auto;
    uint8_t wal_compact_threshold_pct;
    microdb_err_t (*on_migrate)(microdb_t *db, const char *table_name, uint16_t old_version, uint16_t new_version);
    bool storage_loading;
    bool wal_replaying;
    uint32_t ts_dropped_samples;
} microdb_core_t;

typedef struct {
    char name[MICRODB_REL_TABLE_NAME_LEN];
    uint16_t schema_version;
    microdb_col_desc_t cols[MICRODB_REL_MAX_COLS];
    uint32_t col_count;
    uint32_t max_rows;
    size_t row_size;
    uint32_t index_col;
    bool sealed;
} microdb_schema_impl_t;

MICRODB_STATIC_ASSERT(core_size_fits, sizeof(microdb_core_t) <= sizeof(((microdb_t *)0)->_opaque));
MICRODB_STATIC_ASSERT(schema_size_fits, sizeof(microdb_schema_impl_t) <= sizeof(((microdb_schema_t *)0)->_opaque));
MICRODB_STATIC_ASSERT(table_size_fits, sizeof(struct microdb_table_s) >= (MICRODB_REL_TABLE_NAME_LEN + sizeof(size_t)));

microdb_core_t *microdb_core(microdb_t *db);
const microdb_core_t *microdb_core_const(const microdb_t *db);
microdb_err_t microdb_kv_init(microdb_t *db);
microdb_err_t microdb_ts_init(microdb_t *db);
size_t microdb_kv_live_bytes(const microdb_t *db);
microdb_err_t microdb_kv_set_at(microdb_t *db, const char *key, const void *val, size_t len, uint32_t expires_at);
microdb_err_t microdb_storage_bootstrap(microdb_t *db);
microdb_err_t microdb_storage_flush(microdb_t *db);
microdb_err_t microdb_persist_kv_set(microdb_t *db, const char *key, const void *val, size_t len, uint32_t expires_at);
microdb_err_t microdb_persist_kv_del(microdb_t *db, const char *key);
microdb_err_t microdb_persist_kv_clear(microdb_t *db);
microdb_err_t microdb_persist_kv_set_txn(microdb_t *db, const char *key, const void *val, size_t len, uint32_t expires_at);
microdb_err_t microdb_persist_kv_del_txn(microdb_t *db, const char *key);
microdb_err_t microdb_persist_txn_commit(microdb_t *db);
microdb_err_t microdb_persist_ts_insert(microdb_t *db, const char *name, microdb_timestamp_t ts, const void *val, size_t val_len);
microdb_err_t microdb_persist_ts_register(microdb_t *db, const char *name, microdb_ts_type_t type, size_t raw_size);
microdb_err_t microdb_persist_ts_clear(microdb_t *db, const char *name);
microdb_err_t microdb_persist_rel_insert(microdb_t *db, const microdb_table_t *table, const void *row_buf);
microdb_err_t microdb_persist_rel_delete(microdb_t *db, const microdb_table_t *table, const void *search_val);
microdb_err_t microdb_persist_rel_table_create(microdb_t *db, const microdb_schema_t *schema);
microdb_err_t microdb_persist_rel_clear(microdb_t *db, const microdb_table_t *table);

static inline void microdb__maybe_compact(microdb_t *db) {
    microdb_core_t *core = microdb_core(db);
    uint32_t wal_total;
    uint32_t wal_used;
    uint32_t wal_fill_pct;
    uint32_t threshold;

    if (!core->wal_enabled || core->layout.wal_size <= 32u || core->wal_compact_auto == 0u) {
        return;
    }

    wal_total = core->layout.wal_size - 32u;
    wal_used = (core->wal_used > 32u) ? (core->wal_used - 32u) : 0u;
    wal_fill_pct = (wal_total == 0u) ? 0u : ((wal_used * 100u) / wal_total);
    threshold = (core->wal_compact_threshold_pct != 0u) ? core->wal_compact_threshold_pct : 75u;
    if (wal_fill_pct >= threshold) {
        (void)microdb_storage_flush(db);
    }
}

static inline void microdb_record_error(microdb_core_t *core, microdb_err_t err) {
    if (core != NULL && err != MICRODB_OK) {
        core->last_runtime_error = err;
    }
}

#endif
