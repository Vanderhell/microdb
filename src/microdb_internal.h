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
    uint8_t *value_store;
    uint32_t value_capacity;
    uint32_t value_used;
    uint32_t access_clock;
} microdb_kv_state_t;

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
    uint32_t registered_tables;
} microdb_rel_state_t;

typedef struct {
    char name[MICRODB_REL_COL_NAME_LEN];
    microdb_col_type_t type;
    size_t size;
    size_t offset;
    bool is_index;
} microdb_col_desc_t;

typedef struct {
    uint32_t magic;
    uint8_t *heap;
    size_t heap_size;
    size_t live_bytes;
    microdb_storage_t *storage;
    microdb_timestamp_t (*now)(void);
    uint32_t storage_bytes_written;
    bool wal_enabled;
    microdb_arena_t arena;
    microdb_arena_t kv_arena;
    microdb_arena_t ts_arena;
    microdb_arena_t rel_arena;
    microdb_kv_state_t kv;
    microdb_ts_state_t ts;
    microdb_rel_state_t rel;
} microdb_core_t;

typedef struct {
    char name[MICRODB_REL_TABLE_NAME_LEN];
    microdb_col_desc_t cols[MICRODB_REL_MAX_COLS];
    uint32_t col_count;
    uint32_t max_rows;
    uint32_t row_size;
    uint32_t index_col;
    bool sealed;
} microdb_schema_impl_t;

struct microdb_table_s {
    char name[MICRODB_REL_TABLE_NAME_LEN];
    microdb_col_desc_t cols[MICRODB_REL_MAX_COLS];
    uint32_t col_count;
    uint32_t max_rows;
    uint32_t row_size;
    uint32_t index_col;
};

MICRODB_STATIC_ASSERT(core_size_fits, sizeof(microdb_core_t) <= sizeof(((microdb_t *)0)->_opaque));
MICRODB_STATIC_ASSERT(schema_size_fits, sizeof(microdb_schema_impl_t) <= sizeof(((microdb_schema_t *)0)->_opaque));
MICRODB_STATIC_ASSERT(table_size_fits, sizeof(struct microdb_table_s) >= (MICRODB_REL_TABLE_NAME_LEN + sizeof(uint32_t)));

microdb_core_t *microdb_core(microdb_t *db);
const microdb_core_t *microdb_core_const(const microdb_t *db);
microdb_err_t microdb_kv_init(microdb_t *db);
microdb_err_t microdb_ts_init(microdb_t *db);
size_t microdb_kv_live_bytes(const microdb_t *db);

#endif
