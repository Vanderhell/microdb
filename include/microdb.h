#ifndef MICRODB_H
#define MICRODB_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef MICRODB_RAM_KB
#define MICRODB_RAM_KB 32u
#endif
#ifndef MICRODB_ENABLE_KV
#define MICRODB_ENABLE_KV 1
#endif
#ifndef MICRODB_ENABLE_TS
#define MICRODB_ENABLE_TS 1
#endif
#ifndef MICRODB_ENABLE_REL
#define MICRODB_ENABLE_REL 1
#endif
#ifndef MICRODB_RAM_KV_PCT
#define MICRODB_RAM_KV_PCT 40u
#endif
#ifndef MICRODB_RAM_TS_PCT
#define MICRODB_RAM_TS_PCT 40u
#endif
#ifndef MICRODB_RAM_REL_PCT
#define MICRODB_RAM_REL_PCT 20u
#endif
#ifndef MICRODB_KV_MAX_KEYS
#define MICRODB_KV_MAX_KEYS 64u
#endif
#ifndef MICRODB_KV_KEY_MAX_LEN
#define MICRODB_KV_KEY_MAX_LEN 32u
#endif
#ifndef MICRODB_KV_VAL_MAX_LEN
#define MICRODB_KV_VAL_MAX_LEN 128u
#endif
#ifndef MICRODB_KV_ENABLE_TTL
#define MICRODB_KV_ENABLE_TTL 1
#endif
#define MICRODB_KV_POLICY_OVERWRITE 0u
#define MICRODB_KV_POLICY_REJECT 1u
#ifndef MICRODB_KV_OVERFLOW_POLICY
#define MICRODB_KV_OVERFLOW_POLICY MICRODB_KV_POLICY_OVERWRITE
#endif
#ifndef MICRODB_TS_MAX_STREAMS
#define MICRODB_TS_MAX_STREAMS 8u
#endif
#ifndef MICRODB_TS_STREAM_NAME_LEN
#define MICRODB_TS_STREAM_NAME_LEN 16u
#endif
#ifndef MICRODB_TS_RAW_MAX
#define MICRODB_TS_RAW_MAX 16u
#endif
#define MICRODB_TS_POLICY_DROP_OLDEST 0u
#define MICRODB_TS_POLICY_REJECT 1u
#define MICRODB_TS_POLICY_DOWNSAMPLE 2u
#ifndef MICRODB_TS_OVERFLOW_POLICY
#define MICRODB_TS_OVERFLOW_POLICY MICRODB_TS_POLICY_DROP_OLDEST
#endif
#ifndef MICRODB_REL_MAX_TABLES
#define MICRODB_REL_MAX_TABLES 4u
#endif
#ifndef MICRODB_REL_MAX_COLS
#define MICRODB_REL_MAX_COLS 16u
#endif
#ifndef MICRODB_REL_COL_NAME_LEN
#define MICRODB_REL_COL_NAME_LEN 16u
#endif
#ifndef MICRODB_REL_TABLE_NAME_LEN
#define MICRODB_REL_TABLE_NAME_LEN 16u
#endif
#ifndef MICRODB_ENABLE_WAL
#define MICRODB_ENABLE_WAL 1
#endif
#ifndef MICRODB_TIMESTAMP_TYPE
#define MICRODB_TIMESTAMP_TYPE uint32_t
#endif

#define MICRODB_STATIC_ASSERT(name, expr) typedef char microdb_static_assert_##name[(expr) ? 1 : -1]

MICRODB_STATIC_ASSERT(ram_pct_sum, (MICRODB_RAM_KV_PCT + MICRODB_RAM_TS_PCT + MICRODB_RAM_REL_PCT) == 100u);
MICRODB_STATIC_ASSERT(ram_kb_min, MICRODB_RAM_KB >= 8u);

typedef MICRODB_TIMESTAMP_TYPE microdb_timestamp_t;

#ifndef MICRODB_HANDLE_SIZE
#define MICRODB_HANDLE_SIZE 8192u
#endif
#ifndef MICRODB_SCHEMA_SIZE
#define MICRODB_SCHEMA_SIZE 880u
#endif
#ifndef MICRODB_REL_INDEX_KEY_MAX
#define MICRODB_REL_INDEX_KEY_MAX 16u
#endif

typedef struct {
    uint8_t _opaque[MICRODB_HANDLE_SIZE];
} microdb_t;

typedef struct {
    uint8_t _opaque[MICRODB_SCHEMA_SIZE];
} microdb_schema_t;

typedef struct microdb_table_s microdb_table_t;

typedef enum {
    MICRODB_OK = 0,
    MICRODB_ERR_INVALID = -1,
    MICRODB_ERR_NO_MEM = -2,
    MICRODB_ERR_FULL = -3,
    MICRODB_ERR_NOT_FOUND = -4,
    MICRODB_ERR_EXPIRED = -5,
    MICRODB_ERR_STORAGE = -6,
    MICRODB_ERR_CORRUPT = -7,
    MICRODB_ERR_SEALED = -8,
    MICRODB_ERR_EXISTS = -9,
    MICRODB_ERR_DISABLED = -10,
    MICRODB_ERR_OVERFLOW = -11,
    MICRODB_ERR_SCHEMA = -12
} microdb_err_t;

typedef enum {
    MICRODB_TS_F32 = 0,
    MICRODB_TS_I32 = 1,
    MICRODB_TS_U32 = 2,
    MICRODB_TS_RAW = 3
} microdb_ts_type_t;

typedef enum {
    MICRODB_COL_U8 = 0,
    MICRODB_COL_U16 = 1,
    MICRODB_COL_U32 = 2,
    MICRODB_COL_U64 = 3,
    MICRODB_COL_I8 = 4,
    MICRODB_COL_I16 = 5,
    MICRODB_COL_I32 = 6,
    MICRODB_COL_I64 = 7,
    MICRODB_COL_F32 = 8,
    MICRODB_COL_F64 = 9,
    MICRODB_COL_BOOL = 10,
    MICRODB_COL_STR = 11,
    MICRODB_COL_BLOB = 12
} microdb_col_type_t;

typedef struct {
    microdb_err_t (*read)(void *ctx, uint32_t offset, void *buf, size_t len);
    microdb_err_t (*write)(void *ctx, uint32_t offset, const void *buf, size_t len);
    microdb_err_t (*erase)(void *ctx, uint32_t offset);
    microdb_err_t (*sync)(void *ctx);
    uint32_t capacity;
    uint32_t erase_size;
    uint32_t write_size;
    void *ctx;
} microdb_storage_t;

typedef struct {
    microdb_storage_t *storage;
    uint32_t ram_kb;
    microdb_timestamp_t (*now)(void);
} microdb_cfg_t;

typedef struct {
    size_t ram_total_bytes;
    size_t ram_used_bytes;
    uint32_t kv_entries;
    uint32_t kv_capacity;
    uint32_t ts_streams;
    uint32_t rel_tables;
    uint32_t storage_bytes_written;
} microdb_stats_t;

typedef struct {
    microdb_timestamp_t ts;
    union {
        float f32;
        int32_t i32;
        uint32_t u32;
        uint8_t raw[MICRODB_TS_RAW_MAX];
    } v;
} microdb_ts_sample_t;

microdb_err_t microdb_init(microdb_t *db, const microdb_cfg_t *cfg);
microdb_err_t microdb_deinit(microdb_t *db);
microdb_err_t microdb_flush(microdb_t *db);
microdb_err_t microdb_stats(const microdb_t *db, microdb_stats_t *out);

#if MICRODB_ENABLE_KV
typedef bool (*microdb_kv_iter_cb_t)(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx);
microdb_err_t microdb_kv_set(microdb_t *db, const char *key, const void *val, size_t len, uint32_t ttl);
microdb_err_t microdb_kv_get(microdb_t *db, const char *key, void *buf, size_t buf_len, size_t *out_len);
microdb_err_t microdb_kv_del(microdb_t *db, const char *key);
microdb_err_t microdb_kv_exists(microdb_t *db, const char *key);
microdb_err_t microdb_kv_iter(microdb_t *db, microdb_kv_iter_cb_t cb, void *ctx);
microdb_err_t microdb_kv_purge_expired(microdb_t *db);
microdb_err_t microdb_kv_clear(microdb_t *db);
#define microdb_kv_put(db, key, val, len) microdb_kv_set((db), (key), (val), (len), 0u)
#endif

#if MICRODB_ENABLE_TS
typedef bool (*microdb_ts_query_cb_t)(const microdb_ts_sample_t *sample, void *ctx);
microdb_err_t microdb_ts_register(microdb_t *db, const char *name, microdb_ts_type_t type, size_t raw_size);
microdb_err_t microdb_ts_insert(microdb_t *db, const char *name, microdb_timestamp_t ts, const void *val);
microdb_err_t microdb_ts_last(microdb_t *db, const char *name, microdb_ts_sample_t *out);
microdb_err_t microdb_ts_query(microdb_t *db, const char *name, microdb_timestamp_t from, microdb_timestamp_t to, microdb_ts_query_cb_t cb, void *ctx);
microdb_err_t microdb_ts_query_buf(microdb_t *db, const char *name, microdb_timestamp_t from, microdb_timestamp_t to, microdb_ts_sample_t *buf, size_t max_count, size_t *out_count);
microdb_err_t microdb_ts_count(microdb_t *db, const char *name, microdb_timestamp_t from, microdb_timestamp_t to, size_t *out_count);
microdb_err_t microdb_ts_clear(microdb_t *db, const char *name);
#endif

#if MICRODB_ENABLE_REL
typedef bool (*microdb_rel_iter_cb_t)(const void *row_buf, void *ctx);
microdb_err_t microdb_schema_init(microdb_schema_t *schema, const char *name, uint32_t max_rows);
microdb_err_t microdb_schema_add(microdb_schema_t *schema, const char *col_name, microdb_col_type_t type, size_t size, bool is_index);
microdb_err_t microdb_schema_seal(microdb_schema_t *schema);
microdb_err_t microdb_table_create(microdb_t *db, microdb_schema_t *schema);
microdb_err_t microdb_table_get(microdb_t *db, const char *name, microdb_table_t **out_table);
size_t microdb_table_row_size(const microdb_table_t *table);
microdb_err_t microdb_row_set(const microdb_table_t *table, void *row_buf, const char *col_name, const void *val);
microdb_err_t microdb_row_get(const microdb_table_t *table, const void *row_buf, const char *col_name, void *out, size_t *out_len);
microdb_err_t microdb_rel_insert(microdb_t *db, microdb_table_t *table, const void *row_buf);
microdb_err_t microdb_rel_find(microdb_t *db, microdb_table_t *table, const void *search_val, microdb_rel_iter_cb_t cb, void *ctx);
microdb_err_t microdb_rel_find_by(microdb_t *db, microdb_table_t *table, const char *col_name, const void *search_val, void *out_buf);
microdb_err_t microdb_rel_delete(microdb_t *db, microdb_table_t *table, const void *search_val, uint32_t *out_deleted);
microdb_err_t microdb_rel_iter(microdb_t *db, microdb_table_t *table, microdb_rel_iter_cb_t cb, void *ctx);
microdb_err_t microdb_rel_count(const microdb_table_t *table, uint32_t *out_count);
microdb_err_t microdb_rel_clear(microdb_t *db, microdb_table_t *table);
#endif

#endif
