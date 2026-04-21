// SPDX-License-Identifier: MIT
#ifndef MICRODB_H
#define MICRODB_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef MICRODB_PROFILE_CORE_MIN
#define MICRODB_PROFILE_CORE_MIN 0
#endif
#ifndef MICRODB_PROFILE_CORE_WAL
#define MICRODB_PROFILE_CORE_WAL 0
#endif
#ifndef MICRODB_PROFILE_CORE_PERF
#define MICRODB_PROFILE_CORE_PERF 0
#endif
#ifndef MICRODB_PROFILE_CORE_HIMEM
#define MICRODB_PROFILE_CORE_HIMEM 0
#endif
#ifndef MICRODB_PROFILE_FOOTPRINT_MIN
#define MICRODB_PROFILE_FOOTPRINT_MIN 0
#endif

#if (MICRODB_PROFILE_CORE_MIN + MICRODB_PROFILE_CORE_WAL + MICRODB_PROFILE_CORE_PERF + MICRODB_PROFILE_CORE_HIMEM + MICRODB_PROFILE_FOOTPRINT_MIN) > 1
#error "Only one MICRODB_PROFILE_* profile may be enabled"
#endif
#if (MICRODB_PROFILE_CORE_MIN + MICRODB_PROFILE_CORE_WAL + MICRODB_PROFILE_CORE_PERF + MICRODB_PROFILE_CORE_HIMEM + MICRODB_PROFILE_FOOTPRINT_MIN) == 0
#undef MICRODB_PROFILE_CORE_WAL
#define MICRODB_PROFILE_CORE_WAL 1
#endif

#if MICRODB_PROFILE_FOOTPRINT_MIN
#ifndef MICRODB_RAM_KB
#define MICRODB_RAM_KB 8u
#endif
#ifndef MICRODB_ENABLE_KV
#define MICRODB_ENABLE_KV 1
#endif
#ifndef MICRODB_ENABLE_TS
#define MICRODB_ENABLE_TS 0
#endif
#ifndef MICRODB_ENABLE_REL
#define MICRODB_ENABLE_REL 0
#endif
#ifndef MICRODB_ENABLE_WAL
#define MICRODB_ENABLE_WAL 1
#endif
#ifndef MICRODB_KV_MAX_KEYS
#define MICRODB_KV_MAX_KEYS 16u
#endif
#ifndef MICRODB_TXN_STAGE_KEYS
#define MICRODB_TXN_STAGE_KEYS 2u
#endif
#ifndef MICRODB_KV_KEY_MAX_LEN
#define MICRODB_KV_KEY_MAX_LEN 16u
#endif
#ifndef MICRODB_KV_VAL_MAX_LEN
#define MICRODB_KV_VAL_MAX_LEN 64u
#endif
#ifndef MICRODB_TS_MAX_STREAMS
#define MICRODB_TS_MAX_STREAMS 1u
#endif
#ifndef MICRODB_REL_MAX_TABLES
#define MICRODB_REL_MAX_TABLES 1u
#endif
#ifndef MICRODB_REL_MAX_COLS
#define MICRODB_REL_MAX_COLS 1u
#endif
#endif

#if MICRODB_PROFILE_CORE_MIN
#ifndef MICRODB_RAM_KB
#define MICRODB_RAM_KB 32u
#endif
#ifndef MICRODB_KV_MAX_KEYS
#define MICRODB_KV_MAX_KEYS 48u
#endif
#ifndef MICRODB_TS_MAX_STREAMS
#define MICRODB_TS_MAX_STREAMS 4u
#endif
#ifndef MICRODB_REL_MAX_TABLES
#define MICRODB_REL_MAX_TABLES 2u
#endif
#ifndef MICRODB_REL_MAX_COLS
#define MICRODB_REL_MAX_COLS 8u
#endif
#endif

#if MICRODB_PROFILE_CORE_WAL
#ifndef MICRODB_RAM_KB
#define MICRODB_RAM_KB 32u
#endif
#ifndef MICRODB_KV_MAX_KEYS
#define MICRODB_KV_MAX_KEYS 64u
#endif
#endif

#if MICRODB_PROFILE_CORE_PERF
#ifndef MICRODB_RAM_KB
#define MICRODB_RAM_KB 64u
#endif
#ifndef MICRODB_KV_MAX_KEYS
#define MICRODB_KV_MAX_KEYS 128u
#endif
#endif

#if MICRODB_PROFILE_CORE_HIMEM
#ifndef MICRODB_RAM_KB
#define MICRODB_RAM_KB 128u
#endif
#ifndef MICRODB_KV_MAX_KEYS
#define MICRODB_KV_MAX_KEYS 256u
#endif
#endif

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
/* Transaction staging reserves this many KV slots from kv_arena at init time. */
#ifndef MICRODB_TXN_STAGE_KEYS
#define MICRODB_TXN_STAGE_KEYS 8u
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
#ifndef MICRODB_THREAD_SAFE
#define MICRODB_THREAD_SAFE 0
#endif

/* Debug logging
 * Define MICRODB_LOG before #include "microdb.h" to enable internal logging.
 * Default is a no-op with zero production overhead.
 *
 * Example (printf):
 *   #define MICRODB_LOG(level, fmt, ...) \
 *       printf("[microdb][%s] " fmt "\n", level, ##__VA_ARGS__)
 *
 * Example (ESP-IDF):
 *   #define MICRODB_LOG(level, fmt, ...) \
 *       ESP_LOGI("microdb", "[%s] " fmt, level, ##__VA_ARGS__)
 */
#ifndef MICRODB_LOG
#define MICRODB_LOG(level, fmt, ...) ((void)0)
#endif

/* Optional platform I/O hooks for aligned/DMA-friendly integrations.
 * Hooks must not change persistence semantics; defaults are strict no-op.
 */
#ifndef MICRODB_IO_BEFORE_READ
#define MICRODB_IO_BEFORE_READ(offset, len) ((void)(offset), (void)(len))
#endif
#ifndef MICRODB_IO_AFTER_READ
#define MICRODB_IO_AFTER_READ(offset, len, rc) ((void)(offset), (void)(len), (void)(rc))
#endif
#ifndef MICRODB_IO_BEFORE_WRITE
#define MICRODB_IO_BEFORE_WRITE(offset, len) ((void)(offset), (void)(len))
#endif
#ifndef MICRODB_IO_AFTER_WRITE
#define MICRODB_IO_AFTER_WRITE(offset, len, rc) ((void)(offset), (void)(len), (void)(rc))
#endif
#ifndef MICRODB_IO_BEFORE_ERASE
#define MICRODB_IO_BEFORE_ERASE(offset, len) ((void)(offset), (void)(len))
#endif
#ifndef MICRODB_IO_AFTER_ERASE
#define MICRODB_IO_AFTER_ERASE(offset, len, rc) ((void)(offset), (void)(len), (void)(rc))
#endif
#ifndef MICRODB_IO_BEFORE_SYNC
#define MICRODB_IO_BEFORE_SYNC() ((void)0)
#endif
#ifndef MICRODB_IO_AFTER_SYNC
#define MICRODB_IO_AFTER_SYNC(rc) ((void)(rc))
#endif

#define MICRODB_STATIC_ASSERT(name, expr) typedef char microdb_static_assert_##name[(expr) ? 1 : -1]

MICRODB_STATIC_ASSERT(ram_pct_sum, (MICRODB_RAM_KV_PCT + MICRODB_RAM_TS_PCT + MICRODB_RAM_REL_PCT) == 100u);
MICRODB_STATIC_ASSERT(ram_kb_min, MICRODB_RAM_KB >= 8u);
MICRODB_STATIC_ASSERT(ram_kb_max, MICRODB_RAM_KB <= 4096u);
MICRODB_STATIC_ASSERT(txn_stage_lt_kv_keys, MICRODB_TXN_STAGE_KEYS < MICRODB_KV_MAX_KEYS);

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
    uint16_t schema_version;
    uintptr_t _align;
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
    MICRODB_ERR_SCHEMA = -12,
    MICRODB_ERR_TXN_ACTIVE = -13
} microdb_err_t;

/* Returns a stable symbolic name for a microdb error code.
 * Unknown values return "MICRODB_ERR_UNKNOWN".
 */
const char *microdb_err_to_string(microdb_err_t err);

typedef struct {
    /* Legacy aggregate stats (kept for backward compatibility). */
    uint32_t kv_entries_used;
    uint32_t kv_entries_max;
    uint8_t kv_fill_pct;
    uint32_t kv_collision_count;
    uint32_t kv_eviction_count;
    uint32_t ts_streams_registered;
    uint32_t ts_samples_total;
    uint8_t ts_fill_pct;
    uint32_t wal_bytes_used;
    uint32_t wal_bytes_total;
    uint8_t wal_fill_pct;
    uint32_t rel_tables_count;
    uint32_t rel_rows_total;
} microdb_stats_t;

typedef struct {
    uint32_t effective_capacity_bytes;
    uint32_t wal_bytes_total;
    uint32_t wal_bytes_used;
    uint8_t wal_fill_pct;
    /* Runtime-only counters; reset on each successful microdb_init. */
    uint32_t compact_count;
    uint32_t reopen_count;
    uint32_t recovery_count;
    /* Sticky last non-OK runtime operation status since init. */
    microdb_err_t last_runtime_error;
    /* Last status produced by open/recovery path in current process lifetime. */
    microdb_err_t last_recovery_status;
    uint32_t active_generation;
    uint32_t active_bank;
} microdb_db_stats_t;

typedef struct {
    uint32_t live_keys;
    uint32_t collisions;
    uint32_t evictions;
    uint32_t tombstones;
    uint32_t value_bytes_used;
    uint8_t fill_pct;
} microdb_kv_stats_t;

typedef struct {
    uint32_t stream_count;
    uint32_t retained_samples;
    uint32_t dropped_samples;
    uint8_t fill_pct;
} microdb_ts_stats_t;

typedef struct {
    uint32_t table_count;
    uint32_t rows_live;
    uint32_t rows_free;
    uint32_t indexed_tables;
    uint32_t index_entries;
} microdb_rel_stats_t;

typedef struct {
    uint32_t kv_entries_usable;
    uint32_t kv_entries_free;
    uint32_t kv_value_bytes_usable;
    uint32_t kv_value_bytes_free_now;
    uint32_t ts_samples_usable;
    uint32_t ts_samples_retained;
    uint32_t ts_samples_free;
    uint32_t wal_budget_total;
    uint32_t wal_budget_used;
    uint32_t wal_budget_free;
    uint32_t wal_safety_reserved;
    uint32_t compact_threshold_pct;
    uint32_t limiting_flags;
} microdb_effective_capacity_t;

typedef struct {
    uint8_t kv_fill_pct;
    uint8_t ts_fill_pct;
    uint8_t rel_fill_pct;
    uint8_t wal_fill_pct;
    uint8_t compact_pressure_pct;
    uint8_t near_full_risk_pct;
    uint32_t risk_flags;
} microdb_pressure_t;

#define MICRODB_CAP_LIMIT_NONE 0u
#define MICRODB_CAP_LIMIT_KV_ENTRIES (1u << 0)
#define MICRODB_CAP_LIMIT_KV_VALUE_BYTES (1u << 1)
#define MICRODB_CAP_LIMIT_TS_SAMPLES (1u << 2)
#define MICRODB_CAP_LIMIT_WAL_BUDGET (1u << 3)
#define MICRODB_CAP_LIMIT_STORAGE_DISABLED (1u << 4)

typedef struct {
    microdb_err_t status;
    uint8_t would_compact;
    uint8_t would_degrade;
    uint8_t deterministic_budget_ok;
    uint32_t required_bytes;
    uint32_t available_bytes;
    uint32_t required_wal_bytes;
    uint32_t wal_bytes_free;
} microdb_admission_t;

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
    /* Storage contract (validated at microdb_init):
     * - erase_size must be > 0
     * - write_size must be exactly 1 in current releases
     *   (write_size > 1 is not yet supported and fails fast with MICRODB_ERR_INVALID)
     */
    uint32_t erase_size;
    uint32_t write_size;
    void *ctx;
} microdb_storage_t;

#define MICRODB_WAL_SYNC_ALWAYS 0u
#define MICRODB_WAL_SYNC_FLUSH_ONLY 1u

typedef struct {
    microdb_storage_t *storage;
    uint32_t ram_kb;
    microdb_timestamp_t (*now)(void);
    uint8_t kv_pct;
    uint8_t ts_pct;
    uint8_t rel_pct;
    void *(*lock_create)(void);
    void (*lock)(void *hdl);
    void (*unlock)(void *hdl);
    void (*lock_destroy)(void *hdl);
    uint8_t wal_compact_auto;
    uint8_t wal_compact_threshold_pct;
    uint8_t wal_sync_mode;
    microdb_err_t (*on_migrate)(microdb_t *db, const char *table_name, uint16_t old_version, uint16_t new_version);
} microdb_cfg_t;

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
microdb_err_t microdb_inspect(microdb_t *db, microdb_stats_t *out);
microdb_err_t microdb_get_db_stats(microdb_t *db, microdb_db_stats_t *out);
microdb_err_t microdb_get_kv_stats(microdb_t *db, microdb_kv_stats_t *out);
microdb_err_t microdb_get_ts_stats(microdb_t *db, microdb_ts_stats_t *out);
microdb_err_t microdb_get_rel_stats(microdb_t *db, microdb_rel_stats_t *out);
microdb_err_t microdb_get_effective_capacity(microdb_t *db, microdb_effective_capacity_t *out);
microdb_err_t microdb_get_pressure(microdb_t *db, microdb_pressure_t *out);
microdb_err_t microdb_admit_kv_set(microdb_t *db, const char *key, size_t val_len, microdb_admission_t *out);
microdb_err_t microdb_admit_ts_insert(microdb_t *db, const char *stream_name, size_t sample_len, microdb_admission_t *out);
microdb_err_t microdb_admit_rel_insert(microdb_t *db, const char *table_name, size_t row_len, microdb_admission_t *out);
microdb_err_t microdb_compact(microdb_t *db);

typedef bool (*microdb_kv_iter_cb_t)(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx);
microdb_err_t microdb_kv_set(microdb_t *db, const char *key, const void *val, size_t len, uint32_t ttl);
microdb_err_t microdb_kv_get(microdb_t *db, const char *key, void *buf, size_t buf_len, size_t *out_len);
microdb_err_t microdb_kv_del(microdb_t *db, const char *key);
microdb_err_t microdb_kv_exists(microdb_t *db, const char *key);
microdb_err_t microdb_kv_iter(microdb_t *db, microdb_kv_iter_cb_t cb, void *ctx);
microdb_err_t microdb_kv_purge_expired(microdb_t *db);
microdb_err_t microdb_kv_clear(microdb_t *db);
#define microdb_kv_put(db, key, val, len) microdb_kv_set((db), (key), (val), (len), 0u)
microdb_err_t microdb_txn_begin(microdb_t *db);
microdb_err_t microdb_txn_commit(microdb_t *db);
microdb_err_t microdb_txn_rollback(microdb_t *db);

typedef bool (*microdb_ts_query_cb_t)(const microdb_ts_sample_t *sample, void *ctx);
microdb_err_t microdb_ts_register(microdb_t *db, const char *name, microdb_ts_type_t type, size_t raw_size);
microdb_err_t microdb_ts_insert(microdb_t *db, const char *name, microdb_timestamp_t ts, const void *val);
microdb_err_t microdb_ts_last(microdb_t *db, const char *name, microdb_ts_sample_t *out);
microdb_err_t microdb_ts_query(microdb_t *db, const char *name, microdb_timestamp_t from, microdb_timestamp_t to, microdb_ts_query_cb_t cb, void *ctx);
microdb_err_t microdb_ts_query_buf(microdb_t *db, const char *name, microdb_timestamp_t from, microdb_timestamp_t to, microdb_ts_sample_t *buf, size_t max_count, size_t *out_count);
microdb_err_t microdb_ts_count(microdb_t *db, const char *name, microdb_timestamp_t from, microdb_timestamp_t to, size_t *out_count);
microdb_err_t microdb_ts_clear(microdb_t *db, const char *name);

typedef bool (*microdb_rel_iter_cb_t)(const void *row_buf, void *ctx);
microdb_err_t microdb_schema_init(microdb_schema_t *schema, const char *name, uint32_t max_rows);
microdb_err_t microdb_schema_add(microdb_schema_t *schema, const char *col_name, microdb_col_type_t type, size_t size, bool is_index);
microdb_err_t microdb_schema_seal(microdb_schema_t *schema);
microdb_err_t microdb_table_create(microdb_t *db, microdb_schema_t *schema);
microdb_err_t microdb_table_get(microdb_t *db, const char *name, microdb_table_t **out_table);
/* Pure metadata helper; no db handle, no internal DB lock. */
size_t microdb_table_row_size(const microdb_table_t *table);
/* Row buffer formatter/parser helpers; no db handle, no internal DB lock. */
microdb_err_t microdb_row_set(const microdb_table_t *table, void *row_buf, const char *col_name, const void *val);
microdb_err_t microdb_row_get(const microdb_table_t *table, const void *row_buf, const char *col_name, void *out, size_t *out_len);
microdb_err_t microdb_rel_insert(microdb_t *db, microdb_table_t *table, const void *row_buf);
microdb_err_t microdb_rel_find(microdb_t *db, microdb_table_t *table, const void *search_val, microdb_rel_iter_cb_t cb, void *ctx);
microdb_err_t microdb_rel_find_by(microdb_t *db, microdb_table_t *table, const char *col_name, const void *search_val, void *out_buf);
microdb_err_t microdb_rel_delete(microdb_t *db, microdb_table_t *table, const void *search_val, uint32_t *out_deleted);
microdb_err_t microdb_rel_iter(microdb_t *db, microdb_table_t *table, microdb_rel_iter_cb_t cb, void *ctx);
/* Table metadata query helper; no db handle, no internal DB lock. */
microdb_err_t microdb_rel_count(const microdb_table_t *table, uint32_t *out_count);
microdb_err_t microdb_rel_clear(microdb_t *db, microdb_table_t *table);

#endif
