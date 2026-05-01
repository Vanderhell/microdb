// SPDX-License-Identifier: MIT
#ifndef LOX_H
#define LOX_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef LOX_PROFILE_CORE_MIN
#define LOX_PROFILE_CORE_MIN 0
#endif
#ifndef LOX_PROFILE_CORE_WAL
#define LOX_PROFILE_CORE_WAL 0
#endif
#ifndef LOX_PROFILE_CORE_PERF
#define LOX_PROFILE_CORE_PERF 0
#endif
#ifndef LOX_PROFILE_CORE_HIMEM
#define LOX_PROFILE_CORE_HIMEM 0
#endif
#ifndef LOX_PROFILE_FOOTPRINT_MIN
#define LOX_PROFILE_FOOTPRINT_MIN 0
#endif

#if (LOX_PROFILE_CORE_MIN + LOX_PROFILE_CORE_WAL + LOX_PROFILE_CORE_PERF + LOX_PROFILE_CORE_HIMEM + LOX_PROFILE_FOOTPRINT_MIN) > 1
#error "Only one LOX_PROFILE_* profile may be enabled"
#endif
#if (LOX_PROFILE_CORE_MIN + LOX_PROFILE_CORE_WAL + LOX_PROFILE_CORE_PERF + LOX_PROFILE_CORE_HIMEM + LOX_PROFILE_FOOTPRINT_MIN) == 0
#undef LOX_PROFILE_CORE_WAL
#define LOX_PROFILE_CORE_WAL 1
#endif

#if LOX_PROFILE_FOOTPRINT_MIN
#ifndef LOX_RAM_KB
#define LOX_RAM_KB 8u
#endif
#ifndef LOX_ENABLE_KV
#define LOX_ENABLE_KV 1
#endif
#ifndef LOX_ENABLE_TS
#define LOX_ENABLE_TS 0
#endif
#ifndef LOX_ENABLE_REL
#define LOX_ENABLE_REL 0
#endif
#ifndef LOX_ENABLE_WAL
#define LOX_ENABLE_WAL 1
#endif
#ifndef LOX_KV_MAX_KEYS
#define LOX_KV_MAX_KEYS 16u
#endif
#ifndef LOX_TXN_STAGE_KEYS
#define LOX_TXN_STAGE_KEYS 2u
#endif
#ifndef LOX_KV_KEY_MAX_LEN
#define LOX_KV_KEY_MAX_LEN 16u
#endif
#ifndef LOX_KV_VAL_MAX_LEN
#define LOX_KV_VAL_MAX_LEN 64u
#endif
#ifndef LOX_TS_MAX_STREAMS
#define LOX_TS_MAX_STREAMS 1u
#endif
#ifndef LOX_REL_MAX_TABLES
#define LOX_REL_MAX_TABLES 1u
#endif
#ifndef LOX_REL_MAX_COLS
#define LOX_REL_MAX_COLS 1u
#endif
#endif

#if LOX_PROFILE_CORE_MIN
#ifndef LOX_RAM_KB
#define LOX_RAM_KB 32u
#endif
#ifndef LOX_KV_MAX_KEYS
#define LOX_KV_MAX_KEYS 48u
#endif
#ifndef LOX_TS_MAX_STREAMS
#define LOX_TS_MAX_STREAMS 4u
#endif
#ifndef LOX_REL_MAX_TABLES
#define LOX_REL_MAX_TABLES 2u
#endif
#ifndef LOX_REL_MAX_COLS
#define LOX_REL_MAX_COLS 8u
#endif
#endif

#if LOX_PROFILE_CORE_WAL
#ifndef LOX_RAM_KB
#define LOX_RAM_KB 32u
#endif
#ifndef LOX_KV_MAX_KEYS
#define LOX_KV_MAX_KEYS 64u
#endif
#endif

#if LOX_PROFILE_CORE_PERF
#ifndef LOX_RAM_KB
#define LOX_RAM_KB 64u
#endif
#ifndef LOX_KV_MAX_KEYS
#define LOX_KV_MAX_KEYS 128u
#endif
#endif

#if LOX_PROFILE_CORE_HIMEM
#ifndef LOX_RAM_KB
#define LOX_RAM_KB 128u
#endif
#ifndef LOX_KV_MAX_KEYS
#define LOX_KV_MAX_KEYS 256u
#endif
#endif

#ifndef LOX_RAM_KB
#define LOX_RAM_KB 32u
#endif
#ifndef LOX_ENABLE_KV
#define LOX_ENABLE_KV 1
#endif
#ifndef LOX_ENABLE_TS
#define LOX_ENABLE_TS 1
#endif
#ifndef LOX_ENABLE_REL
#define LOX_ENABLE_REL 1
#endif
#ifndef LOX_RAM_KV_PCT
#define LOX_RAM_KV_PCT 40u
#endif
#ifndef LOX_RAM_TS_PCT
#define LOX_RAM_TS_PCT 40u
#endif
#ifndef LOX_RAM_REL_PCT
#define LOX_RAM_REL_PCT 20u
#endif
#ifndef LOX_KV_MAX_KEYS
#define LOX_KV_MAX_KEYS 64u
#endif
#ifndef LOX_KV_KEY_MAX_LEN
#define LOX_KV_KEY_MAX_LEN 32u
#endif
#ifndef LOX_KV_VAL_MAX_LEN
#define LOX_KV_VAL_MAX_LEN 128u
#endif
/* Transaction staging reserves this many KV slots from kv_arena at init time. */
#ifndef LOX_TXN_STAGE_KEYS
#define LOX_TXN_STAGE_KEYS 8u
#endif
#ifndef LOX_KV_ENABLE_TTL
#define LOX_KV_ENABLE_TTL 1
#endif
#define LOX_KV_POLICY_OVERWRITE 0u
#define LOX_KV_POLICY_REJECT 1u
#ifndef LOX_KV_OVERFLOW_POLICY
#define LOX_KV_OVERFLOW_POLICY LOX_KV_POLICY_OVERWRITE
#endif
#ifndef LOX_TS_MAX_STREAMS
#define LOX_TS_MAX_STREAMS 8u
#endif
#ifndef LOX_TS_STREAM_NAME_LEN
#define LOX_TS_STREAM_NAME_LEN 16u
#endif
#ifndef LOX_TS_RAW_MAX
#define LOX_TS_RAW_MAX 16u
#endif
#define LOX_TS_POLICY_DROP_OLDEST 0u
#define LOX_TS_POLICY_REJECT 1u
#define LOX_TS_POLICY_DOWNSAMPLE 2u
#define LOX_TS_POLICY_LOG_RETAIN 3u
#ifndef LOX_TS_OVERFLOW_POLICY
#define LOX_TS_OVERFLOW_POLICY LOX_TS_POLICY_DROP_OLDEST
#endif
#ifndef LOX_REL_MAX_TABLES
#define LOX_REL_MAX_TABLES 4u
#endif
#ifndef LOX_REL_MAX_COLS
#define LOX_REL_MAX_COLS 16u
#endif
#ifndef LOX_REL_COL_NAME_LEN
#define LOX_REL_COL_NAME_LEN 16u
#endif
#ifndef LOX_REL_TABLE_NAME_LEN
#define LOX_REL_TABLE_NAME_LEN 16u
#endif
#ifndef LOX_ENABLE_WAL
#define LOX_ENABLE_WAL 1
#endif
#ifndef LOX_TIMESTAMP_TYPE
#define LOX_TIMESTAMP_TYPE uint32_t
#endif
#ifndef LOX_THREAD_SAFE
#define LOX_THREAD_SAFE 0
#endif

/* Debug logging
 * Define LOX_LOG before #include "lox.h" to enable internal logging.
 * Default is a no-op with zero production overhead.
 *
 * Example (printf):
 *   #define LOX_LOG(level, fmt, ...) \
 *       printf("[loxdb][%s] " fmt "\n", level, ##__VA_ARGS__)
 *
 * Example (ESP-IDF):
 *   #define LOX_LOG(level, fmt, ...) \
 *       ESP_LOGI("loxdb", "[%s] " fmt, level, ##__VA_ARGS__)
 */
#ifndef LOX_LOG
#define LOX_LOG(level, fmt, ...) ((void)0)
#endif

/* Optional platform I/O hooks for aligned/DMA-friendly integrations.
 * Hooks must not change persistence semantics; defaults are strict no-op.
 */
#ifndef LOX_IO_BEFORE_READ
#define LOX_IO_BEFORE_READ(offset, len) ((void)(offset), (void)(len))
#endif
#ifndef LOX_IO_AFTER_READ
#define LOX_IO_AFTER_READ(offset, len, rc) ((void)(offset), (void)(len), (void)(rc))
#endif
#ifndef LOX_IO_BEFORE_WRITE
#define LOX_IO_BEFORE_WRITE(offset, len) ((void)(offset), (void)(len))
#endif
#ifndef LOX_IO_AFTER_WRITE
#define LOX_IO_AFTER_WRITE(offset, len, rc) ((void)(offset), (void)(len), (void)(rc))
#endif
#ifndef LOX_IO_BEFORE_ERASE
#define LOX_IO_BEFORE_ERASE(offset, len) ((void)(offset), (void)(len))
#endif
#ifndef LOX_IO_AFTER_ERASE
#define LOX_IO_AFTER_ERASE(offset, len, rc) ((void)(offset), (void)(len), (void)(rc))
#endif
#ifndef LOX_IO_BEFORE_SYNC
#define LOX_IO_BEFORE_SYNC() ((void)0)
#endif
#ifndef LOX_IO_AFTER_SYNC
#define LOX_IO_AFTER_SYNC(rc) ((void)(rc))
#endif

#define LOX_STATIC_ASSERT(name, expr) typedef char lox_static_assert_##name[(expr) ? 1 : -1]

LOX_STATIC_ASSERT(ram_pct_sum, (LOX_RAM_KV_PCT + LOX_RAM_TS_PCT + LOX_RAM_REL_PCT) == 100u);
LOX_STATIC_ASSERT(ram_kb_min, LOX_RAM_KB >= 8u);
LOX_STATIC_ASSERT(ram_kb_max, LOX_RAM_KB <= 4096u);
LOX_STATIC_ASSERT(txn_stage_lt_kv_keys, LOX_TXN_STAGE_KEYS < LOX_KV_MAX_KEYS);

typedef LOX_TIMESTAMP_TYPE lox_timestamp_t;

#ifndef LOX_HANDLE_SIZE
#define LOX_HANDLE_SIZE 8192u
#endif
#ifndef LOX_SCHEMA_SIZE
#define LOX_SCHEMA_SIZE 880u
#endif
#ifndef LOX_REL_INDEX_KEY_MAX
#define LOX_REL_INDEX_KEY_MAX 16u
#endif

typedef struct {
    uint8_t _opaque[LOX_HANDLE_SIZE];
} lox_t;

typedef struct {
    uint16_t schema_version;
    uintptr_t _align;
    uint8_t _opaque[LOX_SCHEMA_SIZE];
} lox_schema_t;

typedef struct lox_table_s lox_table_t;

typedef enum {
    LOX_OK = 0,
    LOX_ERR_INVALID = -1,
    LOX_ERR_NO_MEM = -2,
    LOX_ERR_FULL = -3,
    LOX_ERR_NOT_FOUND = -4,
    LOX_ERR_EXPIRED = -5,
    LOX_ERR_STORAGE = -6,
    LOX_ERR_CORRUPT = -7,
    LOX_ERR_SEALED = -8,
    LOX_ERR_EXISTS = -9,
    LOX_ERR_DISABLED = -10,
    LOX_ERR_OVERFLOW = -11,
    LOX_ERR_SCHEMA = -12,
    LOX_ERR_TXN_ACTIVE = -13,
    LOX_ERR_MODIFIED = -14
} lox_err_t;

/* Returns a stable symbolic name for a loxdb error code.
 * Unknown values return "LOX_ERR_UNKNOWN".
 */
const char *lox_err_to_string(lox_err_t err);

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
} lox_stats_t;

typedef struct {
    uint32_t effective_capacity_bytes;
    uint32_t wal_bytes_total;
    uint32_t wal_bytes_used;
    uint8_t wal_fill_pct;
    /* Runtime-only counters; reset on each successful lox_init. */
    uint32_t compact_count;
    uint32_t reopen_count;
    uint32_t recovery_count;
    /* Sticky last non-OK runtime operation status since init. */
    lox_err_t last_runtime_error;
    /* Last status produced by open/recovery path in current process lifetime. */
    lox_err_t last_recovery_status;
    uint32_t active_generation;
    uint32_t active_bank;
} lox_db_stats_t;

typedef struct {
    uint32_t live_keys;
    uint32_t collisions;
    uint32_t evictions;
    uint32_t tombstones;
    uint32_t value_bytes_used;
    uint8_t fill_pct;
} lox_kv_stats_t;

typedef struct {
    uint32_t stream_count;
    uint32_t retained_samples;
    uint32_t dropped_samples;
    uint8_t fill_pct;
} lox_ts_stats_t;

typedef struct {
    uint32_t table_count;
    uint32_t rows_live;
    uint32_t rows_free;
    uint32_t indexed_tables;
    uint32_t index_entries;
} lox_rel_stats_t;

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
} lox_effective_capacity_t;

typedef struct {
    uint8_t kv_fill_pct;
    uint8_t ts_fill_pct;
    uint8_t rel_fill_pct;
    uint8_t wal_fill_pct;
    uint8_t compact_pressure_pct;
    uint8_t near_full_risk_pct;
    uint32_t risk_flags;
} lox_pressure_t;

#define LOX_CAP_LIMIT_NONE 0u
#define LOX_CAP_LIMIT_KV_ENTRIES (1u << 0)
#define LOX_CAP_LIMIT_KV_VALUE_BYTES (1u << 1)
#define LOX_CAP_LIMIT_TS_SAMPLES (1u << 2)
#define LOX_CAP_LIMIT_WAL_BUDGET (1u << 3)
#define LOX_CAP_LIMIT_STORAGE_DISABLED (1u << 4)

typedef struct {
    lox_err_t status;
    uint8_t would_compact;
    uint8_t would_degrade;
    uint8_t deterministic_budget_ok;
    uint32_t required_bytes;
    uint32_t available_bytes;
    uint32_t required_wal_bytes;
    uint32_t wal_bytes_free;
} lox_admission_t;

typedef enum {
    LOX_TS_F32 = 0,
    LOX_TS_I32 = 1,
    LOX_TS_U32 = 2,
    LOX_TS_RAW = 3
} lox_ts_type_t;

typedef enum {
    LOX_COL_U8 = 0,
    LOX_COL_U16 = 1,
    LOX_COL_U32 = 2,
    LOX_COL_U64 = 3,
    LOX_COL_I8 = 4,
    LOX_COL_I16 = 5,
    LOX_COL_I32 = 6,
    LOX_COL_I64 = 7,
    LOX_COL_F32 = 8,
    LOX_COL_F64 = 9,
    LOX_COL_BOOL = 10,
    LOX_COL_STR = 11,
    LOX_COL_BLOB = 12
} lox_col_type_t;

typedef struct {
    lox_err_t (*read)(void *ctx, uint32_t offset, void *buf, size_t len);
    lox_err_t (*write)(void *ctx, uint32_t offset, const void *buf, size_t len);
    lox_err_t (*erase)(void *ctx, uint32_t offset);
    lox_err_t (*sync)(void *ctx);
    uint32_t capacity;
    /* Storage contract (validated at lox_init):
     * - erase_size must be > 0
     * - write_size must be exactly 1 in current releases
     *   (write_size > 1 is not yet supported and fails fast with LOX_ERR_INVALID)
     */
    uint32_t erase_size;
    uint32_t write_size;
    void *ctx;
} lox_storage_t;

#define LOX_WAL_SYNC_ALWAYS 0u
#define LOX_WAL_SYNC_FLUSH_ONLY 1u

typedef struct {
    lox_storage_t *storage;
    uint32_t ram_kb;
    lox_timestamp_t (*now)(void);
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
    lox_err_t (*on_migrate)(lox_t *db, const char *table_name, uint16_t old_version, uint16_t new_version);
} lox_cfg_t;

typedef struct {
    lox_err_t status;
    uint32_t ram_kb;
    uint8_t kv_pct;
    uint8_t ts_pct;
    uint8_t rel_pct;
    uint32_t heap_total_bytes;
    uint32_t kv_arena_bytes;
    uint32_t ts_arena_bytes;
    uint32_t rel_arena_bytes;
    uint32_t wal_enabled;
    uint32_t storage_required_bytes;
    uint32_t storage_capacity_bytes;
    uint32_t storage_erase_size;
    uint32_t storage_write_size;
    uint32_t wal_size;
    uint32_t bank_size;
    uint32_t kv_snapshot_bytes;
    uint32_t ts_snapshot_bytes;
    uint32_t rel_snapshot_bytes;
} lox_preflight_report_t;

typedef struct {
    lox_timestamp_t ts;
    union {
        float f32;
        int32_t i32;
        uint32_t u32;
        uint8_t raw[LOX_TS_RAW_MAX];
    } v;
} lox_ts_sample_t;

typedef struct {
    uint8_t kv_ok;
    uint8_t ts_ok;
    uint8_t rel_ok;
    uint8_t wal_ok;
    uint32_t kv_anomalies;
    uint32_t ts_anomalies;
    uint32_t rel_anomalies;
    char first_anomaly[64];
} lox_selfcheck_result_t;

typedef struct {
    uint8_t log_retain_zones;
    uint8_t log_retain_zone_pct;
} lox_ts_log_retain_cfg_t;

lox_err_t lox_init(lox_t *db, const lox_cfg_t *cfg);
lox_err_t lox_preflight(const lox_cfg_t *cfg, lox_preflight_report_t *out);
lox_err_t lox_deinit(lox_t *db);
lox_err_t lox_flush(lox_t *db);
lox_err_t lox_stats(const lox_t *db, lox_stats_t *out);
lox_err_t lox_inspect(lox_t *db, lox_stats_t *out);
lox_err_t lox_get_db_stats(lox_t *db, lox_db_stats_t *out);
lox_err_t lox_get_kv_stats(lox_t *db, lox_kv_stats_t *out);
lox_err_t lox_get_ts_stats(lox_t *db, lox_ts_stats_t *out);
lox_err_t lox_get_rel_stats(lox_t *db, lox_rel_stats_t *out);
lox_err_t lox_get_effective_capacity(lox_t *db, lox_effective_capacity_t *out);
lox_err_t lox_get_pressure(lox_t *db, lox_pressure_t *out);
lox_err_t lox_selfcheck(lox_t *db, lox_selfcheck_result_t *out);
lox_err_t lox_admit_kv_set(lox_t *db, const char *key, size_t val_len, lox_admission_t *out);
lox_err_t lox_admit_ts_insert(lox_t *db, const char *stream_name, size_t sample_len, lox_admission_t *out);
lox_err_t lox_admit_rel_insert(lox_t *db, const char *table_name, size_t row_len, lox_admission_t *out);
lox_err_t lox_compact(lox_t *db);

typedef bool (*lox_kv_iter_cb_t)(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx);
lox_err_t lox_kv_set(lox_t *db, const char *key, const void *val, size_t len, uint32_t ttl);
lox_err_t lox_kv_get(lox_t *db, const char *key, void *buf, size_t buf_len, size_t *out_len);
lox_err_t lox_kv_del(lox_t *db, const char *key);
lox_err_t lox_kv_exists(lox_t *db, const char *key);
/* Iteration contract: callback executes without DB lock held.
 * Concurrent modifications during iteration are weakly-consistent:
 * keys added/removed during the walk may or may not be observed.
 * For stronger persistence ordering, use LOX_WAL_SYNC_ALWAYS and
 * call lox_flush() before iterating.
 */
lox_err_t lox_kv_iter(lox_t *db, lox_kv_iter_cb_t cb, void *ctx);
lox_err_t lox_kv_purge_expired(lox_t *db);
lox_err_t lox_kv_clear(lox_t *db);
#define lox_kv_put(db, key, val, len) lox_kv_set((db), (key), (val), (len), 0u)
lox_err_t lox_txn_begin(lox_t *db);
lox_err_t lox_txn_commit(lox_t *db);
lox_err_t lox_txn_rollback(lox_t *db);

typedef bool (*lox_ts_query_cb_t)(const lox_ts_sample_t *sample, void *ctx);
lox_err_t lox_ts_register(lox_t *db, const char *name, lox_ts_type_t type, size_t raw_size);
lox_err_t lox_ts_register_ex(lox_t *db,
                                     const char *name,
                                     lox_ts_type_t type,
                                     size_t raw_size,
                                     const lox_ts_log_retain_cfg_t *cfg);
lox_err_t lox_ts_insert(lox_t *db, const char *name, lox_timestamp_t ts, const void *val);
lox_err_t lox_ts_last(lox_t *db, const char *name, lox_ts_sample_t *out);
lox_err_t lox_ts_query(lox_t *db, const char *name, lox_timestamp_t from, lox_timestamp_t to, lox_ts_query_cb_t cb, void *ctx);
lox_err_t lox_ts_query_buf(lox_t *db, const char *name, lox_timestamp_t from, lox_timestamp_t to, lox_ts_sample_t *buf, size_t max_count, size_t *out_count);
lox_err_t lox_ts_count(lox_t *db, const char *name, lox_timestamp_t from, lox_timestamp_t to, size_t *out_count);
lox_err_t lox_ts_clear(lox_t *db, const char *name);

typedef bool (*lox_rel_iter_cb_t)(const void *row_buf, void *ctx);
lox_err_t lox_schema_init(lox_schema_t *schema, const char *name, uint32_t max_rows);
lox_err_t lox_schema_add(lox_schema_t *schema, const char *col_name, lox_col_type_t type, size_t size, bool is_index);
lox_err_t lox_schema_seal(lox_schema_t *schema);
lox_err_t lox_table_create(lox_t *db, lox_schema_t *schema);
lox_err_t lox_table_get(lox_t *db, const char *name, lox_table_t **out_table);
/* Pure metadata helper; no db handle, no internal DB lock. */
size_t lox_table_row_size(const lox_table_t *table);
/* Row buffer formatter/parser helpers; no db handle, no internal DB lock. */
lox_err_t lox_row_set(const lox_table_t *table, void *row_buf, const char *col_name, const void *val);
lox_err_t lox_row_get(const lox_table_t *table, const void *row_buf, const char *col_name, void *out, size_t *out_len);
lox_err_t lox_rel_insert(lox_t *db, lox_table_t *table, const void *row_buf);
lox_err_t lox_rel_find(lox_t *db, lox_table_t *table, const void *search_val, lox_rel_iter_cb_t cb, void *ctx);
lox_err_t lox_rel_find_by(lox_t *db, lox_table_t *table, const char *col_name, const void *search_val, void *out_buf);
lox_err_t lox_rel_delete(lox_t *db, lox_table_t *table, const void *search_val, uint32_t *out_deleted);
lox_err_t lox_rel_iter(lox_t *db, lox_table_t *table, lox_rel_iter_cb_t cb, void *ctx);
/* Table metadata query helper; no db handle, no internal DB lock. */
lox_err_t lox_rel_count(const lox_table_t *table, uint32_t *out_count);
lox_err_t lox_rel_clear(lox_t *db, lox_table_t *table);

#endif
