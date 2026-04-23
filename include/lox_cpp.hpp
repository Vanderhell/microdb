// SPDX-License-Identifier: MIT
#ifndef LOX_CPP_HPP
#define LOX_CPP_HPP

#include <cstring>
#include <type_traits>

extern "C" {
#include "lox.h"
}

namespace loxdb {
namespace cpp {

inline const char *error_string(lox_err_t err) {
    return lox_err_to_string(err);
}

class Database final {
public:
    Database() = default;
    ~Database() {
        if (initialized_) {
            (void)lox_deinit(&db_);
        }
    }

    Database(const Database &) = delete;
    Database &operator=(const Database &) = delete;
    Database(Database &&) = delete;
    Database &operator=(Database &&) = delete;

    lox_err_t init(const lox_cfg_t &cfg) {
        lox_err_t rc;
        if (initialized_) {
            return LOX_ERR_INVALID;
        }
        rc = lox_init(&db_, &cfg);
        if (rc == LOX_OK) {
            initialized_ = true;
        }
        return rc;
    }

    lox_err_t deinit() {
        lox_err_t rc;
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        rc = lox_deinit(&db_);
        if (rc == LOX_OK) {
            initialized_ = false;
            std::memset(&db_, 0, sizeof(db_));
        }
        return rc;
    }

    bool initialized() const {
        return initialized_;
    }

    lox_t *handle() {
        return initialized_ ? &db_ : nullptr;
    }

    const lox_t *handle() const {
        return initialized_ ? &db_ : nullptr;
    }

    lox_err_t flush() {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_flush(&db_);
    }

    lox_err_t stats(lox_stats_t *out) const {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_stats(&db_, out);
    }

#ifdef LOX_CAP_LIMIT_NONE
    lox_err_t db_stats(lox_db_stats_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_get_db_stats(&db_, out);
    }

    lox_err_t kv_stats(lox_kv_stats_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_get_kv_stats(&db_, out);
    }

    lox_err_t ts_stats(lox_ts_stats_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_get_ts_stats(&db_, out);
    }

    lox_err_t rel_stats(lox_rel_stats_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_get_rel_stats(&db_, out);
    }

    lox_err_t effective_capacity(lox_effective_capacity_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_get_effective_capacity(&db_, out);
    }

    lox_err_t pressure(lox_pressure_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_get_pressure(&db_, out);
    }
#endif

    lox_err_t kv_set(const char *key, const void *val, size_t len, uint32_t ttl = 0u) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_kv_set(&db_, key, val, len, ttl);
    }

    lox_err_t kv_put(const char *key, const void *val, size_t len) {
        return kv_set(key, val, len, 0u);
    }

    lox_err_t kv_get(const char *key, void *buf, size_t buf_len, size_t *out_len = nullptr) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_kv_get(&db_, key, buf, buf_len, out_len);
    }

    lox_err_t kv_del(const char *key) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_kv_del(&db_, key);
    }

    lox_err_t kv_exists(const char *key) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_kv_exists(&db_, key);
    }

    lox_err_t kv_clear() {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_kv_clear(&db_);
    }

    lox_err_t kv_purge_expired() {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_kv_purge_expired(&db_);
    }

    lox_err_t kv_iter(lox_kv_iter_cb_t cb, void *ctx) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_kv_iter(&db_, cb, ctx);
    }

#ifdef LOX_CAP_LIMIT_NONE
    lox_err_t admit_kv_set(const char *key, size_t val_len, lox_admission_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_admit_kv_set(&db_, key, val_len, out);
    }
#endif

    template <typename T>
    lox_err_t kv_put_pod(const char *key, const T &value, uint32_t ttl = 0u) {
        static_assert(std::is_trivially_copyable<T>::value, "kv_put_pod requires trivially copyable T");
        return kv_set(key, &value, sizeof(T), ttl);
    }

    template <typename T>
    lox_err_t kv_get_pod(const char *key, T *out_value) {
        static_assert(std::is_trivially_copyable<T>::value, "kv_get_pod requires trivially copyable T");
        if (out_value == nullptr) {
            return LOX_ERR_INVALID;
        }
        return kv_get(key, out_value, sizeof(T), nullptr);
    }

    lox_err_t ts_register(const char *name, lox_ts_type_t type, size_t raw_size = 0u) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_ts_register(&db_, name, type, raw_size);
    }

    lox_err_t ts_insert(const char *name, lox_timestamp_t ts, const void *val) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_ts_insert(&db_, name, ts, val);
    }

    lox_err_t ts_last(const char *name, lox_ts_sample_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_ts_last(&db_, name, out);
    }

    lox_err_t ts_query(const char *name,
                           lox_timestamp_t from,
                           lox_timestamp_t to,
                           lox_ts_query_cb_t cb,
                           void *ctx) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_ts_query(&db_, name, from, to, cb, ctx);
    }

    lox_err_t ts_query_buf(const char *name,
                               lox_timestamp_t from,
                               lox_timestamp_t to,
                               lox_ts_sample_t *buf,
                               size_t max_count,
                               size_t *out_count) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_ts_query_buf(&db_, name, from, to, buf, max_count, out_count);
    }

    lox_err_t ts_count(const char *name, lox_timestamp_t from, lox_timestamp_t to, size_t *out_count) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_ts_count(&db_, name, from, to, out_count);
    }

    lox_err_t ts_clear(const char *name) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_ts_clear(&db_, name);
    }

#ifdef LOX_CAP_LIMIT_NONE
    lox_err_t admit_ts_insert(const char *stream_name, size_t sample_len, lox_admission_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_admit_ts_insert(&db_, stream_name, sample_len, out);
    }
#endif

    lox_err_t ts_register_f32(const char *name) {
        return ts_register(name, LOX_TS_F32, 0u);
    }

    lox_err_t ts_register_i32(const char *name) {
        return ts_register(name, LOX_TS_I32, 0u);
    }

    lox_err_t ts_register_u32(const char *name) {
        return ts_register(name, LOX_TS_U32, 0u);
    }

    lox_err_t ts_insert_f32(const char *name, lox_timestamp_t ts, float value) {
        return ts_insert(name, ts, &value);
    }

    lox_err_t ts_insert_i32(const char *name, lox_timestamp_t ts, int32_t value) {
        return ts_insert(name, ts, &value);
    }

    lox_err_t ts_insert_u32(const char *name, lox_timestamp_t ts, uint32_t value) {
        return ts_insert(name, ts, &value);
    }

    lox_err_t rel_schema_init(lox_schema_t *schema, const char *name, uint32_t max_rows) {
        return lox_schema_init(schema, name, max_rows);
    }

    lox_err_t rel_schema_add(lox_schema_t *schema,
                                 const char *col_name,
                                 lox_col_type_t type,
                                 size_t size,
                                 bool is_index) {
        return lox_schema_add(schema, col_name, type, size, is_index);
    }

    lox_err_t rel_schema_seal(lox_schema_t *schema) {
        return lox_schema_seal(schema);
    }

    lox_err_t rel_table_create(lox_schema_t *schema) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_table_create(&db_, schema);
    }

    lox_err_t rel_table_get(const char *name, lox_table_t **out_table) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_table_get(&db_, name, out_table);
    }

    size_t rel_table_row_size(const lox_table_t *table) {
        return lox_table_row_size(table);
    }

    lox_err_t rel_row_set(const lox_table_t *table, void *row_buf, const char *col_name, const void *val) {
        return lox_row_set(table, row_buf, col_name, val);
    }

    lox_err_t rel_row_get(const lox_table_t *table,
                              const void *row_buf,
                              const char *col_name,
                              void *out,
                              size_t *out_len) {
        return lox_row_get(table, row_buf, col_name, out, out_len);
    }

    template <typename T>
    lox_err_t rel_row_set_pod(const lox_table_t *table, void *row_buf, const char *col_name, const T &value) {
        static_assert(std::is_trivially_copyable<T>::value, "rel_row_set_pod requires trivially copyable T");
        return rel_row_set(table, row_buf, col_name, &value);
    }

    template <typename T>
    lox_err_t rel_row_get_pod(const lox_table_t *table, const void *row_buf, const char *col_name, T *out_value) {
        static_assert(std::is_trivially_copyable<T>::value, "rel_row_get_pod requires trivially copyable T");
        if (out_value == nullptr) {
            return LOX_ERR_INVALID;
        }
        return rel_row_get(table, row_buf, col_name, out_value, nullptr);
    }

    lox_err_t rel_insert(lox_table_t *table, const void *row_buf) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_rel_insert(&db_, table, row_buf);
    }

    lox_err_t rel_find(lox_table_t *table, const void *search_val, lox_rel_iter_cb_t cb, void *ctx) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_rel_find(&db_, table, search_val, cb, ctx);
    }

    lox_err_t rel_find_by(lox_table_t *table, const char *col_name, const void *search_val, void *out_buf) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_rel_find_by(&db_, table, col_name, search_val, out_buf);
    }

    lox_err_t rel_delete(lox_table_t *table, const void *search_val, uint32_t *out_deleted) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_rel_delete(&db_, table, search_val, out_deleted);
    }

    lox_err_t rel_iter(lox_table_t *table, lox_rel_iter_cb_t cb, void *ctx) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_rel_iter(&db_, table, cb, ctx);
    }

    lox_err_t rel_count(const lox_table_t *table, uint32_t *out_count) {
        return lox_rel_count(table, out_count);
    }

    lox_err_t rel_clear(lox_table_t *table) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_rel_clear(&db_, table);
    }

#ifdef LOX_CAP_LIMIT_NONE
    lox_err_t admit_rel_insert(const char *table_name, size_t row_len, lox_admission_t *out) {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_admit_rel_insert(&db_, table_name, row_len, out);
    }
#endif

    lox_err_t txn_begin() {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_txn_begin(&db_);
    }

    lox_err_t txn_commit() {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_txn_commit(&db_);
    }

    lox_err_t txn_rollback() {
        if (!initialized_) {
            return LOX_ERR_INVALID;
        }
        return lox_txn_rollback(&db_);
    }

private:
    lox_t db_ {};
    bool initialized_ = false;
};

}  // namespace cpp
}  // namespace loxdb

#endif
