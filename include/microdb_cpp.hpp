#ifndef MICRODB_CPP_HPP
#define MICRODB_CPP_HPP

#include <cstring>
#include <type_traits>

extern "C" {
#include "microdb.h"
}

namespace microdb {
namespace cpp {

class Database final {
public:
    Database() = default;
    ~Database() {
        if (initialized_) {
            (void)microdb_deinit(&db_);
        }
    }

    Database(const Database &) = delete;
    Database &operator=(const Database &) = delete;
    Database(Database &&) = delete;
    Database &operator=(Database &&) = delete;

    microdb_err_t init(const microdb_cfg_t &cfg) {
        microdb_err_t rc;
        if (initialized_) {
            return MICRODB_ERR_INVALID;
        }
        rc = microdb_init(&db_, &cfg);
        if (rc == MICRODB_OK) {
            initialized_ = true;
        }
        return rc;
    }

    microdb_err_t deinit() {
        microdb_err_t rc;
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        rc = microdb_deinit(&db_);
        if (rc == MICRODB_OK) {
            initialized_ = false;
            std::memset(&db_, 0, sizeof(db_));
        }
        return rc;
    }

    bool initialized() const {
        return initialized_;
    }

    microdb_t *handle() {
        return initialized_ ? &db_ : nullptr;
    }

    const microdb_t *handle() const {
        return initialized_ ? &db_ : nullptr;
    }

    microdb_err_t flush() {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_flush(&db_);
    }

    microdb_err_t stats(microdb_stats_t *out) const {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_stats(&db_, out);
    }

    microdb_err_t db_stats(microdb_db_stats_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_get_db_stats(&db_, out);
    }

    microdb_err_t kv_stats(microdb_kv_stats_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_get_kv_stats(&db_, out);
    }

    microdb_err_t ts_stats(microdb_ts_stats_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_get_ts_stats(&db_, out);
    }

    microdb_err_t rel_stats(microdb_rel_stats_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_get_rel_stats(&db_, out);
    }

    microdb_err_t effective_capacity(microdb_effective_capacity_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_get_effective_capacity(&db_, out);
    }

    microdb_err_t pressure(microdb_pressure_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_get_pressure(&db_, out);
    }

    microdb_err_t kv_set(const char *key, const void *val, size_t len, uint32_t ttl = 0u) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_kv_set(&db_, key, val, len, ttl);
    }

    microdb_err_t kv_put(const char *key, const void *val, size_t len) {
        return kv_set(key, val, len, 0u);
    }

    microdb_err_t kv_get(const char *key, void *buf, size_t buf_len, size_t *out_len = nullptr) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_kv_get(&db_, key, buf, buf_len, out_len);
    }

    microdb_err_t kv_del(const char *key) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_kv_del(&db_, key);
    }

    microdb_err_t kv_exists(const char *key) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_kv_exists(&db_, key);
    }

    microdb_err_t kv_clear() {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_kv_clear(&db_);
    }

    microdb_err_t kv_purge_expired() {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_kv_purge_expired(&db_);
    }

    microdb_err_t kv_iter(microdb_kv_iter_cb_t cb, void *ctx) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_kv_iter(&db_, cb, ctx);
    }

    microdb_err_t admit_kv_set(const char *key, size_t val_len, microdb_admission_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_admit_kv_set(&db_, key, val_len, out);
    }

    template <typename T>
    microdb_err_t kv_put_pod(const char *key, const T &value, uint32_t ttl = 0u) {
        static_assert(std::is_trivially_copyable<T>::value, "kv_put_pod requires trivially copyable T");
        return kv_set(key, &value, sizeof(T), ttl);
    }

    template <typename T>
    microdb_err_t kv_get_pod(const char *key, T *out_value) {
        static_assert(std::is_trivially_copyable<T>::value, "kv_get_pod requires trivially copyable T");
        if (out_value == nullptr) {
            return MICRODB_ERR_INVALID;
        }
        return kv_get(key, out_value, sizeof(T), nullptr);
    }

    microdb_err_t ts_register(const char *name, microdb_ts_type_t type, size_t raw_size = 0u) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_ts_register(&db_, name, type, raw_size);
    }

    microdb_err_t ts_insert(const char *name, microdb_timestamp_t ts, const void *val) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_ts_insert(&db_, name, ts, val);
    }

    microdb_err_t ts_last(const char *name, microdb_ts_sample_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_ts_last(&db_, name, out);
    }

    microdb_err_t ts_query(const char *name,
                           microdb_timestamp_t from,
                           microdb_timestamp_t to,
                           microdb_ts_query_cb_t cb,
                           void *ctx) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_ts_query(&db_, name, from, to, cb, ctx);
    }

    microdb_err_t ts_query_buf(const char *name,
                               microdb_timestamp_t from,
                               microdb_timestamp_t to,
                               microdb_ts_sample_t *buf,
                               size_t max_count,
                               size_t *out_count) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_ts_query_buf(&db_, name, from, to, buf, max_count, out_count);
    }

    microdb_err_t ts_count(const char *name, microdb_timestamp_t from, microdb_timestamp_t to, size_t *out_count) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_ts_count(&db_, name, from, to, out_count);
    }

    microdb_err_t ts_clear(const char *name) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_ts_clear(&db_, name);
    }

    microdb_err_t admit_ts_insert(const char *stream_name, size_t sample_len, microdb_admission_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_admit_ts_insert(&db_, stream_name, sample_len, out);
    }

    microdb_err_t rel_schema_init(microdb_schema_t *schema, const char *name, uint32_t max_rows) {
        return microdb_schema_init(schema, name, max_rows);
    }

    microdb_err_t rel_schema_add(microdb_schema_t *schema,
                                 const char *col_name,
                                 microdb_col_type_t type,
                                 size_t size,
                                 bool is_index) {
        return microdb_schema_add(schema, col_name, type, size, is_index);
    }

    microdb_err_t rel_schema_seal(microdb_schema_t *schema) {
        return microdb_schema_seal(schema);
    }

    microdb_err_t rel_table_create(microdb_schema_t *schema) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_table_create(&db_, schema);
    }

    microdb_err_t rel_table_get(const char *name, microdb_table_t **out_table) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_table_get(&db_, name, out_table);
    }

    size_t rel_table_row_size(const microdb_table_t *table) {
        return microdb_table_row_size(table);
    }

    microdb_err_t rel_row_set(const microdb_table_t *table, void *row_buf, const char *col_name, const void *val) {
        return microdb_row_set(table, row_buf, col_name, val);
    }

    microdb_err_t rel_row_get(const microdb_table_t *table,
                              const void *row_buf,
                              const char *col_name,
                              void *out,
                              size_t *out_len) {
        return microdb_row_get(table, row_buf, col_name, out, out_len);
    }

    microdb_err_t rel_insert(microdb_table_t *table, const void *row_buf) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_rel_insert(&db_, table, row_buf);
    }

    microdb_err_t rel_find(microdb_table_t *table, const void *search_val, microdb_rel_iter_cb_t cb, void *ctx) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_rel_find(&db_, table, search_val, cb, ctx);
    }

    microdb_err_t rel_find_by(microdb_table_t *table, const char *col_name, const void *search_val, void *out_buf) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_rel_find_by(&db_, table, col_name, search_val, out_buf);
    }

    microdb_err_t rel_delete(microdb_table_t *table, const void *search_val, uint32_t *out_deleted) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_rel_delete(&db_, table, search_val, out_deleted);
    }

    microdb_err_t rel_iter(microdb_table_t *table, microdb_rel_iter_cb_t cb, void *ctx) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_rel_iter(&db_, table, cb, ctx);
    }

    microdb_err_t rel_count(const microdb_table_t *table, uint32_t *out_count) {
        return microdb_rel_count(table, out_count);
    }

    microdb_err_t rel_clear(microdb_table_t *table) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_rel_clear(&db_, table);
    }

    microdb_err_t admit_rel_insert(const char *table_name, size_t row_len, microdb_admission_t *out) {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_admit_rel_insert(&db_, table_name, row_len, out);
    }

    microdb_err_t txn_begin() {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_txn_begin(&db_);
    }

    microdb_err_t txn_commit() {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_txn_commit(&db_);
    }

    microdb_err_t txn_rollback() {
        if (!initialized_) {
            return MICRODB_ERR_INVALID;
        }
        return microdb_txn_rollback(&db_);
    }

private:
    microdb_t db_ {};
    bool initialized_ = false;
};

}  // namespace cpp
}  // namespace microdb

#endif
