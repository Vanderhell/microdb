// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox_cpp.hpp"

#include <cstring>

static loxdb::cpp::Database g_db;
typedef struct {
    uint32_t count;
} iter_ctx_t;
typedef struct {
    uint32_t count;
} ts_iter_ctx_t;
typedef struct {
    uint32_t count;
} rel_iter_ctx_t;

static bool kv_iter_count_cb(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    iter_ctx_t *it = (iter_ctx_t *)ctx;
    (void)key;
    (void)val;
    (void)val_len;
    (void)ttl_remaining;
    it->count++;
    return true;
}

static bool ts_iter_count_cb(const lox_ts_sample_t *sample, void *ctx) {
    ts_iter_ctx_t *it = (ts_iter_ctx_t *)ctx;
    (void)sample;
    it->count++;
    return true;
}

static bool rel_iter_count_cb(const void *row_buf, void *ctx) {
    rel_iter_ctx_t *it = (rel_iter_ctx_t *)ctx;
    (void)row_buf;
    it->count++;
    return true;
}

static void setup_noop(void) {
}

static void setup_db(void) {
    lox_cfg_t cfg;

    std::memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    ASSERT_EQ(g_db.init(cfg), LOX_OK);
}

static void teardown_db(void) {
    if (g_db.initialized()) {
        ASSERT_EQ(g_db.deinit(), LOX_OK);
    }
}

MDB_TEST(cpp_wrapper_reports_invalid_before_init) {
    loxdb::cpp::Database db;
    lox_stats_t stats;

    ASSERT_EQ(db.initialized(), 0);
    ASSERT_EQ(db.handle() == nullptr, 1);
    ASSERT_EQ(db.flush(), LOX_ERR_INVALID);
    ASSERT_EQ(db.stats(&stats), LOX_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_preflight_ram_only_ok) {
    lox_cfg_t cfg;
    lox_preflight_report_t rep;
    std::memset(&cfg, 0, sizeof(cfg));
    std::memset(&rep, 0, sizeof(rep));
    cfg.ram_kb = 64u;
    ASSERT_EQ(loxdb::cpp::preflight(cfg, &rep), LOX_OK);
    ASSERT_EQ(rep.status, LOX_OK);
    ASSERT_EQ(rep.ram_kb, 64u);
    ASSERT_GT(rep.kv_arena_bytes, 0u);
}

MDB_TEST(cpp_wrapper_preflight_invalid_split) {
    lox_cfg_t cfg;
    lox_preflight_report_t rep;
    std::memset(&cfg, 0, sizeof(cfg));
    std::memset(&rep, 0, sizeof(rep));
    cfg.ram_kb = 64u;
    cfg.kv_pct = 40u;
    cfg.ts_pct = 40u;
    cfg.rel_pct = 30u;
    ASSERT_EQ(loxdb::cpp::Database::preflight(cfg, &rep), LOX_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_init_and_stats) {
    lox_stats_t stats;
#ifdef LOX_CAP_LIMIT_NONE
    lox_db_stats_t dbs;
#endif

    ASSERT_EQ(g_db.initialized(), 1);
    ASSERT_EQ(g_db.handle() != nullptr, 1);
    ASSERT_EQ(g_db.stats(&stats), LOX_OK);
#ifdef LOX_CAP_LIMIT_NONE
    ASSERT_EQ(g_db.db_stats(&dbs), LOX_OK);
    ASSERT_GT(stats.kv_entries_max, 0u);
    ASSERT_EQ(dbs.last_runtime_error, LOX_OK);
#else
    ASSERT_GT(stats.kv_entries_max, 0u);
#endif
}

MDB_TEST(cpp_wrapper_handle_allows_core_api_usage) {
    uint8_t value = 7u;
    uint8_t out = 0u;

    ASSERT_EQ(lox_kv_set(g_db.handle(), "cpp", &value, sizeof(value), 0u), LOX_OK);
    ASSERT_EQ(lox_kv_get(g_db.handle(), "cpp", &out, sizeof(out), NULL), LOX_OK);
    ASSERT_EQ(out, value);
}

MDB_TEST(cpp_wrapper_prevents_double_init) {
    lox_cfg_t cfg;

    std::memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    ASSERT_EQ(g_db.init(cfg), LOX_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_kv_set_get_del_exists) {
    uint8_t value = 42u;
    uint8_t out = 0u;
    size_t out_len = 0u;

    ASSERT_EQ(g_db.kv_set("k", &value, sizeof(value), 0u), LOX_OK);
    ASSERT_EQ(g_db.kv_exists("k"), LOX_OK);
    ASSERT_EQ(g_db.kv_get("k", &out, sizeof(out), &out_len), LOX_OK);
    ASSERT_EQ(out_len, 1u);
    ASSERT_EQ(out, value);
    ASSERT_EQ(g_db.kv_del("k"), LOX_OK);
    ASSERT_EQ(g_db.kv_exists("k"), LOX_ERR_NOT_FOUND);
}

MDB_TEST(cpp_wrapper_kv_iter_and_clear) {
    uint8_t value = 1u;
    iter_ctx_t it;

    ASSERT_EQ(g_db.kv_put("a", &value, 1u), LOX_OK);
    ASSERT_EQ(g_db.kv_put("b", &value, 1u), LOX_OK);
    ASSERT_EQ(g_db.kv_put("c", &value, 1u), LOX_OK);
    it.count = 0u;
    ASSERT_EQ(g_db.kv_iter(kv_iter_count_cb, &it), LOX_OK);
    ASSERT_EQ(it.count, 3u);
    ASSERT_EQ(g_db.kv_clear(), LOX_OK);
    it.count = 0u;
    ASSERT_EQ(g_db.kv_iter(kv_iter_count_cb, &it), LOX_OK);
    ASSERT_EQ(it.count, 0u);
}

#ifdef LOX_CAP_LIMIT_NONE
MDB_TEST(cpp_wrapper_admit_kv_set) {
    lox_admission_t a;
    uint8_t value = 5u;

    ASSERT_EQ(g_db.admit_kv_set("admit", 1u, &a), LOX_OK);
    ASSERT_EQ(a.status, LOX_OK);
    ASSERT_EQ(g_db.kv_put("admit", &value, 1u), LOX_OK);
}
#endif

MDB_TEST(cpp_wrapper_kv_pod_helpers_roundtrip) {
    uint32_t in = 0xAABBCCDDu;
    uint32_t out = 0u;

    ASSERT_EQ(g_db.kv_put_pod("pod_u32", in), LOX_OK);
    ASSERT_EQ(g_db.kv_get_pod("pod_u32", &out), LOX_OK);
    ASSERT_EQ(out, in);
}

MDB_TEST(cpp_wrapper_kv_pod_helpers_null_out_invalid) {
    uint32_t in = 123u;

    ASSERT_EQ(g_db.kv_put_pod("pod_null", in), LOX_OK);
    ASSERT_EQ(g_db.kv_get_pod<uint32_t>("pod_null", nullptr), LOX_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_ts_register_insert_last) {
    lox_ts_sample_t last;
    uint32_t v1 = 11u;
    uint32_t v2 = 22u;

    ASSERT_EQ(g_db.ts_register("temp", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(g_db.ts_insert("temp", 100u, &v1), LOX_OK);
    ASSERT_EQ(g_db.ts_insert("temp", 101u, &v2), LOX_OK);
    ASSERT_EQ(g_db.ts_last("temp", &last), LOX_OK);
    ASSERT_EQ(last.ts, 101u);
    ASSERT_EQ(last.v.u32, v2);
}

MDB_TEST(cpp_wrapper_ts_query_count_clear) {
    uint32_t i;
    size_t count = 0u;
    lox_ts_sample_t buf[8];
    ts_iter_ctx_t it;

    ASSERT_EQ(g_db.ts_register("q", LOX_TS_U32, 0u), LOX_OK);
    for (i = 0u; i < 5u; ++i) {
        uint32_t v = i;
        ASSERT_EQ(g_db.ts_insert("q", 200u + i, &v), LOX_OK);
    }

    ASSERT_EQ(g_db.ts_count("q", 200u, 210u, &count), LOX_OK);
    ASSERT_EQ(count, 5u);
    ASSERT_EQ(g_db.ts_query_buf("q", 200u, 210u, buf, 8u, &count), LOX_OK);
    ASSERT_EQ(count, 5u);
    it.count = 0u;
    ASSERT_EQ(g_db.ts_query("q", 200u, 210u, ts_iter_count_cb, &it), LOX_OK);
    ASSERT_EQ(it.count, 5u);
    ASSERT_EQ(g_db.ts_clear("q"), LOX_OK);
    ASSERT_EQ(g_db.ts_count("q", 200u, 210u, &count), LOX_OK);
    ASSERT_EQ(count, 0u);
}

#ifdef LOX_CAP_LIMIT_NONE
MDB_TEST(cpp_wrapper_admit_ts_insert) {
    lox_admission_t a;
    uint32_t v = 1u;

    ASSERT_EQ(g_db.ts_register("admit_ts", LOX_TS_U32, 0u), LOX_OK);
    ASSERT_EQ(g_db.admit_ts_insert("admit_ts", sizeof(v), &a), LOX_OK);
    ASSERT_EQ(a.status, LOX_OK);
    ASSERT_EQ(g_db.ts_insert("admit_ts", 1u, &v), LOX_OK);
}
#endif

MDB_TEST(cpp_wrapper_ts_typed_helpers_u32) {
    lox_ts_sample_t last;

    ASSERT_EQ(g_db.ts_register_u32("typed_u32"), LOX_OK);
    ASSERT_EQ(g_db.ts_insert_u32("typed_u32", 300u, 123u), LOX_OK);
    ASSERT_EQ(g_db.ts_last("typed_u32", &last), LOX_OK);
    ASSERT_EQ(last.ts, 300u);
    ASSERT_EQ(last.v.u32, 123u);
}

MDB_TEST(cpp_wrapper_ts_typed_helpers_f32) {
    lox_ts_sample_t last;

    ASSERT_EQ(g_db.ts_register_f32("typed_f32"), LOX_OK);
    ASSERT_EQ(g_db.ts_insert_f32("typed_f32", 400u, 9.5f), LOX_OK);
    ASSERT_EQ(g_db.ts_last("typed_f32", &last), LOX_OK);
    ASSERT_EQ(last.ts, 400u);
    ASSERT_EQ(last.v.f32 == 9.5f, 1);
}

MDB_TEST(cpp_wrapper_rel_create_insert_find_count) {
    lox_schema_t schema;
    lox_table_t *table = nullptr;
    uint8_t row[128] = { 0 };
    uint8_t out_row[128] = { 0 };
    uint32_t id = 7u;
    uint8_t age = 31u;
    uint8_t age_out = 0u;
    size_t age_len = 0u;
    size_t row_size = 0u;
    uint32_t count = 0u;

    ASSERT_EQ(g_db.rel_schema_init(&schema, "users", 4u), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "age", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_get("users", &table), LOX_OK);
    row_size = g_db.rel_table_row_size(table);
    ASSERT_EQ(row_size <= sizeof(row), 1);
    ASSERT_EQ(row_size <= sizeof(out_row), 1);
    ASSERT_EQ(g_db.rel_row_set(table, row, "id", &id), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set(table, row, "age", &age), LOX_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), LOX_OK);
    ASSERT_EQ(g_db.rel_count(table, &count), LOX_OK);
    ASSERT_EQ(count, 1u);
    ASSERT_EQ(g_db.rel_find_by(table, "id", &id, out_row), LOX_OK);
    ASSERT_EQ(g_db.rel_row_get(table, out_row, "age", &age_out, &age_len), LOX_OK);
    ASSERT_EQ(age_len, 1u);
    ASSERT_EQ(age_out, age);
}

MDB_TEST(cpp_wrapper_rel_iter_delete_clear_and_admit) {
    lox_schema_t schema;
    lox_table_t *table = nullptr;
    uint8_t row[128] = { 0 };
    uint32_t id1 = 1u;
    uint32_t id2 = 2u;
    uint8_t state = 1u;
    uint32_t deleted = 0u;
    uint32_t count = 0u;
    rel_iter_ctx_t it;
#ifdef LOX_CAP_LIMIT_NONE
    lox_admission_t a;
#endif
    size_t row_size = 0u;

    ASSERT_EQ(g_db.rel_schema_init(&schema, "devices", 3u), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "state", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_get("devices", &table), LOX_OK);
    row_size = g_db.rel_table_row_size(table);
    ASSERT_EQ(row_size <= sizeof(row), 1);

    ASSERT_EQ(g_db.rel_row_set(table, row, "id", &id1), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set(table, row, "state", &state), LOX_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set(table, row, "id", &id2), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set(table, row, "state", &state), LOX_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), LOX_OK);

    it.count = 0u;
    ASSERT_EQ(g_db.rel_iter(table, rel_iter_count_cb, &it), LOX_OK);
    ASSERT_EQ(it.count, 2u);

#ifdef LOX_CAP_LIMIT_NONE
    ASSERT_EQ(g_db.admit_rel_insert("devices", row_size, &a), LOX_OK);
    ASSERT_EQ(a.status, LOX_OK);
#endif

    ASSERT_EQ(g_db.rel_delete(table, &id1, &deleted), LOX_OK);
    ASSERT_EQ(deleted, 1u);
    ASSERT_EQ(g_db.rel_count(table, &count), LOX_OK);
    ASSERT_EQ(count, 1u);
    ASSERT_EQ(g_db.rel_clear(table), LOX_OK);
    ASSERT_EQ(g_db.rel_count(table, &count), LOX_OK);
    ASSERT_EQ(count, 0u);
}

MDB_TEST(cpp_wrapper_rel_pod_row_helpers_roundtrip) {
    lox_schema_t schema;
    lox_table_t *table = nullptr;
    uint8_t row[128] = { 0 };
    uint8_t out_row[128] = { 0 };
    uint32_t id = 88u;
    uint8_t age = 19u;
    uint32_t id_out = 0u;
    uint8_t age_out = 0u;

    ASSERT_EQ(g_db.rel_schema_init(&schema, "pod_users", 2u), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "age", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_get("pod_users", &table), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set_pod(table, row, "id", id), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set_pod(table, row, "age", age), LOX_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), LOX_OK);
    ASSERT_EQ(g_db.rel_find_by(table, "id", &id, out_row), LOX_OK);
    ASSERT_EQ(g_db.rel_row_get_pod(table, out_row, "id", &id_out), LOX_OK);
    ASSERT_EQ(g_db.rel_row_get_pod(table, out_row, "age", &age_out), LOX_OK);
    ASSERT_EQ(id_out, id);
    ASSERT_EQ(age_out, age);
}

MDB_TEST(cpp_wrapper_rel_pod_row_helpers_null_invalid) {
    lox_schema_t schema;
    lox_table_t *table = nullptr;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;

    ASSERT_EQ(g_db.rel_schema_init(&schema, "pod_null", 1u), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_get("pod_null", &table), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set_pod(table, row, "id", id), LOX_OK);
    ASSERT_EQ(g_db.rel_row_get_pod<uint32_t>(table, row, "id", nullptr), LOX_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_txn_reports_invalid_before_init) {
    loxdb::cpp::Database db;

    ASSERT_EQ(db.txn_begin(), LOX_ERR_INVALID);
    ASSERT_EQ(db.txn_commit(), LOX_ERR_INVALID);
    ASSERT_EQ(db.txn_rollback(), LOX_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_txn_commit_persists_kv) {
    uint8_t value = 33u;
    uint8_t out = 0u;

    ASSERT_EQ(g_db.txn_begin(), LOX_OK);
    ASSERT_EQ(g_db.kv_put("txn_k", &value, 1u), LOX_OK);
    ASSERT_EQ(g_db.txn_commit(), LOX_OK);
    ASSERT_EQ(g_db.kv_get("txn_k", &out, sizeof(out), nullptr), LOX_OK);
    ASSERT_EQ(out, value);
}

MDB_TEST(cpp_wrapper_txn_rollback_discards_kv) {
    uint8_t value = 44u;
    uint8_t out = 0u;

    ASSERT_EQ(g_db.txn_begin(), LOX_OK);
    ASSERT_EQ(g_db.kv_put("txn_r", &value, 1u), LOX_OK);
    ASSERT_EQ(g_db.txn_rollback(), LOX_OK);
    ASSERT_EQ(g_db.kv_get("txn_r", &out, sizeof(out), nullptr), LOX_ERR_NOT_FOUND);
}

MDB_TEST(cpp_wrapper_realdata_typed_flow) {
    uint32_t sensor_interval_ms = 5000u;
    uint32_t sensor_interval_out = 0u;
    lox_ts_sample_t last;
    lox_schema_t schema;
    lox_table_t *table = nullptr;
    uint8_t row[128] = {0};
    uint8_t out_row[128] = {0};
    uint32_t id = 21u;
    uint32_t id_out = 0u;
    uint8_t sev = 1u;
    uint8_t sev_out = 0u;

    ASSERT_EQ(g_db.kv_put_pod<uint32_t>("sensor.interval_ms", sensor_interval_ms), LOX_OK);
    ASSERT_EQ(g_db.kv_get_pod<uint32_t>("sensor.interval_ms", &sensor_interval_out), LOX_OK);
    ASSERT_EQ(sensor_interval_out, sensor_interval_ms);

    ASSERT_EQ(g_db.ts_register_f32("vcc"), LOX_OK);
    ASSERT_EQ(g_db.ts_insert_f32("vcc", 1700000200u, 3.31f), LOX_OK);
    ASSERT_EQ(g_db.ts_insert_f32("vcc", 1700000260u, 3.28f), LOX_OK);
    ASSERT_EQ(g_db.ts_last("vcc", &last), LOX_OK);
    ASSERT_EQ(last.ts, 1700000260u);
    ASSERT_EQ(last.v.f32 == 3.28f, 1);

    ASSERT_EQ(g_db.rel_schema_init(&schema, "cpp_event_log", 8u), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "severity", LOX_COL_U8, sizeof(uint8_t), false), LOX_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), LOX_OK);
    ASSERT_EQ(g_db.rel_table_get("cpp_event_log", &table), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set_pod<uint32_t>(table, row, "id", id), LOX_OK);
    ASSERT_EQ(g_db.rel_row_set_pod<uint8_t>(table, row, "severity", sev), LOX_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), LOX_OK);
    ASSERT_EQ(g_db.rel_find_by(table, "id", &id, out_row), LOX_OK);
    ASSERT_EQ(g_db.rel_row_get_pod<uint32_t>(table, out_row, "id", &id_out), LOX_OK);
    ASSERT_EQ(g_db.rel_row_get_pod<uint8_t>(table, out_row, "severity", &sev_out), LOX_OK);
    ASSERT_EQ(id_out, id);
    ASSERT_EQ(sev_out, sev);
}

int main(void) {
    MDB_RUN_TEST(setup_noop, teardown_db, cpp_wrapper_preflight_ram_only_ok);
    MDB_RUN_TEST(setup_noop, teardown_db, cpp_wrapper_preflight_invalid_split);
    MDB_RUN_TEST(setup_noop, teardown_db, cpp_wrapper_txn_reports_invalid_before_init);
    MDB_RUN_TEST(setup_noop, teardown_db, cpp_wrapper_reports_invalid_before_init);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_init_and_stats);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_handle_allows_core_api_usage);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_prevents_double_init);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_kv_set_get_del_exists);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_kv_iter_and_clear);
#ifdef LOX_CAP_LIMIT_NONE
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_admit_kv_set);
#endif
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_kv_pod_helpers_roundtrip);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_kv_pod_helpers_null_out_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_ts_register_insert_last);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_ts_query_count_clear);
#ifdef LOX_CAP_LIMIT_NONE
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_admit_ts_insert);
#endif
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_ts_typed_helpers_u32);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_ts_typed_helpers_f32);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_rel_create_insert_find_count);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_rel_iter_delete_clear_and_admit);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_rel_pod_row_helpers_roundtrip);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_rel_pod_row_helpers_null_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_txn_commit_persists_kv);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_txn_rollback_discards_kv);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_realdata_typed_flow);
    return MDB_RESULT();
}
