#include "microtest.h"
#include "microdb_cpp.hpp"

#include <cstring>

static microdb::cpp::Database g_db;
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

static bool ts_iter_count_cb(const microdb_ts_sample_t *sample, void *ctx) {
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
    microdb_cfg_t cfg;

    std::memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    ASSERT_EQ(g_db.init(cfg), MICRODB_OK);
}

static void teardown_db(void) {
    if (g_db.initialized()) {
        ASSERT_EQ(g_db.deinit(), MICRODB_OK);
    }
}

MDB_TEST(cpp_wrapper_reports_invalid_before_init) {
    microdb::cpp::Database db;
    microdb_stats_t stats;

    ASSERT_EQ(db.initialized(), 0);
    ASSERT_EQ(db.handle() == nullptr, 1);
    ASSERT_EQ(db.flush(), MICRODB_ERR_INVALID);
    ASSERT_EQ(db.stats(&stats), MICRODB_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_init_and_stats) {
    microdb_stats_t stats;
    microdb_db_stats_t dbs;

    ASSERT_EQ(g_db.initialized(), 1);
    ASSERT_EQ(g_db.handle() != nullptr, 1);
    ASSERT_EQ(g_db.stats(&stats), MICRODB_OK);
    ASSERT_EQ(g_db.db_stats(&dbs), MICRODB_OK);
    ASSERT_GT(stats.kv_entries_max, 0u);
    ASSERT_EQ(dbs.last_runtime_error, MICRODB_OK);
}

MDB_TEST(cpp_wrapper_handle_allows_core_api_usage) {
    uint8_t value = 7u;
    uint8_t out = 0u;

    ASSERT_EQ(microdb_kv_set(g_db.handle(), "cpp", &value, sizeof(value), 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_get(g_db.handle(), "cpp", &out, sizeof(out), NULL), MICRODB_OK);
    ASSERT_EQ(out, value);
}

MDB_TEST(cpp_wrapper_prevents_double_init) {
    microdb_cfg_t cfg;

    std::memset(&cfg, 0, sizeof(cfg));
    cfg.ram_kb = 32u;
    ASSERT_EQ(g_db.init(cfg), MICRODB_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_kv_set_get_del_exists) {
    uint8_t value = 42u;
    uint8_t out = 0u;
    size_t out_len = 0u;

    ASSERT_EQ(g_db.kv_set("k", &value, sizeof(value), 0u), MICRODB_OK);
    ASSERT_EQ(g_db.kv_exists("k"), MICRODB_OK);
    ASSERT_EQ(g_db.kv_get("k", &out, sizeof(out), &out_len), MICRODB_OK);
    ASSERT_EQ(out_len, 1u);
    ASSERT_EQ(out, value);
    ASSERT_EQ(g_db.kv_del("k"), MICRODB_OK);
    ASSERT_EQ(g_db.kv_exists("k"), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(cpp_wrapper_kv_iter_and_clear) {
    uint8_t value = 1u;
    iter_ctx_t it;

    ASSERT_EQ(g_db.kv_put("a", &value, 1u), MICRODB_OK);
    ASSERT_EQ(g_db.kv_put("b", &value, 1u), MICRODB_OK);
    ASSERT_EQ(g_db.kv_put("c", &value, 1u), MICRODB_OK);
    it.count = 0u;
    ASSERT_EQ(g_db.kv_iter(kv_iter_count_cb, &it), MICRODB_OK);
    ASSERT_EQ(it.count, 3u);
    ASSERT_EQ(g_db.kv_clear(), MICRODB_OK);
    it.count = 0u;
    ASSERT_EQ(g_db.kv_iter(kv_iter_count_cb, &it), MICRODB_OK);
    ASSERT_EQ(it.count, 0u);
}

MDB_TEST(cpp_wrapper_admit_kv_set) {
    microdb_admission_t a;
    uint8_t value = 5u;

    ASSERT_EQ(g_db.admit_kv_set("admit", 1u, &a), MICRODB_OK);
    ASSERT_EQ(a.status, MICRODB_OK);
    ASSERT_EQ(g_db.kv_put("admit", &value, 1u), MICRODB_OK);
}

MDB_TEST(cpp_wrapper_kv_pod_helpers_roundtrip) {
    uint32_t in = 0xAABBCCDDu;
    uint32_t out = 0u;

    ASSERT_EQ(g_db.kv_put_pod("pod_u32", in), MICRODB_OK);
    ASSERT_EQ(g_db.kv_get_pod("pod_u32", &out), MICRODB_OK);
    ASSERT_EQ(out, in);
}

MDB_TEST(cpp_wrapper_kv_pod_helpers_null_out_invalid) {
    uint32_t in = 123u;

    ASSERT_EQ(g_db.kv_put_pod("pod_null", in), MICRODB_OK);
    ASSERT_EQ(g_db.kv_get_pod<uint32_t>("pod_null", nullptr), MICRODB_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_ts_register_insert_last) {
    microdb_ts_sample_t last;
    uint32_t v1 = 11u;
    uint32_t v2 = 22u;

    ASSERT_EQ(g_db.ts_register("temp", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(g_db.ts_insert("temp", 100u, &v1), MICRODB_OK);
    ASSERT_EQ(g_db.ts_insert("temp", 101u, &v2), MICRODB_OK);
    ASSERT_EQ(g_db.ts_last("temp", &last), MICRODB_OK);
    ASSERT_EQ(last.ts, 101u);
    ASSERT_EQ(last.v.u32, v2);
}

MDB_TEST(cpp_wrapper_ts_query_count_clear) {
    uint32_t i;
    size_t count = 0u;
    microdb_ts_sample_t buf[8];
    ts_iter_ctx_t it;

    ASSERT_EQ(g_db.ts_register("q", MICRODB_TS_U32, 0u), MICRODB_OK);
    for (i = 0u; i < 5u; ++i) {
        uint32_t v = i;
        ASSERT_EQ(g_db.ts_insert("q", 200u + i, &v), MICRODB_OK);
    }

    ASSERT_EQ(g_db.ts_count("q", 200u, 210u, &count), MICRODB_OK);
    ASSERT_EQ(count, 5u);
    ASSERT_EQ(g_db.ts_query_buf("q", 200u, 210u, buf, 8u, &count), MICRODB_OK);
    ASSERT_EQ(count, 5u);
    it.count = 0u;
    ASSERT_EQ(g_db.ts_query("q", 200u, 210u, ts_iter_count_cb, &it), MICRODB_OK);
    ASSERT_EQ(it.count, 5u);
    ASSERT_EQ(g_db.ts_clear("q"), MICRODB_OK);
    ASSERT_EQ(g_db.ts_count("q", 200u, 210u, &count), MICRODB_OK);
    ASSERT_EQ(count, 0u);
}

MDB_TEST(cpp_wrapper_admit_ts_insert) {
    microdb_admission_t a;
    uint32_t v = 1u;

    ASSERT_EQ(g_db.ts_register("admit_ts", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(g_db.admit_ts_insert("admit_ts", sizeof(v), &a), MICRODB_OK);
    ASSERT_EQ(a.status, MICRODB_OK);
    ASSERT_EQ(g_db.ts_insert("admit_ts", 1u, &v), MICRODB_OK);
}

MDB_TEST(cpp_wrapper_ts_typed_helpers_u32) {
    microdb_ts_sample_t last;

    ASSERT_EQ(g_db.ts_register_u32("typed_u32"), MICRODB_OK);
    ASSERT_EQ(g_db.ts_insert_u32("typed_u32", 300u, 123u), MICRODB_OK);
    ASSERT_EQ(g_db.ts_last("typed_u32", &last), MICRODB_OK);
    ASSERT_EQ(last.ts, 300u);
    ASSERT_EQ(last.v.u32, 123u);
}

MDB_TEST(cpp_wrapper_ts_typed_helpers_f32) {
    microdb_ts_sample_t last;

    ASSERT_EQ(g_db.ts_register_f32("typed_f32"), MICRODB_OK);
    ASSERT_EQ(g_db.ts_insert_f32("typed_f32", 400u, 9.5f), MICRODB_OK);
    ASSERT_EQ(g_db.ts_last("typed_f32", &last), MICRODB_OK);
    ASSERT_EQ(last.ts, 400u);
    ASSERT_EQ(last.v.f32 == 9.5f, 1);
}

MDB_TEST(cpp_wrapper_rel_create_insert_find_count) {
    microdb_schema_t schema;
    microdb_table_t *table = nullptr;
    uint8_t row[128] = { 0 };
    uint8_t out_row[128] = { 0 };
    uint32_t id = 7u;
    uint8_t age = 31u;
    uint8_t age_out = 0u;
    size_t age_len = 0u;
    size_t row_size = 0u;
    uint32_t count = 0u;

    ASSERT_EQ(g_db.rel_schema_init(&schema, "users", 4u), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), MICRODB_OK);
    ASSERT_EQ(g_db.rel_table_get("users", &table), MICRODB_OK);
    row_size = g_db.rel_table_row_size(table);
    ASSERT_EQ(row_size <= sizeof(row), 1);
    ASSERT_EQ(row_size <= sizeof(out_row), 1);
    ASSERT_EQ(g_db.rel_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), MICRODB_OK);
    ASSERT_EQ(g_db.rel_count(table, &count), MICRODB_OK);
    ASSERT_EQ(count, 1u);
    ASSERT_EQ(g_db.rel_find_by(table, "id", &id, out_row), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_get(table, out_row, "age", &age_out, &age_len), MICRODB_OK);
    ASSERT_EQ(age_len, 1u);
    ASSERT_EQ(age_out, age);
}

MDB_TEST(cpp_wrapper_rel_iter_delete_clear_and_admit) {
    microdb_schema_t schema;
    microdb_table_t *table = nullptr;
    uint8_t row[128] = { 0 };
    uint32_t id1 = 1u;
    uint32_t id2 = 2u;
    uint8_t state = 1u;
    uint32_t deleted = 0u;
    uint32_t count = 0u;
    rel_iter_ctx_t it;
    microdb_admission_t a;
    size_t row_size = 0u;

    ASSERT_EQ(g_db.rel_schema_init(&schema, "devices", 3u), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "state", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), MICRODB_OK);
    ASSERT_EQ(g_db.rel_table_get("devices", &table), MICRODB_OK);
    row_size = g_db.rel_table_row_size(table);
    ASSERT_EQ(row_size <= sizeof(row), 1);

    ASSERT_EQ(g_db.rel_row_set(table, row, "id", &id1), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_set(table, row, "state", &state), MICRODB_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_set(table, row, "id", &id2), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_set(table, row, "state", &state), MICRODB_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), MICRODB_OK);

    it.count = 0u;
    ASSERT_EQ(g_db.rel_iter(table, rel_iter_count_cb, &it), MICRODB_OK);
    ASSERT_EQ(it.count, 2u);

    ASSERT_EQ(g_db.admit_rel_insert("devices", row_size, &a), MICRODB_OK);
    ASSERT_EQ(a.status, MICRODB_OK);

    ASSERT_EQ(g_db.rel_delete(table, &id1, &deleted), MICRODB_OK);
    ASSERT_EQ(deleted, 1u);
    ASSERT_EQ(g_db.rel_count(table, &count), MICRODB_OK);
    ASSERT_EQ(count, 1u);
    ASSERT_EQ(g_db.rel_clear(table), MICRODB_OK);
    ASSERT_EQ(g_db.rel_count(table, &count), MICRODB_OK);
    ASSERT_EQ(count, 0u);
}

MDB_TEST(cpp_wrapper_rel_pod_row_helpers_roundtrip) {
    microdb_schema_t schema;
    microdb_table_t *table = nullptr;
    uint8_t row[128] = { 0 };
    uint8_t out_row[128] = { 0 };
    uint32_t id = 88u;
    uint8_t age = 19u;
    uint32_t id_out = 0u;
    uint8_t age_out = 0u;

    ASSERT_EQ(g_db.rel_schema_init(&schema, "pod_users", 2u), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), MICRODB_OK);
    ASSERT_EQ(g_db.rel_table_get("pod_users", &table), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_set_pod(table, row, "id", id), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_set_pod(table, row, "age", age), MICRODB_OK);
    ASSERT_EQ(g_db.rel_insert(table, row), MICRODB_OK);
    ASSERT_EQ(g_db.rel_find_by(table, "id", &id, out_row), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_get_pod(table, out_row, "id", &id_out), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_get_pod(table, out_row, "age", &age_out), MICRODB_OK);
    ASSERT_EQ(id_out, id);
    ASSERT_EQ(age_out, age);
}

MDB_TEST(cpp_wrapper_rel_pod_row_helpers_null_invalid) {
    microdb_schema_t schema;
    microdb_table_t *table = nullptr;
    uint8_t row[128] = { 0 };
    uint32_t id = 1u;

    ASSERT_EQ(g_db.rel_schema_init(&schema, "pod_null", 1u), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(g_db.rel_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(g_db.rel_table_create(&schema), MICRODB_OK);
    ASSERT_EQ(g_db.rel_table_get("pod_null", &table), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_set_pod(table, row, "id", id), MICRODB_OK);
    ASSERT_EQ(g_db.rel_row_get_pod<uint32_t>(table, row, "id", nullptr), MICRODB_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_txn_reports_invalid_before_init) {
    microdb::cpp::Database db;

    ASSERT_EQ(db.txn_begin(), MICRODB_ERR_INVALID);
    ASSERT_EQ(db.txn_commit(), MICRODB_ERR_INVALID);
    ASSERT_EQ(db.txn_rollback(), MICRODB_ERR_INVALID);
}

MDB_TEST(cpp_wrapper_txn_commit_persists_kv) {
    uint8_t value = 33u;
    uint8_t out = 0u;

    ASSERT_EQ(g_db.txn_begin(), MICRODB_OK);
    ASSERT_EQ(g_db.kv_put("txn_k", &value, 1u), MICRODB_OK);
    ASSERT_EQ(g_db.txn_commit(), MICRODB_OK);
    ASSERT_EQ(g_db.kv_get("txn_k", &out, sizeof(out), nullptr), MICRODB_OK);
    ASSERT_EQ(out, value);
}

MDB_TEST(cpp_wrapper_txn_rollback_discards_kv) {
    uint8_t value = 44u;
    uint8_t out = 0u;

    ASSERT_EQ(g_db.txn_begin(), MICRODB_OK);
    ASSERT_EQ(g_db.kv_put("txn_r", &value, 1u), MICRODB_OK);
    ASSERT_EQ(g_db.txn_rollback(), MICRODB_OK);
    ASSERT_EQ(g_db.kv_get("txn_r", &out, sizeof(out), nullptr), MICRODB_ERR_NOT_FOUND);
}

int main(void) {
    MDB_RUN_TEST(setup_noop, teardown_db, cpp_wrapper_txn_reports_invalid_before_init);
    MDB_RUN_TEST(setup_noop, teardown_db, cpp_wrapper_reports_invalid_before_init);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_init_and_stats);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_handle_allows_core_api_usage);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_prevents_double_init);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_kv_set_get_del_exists);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_kv_iter_and_clear);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_admit_kv_set);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_kv_pod_helpers_roundtrip);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_kv_pod_helpers_null_out_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_ts_register_insert_last);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_ts_query_count_clear);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_admit_ts_insert);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_ts_typed_helpers_u32);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_ts_typed_helpers_f32);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_rel_create_insert_find_count);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_rel_iter_delete_clear_and_admit);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_rel_pod_row_helpers_roundtrip);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_rel_pod_row_helpers_null_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_txn_commit_persists_kv);
    MDB_RUN_TEST(setup_db, teardown_db, cpp_wrapper_txn_rollback_discards_kv);
    return MDB_RESULT();
}
