// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "../port/posix/microdb_port_posix.h"

#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_storage;
static const char *g_path = "fail_code_contract.bin";

typedef struct {
    uint8_t bytes[262144];
    bool fail_read;
    bool fail_write;
    bool fail_erase;
    bool fail_sync;
} failing_mem_storage_t;

static failing_mem_storage_t g_failmem;

static microdb_err_t fs_read(void *ctx, uint32_t offset, void *buf, size_t len) {
    failing_mem_storage_t *s = (failing_mem_storage_t *)ctx;
    if (s == NULL || buf == NULL || offset + len > sizeof(s->bytes)) return MICRODB_ERR_INVALID;
    if (s->fail_read) return MICRODB_ERR_STORAGE;
    memcpy(buf, &s->bytes[offset], len);
    return MICRODB_OK;
}

static microdb_err_t fs_write(void *ctx, uint32_t offset, const void *buf, size_t len) {
    failing_mem_storage_t *s = (failing_mem_storage_t *)ctx;
    if (s == NULL || buf == NULL || offset + len > sizeof(s->bytes)) return MICRODB_ERR_INVALID;
    if (s->fail_write) return MICRODB_ERR_STORAGE;
    memcpy(&s->bytes[offset], buf, len);
    return MICRODB_OK;
}

static microdb_err_t fs_erase(void *ctx, uint32_t offset) {
    failing_mem_storage_t *s = (failing_mem_storage_t *)ctx;
    if (s == NULL || offset + 4096u > sizeof(s->bytes)) return MICRODB_ERR_INVALID;
    if (s->fail_erase) return MICRODB_ERR_STORAGE;
    memset(&s->bytes[offset], 0xFF, 4096u);
    return MICRODB_OK;
}

static microdb_err_t fs_sync(void *ctx) {
    failing_mem_storage_t *s = (failing_mem_storage_t *)ctx;
    if (s == NULL) return MICRODB_ERR_INVALID;
    if (s->fail_sync) return MICRODB_ERR_STORAGE;
    return MICRODB_OK;
}

static void setup_db(void) {
    microdb_cfg_t cfg;
    microdb_port_posix_remove(g_path);
    memset(&g_db, 0, sizeof(g_db));
    memset(&g_storage, 0, sizeof(g_storage));
    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 65536u), MICRODB_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&g_db, &cfg), MICRODB_OK);
}

static void teardown_db(void) {
    (void)microdb_deinit(&g_db);
    microdb_port_posix_deinit(&g_storage);
    microdb_port_posix_remove(g_path);
}

static void setup_mem_storage(microdb_storage_t *out) {
    memset(&g_failmem, 0, sizeof(g_failmem));
    memset(g_failmem.bytes, 0xFF, sizeof(g_failmem.bytes));
    memset(out, 0, sizeof(*out));
    out->read = fs_read;
    out->write = fs_write;
    out->erase = fs_erase;
    out->sync = fs_sync;
    out->capacity = 262144u;
    out->erase_size = 4096u;
    out->write_size = 1u;
    out->ctx = &g_failmem;
}

static void create_rel_table_versioned(microdb_t *db, uint16_t version) {
    microdb_schema_t s;
    ASSERT_EQ(microdb_schema_init(&s, "migr", 8u), MICRODB_OK);
    s.schema_version = version;
    ASSERT_EQ(microdb_schema_add(&s, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&s), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(db, &s), MICRODB_OK);
}

static microdb_err_t reject_migration(microdb_t *db, const char *table_name, uint16_t old_v, uint16_t new_v) {
    (void)db;
    (void)table_name;
    (void)old_v;
    (void)new_v;
    return MICRODB_ERR_SCHEMA;
}

MDB_TEST(contract_full_rel_insert_returns_full) {
    microdb_schema_t s;
    microdb_table_t *t = NULL;
    uint8_t row[64] = {0};
    uint32_t id;
    uint8_t v = 1u;

    ASSERT_EQ(microdb_schema_init(&s, "full_t", 1u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&s, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&s, "v", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&s), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&g_db, &s), MICRODB_OK);
    ASSERT_EQ(microdb_table_get(&g_db, "full_t", &t), MICRODB_OK);

    id = 1u;
    ASSERT_EQ(microdb_row_set(t, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(t, row, "v", &v), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, t, row), MICRODB_OK);

    id = 2u;
    ASSERT_EQ(microdb_row_set(t, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(t, row, "v", &v), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, t, row), MICRODB_ERR_FULL);
}

MDB_TEST(contract_schema_mismatch_without_callback_returns_schema) {
    microdb_t db2;
    microdb_storage_t st2;
    microdb_cfg_t cfg2;
    microdb_schema_t s;

    create_rel_table_versioned(&g_db, 1u);
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_OK);
    microdb_port_posix_deinit(&g_storage);

    memset(&db2, 0, sizeof(db2));
    memset(&st2, 0, sizeof(st2));
    ASSERT_EQ(microdb_port_posix_init(&st2, g_path, 65536u), MICRODB_OK);
    memset(&cfg2, 0, sizeof(cfg2));
    cfg2.storage = &st2;
    cfg2.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db2, &cfg2), MICRODB_OK);

    ASSERT_EQ(microdb_schema_init(&s, "migr", 8u), MICRODB_OK);
    s.schema_version = 2u;
    ASSERT_EQ(microdb_schema_add(&s, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&s), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&db2, &s), MICRODB_ERR_SCHEMA);

    ASSERT_EQ(microdb_deinit(&db2), MICRODB_OK);
    microdb_port_posix_deinit(&st2);

    /* Reopen default handles for teardown consistency. */
    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 65536u), MICRODB_OK);
    memset(&cfg2, 0, sizeof(cfg2));
    cfg2.storage = &g_storage;
    cfg2.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&g_db, &cfg2), MICRODB_OK);
}

MDB_TEST(contract_unsupported_migration_callback_returns_schema) {
    microdb_t db2;
    microdb_storage_t st2;
    microdb_cfg_t cfg2;
    microdb_schema_t s;

    create_rel_table_versioned(&g_db, 1u);
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_OK);
    microdb_port_posix_deinit(&g_storage);

    memset(&db2, 0, sizeof(db2));
    memset(&st2, 0, sizeof(st2));
    ASSERT_EQ(microdb_port_posix_init(&st2, g_path, 65536u), MICRODB_OK);
    memset(&cfg2, 0, sizeof(cfg2));
    cfg2.storage = &st2;
    cfg2.ram_kb = 32u;
    cfg2.on_migrate = reject_migration;
    ASSERT_EQ(microdb_init(&db2, &cfg2), MICRODB_OK);

    ASSERT_EQ(microdb_schema_init(&s, "migr", 8u), MICRODB_OK);
    s.schema_version = 2u;
    ASSERT_EQ(microdb_schema_add(&s, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&s), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(&db2, &s), MICRODB_ERR_SCHEMA);

    ASSERT_EQ(microdb_deinit(&db2), MICRODB_OK);
    microdb_port_posix_deinit(&st2);

    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 65536u), MICRODB_OK);
    memset(&cfg2, 0, sizeof(cfg2));
    cfg2.storage = &g_storage;
    cfg2.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&g_db, &cfg2), MICRODB_OK);
}

MDB_TEST(contract_storage_read_error_on_init_returns_storage) {
    microdb_t db;
    microdb_cfg_t cfg;
    microdb_storage_t st;

    setup_mem_storage(&st);
    g_failmem.fail_read = true;
    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &st;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_ERR_STORAGE);
}

MDB_TEST(contract_storage_write_error_on_kv_put_returns_storage) {
    microdb_t db;
    microdb_cfg_t cfg;
    microdb_storage_t st;
    uint8_t v = 7u;

    setup_mem_storage(&st);
    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &st;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_OK);
    g_failmem.fail_write = true;
    ASSERT_EQ(microdb_kv_put(&db, "a", &v, 1u), MICRODB_ERR_STORAGE);
    ASSERT_EQ(microdb_deinit(&db), MICRODB_ERR_STORAGE);
}

MDB_TEST(contract_storage_sync_error_on_flush_returns_storage) {
    microdb_t db;
    microdb_cfg_t cfg;
    microdb_storage_t st;

    setup_mem_storage(&st);
    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &st;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_OK);
    g_failmem.fail_sync = true;
    ASSERT_EQ(microdb_flush(&db), MICRODB_ERR_STORAGE);
    ASSERT_EQ(microdb_deinit(&db), MICRODB_ERR_STORAGE);
}

MDB_TEST(contract_storage_zero_erase_size_on_init_returns_invalid) {
    microdb_t db;
    microdb_cfg_t cfg;
    microdb_storage_t st;

    setup_mem_storage(&st);
    st.erase_size = 0u;
    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &st;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_ERR_INVALID);
}

MDB_TEST(contract_storage_zero_write_size_on_init_returns_invalid) {
    microdb_t db;
    microdb_cfg_t cfg;
    microdb_storage_t st;

    setup_mem_storage(&st);
    st.write_size = 0u;
    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &st;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_ERR_INVALID);
}

MDB_TEST(contract_storage_write_size_one_on_init_ok) {
    microdb_t db;
    microdb_cfg_t cfg;
    microdb_storage_t st;

    setup_mem_storage(&st);
    st.write_size = 1u;
    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &st;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_OK);
    ASSERT_EQ(microdb_deinit(&db), MICRODB_OK);
}

MDB_TEST(contract_storage_write_size_unsupported_on_init_returns_invalid) {
    static const uint32_t bad_sizes[] = {2u, 4u, 8u};
    uint32_t i;

    for (i = 0u; i < (uint32_t)(sizeof(bad_sizes) / sizeof(bad_sizes[0])); ++i) {
        microdb_t db;
        microdb_cfg_t cfg;
        microdb_storage_t st;

        setup_mem_storage(&st);
        st.write_size = bad_sizes[i];
        memset(&db, 0, sizeof(db));
        memset(&cfg, 0, sizeof(cfg));
        cfg.storage = &st;
        cfg.ram_kb = 32u;
        ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_ERR_INVALID);
    }
}

MDB_TEST(contract_wal_crc_tail_corruption_recovery_drops_tail) {
    uint8_t v1 = 1u, v2 = 2u, out = 0u;
    uint32_t second = 0u;
    uint32_t bad_crc = 0u;
    uint16_t data_len = 0u;
    microdb_t db2;
    microdb_storage_t st2;
    microdb_cfg_t cfg2;

    ASSERT_EQ(microdb_kv_put(&g_db, "x1", &v1, 1u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_put(&g_db, "x2", &v2, 1u), MICRODB_OK);
    /* Find second entry offset from first entry header (32B WAL header, 16B entry header). */
    ASSERT_EQ(g_storage.read(g_storage.ctx, 42u, &data_len, sizeof(data_len)), MICRODB_OK);
    second = 32u + 16u + (((uint32_t)data_len + 3u) & ~3u);
    ASSERT_EQ(g_storage.write(g_storage.ctx, second + 12u, &bad_crc, sizeof(bad_crc)), MICRODB_OK);
    ASSERT_EQ(g_storage.sync(g_storage.ctx), MICRODB_OK);

    microdb_port_posix_simulate_power_loss(&g_storage);
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_ERR_STORAGE);
    microdb_port_posix_deinit(&g_storage);

    memset(&db2, 0, sizeof(db2));
    memset(&st2, 0, sizeof(st2));
    ASSERT_EQ(microdb_port_posix_init(&st2, g_path, 65536u), MICRODB_OK);
    memset(&cfg2, 0, sizeof(cfg2));
    cfg2.storage = &st2;
    cfg2.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db2, &cfg2), MICRODB_OK);
    ASSERT_EQ(microdb_kv_get(&db2, "x1", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_kv_get(&db2, "x2", &out, 1u, NULL), MICRODB_ERR_NOT_FOUND);
    ASSERT_EQ(microdb_deinit(&db2), MICRODB_OK);
    microdb_port_posix_deinit(&st2);

    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 65536u), MICRODB_OK);
    memset(&cfg2, 0, sizeof(cfg2));
    cfg2.storage = &g_storage;
    cfg2.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&g_db, &cfg2), MICRODB_OK);
}

MDB_TEST(contract_wal_header_crc_corruption_recovers_clean) {
    uint32_t bad = 0u;
    uint8_t out = 0u;
    microdb_t db2;
    microdb_storage_t st2;
    microdb_cfg_t cfg2;

    ASSERT_EQ(g_storage.write(g_storage.ctx, 16u, &bad, sizeof(bad)), MICRODB_OK);
    ASSERT_EQ(g_storage.sync(g_storage.ctx), MICRODB_OK);
    microdb_port_posix_simulate_power_loss(&g_storage);
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_ERR_STORAGE);
    microdb_port_posix_deinit(&g_storage);

    memset(&db2, 0, sizeof(db2));
    memset(&st2, 0, sizeof(st2));
    ASSERT_EQ(microdb_port_posix_init(&st2, g_path, 65536u), MICRODB_OK);
    memset(&cfg2, 0, sizeof(cfg2));
    cfg2.storage = &st2;
    cfg2.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&db2, &cfg2), MICRODB_OK);
    ASSERT_EQ(microdb_kv_get(&db2, "missing", &out, 1u, NULL), MICRODB_ERR_NOT_FOUND);
    ASSERT_EQ(microdb_deinit(&db2), MICRODB_OK);
    microdb_port_posix_deinit(&st2);

    ASSERT_EQ(microdb_port_posix_init(&g_storage, g_path, 65536u), MICRODB_OK);
    memset(&cfg2, 0, sizeof(cfg2));
    cfg2.storage = &g_storage;
    cfg2.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&g_db, &cfg2), MICRODB_OK);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, contract_full_rel_insert_returns_full);
    MDB_RUN_TEST(setup_db, teardown_db, contract_schema_mismatch_without_callback_returns_schema);
    MDB_RUN_TEST(setup_db, teardown_db, contract_unsupported_migration_callback_returns_schema);
    MDB_RUN_TEST(setup_db, teardown_db, contract_storage_read_error_on_init_returns_storage);
    MDB_RUN_TEST(setup_db, teardown_db, contract_storage_write_error_on_kv_put_returns_storage);
    MDB_RUN_TEST(setup_db, teardown_db, contract_storage_sync_error_on_flush_returns_storage);
    MDB_RUN_TEST(setup_db, teardown_db, contract_storage_zero_erase_size_on_init_returns_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_storage_zero_write_size_on_init_returns_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_storage_write_size_one_on_init_ok);
    MDB_RUN_TEST(setup_db, teardown_db, contract_storage_write_size_unsupported_on_init_returns_invalid);
    MDB_RUN_TEST(setup_db, teardown_db, contract_wal_crc_tail_corruption_recovery_drops_tail);
    MDB_RUN_TEST(setup_db, teardown_db, contract_wal_header_crc_corruption_recovers_clean);
    return MDB_RESULT();
}
