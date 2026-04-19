// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "../port/posix/microdb_port_posix.h"
#include "../src/microdb_internal.h"

#include <string.h>

static microdb_t g_db;
static microdb_storage_t g_storage;
static const char *g_path = "wal_test.bin";
static uint32_t g_now = 1000u;

typedef struct {
    uint32_t count;
    uint32_t ids[8];
} rel_ids_t;

static microdb_timestamp_t mock_now(void) {
    return g_now;
}

static void cleanup_handles(microdb_t *db, microdb_storage_t *storage) {
    if (microdb_core_const(db)->magic == MICRODB_MAGIC) {
        (void)microdb_deinit(db);
    }
    microdb_port_posix_deinit(storage);
}

static void open_db(microdb_t *db, microdb_storage_t *storage) {
    microdb_cfg_t cfg;

    memset(db, 0, sizeof(*db));
    memset(storage, 0, sizeof(*storage));
    g_now = 1000u;
    ASSERT_EQ(microdb_port_posix_init(storage, g_path, 65536u), MICRODB_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(microdb_init(db, &cfg), MICRODB_OK);
}

static void setup_db(void) {
    microdb_port_posix_remove(g_path);
    open_db(&g_db, &g_storage);
}

static void teardown_db(void) {
    cleanup_handles(&g_db, &g_storage);
    microdb_port_posix_remove(g_path);
}

static uint32_t read_u32(microdb_storage_t *storage, uint32_t offset) {
    uint32_t value = 0u;
    if (storage->read(storage->ctx, offset, &value, sizeof(value)) != MICRODB_OK) {
        return 0u;
    }
    return value;
}

static uint8_t read_u8(microdb_storage_t *storage, uint32_t offset) {
    uint8_t value = 0u;
    if (storage->read(storage->ctx, offset, &value, sizeof(value)) != MICRODB_OK) {
        return 0u;
    }
    return value;
}

static uint32_t next_entry_offset(microdb_storage_t *storage, uint32_t entry_offset) {
    uint16_t data_len = 0u;
    if (storage->read(storage->ctx, entry_offset + 10u, &data_len, sizeof(data_len)) != MICRODB_OK) {
        return 0u;
    }
    return entry_offset + 16u + (((uint32_t)data_len + 3u) & ~3u);
}

static void reopen_after_power_loss(microdb_t *db, microdb_storage_t *storage) {
    microdb_port_posix_simulate_power_loss(storage);
    microdb_port_posix_deinit(storage);
    open_db(db, storage);
}

static void reopen_after_clean_shutdown(microdb_t *db, microdb_storage_t *storage) {
    ASSERT_EQ(microdb_deinit(db), MICRODB_OK);
    microdb_port_posix_deinit(storage);
    open_db(db, storage);
}

static void make_rel_table(microdb_t *db, microdb_table_t **out) {
    microdb_schema_t schema;

    ASSERT_EQ(microdb_schema_init(&schema, "users", 8u), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true), MICRODB_OK);
    ASSERT_EQ(microdb_schema_add(&schema, "age", MICRODB_COL_U8, sizeof(uint8_t), false), MICRODB_OK);
    ASSERT_EQ(microdb_schema_seal(&schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_create(db, &schema), MICRODB_OK);
    ASSERT_EQ(microdb_table_get(db, "users", out), MICRODB_OK);
}

static bool rel_collect_ids(const void *row_buf, void *ctx) {
    rel_ids_t *ids = (rel_ids_t *)ctx;
    uint32_t id = 0u;
    memcpy(&id, row_buf, sizeof(id));
    ids->ids[ids->count++] = id;
    return true;
}

MDB_TEST(wal_entry_written_after_kv_set) {
    uint8_t value = 7u;
    ASSERT_EQ(microdb_kv_set(&g_db, "a", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 8u), 1u);
    ASSERT_EQ(read_u32(&g_storage, 32u), 0x454E5452u);
    ASSERT_EQ(read_u8(&g_storage, 40u), 0u);
}

MDB_TEST(wal_entry_written_after_ts_insert) {
    float value = 1.5f;
    uint32_t second;
    ASSERT_EQ(microdb_ts_register(&g_db, "temp", MICRODB_TS_F32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "temp", 10u, &value), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 8u), 2u);
    second = next_entry_offset(&g_storage, 32u);
    ASSERT_EQ(read_u8(&g_storage, second + 8u), 1u);
    ASSERT_EQ(read_u8(&g_storage, second + 9u), 0u);
}

MDB_TEST(wal_entry_written_after_rel_insert) {
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 3u;
    uint8_t age = 9u;
    uint32_t second;

    make_rel_table(&g_db, &table);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 8u), 2u);
    second = next_entry_offset(&g_storage, 32u);
    ASSERT_EQ(read_u8(&g_storage, second + 8u), 2u);
    ASSERT_EQ(read_u8(&g_storage, second + 9u), 0u);
}

MDB_TEST(wal_power_loss_after_kv_set_replays) {
    uint8_t in = 99u;
    uint8_t out = 0u;

    ASSERT_EQ(microdb_kv_set(&g_db, "survive", &in, 1u, 0u), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_get(&g_db, "survive", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(out, 99u);
}

MDB_TEST(wal_power_loss_after_ts_insert_replays) {
    microdb_ts_sample_t sample;
    int32_t value = -12;

    ASSERT_EQ(microdb_ts_register(&g_db, "i32", MICRODB_TS_I32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "i32", 55u, &value), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_ts_last(&g_db, "i32", &sample), MICRODB_OK);
    ASSERT_EQ(sample.ts, 55u);
    ASSERT_EQ(sample.v.i32, -12);
}

MDB_TEST(wal_power_loss_after_rel_insert_replays) {
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint8_t out[64] = { 0 };
    uint32_t id = 10u;
    uint8_t age = 11u;

    make_rel_table(&g_db, &table);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find_by(&g_db, table, "id", &id, out), MICRODB_OK);
}

MDB_TEST(wal_crc_corruption_discards_tail) {
    uint8_t v1 = 1u, v2 = 2u, out = 0u;
    uint32_t bad = 0u;
    uint32_t second = 0u;

    ASSERT_EQ(microdb_kv_set(&g_db, "k1", &v1, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&g_db, "k2", &v2, 1u, 0u), MICRODB_OK);
    second = next_entry_offset(&g_storage, 32u);
    ASSERT_EQ(g_storage.write(g_storage.ctx, second + 12u, &bad, sizeof(bad)), MICRODB_OK);
    ASSERT_EQ(g_storage.sync(g_storage.ctx), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_get(&g_db, "k1", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(out, 1u);
    ASSERT_EQ(microdb_kv_get(&g_db, "k2", &out, 1u, NULL), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(wal_corrupt_header_magic_resets_clean) {
    uint32_t bad = 0u;
    uint8_t out = 0u;

    ASSERT_EQ(g_storage.write(g_storage.ctx, 0u, &bad, sizeof(bad)), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(read_u32(&g_storage, 0u), 0x4D44424Cu);
    ASSERT_EQ(microdb_kv_get(&g_db, "missing", &out, 1u, NULL), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(wal_corrupt_header_crc_resets) {
    uint32_t bad = 0u;
    ASSERT_EQ(g_storage.write(g_storage.ctx, 16u, &bad, sizeof(bad)), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(read_u32(&g_storage, 8u), 0u);
}

MDB_TEST(wal_partial_entry_discarded) {
    uint8_t v1 = 1u, v2 = 2u, out = 0u;
    uint16_t bad_len = 300u;
    uint32_t second = 0u;

    ASSERT_EQ(microdb_kv_set(&g_db, "a1", &v1, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&g_db, "a2", &v2, 1u, 0u), MICRODB_OK);
    second = next_entry_offset(&g_storage, 32u);
    ASSERT_EQ(g_storage.write(g_storage.ctx, second + 10u, &bad_len, sizeof(bad_len)), MICRODB_OK);
    ASSERT_EQ(g_storage.sync(g_storage.ctx), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_get(&g_db, "a1", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_kv_get(&g_db, "a2", &out, 1u, NULL), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(wal_compaction_triggers_main_pages_update) {
    uint8_t value = 1u, out = 0u;
    char key[8];
    char last_key[8] = { 0 };
    uint32_t i;

    for (i = 0u; i < 40u; ++i) {
        memset(key, 0, sizeof(key));
        key[0] = 'k';
        key[1] = (char)('0' + (char)(i / 10u));
        key[2] = (char)('0' + (char)(i % 10u));
        memcpy(last_key, key, sizeof(key));
        ASSERT_EQ(microdb_kv_set(&g_db, key, &value, 1u, 0u), MICRODB_OK);
    }
    ASSERT_EQ(microdb_compact(&g_db), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 8u), 0u);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_get(&g_db, last_key, &out, 1u, NULL), MICRODB_OK);
}

MDB_TEST(wal_compaction_resets_entry_count) {
    uint8_t value = 5u;
    char key[8];
    uint32_t i;

    for (i = 0u; i < 40u; ++i) {
        memset(key, 0, sizeof(key));
        key[0] = 'x';
        key[1] = (char)('0' + (char)(i / 10u));
        key[2] = (char)('0' + (char)(i % 10u));
        ASSERT_EQ(microdb_kv_set(&g_db, key, &value, 1u, 0u), MICRODB_OK);
    }
    ASSERT_EQ(microdb_compact(&g_db), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 8u), 0u);
}

MDB_TEST(wal_multiple_engines_replay_correctly) {
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint8_t out[64] = { 0 };
    uint32_t id = 4u;
    uint8_t age = 8u;
    uint8_t kv = 42u;
    uint32_t ts = 7u;
    microdb_ts_sample_t sample;

    ASSERT_EQ(microdb_ts_register(&g_db, "u32", MICRODB_TS_U32, 0u), MICRODB_OK);
    make_rel_table(&g_db, &table);
    ASSERT_EQ(microdb_kv_set(&g_db, "mix", &kv, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "u32", 77u, &ts), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_get(&g_db, "mix", &kv, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_ts_last(&g_db, "u32", &sample), MICRODB_OK);
    ASSERT_EQ(sample.v.u32, 7u);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find_by(&g_db, table, "id", &id, out), MICRODB_OK);
}

MDB_TEST(wal_kv_delete_replayed) {
    uint8_t value = 3u;
    ASSERT_EQ(microdb_kv_set(&g_db, "gone", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_del(&g_db, "gone"), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_exists(&g_db, "gone"), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(wal_rel_delete_replayed) {
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint32_t id = 6u;
    uint8_t age = 1u;
    rel_ids_t ids = { 0 };

    make_rel_table(&g_db, &table);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    ASSERT_EQ(microdb_rel_delete(&g_db, table, &id, NULL), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find(&g_db, table, &id, rel_collect_ids, &ids), MICRODB_OK);
    ASSERT_EQ(ids.count, 0u);
}

MDB_TEST(wal_flush_compacts_and_persists) {
    uint8_t value = 8u, out = 0u;
    ASSERT_EQ(microdb_kv_set(&g_db, "flush", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 8u), 0u);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_get(&g_db, "flush", &out, 1u, NULL), MICRODB_OK);
}

MDB_TEST(wal_deinit_compacts_before_free) {
    uint8_t value = 9u, out = 0u;
    ASSERT_EQ(microdb_kv_set(&g_db, "bye", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_deinit(&g_db), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 8u), 0u);
    microdb_port_posix_deinit(&g_storage);
    open_db(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_get(&g_db, "bye", &out, 1u, NULL), MICRODB_OK);
}

MDB_TEST(wal_ram_only_mode_skips_storage) {
    microdb_t db;
    microdb_cfg_t cfg;
    uint8_t value = 11u;
    uint8_t out = 0u;

    memset(&db, 0, sizeof(db));
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = NULL;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&db, "ram", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_get(&db, "ram", &out, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_deinit(&db), MICRODB_OK);
}

MDB_TEST(wal_clean_reinit_after_deinit_persists_all) {
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint8_t rel_out[64] = { 0 };
    uint8_t kv = 22u;
    float tsv = 2.0f;
    uint32_t id = 2u;
    uint8_t age = 4u;
    microdb_ts_sample_t sample;

    ASSERT_EQ(microdb_ts_register(&g_db, "f", MICRODB_TS_F32, 0u), MICRODB_OK);
    make_rel_table(&g_db, &table);
    ASSERT_EQ(microdb_kv_set(&g_db, "keep", &kv, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_db, "f", 12u, &tsv), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    reopen_after_clean_shutdown(&g_db, &g_storage);
    ASSERT_EQ(microdb_kv_get(&g_db, "keep", &kv, 1u, NULL), MICRODB_OK);
    ASSERT_EQ(microdb_ts_last(&g_db, "f", &sample), MICRODB_OK);
    ASSERT_EQ(sample.v.f32 == 2.0f, 1);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_find_by(&g_db, table, "id", &id, rel_out), MICRODB_OK);
}

MDB_TEST(wal_kv_ttl_persisted_after_reload) {
    uint8_t value = 33u;
    uint8_t out = 0u;

    ASSERT_EQ(microdb_kv_set(&g_db, "ttl", &value, 1u, 10u), MICRODB_OK);
    reopen_after_clean_shutdown(&g_db, &g_storage);
    g_now = 1009u;
    ASSERT_EQ(microdb_kv_get(&g_db, "ttl", &out, 1u, NULL), MICRODB_OK);
    g_now = 1011u;
    ASSERT_EQ(microdb_kv_get(&g_db, "ttl", &out, 1u, NULL), MICRODB_ERR_EXPIRED);
}

MDB_TEST(wal_kv_purge_expired_persists_deletion) {
    uint8_t value = 77u;
    uint8_t out = 0u;

    ASSERT_EQ(microdb_kv_set(&g_db, "purge_ttl", &value, 1u, 1u), MICRODB_OK);
    g_now = 2000u;
    ASSERT_EQ(microdb_kv_purge_expired(&g_db), MICRODB_OK);
    reopen_after_power_loss(&g_db, &g_storage);
    g_now = 0u;
    ASSERT_EQ(microdb_kv_get(&g_db, "purge_ttl", &out, 1u, NULL), MICRODB_ERR_NOT_FOUND);
}

MDB_TEST(wal_insufficient_storage_returns_storage) {
    microdb_t db;
    microdb_storage_t storage;
    microdb_cfg_t cfg;
    const char *small_path = "wal_small.bin";

    memset(&db, 0, sizeof(db));
    microdb_port_posix_remove(small_path);
    ASSERT_EQ(microdb_port_posix_init(&storage, small_path, 1024u), MICRODB_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &storage;
    cfg.ram_kb = 32u;
    cfg.now = mock_now;
    ASSERT_EQ(microdb_init(&db, &cfg), MICRODB_ERR_STORAGE);
    microdb_port_posix_deinit(&storage);
    microdb_port_posix_remove(small_path);
}

MDB_TEST(wal_header_entry_count_tracks_multiple_writes) {
    uint8_t value = 1u;
    ASSERT_EQ(microdb_kv_set(&g_db, "a", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&g_db, "b", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&g_db, "c", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 8u), 3u);
}

MDB_TEST(wal_sequence_advances_after_flush) {
    uint8_t value = 1u;
    uint32_t before = read_u32(&g_storage, 12u);
    ASSERT_EQ(microdb_kv_set(&g_db, "seq", &value, 1u, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_flush(&g_db), MICRODB_OK);
    ASSERT_EQ(read_u32(&g_storage, 12u), before + 1u);
}

MDB_TEST(wal_table_metadata_persisted_before_row_replay) {
    microdb_table_t *table = NULL;
    ASSERT_EQ(microdb_ts_register(&g_db, "meta", MICRODB_TS_U32, 0u), MICRODB_OK);
    make_rel_table(&g_db, &table);
    reopen_after_power_loss(&g_db, &g_storage);
    ASSERT_EQ(microdb_ts_last(&g_db, "meta", &(microdb_ts_sample_t){0}), MICRODB_ERR_NOT_FOUND);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
}

MDB_TEST(wal_rel_iter_persists_insertion_order_after_reload) {
    microdb_table_t *table = NULL;
    uint8_t row[64] = { 0 };
    uint8_t age = 1u;
    uint32_t id;
    rel_ids_t ids = { 0 };

    make_rel_table(&g_db, &table);
    for (id = 1u; id <= 3u; ++id) {
        ASSERT_EQ(microdb_row_set(table, row, "id", &id), MICRODB_OK);
        ASSERT_EQ(microdb_row_set(table, row, "age", &age), MICRODB_OK);
        ASSERT_EQ(microdb_rel_insert(&g_db, table, row), MICRODB_OK);
    }
    reopen_after_clean_shutdown(&g_db, &g_storage);
    ASSERT_EQ(microdb_table_get(&g_db, "users", &table), MICRODB_OK);
    ASSERT_EQ(microdb_rel_iter(&g_db, table, rel_collect_ids, &ids), MICRODB_OK);
    ASSERT_EQ(ids.ids[0], 1u);
    ASSERT_EQ(ids.ids[1], 2u);
    ASSERT_EQ(ids.ids[2], 3u);
}

int main(void) {
    MDB_RUN_TEST(setup_db, teardown_db, wal_entry_written_after_kv_set);
    MDB_RUN_TEST(setup_db, teardown_db, wal_entry_written_after_ts_insert);
    MDB_RUN_TEST(setup_db, teardown_db, wal_entry_written_after_rel_insert);
    MDB_RUN_TEST(setup_db, teardown_db, wal_power_loss_after_kv_set_replays);
    MDB_RUN_TEST(setup_db, teardown_db, wal_power_loss_after_ts_insert_replays);
    MDB_RUN_TEST(setup_db, teardown_db, wal_power_loss_after_rel_insert_replays);
    MDB_RUN_TEST(setup_db, teardown_db, wal_crc_corruption_discards_tail);
    MDB_RUN_TEST(setup_db, teardown_db, wal_corrupt_header_magic_resets_clean);
    MDB_RUN_TEST(setup_db, teardown_db, wal_corrupt_header_crc_resets);
    MDB_RUN_TEST(setup_db, teardown_db, wal_partial_entry_discarded);
    MDB_RUN_TEST(setup_db, teardown_db, wal_compaction_triggers_main_pages_update);
    MDB_RUN_TEST(setup_db, teardown_db, wal_compaction_resets_entry_count);
    MDB_RUN_TEST(setup_db, teardown_db, wal_multiple_engines_replay_correctly);
    MDB_RUN_TEST(setup_db, teardown_db, wal_kv_delete_replayed);
    MDB_RUN_TEST(setup_db, teardown_db, wal_rel_delete_replayed);
    MDB_RUN_TEST(setup_db, teardown_db, wal_flush_compacts_and_persists);
    MDB_RUN_TEST(setup_db, teardown_db, wal_deinit_compacts_before_free);
    MDB_RUN_TEST(setup_db, teardown_db, wal_ram_only_mode_skips_storage);
    MDB_RUN_TEST(setup_db, teardown_db, wal_clean_reinit_after_deinit_persists_all);
    MDB_RUN_TEST(setup_db, teardown_db, wal_kv_ttl_persisted_after_reload);
    MDB_RUN_TEST(setup_db, teardown_db, wal_kv_purge_expired_persists_deletion);
    MDB_RUN_TEST(setup_db, teardown_db, wal_insufficient_storage_returns_storage);
    MDB_RUN_TEST(setup_db, teardown_db, wal_header_entry_count_tracks_multiple_writes);
    MDB_RUN_TEST(setup_db, teardown_db, wal_sequence_advances_after_flush);
    MDB_RUN_TEST(setup_db, teardown_db, wal_table_metadata_persisted_before_row_replay);
    MDB_RUN_TEST(setup_db, teardown_db, wal_rel_iter_persists_insertion_order_after_reload);
    return MDB_RESULT();
}
