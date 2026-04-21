// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "microdb.h"
#include "microdb_import_export.h"
#include "../port/ram/microdb_port_ram.h"

#include <string.h>

static microdb_t g_src;
static microdb_t g_dst;
static microdb_storage_t g_src_st;
static microdb_storage_t g_dst_st;

static microdb_err_t make_rel_table(microdb_t *db, const char *name, uint32_t max_rows, microdb_table_t **out) {
    microdb_schema_t s;
    microdb_err_t rc;
    rc = microdb_schema_init(&s, name, max_rows);
    if (rc != MICRODB_OK) return rc;
    rc = microdb_schema_add(&s, "id", MICRODB_COL_U32, sizeof(uint32_t), true);
    if (rc != MICRODB_OK) return rc;
    rc = microdb_schema_add(&s, "v", MICRODB_COL_U8, sizeof(uint8_t), false);
    if (rc != MICRODB_OK) return rc;
    rc = microdb_schema_seal(&s);
    if (rc != MICRODB_OK) return rc;
    rc = microdb_table_create(db, &s);
    if (rc != MICRODB_OK) return rc;
    return microdb_table_get(db, name, out);
}

static void setup_pair(void) {
    microdb_cfg_t cfg;
    ASSERT_EQ(microdb_port_ram_init(&g_src_st, 65536u), MICRODB_OK);
    ASSERT_EQ(microdb_port_ram_init(&g_dst_st, 65536u), MICRODB_OK);
    memset(&cfg, 0, sizeof(cfg));
    cfg.storage = &g_src_st;
    cfg.ram_kb = 32u;
    ASSERT_EQ(microdb_init(&g_src, &cfg), MICRODB_OK);
    cfg.storage = &g_dst_st;
    ASSERT_EQ(microdb_init(&g_dst, &cfg), MICRODB_OK);
}

static void teardown_pair(void) {
    microdb_deinit(&g_src);
    microdb_deinit(&g_dst);
    microdb_port_ram_deinit(&g_src_st);
    microdb_port_ram_deinit(&g_dst_st);
}

MDB_TEST(ie_export_import_roundtrip) {
    const char *keys[] = {"k1", "k2"};
    const uint8_t v1[] = {1u, 2u, 3u};
    const uint8_t v2[] = {0xAAu, 0xBBu};
    char payload[1024];
    size_t used = 0u;
    uint32_t exported = 0u;
    uint32_t imported = 0u;
    uint32_t skipped = 0u;
    uint8_t out[8];
    size_t out_len = 0u;

    ASSERT_EQ(microdb_kv_set(&g_src, "k1", v1, sizeof(v1), 5u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&g_src, "k2", v2, sizeof(v2), 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ie_export_kv_json(&g_src, keys, 2u, payload, sizeof(payload), &used, &exported), MICRODB_OK);
    ASSERT_GT(used, 0u);
    ASSERT_EQ(exported, 2u);
    ASSERT_EQ(strstr(payload, "\"key\":\"k2\",\"ttl\":0") != NULL, 1);

    ASSERT_EQ(microdb_ie_import_kv_json(&g_dst, payload, NULL, &imported, &skipped), MICRODB_OK);
    ASSERT_EQ(imported, 2u);
    ASSERT_EQ(skipped, 0u);

    ASSERT_EQ(microdb_kv_get(&g_dst, "k1", out, sizeof(out), &out_len), MICRODB_OK);
    ASSERT_EQ(out_len, sizeof(v1));
    ASSERT_MEM_EQ(out, v1, sizeof(v1));
    ASSERT_EQ(microdb_kv_get(&g_dst, "k2", out, sizeof(out), &out_len), MICRODB_OK);
    ASSERT_EQ(out_len, sizeof(v2));
    ASSERT_MEM_EQ(out, v2, sizeof(v2));
}

MDB_TEST(ie_import_respects_overwrite_flag) {
    const char *keys[] = {"k"};
    const uint8_t src_v[] = {9u, 9u};
    const uint8_t dst_v[] = {1u, 1u};
    char payload[512];
    size_t used = 0u;
    uint32_t exported = 0u;
    uint32_t imported = 0u;
    uint32_t skipped = 0u;
    microdb_ie_options_t opts = microdb_ie_default_options();
    uint8_t out[8];
    size_t out_len = 0u;

    ASSERT_EQ(microdb_kv_set(&g_src, "k", src_v, sizeof(src_v), 0u), MICRODB_OK);
    ASSERT_EQ(microdb_kv_set(&g_dst, "k", dst_v, sizeof(dst_v), 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ie_export_kv_json(&g_src, keys, 1u, payload, sizeof(payload), &used, &exported), MICRODB_OK);
    ASSERT_EQ(exported, 1u);

    opts.overwrite_existing = 0u;
    ASSERT_EQ(microdb_ie_import_kv_json(&g_dst, payload, &opts, &imported, &skipped), MICRODB_OK);
    ASSERT_EQ(imported, 0u);
    ASSERT_EQ(skipped, 1u);
    ASSERT_EQ(microdb_kv_get(&g_dst, "k", out, sizeof(out), &out_len), MICRODB_OK);
    ASSERT_MEM_EQ(out, dst_v, sizeof(dst_v));

    opts.overwrite_existing = 1u;
    ASSERT_EQ(microdb_ie_import_kv_json(&g_dst, payload, &opts, &imported, &skipped), MICRODB_OK);
    ASSERT_EQ(imported, 1u);
    ASSERT_EQ(microdb_kv_get(&g_dst, "k", out, sizeof(out), &out_len), MICRODB_OK);
    ASSERT_MEM_EQ(out, src_v, sizeof(src_v));
}

MDB_TEST(ie_import_invalid_payload_rejected) {
    uint32_t imported = 0u;
    uint32_t skipped = 0u;
    ASSERT_EQ(microdb_ie_import_kv_json(&g_dst, "{\"format\":\"microdb.kv.v1\",\"items\":[{\"bad\":1}]}", NULL, &imported, &skipped), MICRODB_ERR_INVALID);
}

MDB_TEST(ie_ts_roundtrip_selected_streams) {
    microdb_ie_ts_stream_desc_t streams[2];
    uint32_t v1 = 11u, v2 = 22u;
    uint8_t rawv[3] = {0xA1u, 0xB2u, 0xC3u};
    char payload[2048];
    size_t used = 0u;
    uint32_t exported = 0u;
    uint32_t imported = 0u;
    uint32_t skipped = 0u;
    microdb_ts_sample_t sample;

    ASSERT_EQ(microdb_ts_register(&g_src, "u32s", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_register(&g_src, "raws", MICRODB_TS_RAW, 3u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_src, "u32s", 10u, &v1), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_src, "u32s", 20u, &v2), MICRODB_OK);
    ASSERT_EQ(microdb_ts_insert(&g_src, "raws", 30u, rawv), MICRODB_OK);

    ASSERT_EQ(microdb_ts_register(&g_dst, "u32s", MICRODB_TS_U32, 0u), MICRODB_OK);
    ASSERT_EQ(microdb_ts_register(&g_dst, "raws", MICRODB_TS_RAW, 3u), MICRODB_OK);

    streams[0].name = "u32s";
    streams[0].type = MICRODB_TS_U32;
    streams[0].raw_size = 0u;
    streams[1].name = "raws";
    streams[1].type = MICRODB_TS_RAW;
    streams[1].raw_size = 3u;

    ASSERT_EQ(microdb_ie_export_ts_json(&g_src, streams, 2u, 0u, 100u, payload, sizeof(payload), &used, &exported), MICRODB_OK);
    ASSERT_EQ(exported, 3u);
    ASSERT_GT(used, 0u);

    ASSERT_EQ(microdb_ie_import_ts_json(&g_dst, payload, streams, 2u, NULL, &imported, &skipped), MICRODB_OK);
    ASSERT_EQ(imported, 3u);
    ASSERT_EQ(skipped, 0u);

    ASSERT_EQ(microdb_ts_last(&g_dst, "u32s", &sample), MICRODB_OK);
    ASSERT_EQ(sample.ts, 20u);
    ASSERT_EQ(sample.v.u32, 22u);
    ASSERT_EQ(microdb_ts_last(&g_dst, "raws", &sample), MICRODB_OK);
    ASSERT_EQ(sample.ts, 30u);
    ASSERT_MEM_EQ(sample.v.raw, rawv, 3u);
}

MDB_TEST(ie_rel_roundtrip_selected_tables) {
    microdb_ie_rel_table_desc_t tables[1];
    microdb_table_t *src_t = NULL;
    microdb_table_t *dst_t = NULL;
    uint8_t row[64] = {0};
    uint32_t id = 7u;
    uint8_t v = 3u;
    char payload[4096];
    size_t used = 0u;
    uint32_t exported = 0u;
    uint32_t imported = 0u;
    uint32_t skipped = 0u;
    uint8_t out[64] = {0};

    ASSERT_EQ(make_rel_table(&g_src, "r1", 16u, &src_t), MICRODB_OK);
    ASSERT_EQ(make_rel_table(&g_dst, "r1", 16u, &dst_t), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(src_t, row, "id", &id), MICRODB_OK);
    ASSERT_EQ(microdb_row_set(src_t, row, "v", &v), MICRODB_OK);
    ASSERT_EQ(microdb_rel_insert(&g_src, src_t, row), MICRODB_OK);

    tables[0].name = "r1";
    tables[0].row_size = microdb_table_row_size(src_t);

    ASSERT_EQ(microdb_ie_export_rel_json(&g_src, tables, 1u, payload, sizeof(payload), &used, &exported), MICRODB_OK);
    ASSERT_EQ(exported, 1u);
    ASSERT_GT(used, 0u);

    ASSERT_EQ(microdb_ie_import_rel_json(&g_dst, payload, tables, 1u, NULL, &imported, &skipped), MICRODB_OK);
    ASSERT_EQ(imported, 1u);
    ASSERT_EQ(skipped, 0u);

    ASSERT_EQ(microdb_rel_find_by(&g_dst, dst_t, "id", &id, out), MICRODB_OK);
    ASSERT_EQ(memcmp(out, row, tables[0].row_size), 0);
}

int main(void) {
    MDB_RUN_TEST(setup_pair, teardown_pair, ie_export_import_roundtrip);
    MDB_RUN_TEST(setup_pair, teardown_pair, ie_import_respects_overwrite_flag);
    MDB_RUN_TEST(setup_pair, teardown_pair, ie_import_invalid_payload_rejected);
    MDB_RUN_TEST(setup_pair, teardown_pair, ie_ts_roundtrip_selected_streams);
    MDB_RUN_TEST(setup_pair, teardown_pair, ie_rel_roundtrip_selected_tables);
    return MDB_RESULT();
}
