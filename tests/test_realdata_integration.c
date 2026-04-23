// SPDX-License-Identifier: MIT
#include "microtest.h"
#include "lox.h"
#include "lox_json_wrapper.h"
#include "lox_import_export.h"
#include "../port/ram/lox_port_ram.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static lox_t g_db;
static lox_t g_db2;
static lox_storage_t g_st1;
static lox_storage_t g_st2;
static lox_cfg_t g_cfg;
static lox_cfg_t g_cfg2;
static lox_table_t *g_event_table = NULL;
static lox_table_t *g_event_table2 = NULL;
static lox_timestamp_t g_now = 1700000000u;

static uint64_t rd_now_us(void) {
    return (uint64_t)((double)clock() * 1000000.0 / (double)CLOCKS_PER_SEC);
}

static void rd_log(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    printf("\n");
    va_end(ap);
}

static void rd_fail(const char *label, lox_err_t rc) {
    fprintf(stderr, "[RD_FAIL] %s rc=%s (%d)\n", label, lox_err_to_string(rc), (int)rc);
}

#define RD_CHECK(label, expr)                                                                                 \
    do {                                                                                                      \
        uint64_t _t0 = rd_now_us();                                                                           \
        lox_err_t _rc = (expr);                                                                           \
        uint64_t _dt = rd_now_us() - _t0;                                                                     \
        rd_log("[%-40s] rc=%-20s %6llu us", (label), lox_err_to_string(_rc), (unsigned long long)_dt);  \
        if (_rc != LOX_OK) {                                                                              \
            rd_fail((label), _rc);                                                                            \
            return false;                                                                                     \
        }                                                                                                     \
    } while (0)

#define RD_EXPECT(label, cond)                                                  \
    do {                                                                        \
        rd_log("[%-40s] expect=%s", (label), (cond) ? "OK" : "FAIL");          \
        if (!(cond)) {                                                          \
            fprintf(stderr, "[RD_EXPECT_FAIL] %s\n", (label));                  \
            return false;                                                       \
        }                                                                       \
    } while (0)

static lox_timestamp_t test_now(void) { return g_now; }

typedef struct {
    uint32_t count;
} kv_iter_ctx_t;

typedef struct {
    size_t n;
    lox_timestamp_t last_ts;
    bool ordered;
} ts_query_ctx_t;

typedef struct {
    lox_table_t *table;
    uint32_t sev3_count;
} rel_iter_ctx_t;

typedef struct {
    bool found;
    uint32_t ttl_remaining;
} ttl_check_ctx_t;

static bool kv_count_cb(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    kv_iter_ctx_t *c = (kv_iter_ctx_t *)ctx;
    (void)key;
    (void)val;
    (void)val_len;
    (void)ttl_remaining;
    c->count++;
    return true;
}

static bool ts_query_cb(const lox_ts_sample_t *s, void *ctx) {
    ts_query_ctx_t *c = (ts_query_ctx_t *)ctx;
    if (c->n > 0u && s->ts < c->last_ts) c->ordered = false;
    c->last_ts = s->ts;
    c->n++;
    return true;
}

static bool rel_sev3_iter_cb(const void *row_buf, void *ctx) {
    rel_iter_ctx_t *c = (rel_iter_ctx_t *)ctx;
    uint8_t sev = 0u;
    size_t out_len = 0u;
    if (lox_row_get(c->table, row_buf, "severity", &sev, &out_len) != LOX_OK) return false;
    if (sev == 3u) c->sev3_count++;
    return true;
}

static bool txn_ttl_iter_cb(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    ttl_check_ctx_t *c = (ttl_check_ctx_t *)ctx;
    (void)val;
    (void)val_len;
    if (strcmp(key, "txn.c") == 0) {
        c->found = true;
        c->ttl_remaining = ttl_remaining;
        return false;
    }
    return true;
}

static void test_setup(void) {
    memset(&g_cfg, 0, sizeof(g_cfg));
    memset(&g_cfg2, 0, sizeof(g_cfg2));
    g_now = 1700000000u;
    ASSERT_EQ(lox_port_ram_init(&g_st1, 256u * 1024u), LOX_OK);
    ASSERT_EQ(lox_port_ram_init(&g_st2, 256u * 1024u), LOX_OK);
    g_cfg.storage = &g_st1;
    g_cfg.ram_kb = 128u;
    g_cfg.now = test_now;
    g_cfg.kv_pct = 40u;
    g_cfg.ts_pct = 35u;
    g_cfg.rel_pct = 25u;
    g_cfg.wal_sync_mode = LOX_WAL_SYNC_ALWAYS;
    ASSERT_EQ(lox_init(&g_db, &g_cfg), LOX_OK);

    g_cfg2.storage = &g_st2;
    g_cfg2.ram_kb = 128u;
    /* Keep import-target DB on default clock to avoid TTL interaction noise during round-trip checks. */
    g_cfg2.now = NULL;
    g_cfg2.kv_pct = 40u;
    g_cfg2.ts_pct = 35u;
    g_cfg2.rel_pct = 25u;
    g_cfg2.wal_sync_mode = LOX_WAL_SYNC_ALWAYS;
    ASSERT_EQ(lox_init(&g_db2, &g_cfg2), LOX_OK);
}

static void test_teardown(void) {
    (void)lox_deinit(&g_db);
    (void)lox_deinit(&g_db2);
    lox_port_ram_deinit(&g_st1);
    lox_port_ram_deinit(&g_st2);
}

static bool scenario_a_device_config(void) {
    uint32_t u32v = 0u, out_u32 = 0u;
    int32_t i32v = 0, out_i32 = 0;
    bool out_bool = false;
    uint8_t out_u8 = 0u;
    uint16_t out_u16 = 0u;
    size_t out_len = 0u;
    char buf[64];
    char json_buf[256];
    size_t used = 0u;
    char key_out[64];
    uint8_t val_out[64];
    size_t val_len = 0u;
    uint32_t ttl_out = 0u;
    kv_iter_ctx_t kv_iter = {0u};
    lox_admission_t adm;
    char export_json[4096];
    uint32_t exported = 0u, imported = 0u, skipped = 0u;
    lox_ie_options_t opts = lox_ie_default_options();
    const char *export_keys[] = {
        "wifi.ssid", "device.id", "mqtt.broker", "sensor.interval_ms", "ota.last_version",
        "json.counter", "json.offset", "json.flag", "json.label"
    };
    static const char ssid[] = "HomeNetwork_5G";
    static const char pass[] = "Sup3rS3cr3t!";
    static const char device_id[] = "esp32s3-node-01";
    static const char hw_rev[] = "B2";
    static const char broker[] = "192.168.1.100";
    uint32_t sensor_interval = 5000u;
    uint32_t ota_version = 42u;
    uint32_t boot_count = 1u;
    uint8_t log_level = 2u;
    uint16_t retry_limit = 5u;
    uint8_t out_bin[32];

    RD_CHECK("kv_put/wifi.ssid", lox_kv_put(&g_db, "wifi.ssid", ssid, sizeof(ssid) - 1u));
    RD_CHECK("kv_put/wifi.pass", lox_kv_put(&g_db, "wifi.pass", pass, sizeof(pass) - 1u));
    RD_CHECK("kv_put/device.id", lox_kv_put(&g_db, "device.id", device_id, sizeof(device_id) - 1u));
    RD_CHECK("kv_put/device.hw_rev", lox_kv_put(&g_db, "device.hw_rev", hw_rev, sizeof(hw_rev) - 1u));
    RD_CHECK("kv_put/mqtt.broker", lox_kv_put(&g_db, "mqtt.broker", broker, sizeof(broker) - 1u));
    RD_CHECK("kv_set/sensor.interval", lox_kv_set(&g_db, "sensor.interval_ms", &sensor_interval, sizeof(sensor_interval), 0u));
    RD_CHECK("kv_set/ota.version", lox_kv_set(&g_db, "ota.last_version", &ota_version, sizeof(ota_version), 0u));
    RD_CHECK("kv_set/boot.count", lox_kv_set(&g_db, "boot.count", &boot_count, sizeof(boot_count), 2u));
    RD_CHECK("kv_set/log.level", lox_kv_set(&g_db, "log.level", &log_level, sizeof(log_level), 0u));
    RD_CHECK("kv_set/net.retry_limit", lox_kv_set(&g_db, "net.retry_limit", &retry_limit, sizeof(retry_limit), 0u));

    RD_CHECK("kv_exists/wifi.ssid", lox_kv_exists(&g_db, "wifi.ssid"));
    RD_CHECK("kv_get/wifi.ssid", lox_kv_get(&g_db, "wifi.ssid", buf, sizeof(buf), &out_len));
    RD_EXPECT("assert/wifi.ssid", out_len == (sizeof(ssid) - 1u) && memcmp(buf, ssid, out_len) == 0);

    RD_CHECK("kv_get/sensor.interval_ms", lox_kv_get(&g_db, "sensor.interval_ms", &out_u32, sizeof(out_u32), &out_len));
    RD_EXPECT("assert/sensor.interval_ms", out_len == sizeof(out_u32) && out_u32 == sensor_interval);
    RD_CHECK("kv_get/log.level", lox_kv_get(&g_db, "log.level", &out_u8, sizeof(out_u8), &out_len));
    RD_EXPECT("assert/log.level", out_len == sizeof(out_u8) && out_u8 == log_level);
    RD_CHECK("kv_get/net.retry_limit", lox_kv_get(&g_db, "net.retry_limit", &out_u16, sizeof(out_u16), &out_len));
    RD_EXPECT("assert/net.retry_limit", out_len == sizeof(out_u16) && out_u16 == retry_limit);

    RD_CHECK("kv_del/wifi.pass", lox_kv_del(&g_db, "wifi.pass"));
    {
        lox_err_t rc = lox_kv_get(&g_db, "wifi.pass", buf, sizeof(buf), &out_len);
        rd_log("[%-40s] rc=%s", "kv_get/wifi.pass.after_del", lox_err_to_string(rc));
        RD_EXPECT("assert/wifi.pass.not_found", rc == LOX_ERR_NOT_FOUND);
    }
    RD_CHECK("kv_put/wifi.pass.reinsert", lox_kv_put(&g_db, "wifi.pass", "N3wP@ss!", 8u));

    RD_CHECK("kv_iter/all", lox_kv_iter(&g_db, kv_count_cb, &kv_iter));
    RD_EXPECT("assert/kv_iter.count", kv_iter.count >= 10u);

    RD_CHECK("admit_kv_set/wifi.ssid", lox_admit_kv_set(&g_db, "wifi.ssid", 16u, &adm));
    RD_EXPECT("assert/admit_kv.status", adm.status == LOX_OK);
    rd_log("[admit_kv] avail=%lu compact=%u deterministic=%u", (unsigned long)adm.available_bytes, (unsigned)adm.would_compact,
           (unsigned)adm.deterministic_budget_ok);

    RD_CHECK("json/set_u32", lox_json_kv_set_u32(&g_db, "json.counter", 9876u, 0u));
    RD_CHECK("json/get_u32", lox_json_kv_get_u32(&g_db, "json.counter", &u32v));
    RD_EXPECT("assert/json.u32", u32v == 9876u);
    RD_CHECK("json/set_i32", lox_json_kv_set_i32(&g_db, "json.offset", -42, 0u));
    RD_CHECK("json/get_i32", lox_json_kv_get_i32(&g_db, "json.offset", &i32v));
    RD_EXPECT("assert/json.i32", i32v == -42);
    RD_CHECK("json/set_bool", lox_json_kv_set_bool(&g_db, "json.flag", true, 0u));
    RD_CHECK("json/get_bool", lox_json_kv_get_bool(&g_db, "json.flag", &out_bool));
    RD_EXPECT("assert/json.bool", out_bool == true);
    RD_CHECK("json/set_cstr", lox_json_kv_set_cstr(&g_db, "json.label", "prod-eu-west", 0u));
    RD_CHECK("json/get_cstr", lox_json_kv_get_cstr(&g_db, "json.label", buf, sizeof(buf), &out_len));
    RD_EXPECT("assert/json.cstr", out_len == strlen("prod-eu-west") && memcmp(buf, "prod-eu-west", out_len) == 0);

    out_u32 = 9876u;
    RD_CHECK("json/encode_record", lox_json_encode_kv_record("json.counter", &out_u32, sizeof(out_u32), 0u, json_buf, sizeof(json_buf), &used));
    RD_CHECK("json/decode_record", lox_json_decode_kv_record(json_buf, key_out, sizeof(key_out), val_out, sizeof(val_out), &val_len, &ttl_out));
    memcpy(&out_u32, val_out, sizeof(out_u32));
    RD_EXPECT("assert/json.decode", strcmp(key_out, "json.counter") == 0 && val_len == sizeof(uint32_t) && out_u32 == 9876u && ttl_out == 0u);

    RD_CHECK("ie/export_kv", lox_ie_export_kv_json(&g_db, export_keys, 9u, export_json, sizeof(export_json), &used, &exported));
    RD_EXPECT("assert/ie.exported", exported == 9u);

    RD_CHECK("ie/import_kv", lox_ie_import_kv_json(&g_db2, export_json, &opts, &imported, &skipped));
    RD_EXPECT("assert/ie.imported", imported == 9u && skipped == 0u);

    RD_CHECK("db2/kv_get/json.counter", lox_json_kv_get_u32(&g_db2, "json.counter", &out_u32));
    RD_EXPECT("assert/db2.json.counter", out_u32 == 9876u);

    opts.overwrite_existing = 0u;
    RD_CHECK("ie/import_kv/overwrite0", lox_ie_import_kv_json(&g_db2, export_json, &opts, &imported, &skipped));
    RD_EXPECT("assert/ie.skipped", skipped > 0u);

    opts.overwrite_existing = 1u;
    opts.skip_invalid_items = 1u;
    RD_CHECK("ie/import_kv/skip_invalid",
             lox_ie_import_kv_json(&g_db2, "{\"format\":\"loxdb.kv.v1\",\"items\":[{\"key\":\"ok\",\"ttl\":0,\"value_hex\":\"01\"},{\"bad\":1}]}", &opts, &imported, &skipped));
    RD_EXPECT("assert/ie.skip_invalid", imported == 1u && skipped == 1u);

    g_now += 3u;
    RD_CHECK("kv_purge_expired", lox_kv_purge_expired(&g_db));
    {
        lox_err_t rc = lox_kv_get(&g_db, "boot.count", out_bin, sizeof(out_bin), &out_len);
        rd_log("[%-40s] rc=%s", "kv_get/boot.count.after_expire", lox_err_to_string(rc));
        RD_EXPECT("assert/boot.count.expired", rc == LOX_ERR_NOT_FOUND);
    }

    {
        lox_kv_stats_t kvs;
        RD_CHECK("kv_stats", lox_get_kv_stats(&g_db, &kvs));
        rd_log("[kv_stats] live_keys=%lu value_bytes=%lu collisions=%lu evictions=%lu", (unsigned long)kvs.live_keys,
               (unsigned long)kvs.value_bytes_used, (unsigned long)kvs.collisions, (unsigned long)kvs.evictions);
    }
    return true;
}

static bool scenario_b_timeseries(void) {
    static const float temp_data[] = {18.5f, 19.2f, 20.1f, 21.4f, 22.8f, 23.9f, 24.3f, 24.1f, 23.5f,
                                      22.7f, 21.9f, 21.2f, 20.8f, 20.3f, 19.9f, 19.5f, 19.2f, 18.9f};
    static const float hum_data[] = {45.0f, 46.2f, 48.0f, 50.4f, 54.0f, 58.2f, 62.0f, 65.3f, 68.0f,
                                     66.9f, 64.2f, 61.0f, 58.8f, 56.4f, 55.0f, 53.8f, 52.9f, 52.0f};
    static const float pres_data[] = {1013.2f, 1012.8f, 1012.3f, 1011.9f, 1011.2f, 1010.7f, 1010.1f, 1009.8f, 1010.0f,
                                      1010.3f, 1010.7f, 1011.1f, 1011.5f, 1011.4f, 1011.2f, 1011.0f, 1010.8f, 1010.7f};
    static const int32_t rssi_data[] = {-45, -48, -52, -57, -60, -64, -71, -78, -65, -52};
    static const uint32_t uptime_data[] = {0u, 360u, 720u, 1080u, 1440u, 1800u, 2160u, 2520u, 2880u, 3240u};
    lox_ts_sample_t last_sample;
    lox_ts_sample_t sample_buf[32];
    size_t count = 0u;
    size_t out_count = 0u;
    lox_admission_t adm;
    uint32_t i;
    char json_export[12288];
    size_t used = 0u;
    uint32_t exported = 0u, imported = 0u, skipped = 0u;
    lox_ie_options_t opts = lox_ie_default_options();
    lox_ie_ts_stream_desc_t ts_descs[] = {
        {"temperature", LOX_TS_F32, 0u},
        {"humidity", LOX_TS_F32, 0u},
        {"pressure", LOX_TS_F32, 0u},
        {"rssi", LOX_TS_I32, 0u},
    };

    RD_CHECK("ts/register/temperature", lox_ts_register(&g_db, "temperature", LOX_TS_F32, 0u));
    RD_CHECK("ts/register/humidity", lox_ts_register(&g_db, "humidity", LOX_TS_F32, 0u));
    RD_CHECK("ts/register/pressure", lox_ts_register(&g_db, "pressure", LOX_TS_F32, 0u));
    RD_CHECK("ts/register/rssi", lox_ts_register(&g_db, "rssi", LOX_TS_I32, 0u));
    RD_CHECK("ts/register/uptime", lox_ts_register(&g_db, "uptime", LOX_TS_U32, 0u));

    for (i = 0u; i < 18u; ++i) {
        RD_CHECK("ts/insert/temperature", lox_ts_insert(&g_db, "temperature", 1700000000u + i * 120u, &temp_data[i]));
        RD_CHECK("ts/insert/humidity", lox_ts_insert(&g_db, "humidity", 1700000000u + i * 120u, &hum_data[i]));
        RD_CHECK("ts/insert/pressure", lox_ts_insert(&g_db, "pressure", 1700000000u + i * 120u, &pres_data[i]));
    }
    for (i = 0u; i < 10u; ++i) {
        RD_CHECK("ts/insert/rssi", lox_ts_insert(&g_db, "rssi", 1700000000u + i * 90u, &rssi_data[i]));
        RD_CHECK("ts/insert/uptime", lox_ts_insert(&g_db, "uptime", 1700000000u + i * 90u, &uptime_data[i]));
    }

    RD_CHECK("ts/last/temperature", lox_ts_last(&g_db, "temperature", &last_sample));
    RD_EXPECT("assert/ts.last", last_sample.ts == 1700000000u + (17u * 120u) && last_sample.v.f32 == 18.9f);

    RD_CHECK("ts/count/all", lox_ts_count(&g_db, "temperature", 0u, (lox_timestamp_t)-1, &count));
    RD_EXPECT("assert/ts.count.all", count == 18u);
    RD_CHECK("ts/count/range", lox_ts_count(&g_db, "temperature", 1700000000u, 1700000720u, &count));
    RD_EXPECT("assert/ts.count.range", count == 7u);

    RD_CHECK("ts/query_buf/range", lox_ts_query_buf(&g_db, "temperature", 1700000000u, 1700000360u, sample_buf, 20u, &out_count));
    RD_EXPECT("assert/ts.query_buf.count", out_count == 4u);
    RD_EXPECT("assert/ts.query_buf.first", sample_buf[0].v.f32 == temp_data[0] && sample_buf[3].v.f32 == temp_data[3]);

    {
        ts_query_ctx_t c;
        memset(&c, 0, sizeof(c));
        c.ordered = true;
        RD_CHECK("ts/query/callback", lox_ts_query(&g_db, "humidity", 1700000000u, 1700001000u, ts_query_cb, &c));
        RD_EXPECT("assert/ts.query.cb.count", c.n > 0u && c.ordered);
    }

    RD_CHECK("ts/clear/uptime", lox_ts_clear(&g_db, "uptime"));
    RD_CHECK("ts/count/uptime", lox_ts_count(&g_db, "uptime", 0u, (lox_timestamp_t)-1, &count));
    RD_EXPECT("assert/ts.clear", count == 0u);

    RD_CHECK("admit_ts_insert/temperature", lox_admit_ts_insert(&g_db, "temperature", sizeof(float), &adm));
    rd_log("[admit_ts] avail=%lu compact=%u deterministic=%u", (unsigned long)adm.available_bytes, (unsigned)adm.would_compact,
           (unsigned)adm.deterministic_budget_ok);

    RD_CHECK("ts/register/db2/temperature", lox_ts_register(&g_db2, "temperature", LOX_TS_F32, 0u));
    RD_CHECK("ts/register/db2/humidity", lox_ts_register(&g_db2, "humidity", LOX_TS_F32, 0u));
    RD_CHECK("ts/register/db2/pressure", lox_ts_register(&g_db2, "pressure", LOX_TS_F32, 0u));
    RD_CHECK("ts/register/db2/rssi", lox_ts_register(&g_db2, "rssi", LOX_TS_I32, 0u));

    RD_CHECK("ie/export_ts", lox_ie_export_ts_json(&g_db, ts_descs, 4u, 0u, (lox_timestamp_t)-1, json_export, sizeof(json_export), &used, &exported));
    RD_EXPECT("assert/ie.ts.exported", exported > 0u);
    RD_CHECK("ie/import_ts", lox_ie_import_ts_json(&g_db2, json_export, ts_descs, 4u, &opts, &imported, &skipped));
    RD_EXPECT("assert/ie.ts.import", imported == exported && skipped == 0u);

    RD_CHECK("ts/last/db2/temperature", lox_ts_last(&g_db2, "temperature", &last_sample));
    RD_EXPECT("assert/ts.last.db2", last_sample.v.f32 == 18.9f);

    {
        lox_ts_stats_t tss;
        RD_CHECK("ts_stats", lox_get_ts_stats(&g_db, &tss));
        rd_log("[ts_stats] streams=%lu retained=%lu dropped=%lu", (unsigned long)tss.stream_count, (unsigned long)tss.retained_samples,
               (unsigned long)tss.dropped_samples);
    }
    return true;
}

typedef struct {
    uint32_t id;
    uint32_t timestamp;
    uint8_t source;
    uint8_t severity;
    uint16_t code;
    char message[16];
} event_log_row_t;

static bool make_event_table(lox_t *db, lox_table_t **out) {
    lox_schema_t schema;
    RD_CHECK("rel/schema_init", lox_schema_init(&schema, "event_log", 64u));
    RD_CHECK("rel/schema_add/id", lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true));
    RD_CHECK("rel/schema_add/timestamp", lox_schema_add(&schema, "timestamp", LOX_COL_U32, sizeof(uint32_t), false));
    RD_CHECK("rel/schema_add/source", lox_schema_add(&schema, "source", LOX_COL_U8, sizeof(uint8_t), false));
    RD_CHECK("rel/schema_add/severity", lox_schema_add(&schema, "severity", LOX_COL_U8, sizeof(uint8_t), false));
    RD_CHECK("rel/schema_add/code", lox_schema_add(&schema, "code", LOX_COL_U16, sizeof(uint16_t), false));
    RD_CHECK("rel/schema_add/message", lox_schema_add(&schema, "message", LOX_COL_STR, 16u, false));
    RD_CHECK("rel/schema_seal", lox_schema_seal(&schema));
    {
        lox_err_t rc = lox_table_create(db, &schema);
        rd_log("[%-40s] rc=%s", "rel/table_create", lox_err_to_string(rc));
        if (rc != LOX_OK && rc != LOX_ERR_EXISTS) return false;
    }
    RD_CHECK("rel/table_get", lox_table_get(db, "event_log", out));
    return true;
}

static bool scenario_c_rel_events(void) {
    static const struct {
        uint32_t id, ts;
        uint8_t src, sev;
        uint16_t code;
        const char *msg;
    } events[] = {
        {1, 1700000001u, 4, 1, 0x0001u, "boot"},       {2, 1700000005u, 0, 1, 0x0100u, "sensor_init"},
        {3, 1700000010u, 1, 1, 0x0200u, "wifi_connect"}, {4, 1700000015u, 1, 2, 0x0201u, "wifi_retry"},
        {5, 1700000020u, 1, 1, 0x0202u, "wifi_ok"},    {6, 1700000025u, 2, 1, 0x0300u, "ota_check"},
        {7, 1700000030u, 0, 3, 0x0101u, "sensor_fail"}, {8, 1700000035u, 0, 1, 0x0102u, "sensor_ok"},
        {9, 1700000040u, 1, 3, 0x0203u, "mqtt_timeout"}, {10, 1700000045u, 4, 4, 0x0002u, "watchdog"},
        {11, 1700000050u, 1, 1, 0x0204u, "reconnect"}, {12, 1700000055u, 0, 1, 0x0103u, "temp_ok"},
        {13, 1700000060u, 2, 1, 0x0301u, "ota_skip"},  {14, 1700000065u, 3, 1, 0x0400u, "user_cmd"},
        {15, 1700000070u, 0, 2, 0x0104u, "temp_high"}, {16, 1700000075u, 1, 1, 0x0205u, "ping_ok"},
        {17, 1700000080u, 4, 1, 0x0003u, "heap_ok"},   {18, 1700000085u, 0, 3, 0x0105u, "sensor_err"},
        {19, 1700000090u, 1, 2, 0x0206u, "dns_slow"},  {20, 1700000095u, 4, 2, 0x0004u, "mem_warn"}};
    uint8_t row[128];
    uint8_t out_row[128];
    uint32_t count_rows = 0u;
    uint32_t deleted = 0u;
    uint32_t id = 10u;
    uint32_t id_missing = 99u;
    size_t i;
    lox_admission_t adm;
    lox_ie_rel_table_desc_t rel_descs[1];
    char json_export[12288];
    size_t used = 0u;
    uint32_t exported = 0u, imported = 0u, skipped = 0u;
    lox_ie_options_t opts = lox_ie_default_options();
    uint32_t sev3_count = 0u;

    if (!make_event_table(&g_db, &g_event_table)) return false;
    RD_CHECK("rel/clear", lox_rel_clear(&g_db, g_event_table));

    for (i = 0u; i < (sizeof(events) / sizeof(events[0])); ++i) {
        memset(row, 0, sizeof(row));
        RD_CHECK("rel/row_set/id", lox_row_set(g_event_table, row, "id", &events[i].id));
        RD_CHECK("rel/row_set/timestamp", lox_row_set(g_event_table, row, "timestamp", &events[i].ts));
        RD_CHECK("rel/row_set/source", lox_row_set(g_event_table, row, "source", &events[i].src));
        RD_CHECK("rel/row_set/severity", lox_row_set(g_event_table, row, "severity", &events[i].sev));
        RD_CHECK("rel/row_set/code", lox_row_set(g_event_table, row, "code", &events[i].code));
        RD_CHECK("rel/row_set/message", lox_row_set(g_event_table, row, "message", events[i].msg));
        RD_CHECK("rel/insert", lox_rel_insert(&g_db, g_event_table, row));
    }

    RD_CHECK("rel/count", lox_rel_count(g_event_table, &count_rows));
    RD_EXPECT("assert/rel.count20", count_rows == 20u);

    RD_CHECK("rel/find_by/id10", lox_rel_find_by(&g_db, g_event_table, "id", &id, out_row));
    {
        char msg[16] = {0};
        size_t msg_len = 0u;
        RD_CHECK("rel/row_get/message", lox_row_get(g_event_table, out_row, "message", msg, &msg_len));
        RD_EXPECT("assert/rel.id10.watchdog", strncmp(msg, "watchdog", 8u) == 0);
    }

    {
        rel_iter_ctx_t ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.table = g_event_table;
        RD_CHECK("rel/iter/sev3", lox_rel_iter(&g_db, g_event_table, rel_sev3_iter_cb, &ctx));
        sev3_count = ctx.sev3_count;
        RD_EXPECT("assert/rel.sev3.count", sev3_count == 3u);
    }

    RD_CHECK("rel/delete/id10", lox_rel_delete(&g_db, g_event_table, &id, &deleted));
    RD_EXPECT("assert/rel.delete1", deleted == 1u);
    RD_CHECK("rel/count/after_delete", lox_rel_count(g_event_table, &count_rows));
    RD_EXPECT("assert/rel.count19", count_rows == 19u);
    {
        lox_err_t rc = lox_rel_find_by(&g_db, g_event_table, "id", &id, out_row);
        rd_log("[%-40s] rc=%s", "rel/find_by/id10.after_delete", lox_err_to_string(rc));
        RD_EXPECT("assert/rel.id10.not_found", rc == LOX_ERR_NOT_FOUND);
    }

    RD_CHECK("rel/delete/id99", lox_rel_delete(&g_db, g_event_table, &id_missing, &deleted));
    RD_EXPECT("assert/rel.delete0", deleted == 0u);

    RD_CHECK("admit_rel_insert", lox_admit_rel_insert(&g_db, "event_log", lox_table_row_size(g_event_table), &adm));
    rd_log("[admit_rel] avail=%lu compact=%u deterministic=%u", (unsigned long)adm.available_bytes, (unsigned)adm.would_compact,
           (unsigned)adm.deterministic_budget_ok);

    if (!make_event_table(&g_db2, &g_event_table2)) return false;
    RD_CHECK("rel_clear/db2", lox_rel_clear(&g_db2, g_event_table2));
    rel_descs[0].name = "event_log";
    rel_descs[0].row_size = 0u; /* let export/import validate against runtime table row size */
    RD_CHECK("ie/export_rel", lox_ie_export_rel_json(&g_db, rel_descs, 1u, json_export, sizeof(json_export), &used, &exported));
    RD_EXPECT("assert/ie.rel.exported", exported == 19u);
    RD_CHECK("ie/import_rel", lox_ie_import_rel_json(&g_db2, json_export, rel_descs, 1u, &opts, &imported, &skipped));
    RD_EXPECT("assert/ie.rel.import", imported == 19u && skipped == 0u);
    RD_CHECK("rel/count/db2", lox_rel_count(g_event_table2, &count_rows));
    RD_EXPECT("assert/rel.db2.count", count_rows == 19u);
    {
        uint32_t id7 = 7u;
        RD_CHECK("rel/find_by/db2/id7", lox_rel_find_by(&g_db2, g_event_table2, "id", &id7, out_row));
    }

    {
        lox_rel_stats_t rs;
        RD_CHECK("rel_stats", lox_get_rel_stats(&g_db, &rs));
        rd_log("[rel_stats] tables=%lu rows_live=%lu rows_free=%lu", (unsigned long)rs.table_count, (unsigned long)rs.rows_live,
               (unsigned long)rs.rows_free);
    }
    return true;
}

static bool scenario_d_txn_recovery(void) {
    uint32_t a = 100u, b = 200u, c = 300u, out = 0u;
    size_t out_len = 0u;
    lox_db_stats_t dbs;
    lox_kv_stats_t kvs;
    lox_ts_stats_t tss;
    lox_rel_stats_t rs;
    lox_effective_capacity_t ec;
    lox_pressure_t p;
    size_t count = 0u;
    uint32_t rel_count = 0u;
    ttl_check_ctx_t ttl_ctx;

    RD_CHECK("txn/begin", lox_txn_begin(&g_db));
    RD_CHECK("txn/kv_set/a", lox_kv_set(&g_db, "txn.a", &a, sizeof(a), 0u));
    RD_CHECK("txn/kv_set/b", lox_kv_set(&g_db, "txn.b", &b, sizeof(b), 0u));
    RD_CHECK("txn/kv_set/c", lox_kv_set(&g_db, "txn.c", &c, sizeof(c), 60u));
    RD_CHECK("txn/commit", lox_txn_commit(&g_db));

    RD_CHECK("kv_get/txn.a", lox_kv_get(&g_db, "txn.a", &out, sizeof(out), &out_len));
    RD_EXPECT("assert/txn.a", out == 100u);
    RD_CHECK("kv_get/txn.b", lox_kv_get(&g_db, "txn.b", &out, sizeof(out), &out_len));
    RD_EXPECT("assert/txn.b", out == 200u);
    RD_CHECK("kv_get/txn.c", lox_kv_get(&g_db, "txn.c", &out, sizeof(out), &out_len));
    RD_EXPECT("assert/txn.c", out == 300u);
    memset(&ttl_ctx, 0, sizeof(ttl_ctx));
    RD_CHECK("kv_iter/txn.c.ttl", lox_kv_iter(&g_db, txn_ttl_iter_cb, &ttl_ctx));
    RD_EXPECT("assert/txn.c.ttl", ttl_ctx.found && ttl_ctx.ttl_remaining > 0u && ttl_ctx.ttl_remaining != UINT32_MAX);

    RD_CHECK("txn/begin2", lox_txn_begin(&g_db));
    out = 999u;
    RD_CHECK("txn/kv_set/undo", lox_kv_set(&g_db, "txn.undo", &out, sizeof(out), 0u));
    RD_CHECK("txn/rollback", lox_txn_rollback(&g_db));
    {
        lox_err_t rc = lox_kv_get(&g_db, "txn.undo", &out, sizeof(out), &out_len);
        rd_log("[%-40s] rc=%s", "kv_get/txn.undo.after_rollback", lox_err_to_string(rc));
        RD_EXPECT("assert/txn.undo.not_found", rc == LOX_ERR_NOT_FOUND);
    }

    RD_CHECK("flush", lox_flush(&g_db));
    RD_CHECK("deinit", lox_deinit(&g_db));
    RD_CHECK("reinit", lox_init(&g_db, &g_cfg));

    RD_CHECK("kv_get/recover/wifi.ssid", lox_kv_get(&g_db, "wifi.ssid", (uint8_t[32]){0}, 32u, &out_len));
    RD_CHECK("json/get/recover/json.counter", lox_json_kv_get_u32(&g_db, "json.counter", &out));
    RD_EXPECT("assert/recover.json.counter", out == 9876u);
    RD_CHECK("ts_count/recover/temperature", lox_ts_count(&g_db, "temperature", 0u, (lox_timestamp_t)-1, &count));
    RD_EXPECT("assert/recover.ts_count", count == 18u);
    RD_CHECK("rel/count/recover", lox_rel_count(g_event_table, &rel_count));
    RD_EXPECT("assert/recover.rel_count", rel_count == 19u);
    RD_CHECK("kv_get/recover/txn.a", lox_kv_get(&g_db, "txn.a", &out, sizeof(out), &out_len));
    RD_EXPECT("assert/recover.txn.a", out == 100u);

    RD_CHECK("db_stats", lox_get_db_stats(&g_db, &dbs));
    RD_CHECK("kv_stats", lox_get_kv_stats(&g_db, &kvs));
    RD_CHECK("ts_stats", lox_get_ts_stats(&g_db, &tss));
    RD_CHECK("rel_stats", lox_get_rel_stats(&g_db, &rs));
    RD_CHECK("eff_cap", lox_get_effective_capacity(&g_db, &ec));
    RD_CHECK("pressure", lox_get_pressure(&g_db, &p));
    rd_log("[db_stats] last_err=%s kv_live=%lu ts_retained=%lu rel_live=%lu", lox_err_to_string(dbs.last_runtime_error),
           (unsigned long)kvs.live_keys, (unsigned long)tss.retained_samples, (unsigned long)rs.rows_live);
    rd_log("[capacity] kv_entries=%lu kv_free=%lu ts_samples=%lu wal_free=%lu",
           (unsigned long)ec.kv_entries_usable, (unsigned long)ec.kv_entries_free,
           (unsigned long)ec.ts_samples_usable, (unsigned long)ec.wal_budget_free);
    rd_log("[pressure] kv=%u ts=%u rel=%u wal=%u compact=%u risk=%u",
           (unsigned)p.kv_fill_pct, (unsigned)p.ts_fill_pct, (unsigned)p.rel_fill_pct, (unsigned)p.wal_fill_pct,
           (unsigned)p.compact_pressure_pct, (unsigned)p.near_full_risk_pct);
    return true;
}

MDB_TEST(realdata_integration_full_trace) {
    if (!scenario_a_device_config()) {
        mdb_test_failures++;
        return;
    }
    if (!scenario_b_timeseries()) {
        mdb_test_failures++;
        return;
    }
    if (!scenario_c_rel_events()) {
        mdb_test_failures++;
        return;
    }
    if (!scenario_d_txn_recovery()) {
        mdb_test_failures++;
        return;
    }
}

int main(void) {
    MDB_RUN_TEST(test_setup, test_teardown, realdata_integration_full_trace);
    return MDB_RESULT();
}
