// SPDX-License-Identifier: MIT
#include "microdb.h"
#include "microdb_port_esp32.h"

#include <string.h>

#if defined(IDF_TARGET)

#include "esp_log.h"

static const char *TAG = "microdb_demo";
static microdb_t g_db;
static microdb_storage_t g_storage;
static microdb_timestamp_t g_now = 0u;

typedef struct {
    uint32_t when;
    uint32_t sensor_id;
    float value;
    uint8_t severity;
} alarm_row_t;

static microdb_timestamp_t app_now_seconds(void) {
    return g_now;
}

static microdb_err_t build_alarm_table(microdb_table_t **out_table) {
    microdb_schema_t schema;
    microdb_err_t err;

    err = microdb_schema_init(&schema, "alarm_log", 64u);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(&schema, "when", MICRODB_COL_U32, 4u, false);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(&schema, "sensor_id", MICRODB_COL_U32, 4u, true);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(&schema, "value", MICRODB_COL_F32, 4u, false);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(&schema, "severity", MICRODB_COL_U8, 1u, false);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_seal(&schema);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_table_create(&g_db, &schema);
    if (err != MICRODB_OK && err != MICRODB_ERR_EXISTS) {
        return err;
    }
    return microdb_table_get(&g_db, "alarm_log", out_table);
}

void app_main(void) {
    microdb_cfg_t cfg;
    microdb_table_t *alarm_table = NULL;
    alarm_row_t alarm;
    float temp = 23.5f;
    float hum = 51.0f;

    memset(&cfg, 0, sizeof(cfg));
    if (microdb_port_esp32_init(&g_storage, "microdb") != MICRODB_OK) {
        ESP_LOGE(TAG, "partition 'microdb' not found");
        return;
    }

    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.now = app_now_seconds;
    if (microdb_init(&g_db, &cfg) != MICRODB_OK) {
        ESP_LOGE(TAG, "microdb_init failed");
        return;
    }

    /* KV: WiFi credentials, device config, boot count with TTL */
    (void)microdb_kv_put(&g_db, "wifi_ssid", "demo-ssid", strlen("demo-ssid"));
    (void)microdb_kv_put(&g_db, "wifi_pass", "demo-pass", strlen("demo-pass"));
    (void)microdb_kv_put(&g_db, "device_mode", "normal", strlen("normal"));
    (void)microdb_kv_set(&g_db, "boot_count", &(uint32_t){ 1u }, sizeof(uint32_t), 3600u);

    /* TS: insert rolling temperature and humidity samples every 30 seconds */
    (void)microdb_ts_register(&g_db, "temperature", MICRODB_TS_F32, 0u);
    (void)microdb_ts_register(&g_db, "humidity", MICRODB_TS_F32, 0u);
    g_now += 30u;
    (void)microdb_ts_insert(&g_db, "temperature", g_now, &temp);
    (void)microdb_ts_insert(&g_db, "humidity", g_now, &hum);

    /* REL: append an alarm row */
    if (build_alarm_table(&alarm_table) == MICRODB_OK) {
        memset(&alarm, 0, sizeof(alarm));
        alarm.when = g_now;
        alarm.sensor_id = 1u;
        alarm.value = temp;
        alarm.severity = 2u;
        (void)microdb_row_set(alarm_table, &alarm, "when", &alarm.when);
        (void)microdb_row_set(alarm_table, &alarm, "sensor_id", &alarm.sensor_id);
        (void)microdb_row_set(alarm_table, &alarm, "value", &alarm.value);
        (void)microdb_row_set(alarm_table, &alarm, "severity", &alarm.severity);
        (void)microdb_rel_insert(&g_db, alarm_table, &alarm);
    }

    /* Flush before deep sleep, then reopen on wake-up and inspect last values. */
    (void)microdb_flush(&g_db);
    (void)microdb_deinit(&g_db);
    if (microdb_init(&g_db, &cfg) == MICRODB_OK) {
        microdb_ts_sample_t sample;
        if (microdb_ts_last(&g_db, "temperature", &sample) == MICRODB_OK) {
            ESP_LOGI(TAG, "last temperature ts=%u value=%.2f", (unsigned)sample.ts, sample.v.f32);
        }
    }
}

#else

int main(void) {
    return 0;
}

#endif
