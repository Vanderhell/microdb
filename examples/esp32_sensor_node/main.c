// SPDX-License-Identifier: MIT
#include "lox.h"
#include "lox_port_esp32.h"

#include <string.h>

#if defined(IDF_TARGET)

#include "esp_log.h"

static const char *TAG = "lox_demo";
static lox_t g_db;
static lox_storage_t g_storage;
static lox_timestamp_t g_now = 0u;

typedef struct {
    uint32_t when;
    uint32_t sensor_id;
    float value;
    uint8_t severity;
} alarm_row_t;

static lox_timestamp_t app_now_seconds(void) {
    return g_now;
}

static lox_err_t build_alarm_table(lox_table_t **out_table) {
    lox_schema_t schema;
    lox_err_t err;

    err = lox_schema_init(&schema, "alarm_log", 64u);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(&schema, "when", LOX_COL_U32, 4u, false);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(&schema, "sensor_id", LOX_COL_U32, 4u, true);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(&schema, "value", LOX_COL_F32, 4u, false);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(&schema, "severity", LOX_COL_U8, 1u, false);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_seal(&schema);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_table_create(&g_db, &schema);
    if (err != LOX_OK && err != LOX_ERR_EXISTS) {
        return err;
    }
    return lox_table_get(&g_db, "alarm_log", out_table);
}

void app_main(void) {
    lox_cfg_t cfg;
    lox_table_t *alarm_table = NULL;
    alarm_row_t alarm;
    float temp = 23.5f;
    float hum = 51.0f;

    memset(&cfg, 0, sizeof(cfg));
    if (lox_port_esp32_init(&g_storage, "loxdb") != LOX_OK) {
        ESP_LOGE(TAG, "partition 'loxdb' not found");
        return;
    }

    cfg.storage = &g_storage;
    cfg.ram_kb = 32u;
    cfg.now = app_now_seconds;
    if (lox_init(&g_db, &cfg) != LOX_OK) {
        ESP_LOGE(TAG, "lox_init failed");
        return;
    }

    /* KV: WiFi credentials, device config, boot count with TTL */
    (void)lox_kv_put(&g_db, "wifi_ssid", "demo-ssid", strlen("demo-ssid"));
    (void)lox_kv_put(&g_db, "wifi_pass", "demo-pass", strlen("demo-pass"));
    (void)lox_kv_put(&g_db, "device_mode", "normal", strlen("normal"));
    (void)lox_kv_set(&g_db, "boot_count", &(uint32_t){ 1u }, sizeof(uint32_t), 3600u);

    /* TS: insert rolling temperature and humidity samples every 30 seconds */
    (void)lox_ts_register(&g_db, "temperature", LOX_TS_F32, 0u);
    (void)lox_ts_register(&g_db, "humidity", LOX_TS_F32, 0u);
    g_now += 30u;
    (void)lox_ts_insert(&g_db, "temperature", g_now, &temp);
    (void)lox_ts_insert(&g_db, "humidity", g_now, &hum);

    /* REL: append an alarm row */
    if (build_alarm_table(&alarm_table) == LOX_OK) {
        memset(&alarm, 0, sizeof(alarm));
        alarm.when = g_now;
        alarm.sensor_id = 1u;
        alarm.value = temp;
        alarm.severity = 2u;
        (void)lox_row_set(alarm_table, &alarm, "when", &alarm.when);
        (void)lox_row_set(alarm_table, &alarm, "sensor_id", &alarm.sensor_id);
        (void)lox_row_set(alarm_table, &alarm, "value", &alarm.value);
        (void)lox_row_set(alarm_table, &alarm, "severity", &alarm.severity);
        (void)lox_rel_insert(&g_db, alarm_table, &alarm);
    }

    /* Flush before deep sleep, then reopen on wake-up and inspect last values. */
    (void)lox_flush(&g_db);
    (void)lox_deinit(&g_db);
    if (lox_init(&g_db, &cfg) == LOX_OK) {
        lox_ts_sample_t sample;
        if (lox_ts_last(&g_db, "temperature", &sample) == LOX_OK) {
            ESP_LOGI(TAG, "last temperature ts=%u value=%.2f", (unsigned)sample.ts, sample.v.f32);
        }
    }
}

#else

int main(void) {
    return 0;
}

#endif
