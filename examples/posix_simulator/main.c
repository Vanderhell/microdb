// SPDX-License-Identifier: MIT
#include "lox.h"
#include "lox_port_posix.h"

#include <stdio.h>
#include <string.h>

#if defined(_WIN32)
static const char *g_demo_path = "lox_demo.bin";
#else
static const char *g_demo_path = "/tmp/lox_demo.bin";
#endif

static lox_timestamp_t g_now = 1000u;

typedef struct {
    uint32_t id;
    char name[16];
} sensor_row_t;

static lox_timestamp_t demo_now(void) {
    return g_now;
}

static int fail_loxdb(const char *op, lox_err_t rc) {
    fprintf(stderr, "%s failed: %s (%d)\n", op, lox_err_to_string(rc), (int)rc);
    return 1;
}

static bool print_kv(const char *key, const void *val, size_t val_len, uint32_t ttl_remaining, void *ctx) {
    (void)ctx;
    printf("KV  key=%s len=%zu ttl_remaining=%u value=", key, val_len, ttl_remaining);
    fwrite(val, 1u, val_len, stdout);
    printf("\n");
    return true;
}

static bool print_sensor_row(const void *row_buf, void *ctx) {
    const lox_table_t *table = (const lox_table_t *)ctx;
    uint32_t id = 0u;
    char name[16] = { 0 };

    (void)lox_row_get(table, row_buf, "id", &id, NULL);
    (void)lox_row_get(table, row_buf, "name", name, NULL);
    printf("REL id=%u name=%s\n", id, name);
    return true;
}

static lox_err_t build_sensor_table(lox_t *db, lox_table_t **out_table) {
    lox_schema_t schema;
    lox_err_t err;

    err = lox_schema_init(&schema, "sensors", 32u);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(&schema, "id", LOX_COL_U32, sizeof(uint32_t), true);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_add(&schema, "name", LOX_COL_STR, 16u, false);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_schema_seal(&schema);
    if (err != LOX_OK) {
        return err;
    }
    err = lox_table_create(db, &schema);
    if (err != LOX_OK && err != LOX_ERR_EXISTS) {
        return err;
    }
    return lox_table_get(db, "sensors", out_table);
}

int main(void) {
    lox_t db;
    lox_storage_t storage;
    lox_cfg_t cfg;
    lox_table_t *table = NULL;
    lox_ts_sample_t last_temp;
    lox_ts_sample_t last_hum;
    size_t ts_count = 0u;
    uint32_t i;
    lox_err_t rc;

    lox_port_posix_remove(g_demo_path);
    memset(&db, 0, sizeof(db));
    memset(&storage, 0, sizeof(storage));
    memset(&cfg, 0, sizeof(cfg));

    rc = lox_port_posix_init(&storage, g_demo_path, 262144u);
    if (rc != LOX_OK) {
        return fail_loxdb("lox_port_posix_init", rc);
    }

    cfg.storage = &storage;
    cfg.ram_kb = 32u;
    cfg.now = demo_now;
    rc = lox_init(&db, &cfg);
    if (rc != LOX_OK) {
        return fail_loxdb("lox_init", rc);
    }

    /* 1. KV: store 5 configuration items with TTL */
    for (i = 0u; i < 5u; ++i) {
        char key[16];
        char value[32];

        snprintf(key, sizeof(key), "cfg_%u", i);
        snprintf(value, sizeof(value), "value_%u", i);
        (void)lox_kv_set(&db, key, value, strlen(value), 60u + i);
    }

    /* 2. TS: register two streams and insert 20 samples each */
    (void)lox_ts_register(&db, "temp", LOX_TS_F32, 0u);
    (void)lox_ts_register(&db, "hum", LOX_TS_F32, 0u);
    for (i = 0u; i < 20u; ++i) {
        float temp = 20.0f + (float)i * 0.5f;
        float hum = 50.0f + (float)i;

        g_now++;
        (void)lox_ts_insert(&db, "temp", g_now, &temp);
        (void)lox_ts_insert(&db, "hum", g_now, &hum);
    }

    /* 3. REL: create sensors table and insert 5 devices */
    rc = build_sensor_table(&db, &table);
    if (rc != LOX_OK) {
        return fail_loxdb("build_sensor_table", rc);
    }
    for (i = 0u; i < 5u; ++i) {
        sensor_row_t row;

        memset(&row, 0, sizeof(row));
        row.id = i + 1u;
        snprintf(row.name, sizeof(row.name), "sensor_%u", i + 1u);
        (void)lox_row_set(table, &row, "id", &row.id);
        (void)lox_row_set(table, &row, "name", row.name);
        (void)lox_rel_insert(&db, table, &row);
    }

    /* 4. Flush and shutdown */
    rc = lox_flush(&db);
    if (rc != LOX_OK) {
        return fail_loxdb("lox_flush", rc);
    }
    rc = lox_deinit(&db);
    if (rc != LOX_OK) {
        return fail_loxdb("lox_deinit", rc);
    }
    lox_port_posix_deinit(&storage);

    /* 5. Reopen and print stored state */
    memset(&db, 0, sizeof(db));
    memset(&storage, 0, sizeof(storage));
    rc = lox_port_posix_init(&storage, g_demo_path, 262144u);
    if (rc != LOX_OK) {
        return fail_loxdb("lox_port_posix_init(reopen)", rc);
    }
    cfg.storage = &storage;
    rc = lox_init(&db, &cfg);
    if (rc != LOX_OK) {
        return fail_loxdb("lox_init(reopen)", rc);
    }

    printf("Reloaded KV entries:\n");
    (void)lox_kv_iter(&db, print_kv, NULL);

    if (lox_ts_last(&db, "temp", &last_temp) == LOX_OK &&
        lox_ts_last(&db, "hum", &last_hum) == LOX_OK) {
        (void)lox_ts_count(&db, "temp", 0u, g_now, &ts_count);
        printf("TS  temp_last ts=%u value=%.2f count=%zu\n",
               (unsigned)last_temp.ts,
               last_temp.v.f32,
               ts_count);
        printf("TS  hum_last  ts=%u value=%.2f\n",
               (unsigned)last_hum.ts,
               last_hum.v.f32);
    }

    rc = build_sensor_table(&db, &table);
    if (rc == LOX_OK) {
        printf("Reloaded REL rows:\n");
        (void)lox_rel_iter(&db, table, print_sensor_row, table);
    } else {
        return fail_loxdb("build_sensor_table(reopen)", rc);
    }

    rc = lox_deinit(&db);
    if (rc != LOX_OK) {
        return fail_loxdb("lox_deinit(reopen)", rc);
    }
    lox_port_posix_deinit(&storage);
    return 0;
}
