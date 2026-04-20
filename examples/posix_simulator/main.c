// SPDX-License-Identifier: MIT
#include "microdb.h"
#include "microdb_port_posix.h"

#include <stdio.h>
#include <string.h>

#if defined(_WIN32)
static const char *g_demo_path = "microdb_demo.bin";
#else
static const char *g_demo_path = "/tmp/microdb_demo.bin";
#endif

static microdb_timestamp_t g_now = 1000u;

typedef struct {
    uint32_t id;
    char name[16];
} sensor_row_t;

static microdb_timestamp_t demo_now(void) {
    return g_now;
}

static int fail_microdb(const char *op, microdb_err_t rc) {
    fprintf(stderr, "%s failed: %s (%d)\n", op, microdb_err_to_string(rc), (int)rc);
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
    const microdb_table_t *table = (const microdb_table_t *)ctx;
    uint32_t id = 0u;
    char name[16] = { 0 };

    (void)microdb_row_get(table, row_buf, "id", &id, NULL);
    (void)microdb_row_get(table, row_buf, "name", name, NULL);
    printf("REL id=%u name=%s\n", id, name);
    return true;
}

static microdb_err_t build_sensor_table(microdb_t *db, microdb_table_t **out_table) {
    microdb_schema_t schema;
    microdb_err_t err;

    err = microdb_schema_init(&schema, "sensors", 32u);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_add(&schema, "name", MICRODB_COL_STR, 16u, false);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_schema_seal(&schema);
    if (err != MICRODB_OK) {
        return err;
    }
    err = microdb_table_create(db, &schema);
    if (err != MICRODB_OK && err != MICRODB_ERR_EXISTS) {
        return err;
    }
    return microdb_table_get(db, "sensors", out_table);
}

int main(void) {
    microdb_t db;
    microdb_storage_t storage;
    microdb_cfg_t cfg;
    microdb_table_t *table = NULL;
    microdb_ts_sample_t last_temp;
    microdb_ts_sample_t last_hum;
    size_t ts_count = 0u;
    uint32_t i;
    microdb_err_t rc;

    microdb_port_posix_remove(g_demo_path);
    memset(&db, 0, sizeof(db));
    memset(&storage, 0, sizeof(storage));
    memset(&cfg, 0, sizeof(cfg));

    rc = microdb_port_posix_init(&storage, g_demo_path, 262144u);
    if (rc != MICRODB_OK) {
        return fail_microdb("microdb_port_posix_init", rc);
    }

    cfg.storage = &storage;
    cfg.ram_kb = 32u;
    cfg.now = demo_now;
    rc = microdb_init(&db, &cfg);
    if (rc != MICRODB_OK) {
        return fail_microdb("microdb_init", rc);
    }

    /* 1. KV: store 5 configuration items with TTL */
    for (i = 0u; i < 5u; ++i) {
        char key[16];
        char value[32];

        snprintf(key, sizeof(key), "cfg_%u", i);
        snprintf(value, sizeof(value), "value_%u", i);
        (void)microdb_kv_set(&db, key, value, strlen(value), 60u + i);
    }

    /* 2. TS: register two streams and insert 20 samples each */
    (void)microdb_ts_register(&db, "temp", MICRODB_TS_F32, 0u);
    (void)microdb_ts_register(&db, "hum", MICRODB_TS_F32, 0u);
    for (i = 0u; i < 20u; ++i) {
        float temp = 20.0f + (float)i * 0.5f;
        float hum = 50.0f + (float)i;

        g_now++;
        (void)microdb_ts_insert(&db, "temp", g_now, &temp);
        (void)microdb_ts_insert(&db, "hum", g_now, &hum);
    }

    /* 3. REL: create sensors table and insert 5 devices */
    rc = build_sensor_table(&db, &table);
    if (rc != MICRODB_OK) {
        return fail_microdb("build_sensor_table", rc);
    }
    for (i = 0u; i < 5u; ++i) {
        sensor_row_t row;

        memset(&row, 0, sizeof(row));
        row.id = i + 1u;
        snprintf(row.name, sizeof(row.name), "sensor_%u", i + 1u);
        (void)microdb_row_set(table, &row, "id", &row.id);
        (void)microdb_row_set(table, &row, "name", row.name);
        (void)microdb_rel_insert(&db, table, &row);
    }

    /* 4. Flush and shutdown */
    rc = microdb_flush(&db);
    if (rc != MICRODB_OK) {
        return fail_microdb("microdb_flush", rc);
    }
    rc = microdb_deinit(&db);
    if (rc != MICRODB_OK) {
        return fail_microdb("microdb_deinit", rc);
    }
    microdb_port_posix_deinit(&storage);

    /* 5. Reopen and print stored state */
    memset(&db, 0, sizeof(db));
    memset(&storage, 0, sizeof(storage));
    rc = microdb_port_posix_init(&storage, g_demo_path, 262144u);
    if (rc != MICRODB_OK) {
        return fail_microdb("microdb_port_posix_init(reopen)", rc);
    }
    cfg.storage = &storage;
    rc = microdb_init(&db, &cfg);
    if (rc != MICRODB_OK) {
        return fail_microdb("microdb_init(reopen)", rc);
    }

    printf("Reloaded KV entries:\n");
    (void)microdb_kv_iter(&db, print_kv, NULL);

    if (microdb_ts_last(&db, "temp", &last_temp) == MICRODB_OK &&
        microdb_ts_last(&db, "hum", &last_hum) == MICRODB_OK) {
        (void)microdb_ts_count(&db, "temp", 0u, g_now, &ts_count);
        printf("TS  temp_last ts=%u value=%.2f count=%zu\n",
               (unsigned)last_temp.ts,
               last_temp.v.f32,
               ts_count);
        printf("TS  hum_last  ts=%u value=%.2f\n",
               (unsigned)last_hum.ts,
               last_hum.v.f32);
    }

    rc = build_sensor_table(&db, &table);
    if (rc == MICRODB_OK) {
        printf("Reloaded REL rows:\n");
        (void)microdb_rel_iter(&db, table, print_sensor_row, table);
    } else {
        return fail_microdb("build_sensor_table(reopen)", rc);
    }

    rc = microdb_deinit(&db);
    if (rc != MICRODB_OK) {
        return fail_microdb("microdb_deinit(reopen)", rc);
    }
    microdb_port_posix_deinit(&storage);
    return 0;
}
