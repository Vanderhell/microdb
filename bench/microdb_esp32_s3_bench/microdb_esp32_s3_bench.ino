#include <Arduino.h>
#include <stdlib.h>
#include <string.h>
#if defined(ARDUINO_ARCH_ESP32)
#include "esp_heap_caps.h"
#endif

#define MICRODB_PROFILE_CORE_HIMEM 1
extern "C" {
#include "microdb.h"
}

#if defined(ARDUINO_ARCH_ESP32)
#define MDB_CONSOLE Serial0
#else
#define MDB_CONSOLE Serial
#endif

#define BENCH_STORAGE_BYTES (512u * 1024u)
#define BENCH_STORAGE_ERASE 4096u
#define BENCH_MAX_LAT_SAMPLES 2048u
#define BENCH_MAX_LAST_METRICS 24u
#define BENCH_WAL_COLD_OPS 64u

typedef struct {
  const char *name;
  uint32_t ram_kb;
  uint8_t kv_pct;
  uint8_t ts_pct;
  uint8_t rel_pct;
  uint8_t wal_threshold_pct;
  uint32_t kv_ops;
  uint32_t ts_ops;
  uint32_t rel_rows;
  uint32_t wal_ops;
  uint32_t wal_key_span;
  uint32_t wal_val_bytes;
  uint32_t pace_every_ops;
  uint32_t pace_us;
  uint32_t flush_every_ops;
} bench_profile_t;

typedef struct {
  const char *name;
  uint32_t ops;
  uint64_t total_us;
  uint64_t bytes;
  float avg_us;
  float ops_per_s;
  float mb_per_s;
  uint32_t min_us;
  uint32_t p50_us;
  uint32_t p95_us;
  uint32_t max_us;
  uint32_t max_op_approx;
  uint32_t samples;
  uint32_t spike_gt_1ms;
  uint32_t spike_gt_5ms;
  uint32_t first_spike_1ms_op;
  uint32_t first_spike_5ms_op;
  float max_over_p50;
  int32_t heap_delta;
} bench_metric_t;

typedef struct {
  uint8_t *bytes;
  size_t size;
} bench_storage_ctx_t;

static const bench_profile_t g_profiles[] = {
    {"quick", 128u, 40u, 40u, 20u, 75u, 96u, 256u, 160u, 400u, 40u, 16u, 0u, 0u, 0u},
    {"deterministic", 224u, 45u, 35u, 20u, 70u, 192u, 384u, 240u, 700u, 140u, 24u, 1u, 12u, 0u},
    {"balanced", 256u, 40u, 40u, 20u, 75u, 320u, 640u, 500u, 1200u, 200u, 32u, 0u, 0u, 0u},
    {"stress", 320u, 45u, 35u, 20u, 80u, 900u, 2400u, 1200u, 3200u, 320u, 64u, 0u, 0u, 0u},
};

static bench_storage_ctx_t g_store_ctx;
static microdb_storage_t g_storage;
static microdb_t g_db;
static size_t g_profile_idx = 2u;

static volatile uint32_t g_migrate_calls = 0u;
static volatile uint16_t g_migrate_old = 0u;
static volatile uint16_t g_migrate_new = 0u;

static uint32_t g_lat[BENCH_MAX_LAT_SAMPLES];
static bench_metric_t g_last[BENCH_MAX_LAST_METRICS];
static size_t g_last_count = 0u;
static void reset_db_and_open(bool wipe);
static bool g_paced_mode = false;

static const bench_profile_t *P(void) { return &g_profiles[g_profile_idx]; }

static uint32_t heap_free_8bit(void) {
#if defined(ARDUINO_ARCH_ESP32)
  return (uint32_t)heap_caps_get_free_size(MALLOC_CAP_8BIT);
#else
  return 0u;
#endif
}

static microdb_timestamp_t bench_now(void) { return (microdb_timestamp_t)millis(); }

static microdb_err_t st_read(void *ctx, uint32_t off, void *buf, size_t len) {
  bench_storage_ctx_t *s = (bench_storage_ctx_t *)ctx;
  if (s == NULL || s->bytes == NULL || buf == NULL || (off + len) > s->size) return MICRODB_ERR_INVALID;
  memcpy(buf, &s->bytes[off], len);
  return MICRODB_OK;
}

static microdb_err_t st_write(void *ctx, uint32_t off, const void *buf, size_t len) {
  bench_storage_ctx_t *s = (bench_storage_ctx_t *)ctx;
  if (s == NULL || s->bytes == NULL || buf == NULL || (off + len) > s->size) return MICRODB_ERR_INVALID;
  memcpy(&s->bytes[off], buf, len);
  return MICRODB_OK;
}

static microdb_err_t st_erase(void *ctx, uint32_t off) {
  bench_storage_ctx_t *s = (bench_storage_ctx_t *)ctx;
  if (s == NULL || s->bytes == NULL || off >= s->size || (off + BENCH_STORAGE_ERASE) > s->size) return MICRODB_ERR_INVALID;
  memset(&s->bytes[off], 0xFF, BENCH_STORAGE_ERASE);
  return MICRODB_OK;
}

static microdb_err_t st_sync(void *ctx) {
  (void)ctx;
  return MICRODB_OK;
}

static bool storage_alloc(void) {
  if (g_store_ctx.bytes != NULL) return true;
#if defined(ARDUINO_ARCH_ESP32)
  g_store_ctx.bytes = (uint8_t *)heap_caps_malloc(BENCH_STORAGE_BYTES, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
  if (g_store_ctx.bytes == NULL) g_store_ctx.bytes = (uint8_t *)heap_caps_malloc(BENCH_STORAGE_BYTES, MALLOC_CAP_8BIT);
#else
  g_store_ctx.bytes = (uint8_t *)malloc(BENCH_STORAGE_BYTES);
#endif
  if (g_store_ctx.bytes == NULL) return false;
  g_store_ctx.size = BENCH_STORAGE_BYTES;
  memset(g_store_ctx.bytes, 0xFF, g_store_ctx.size);
  return true;
}

static void storage_reset(void) {
  if (g_store_ctx.bytes != NULL) memset(g_store_ctx.bytes, 0xFF, g_store_ctx.size);
  memset(&g_storage, 0, sizeof(g_storage));
  g_storage.read = st_read;
  g_storage.write = st_write;
  g_storage.erase = st_erase;
  g_storage.sync = st_sync;
  g_storage.capacity = (uint32_t)g_store_ctx.size;
  g_storage.erase_size = BENCH_STORAGE_ERASE;
  g_storage.write_size = 1u;
  g_storage.ctx = &g_store_ctx;
}

static microdb_err_t on_migrate(microdb_t *db, const char *name, uint16_t old_v, uint16_t new_v) {
  (void)db;
  (void)name;
  g_migrate_calls++;
  g_migrate_old = old_v;
  g_migrate_new = new_v;
  return MICRODB_OK;
}

static microdb_err_t db_open(bool wipe, bool with_mig) {
  microdb_cfg_t cfg;
  if (wipe) storage_reset();
  memset(&cfg, 0, sizeof(cfg));
  cfg.storage = &g_storage;
  cfg.ram_kb = P()->ram_kb;
  cfg.now = bench_now;
  cfg.kv_pct = P()->kv_pct;
  cfg.ts_pct = P()->ts_pct;
  cfg.rel_pct = P()->rel_pct;
  cfg.wal_compact_auto = 0u;
  cfg.wal_compact_threshold_pct = P()->wal_threshold_pct;
  cfg.wal_sync_mode = MICRODB_WAL_SYNC_FLUSH_ONLY;
  cfg.on_migrate = with_mig ? on_migrate : NULL;
  return microdb_init(&g_db, &cfg);
}

static int cmp_u32(const void *a, const void *b) {
  uint32_t x = *(const uint32_t *)a;
  uint32_t y = *(const uint32_t *)b;
  return (x > y) - (x < y);
}

static uint32_t pct(const uint32_t *arr, uint32_t n, uint32_t p) {
  if (n == 0u) return 0u;
  return arr[((uint64_t)(n - 1u) * p) / 100u];
}

static uint32_t sample_stride(uint32_t ops) {
  if (ops == 0u || ops <= BENCH_MAX_LAT_SAMPLES) return 1u;
  return (ops + BENCH_MAX_LAT_SAMPLES - 1u) / BENCH_MAX_LAT_SAMPLES;
}

static void clear_metrics(void) {
  g_last_count = 0u;
  memset(g_last, 0, sizeof(g_last));
}

static void print_effective_capacity(void) {
  microdb_stats_t st;
  memset(&st, 0, sizeof(st));
  if (microdb_inspect(&g_db, &st) != MICRODB_OK) return;
  MDB_CONSOLE.printf("[EFFECTIVE] kv_capacity=%lu (target=%lu) wal_total=%luB\n", (unsigned long)st.kv_entries_max,
                     (unsigned long)P()->kv_ops, (unsigned long)st.wal_bytes_total);
}

static void print_phase_split(const char *name, uint32_t cold_ops, uint64_t cold_total, uint32_t steady_ops, uint64_t steady_total) {
  float cold_avg = (cold_ops > 0u) ? ((float)cold_total / (float)cold_ops) : 0.0f;
  float steady_avg = (steady_ops > 0u) ? ((float)steady_total / (float)steady_ops) : 0.0f;
  MDB_CONSOLE.printf("[PHASE] %-16s cold_ops=%lu cold_avg=%.3f us steady_ops=%lu steady_avg=%.3f us\n", name,
                     (unsigned long)cold_ops, (double)cold_avg, (unsigned long)steady_ops, (double)steady_avg);
}

static uint32_t wal_min_steady_ops(void) {
  if (strcmp(P()->name, "deterministic") == 0) return 256u;
  if (strcmp(P()->name, "quick") == 0) return 64u;
  if (strcmp(P()->name, "balanced") == 0) return 128u;
  return 256u;
}

static bool is_deterministic_profile(void) {
  return strcmp(P()->name, "deterministic") == 0;
}

static void maybe_apply_write_control(uint32_t op_index) {
  const bench_profile_t *p = P();
  if (!g_paced_mode) return;
  if (p->pace_every_ops > 0u && p->pace_us > 0u && ((op_index + 1u) % p->pace_every_ops) == 0u) {
    delayMicroseconds((unsigned int)p->pace_us);
  }
}

static void set_paced_mode(bool enabled) {
  g_paced_mode = enabled;
  MDB_CONSOLE.printf("[PACED] mode=%s\n", g_paced_mode ? "ON" : "OFF");
}

static void report_slo(const bench_metric_t *m) {
  uint32_t max_us_limit;
  uint32_t spike_5ms_limit;
  bool ok;

  if (m->ops < 64u) return;

  if (strcmp(P()->name, "deterministic") == 0) {
    max_us_limit = 5000u;
    spike_5ms_limit = 0u;
  } else if (strcmp(P()->name, "quick") == 0) {
    max_us_limit = 12000u;
    spike_5ms_limit = 2u;
  } else if (strcmp(P()->name, "balanced") == 0) {
    max_us_limit = 15000u;
    spike_5ms_limit = 12u;
  } else {
    max_us_limit = 25000u;
    spike_5ms_limit = 30u;
  }

  ok = (m->max_us <= max_us_limit) && (m->spike_gt_5ms <= spike_5ms_limit);
  if (ok) {
    MDB_CONSOLE.printf("[SLO] %-16s OK (max=%lu<=%lu, spk>5ms=%lu<=%lu)\n", m->name, (unsigned long)m->max_us,
                       (unsigned long)max_us_limit, (unsigned long)m->spike_gt_5ms, (unsigned long)spike_5ms_limit);
  } else {
    MDB_CONSOLE.printf("[SLO] %-16s WARN (max=%lu%s%lu, spk>5ms=%lu%s%lu)\n", m->name, (unsigned long)m->max_us,
                       (m->max_us <= max_us_limit) ? "<=" : ">", (unsigned long)max_us_limit, (unsigned long)m->spike_gt_5ms,
                       (m->spike_gt_5ms <= spike_5ms_limit) ? "<=" : ">", (unsigned long)spike_5ms_limit);
  }
}

static void emit_metric(const char *name, uint32_t ops, uint64_t total_us, uint64_t bytes, uint32_t *lat, uint32_t n,
                        uint32_t sample_stride_ops, uint32_t heap0, uint32_t heap1) {
  bench_metric_t m;
  float sec = (float)total_us / 1000000.0f;
  uint32_t i;
  uint32_t max_sample_idx = 0u;
  bool has_spike_1ms = false;
  bool has_spike_5ms = false;
  memset(&m, 0, sizeof(m));
  m.name = name;
  m.ops = ops;
  m.total_us = total_us;
  m.bytes = bytes;
  m.avg_us = (ops == 0u) ? 0.0f : ((float)total_us / (float)ops);
  m.ops_per_s = (sec > 0.0f) ? ((float)ops / sec) : 0.0f;
  m.mb_per_s = (sec > 0.0f) ? (((float)bytes / (1024.0f * 1024.0f)) / sec) : 0.0f;
  m.samples = n;
  m.heap_delta = (int32_t)heap1 - (int32_t)heap0;
  m.first_spike_1ms_op = 0xFFFFFFFFu;
  m.first_spike_5ms_op = 0xFFFFFFFFu;
  for (i = 0u; i < n; ++i) {
    if (lat[i] >= m.max_us) {
      m.max_us = lat[i];
      max_sample_idx = i;
    }
    if (lat[i] > 1000u) {
      m.spike_gt_1ms++;
      if (!has_spike_1ms) {
        m.first_spike_1ms_op = i * sample_stride_ops;
        has_spike_1ms = true;
      }
    }
    if (lat[i] > 5000u) {
      m.spike_gt_5ms++;
      if (!has_spike_5ms) {
        m.first_spike_5ms_op = i * sample_stride_ops;
        has_spike_5ms = true;
      }
    }
  }
  m.max_op_approx = max_sample_idx * sample_stride_ops;
  if (n > 0u) {
    qsort(lat, n, sizeof(uint32_t), cmp_u32);
    m.min_us = lat[0u];
    m.p50_us = pct(lat, n, 50u);
    m.p95_us = pct(lat, n, 95u);
    if (m.p50_us > 0u) m.max_over_p50 = (float)m.max_us / (float)m.p50_us;
  }
  MDB_CONSOLE.printf("[BENCH] %-16s total=%.3f ms avg=%.3f us p50=%lu p95=%lu min=%lu max=%lu max_op~%lu xmax/p50=%.1f spk>1ms=%lu@%lu spk>5ms=%lu@%lu ops/s=%.1f MB/s=%.3f ops=%lu samp=%lu heap_d=%ld\n",
                     m.name, (double)((float)m.total_us / 1000.0f), (double)m.avg_us, (unsigned long)m.p50_us,
                     (unsigned long)m.p95_us, (unsigned long)m.min_us, (unsigned long)m.max_us, (unsigned long)m.max_op_approx,
                     (double)m.max_over_p50, (unsigned long)m.spike_gt_1ms,
                     (unsigned long)(has_spike_1ms ? m.first_spike_1ms_op : 0u), (unsigned long)m.spike_gt_5ms,
                     (unsigned long)(has_spike_5ms ? m.first_spike_5ms_op : 0u), (double)m.ops_per_s, (double)m.mb_per_s,
                     (unsigned long)m.ops, (unsigned long)m.samples, (long)m.heap_delta);
  report_slo(&m);
  if (g_last_count < BENCH_MAX_LAST_METRICS) g_last[g_last_count++] = m;
}

static bool run_kv_bench(void) {
  microdb_stats_t st;
  uint32_t i;
  uint32_t ops;
  uint32_t stride;
  uint32_t n;
  uint64_t total;
  uint64_t bytes;
  uint64_t cold_total;
  uint64_t steady_total;
  uint32_t cold_ops;
  uint32_t steady_ops;
  uint32_t heap0, heap1;
  char key[16];
  uint32_t v;
  uint32_t out;
  size_t out_len;

  memset(&st, 0, sizeof(st));
  if (microdb_inspect(&g_db, &st) != MICRODB_OK || st.kv_entries_max == 0u) return false;

  ops = P()->kv_ops;
  if (ops > st.kv_entries_max) {
    ops = st.kv_entries_max;
    MDB_CONSOLE.printf("[KV] capped ops to capacity: %lu\n", (unsigned long)ops);
  }
  stride = sample_stride(ops);

  heap0 = heap_free_8bit();
  n = 0u;
  total = 0u;
  bytes = 0u;
  cold_total = 0u;
  steady_total = 0u;
  cold_ops = 0u;
  steady_ops = 0u;
  for (i = 0u; i < ops; ++i) {
    uint32_t t0 = micros();
    snprintf(key, sizeof(key), "k%05lu", (unsigned long)i);
    v = i + 1u;
    if (microdb_kv_put(&g_db, key, &v, sizeof(v)) != MICRODB_OK) return false;
    maybe_apply_write_control(i);
    {
      uint32_t dt = micros() - t0;
      total += dt;
      bytes += sizeof(v);
      if (i < 64u) {
        cold_total += dt;
        cold_ops++;
      } else {
        steady_total += dt;
        steady_ops++;
      }
      if ((i % stride) == 0u && n < BENCH_MAX_LAT_SAMPLES) g_lat[n++] = dt;
    }
  }
  heap1 = heap_free_8bit();
  emit_metric("kv_put", ops, total, bytes, g_lat, n, stride, heap0, heap1);
  print_phase_split("kv_put", cold_ops, cold_total, steady_ops, steady_total);

  heap0 = heap_free_8bit();
  n = 0u;
  total = 0u;
  bytes = 0u;
  cold_total = 0u;
  steady_total = 0u;
  cold_ops = 0u;
  steady_ops = 0u;
  for (i = 0u; i < ops; ++i) {
    uint32_t t0 = micros();
    snprintf(key, sizeof(key), "k%05lu", (unsigned long)i);
    out = 0u;
    out_len = 0u;
    if (microdb_kv_get(&g_db, key, &out, sizeof(out), &out_len) != MICRODB_OK) return false;
    if (out != (i + 1u) || out_len != sizeof(out)) return false;
    {
      uint32_t dt = micros() - t0;
      total += dt;
      bytes += sizeof(out);
      if (i < 64u) {
        cold_total += dt;
        cold_ops++;
      } else {
        steady_total += dt;
        steady_ops++;
      }
      if ((i % stride) == 0u && n < BENCH_MAX_LAT_SAMPLES) g_lat[n++] = dt;
    }
  }
  heap1 = heap_free_8bit();
  emit_metric("kv_get", ops, total, bytes, g_lat, n, stride, heap0, heap1);
  print_phase_split("kv_get", cold_ops, cold_total, steady_ops, steady_total);

  heap0 = heap_free_8bit();
  n = 0u;
  total = 0u;
  cold_total = 0u;
  steady_total = 0u;
  cold_ops = 0u;
  steady_ops = 0u;
  for (i = 0u; i < ops; ++i) {
    uint32_t t0 = micros();
    snprintf(key, sizeof(key), "k%05lu", (unsigned long)i);
    if (microdb_kv_del(&g_db, key) != MICRODB_OK) return false;
    maybe_apply_write_control(i);
    {
      uint32_t dt = micros() - t0;
      total += dt;
      if (i < 64u) {
        cold_total += dt;
        cold_ops++;
      } else {
        steady_total += dt;
        steady_ops++;
      }
      if ((i % stride) == 0u && n < BENCH_MAX_LAT_SAMPLES) g_lat[n++] = dt;
    }
  }
  heap1 = heap_free_8bit();
  emit_metric("kv_del", ops, total, 0u, g_lat, n, stride, heap0, heap1);
  print_phase_split("kv_del", cold_ops, cold_total, steady_ops, steady_total);
  return true;
}

static bool run_ts_bench(void) {
  uint32_t i;
  uint32_t ops = P()->ts_ops;
  uint32_t stride = sample_stride(ops);
  uint32_t n = 0u;
  uint64_t total = 0u;
  uint64_t bytes = 0u;
  uint64_t cold_total = 0u;
  uint64_t steady_total = 0u;
  uint32_t cold_ops = 0u;
  uint32_t steady_ops = 0u;
  uint32_t heap0, heap1;
  uint32_t value;
  microdb_ts_sample_t out_buf[64];
  size_t retained = 0u;
  size_t out_count = 0u;
  microdb_err_t e;

  e = microdb_ts_register(&g_db, "temp", MICRODB_TS_U32, 0u);
  if (e != MICRODB_OK && e != MICRODB_ERR_EXISTS) return false;
  (void)microdb_ts_clear(&g_db, "temp");

  heap0 = heap_free_8bit();
  for (i = 0u; i < ops; ++i) {
    uint32_t t0 = micros();
    value = i;
    if (microdb_ts_insert(&g_db, "temp", i, &value) != MICRODB_OK) return false;
    maybe_apply_write_control(i);
    {
      uint32_t dt = micros() - t0;
      total += dt;
      bytes += sizeof(value);
      if (i < 64u) {
        cold_total += dt;
        cold_ops++;
      } else {
        steady_total += dt;
        steady_ops++;
      }
      if ((i % stride) == 0u && n < BENCH_MAX_LAT_SAMPLES) g_lat[n++] = dt;
    }
  }
  heap1 = heap_free_8bit();
  emit_metric("ts_insert", ops, total, bytes, g_lat, n, stride, heap0, heap1);
  print_phase_split("ts_insert", cold_ops, cold_total, steady_ops, steady_total);
  if (microdb_ts_count(&g_db, "temp", 0u, (microdb_timestamp_t)ops, &retained) == MICRODB_OK) {
    MDB_CONSOLE.printf("[TS] target=%lu retained=%lu dropped=%lu\n", (unsigned long)ops, (unsigned long)retained,
                       (unsigned long)((ops > retained) ? (ops - retained) : 0u));
  }

  {
    uint32_t t0 = micros();
    if (microdb_ts_query_buf(&g_db, "temp", 0u, (microdb_timestamp_t)ops, out_buf, 64u, &out_count) != MICRODB_OK && out_count == 0u)
      return false;
    g_lat[0] = micros() - t0;
  }
  emit_metric("ts_query_buf", 1u, g_lat[0], out_count * sizeof(microdb_ts_sample_t), g_lat, 1u, 1u, heap_free_8bit(), heap_free_8bit());
  return true;
}

static bool g_rel_found = false;
static bool rel_find_cb(const void *row_buf, void *ctx) {
  const uint8_t *row = (const uint8_t *)row_buf;
  uint32_t *want_id = (uint32_t *)ctx;
  uint32_t got_id = 0u;
  memcpy(&got_id, row, sizeof(got_id));
  if (got_id == *want_id) g_rel_found = true;
  return false;
}

static bool run_rel_bench(void) {
  microdb_schema_t schema;
  microdb_table_t *table = NULL;
  uint8_t row[64];
  uint32_t rows = P()->rel_rows;
  uint32_t i;
  uint16_t temp_c;
  uint32_t stride = sample_stride(rows);
  uint32_t n = 0u;
  uint64_t total = 0u;
  uint64_t bytes = 0u;
  uint64_t cold_total = 0u;
  uint64_t steady_total = 0u;
  uint32_t cold_ops = 0u;
  uint32_t steady_ops = 0u;
  uint32_t heap0, heap1;
  uint32_t find_id = rows / 2u;
  uint32_t count_rows = 0u;
  microdb_err_t e;

  memset(&schema, 0, sizeof(schema));
  if (microdb_schema_init(&schema, "bench_rel", rows + 16u) != MICRODB_OK) return false;
  schema.schema_version = 1u;
  if (microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true) != MICRODB_OK) return false;
  if (microdb_schema_add(&schema, "temp", MICRODB_COL_U16, sizeof(uint16_t), false) != MICRODB_OK) return false;
  if (microdb_schema_seal(&schema) != MICRODB_OK) return false;

  e = microdb_table_create(&g_db, &schema);
  if (e != MICRODB_OK && e != MICRODB_ERR_EXISTS) return false;
  if (microdb_table_get(&g_db, "bench_rel", &table) != MICRODB_OK) return false;
  if (microdb_rel_clear(&g_db, table) != MICRODB_OK) return false;

  /* Isolate REL timing from prior stage WAL pressure in deterministic mode. */
  if (is_deterministic_profile() && microdb_flush(&g_db) != MICRODB_OK) return false;

  heap0 = heap_free_8bit();
  for (i = 0u; i < rows; ++i) {
    uint32_t t0 = micros();
    memset(row, 0, sizeof(row));
    temp_c = (uint16_t)(200u + (i % 50u));
    if (microdb_row_set(table, row, "id", &i) != MICRODB_OK) return false;
    if (microdb_row_set(table, row, "temp", &temp_c) != MICRODB_OK) return false;
    if (microdb_rel_insert(&g_db, table, row) != MICRODB_OK) return false;
    maybe_apply_write_control(i);
    {
      uint32_t dt = micros() - t0;
      total += dt;
      bytes += microdb_table_row_size(table);
      if (i < 64u) {
        cold_total += dt;
        cold_ops++;
      } else {
        steady_total += dt;
        steady_ops++;
      }
      if ((i % stride) == 0u && n < BENCH_MAX_LAT_SAMPLES) g_lat[n++] = dt;
    }
  }
  heap1 = heap_free_8bit();
  emit_metric("rel_insert", rows, total, bytes, g_lat, n, stride, heap0, heap1);
  print_phase_split("rel_insert", cold_ops, cold_total, steady_ops, steady_total);

  g_rel_found = false;
  {
    uint32_t t0 = micros();
    if (microdb_rel_find(&g_db, table, &find_id, rel_find_cb, &find_id) != MICRODB_OK) return false;
    g_lat[0] = micros() - t0;
  }
  emit_metric("rel_find(index)", 1u, g_lat[0], microdb_table_row_size(table), g_lat, 1u, 1u, heap_free_8bit(), heap_free_8bit());

  if (!g_rel_found) return false;
  if (microdb_rel_count(table, &count_rows) != MICRODB_OK) return false;
  MDB_CONSOLE.printf("[REL] rows_expected=%lu rows_actual=%lu\n", (unsigned long)rows, (unsigned long)count_rows);
  return (count_rows == rows);
}

static bool run_wal_compact_bench(void) {
  static const char *kProbeKey = "wal_probe";
  microdb_stats_t before;
  microdb_stats_t after;
  microdb_stats_t start;
  uint32_t i = 0u;
  uint32_t ops_target = P()->wal_ops;
  uint32_t ops_done = 0u;
  uint32_t key_span = P()->wal_key_span;
  uint32_t val_bytes = P()->wal_val_bytes;
  uint32_t min_steady = wal_min_steady_ops();
  uint32_t stride;
  uint32_t n = 0u;
  uint64_t total = 0u;
  uint64_t bytes = 0u;
  uint64_t cold_total = 0u;
  uint64_t steady_total = 0u;
  uint32_t cold_ops = 0u;
  uint32_t steady_ops = 0u;
  uint32_t heap0, heap1;
  char key[20];
  uint8_t val[96];
  uint8_t probe_before[12];
  uint8_t probe_after[12];
  size_t probe_len = 0u;
  uint8_t target_fill = P()->wal_threshold_pct;
  uint8_t peak_fill = 0u;
  uint32_t max_ops;

  if (key_span == 0u) key_span = 1u;
  if (val_bytes > sizeof(val)) val_bytes = sizeof(val);
  if (target_fill == 0u) target_fill = 75u;
  max_ops = (ops_target == 0u) ? 1024u : (ops_target * 8u);
  if (max_ops < (min_steady + BENCH_WAL_COLD_OPS)) max_ops = (min_steady + BENCH_WAL_COLD_OPS);
  if (max_ops < 512u) max_ops = 512u;
  stride = sample_stride(max_ops);
  memset(&before, 0, sizeof(before));
  memset(&after, 0, sizeof(after));
  memset(&start, 0, sizeof(start));
  memset(val, 0xA5, sizeof(val));
  memset(probe_before, 0x3C, sizeof(probe_before));
  memset(probe_after, 0, sizeof(probe_after));

  if (microdb_compact(&g_db) != MICRODB_OK) return false;
  if (microdb_inspect(&g_db, &start) != MICRODB_OK) return false;
  MDB_CONSOLE.printf("[WAL] baseline before warmup: used=%lu total=%lu fill=%u%%\n", (unsigned long)start.wal_bytes_used,
                     (unsigned long)start.wal_bytes_total, (unsigned)start.wal_fill_pct);
  if (start.kv_entries_max > 2u && key_span >= (start.kv_entries_max - 1u)) {
    key_span = start.kv_entries_max - 2u;
    MDB_CONSOLE.printf("[WAL] key_span adjusted to %lu to keep probe key resident.\n", (unsigned long)key_span);
  }
  if (start.wal_fill_pct > 5u) {
    MDB_CONSOLE.println("[WAL][WARN] baseline fill is not near-empty before warmup.");
  }

  if (microdb_kv_put(&g_db, kProbeKey, probe_before, sizeof(probe_before)) != MICRODB_OK) return false;

  heap0 = heap_free_8bit();
  for (i = 0u; i < max_ops; ++i) {
    uint32_t t0 = micros();
    uint32_t dt;
    uint32_t seq = i + 1u;
    uint32_t salt = (i * 2654435761u) ^ 0xA5A5A5A5u;

    /* Force real WAL growth: every write changes payload contents. */
    memset(val, (uint8_t)(0xA5u ^ (uint8_t)(i & 0xFFu)), val_bytes);
    if (val_bytes >= sizeof(seq)) memcpy(val, &seq, sizeof(seq));
    if (val_bytes >= (2u * sizeof(uint32_t))) memcpy(val + sizeof(uint32_t), &salt, sizeof(salt));

    snprintf(key, sizeof(key), "w%05lu", (unsigned long)(i % key_span));
    if (microdb_kv_put(&g_db, key, val, val_bytes) != MICRODB_OK) return false;
    maybe_apply_write_control(i);
    dt = micros() - t0;
    total += dt;
    bytes += val_bytes;
    ops_done++;
    if (ops_done <= BENCH_WAL_COLD_OPS) {
      cold_total += dt;
      cold_ops++;
    } else {
      steady_total += dt;
      steady_ops++;
    }
    if ((i % stride) == 0u && n < BENCH_MAX_LAT_SAMPLES) g_lat[n++] = dt;

    if (((ops_done % 64u) == 0u) || (ops_done == max_ops)) {
      if (microdb_inspect(&g_db, &before) != MICRODB_OK) return false;
      if (before.wal_fill_pct > peak_fill) peak_fill = before.wal_fill_pct;
      if (before.wal_fill_pct >= target_fill && steady_ops >= min_steady) break;
    }
  }
  heap1 = heap_free_8bit();
  emit_metric("wal_kv_put", ops_done, total, bytes, g_lat, n, stride, heap0, heap1);

  if (microdb_inspect(&g_db, &before) != MICRODB_OK) return false;
  MDB_CONSOLE.printf("[WAL] warmup target_fill=%u%% reached=%u%% peak=%u%% ops_done=%lu/%lu steady_ops=%lu (min=%lu)\n",
                     (unsigned)target_fill, (unsigned)before.wal_fill_pct, (unsigned)peak_fill, (unsigned long)ops_done,
                     (unsigned long)max_ops, (unsigned long)steady_ops, (unsigned long)min_steady);
  if (before.wal_fill_pct < target_fill) {
    MDB_CONSOLE.println("[WAL][WARN] target fill not reached before compact; compact metric is lighter-case.");
  }
  if (peak_fill >= target_fill && before.wal_fill_pct < target_fill) {
    MDB_CONSOLE.println("[WAL][WARN] fill crossed target earlier but dropped before compact (WAL churn).");
  }
  print_phase_split("wal_kv_put", cold_ops, cold_total, steady_ops, steady_total);

  MDB_CONSOLE.printf("[WAL] before compact: used=%lu total=%lu fill=%u%%\n", (unsigned long)before.wal_bytes_used,
                     (unsigned long)before.wal_bytes_total, (unsigned)before.wal_fill_pct);

  {
    uint32_t t0 = micros();
    if (microdb_compact(&g_db) != MICRODB_OK) return false;
    g_lat[0] = micros() - t0;
  }
  emit_metric("compact", 1u, g_lat[0], 0u, g_lat, 1u, 1u, heap_free_8bit(), heap_free_8bit());

  if (microdb_inspect(&g_db, &after) != MICRODB_OK) return false;
  MDB_CONSOLE.printf("[WAL] after compact:  used=%lu total=%lu fill=%u%%\n", (unsigned long)after.wal_bytes_used,
                     (unsigned long)after.wal_bytes_total, (unsigned)after.wal_fill_pct);
  if (microdb_kv_get(&g_db, kProbeKey, probe_after, sizeof(probe_after), &probe_len) != MICRODB_OK) return false;
  if (probe_len != sizeof(probe_before) || memcmp(probe_before, probe_after, sizeof(probe_before)) != 0) {
    MDB_CONSOLE.println("[WAL][ERR] probe key mismatch after compact.");
    return false;
  }
  return after.wal_bytes_used <= before.wal_bytes_used;
}

static bool run_reopen_check(void) {
  microdb_stats_t st;
  if (microdb_deinit(&g_db) != MICRODB_OK) return false;
  {
    uint32_t t0 = micros();
    if (db_open(false, false) != MICRODB_OK) return false;
    g_lat[0] = micros() - t0;
  }
  if (microdb_inspect(&g_db, &st) != MICRODB_OK) return false;
  emit_metric("reopen", 1u, g_lat[0], 0u, g_lat, 1u, 1u, heap_free_8bit(), heap_free_8bit());
  return true;
}

static bool run_migration_check(void) {
  microdb_schema_t schema;
  microdb_table_t *table = NULL;

  g_migrate_calls = 0u;
  g_migrate_old = 0u;
  g_migrate_new = 0u;

  if (microdb_deinit(&g_db) != MICRODB_OK) return false;
  if (db_open(false, false) != MICRODB_OK) return false;

  memset(&schema, 0, sizeof(schema));
  if (microdb_schema_init(&schema, "migr_tbl", 16u) != MICRODB_OK) return false;
  schema.schema_version = 1u;
  if (microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true) != MICRODB_OK) return false;
  if (microdb_schema_seal(&schema) != MICRODB_OK) return false;
  if (microdb_table_create(&g_db, &schema) != MICRODB_OK) return false;
  if (microdb_table_get(&g_db, "migr_tbl", &table) != MICRODB_OK) return false;
  (void)table;

  if (microdb_deinit(&g_db) != MICRODB_OK) return false;
  if (db_open(false, true) != MICRODB_OK) return false;

  memset(&schema, 0, sizeof(schema));
  if (microdb_schema_init(&schema, "migr_tbl", 16u) != MICRODB_OK) return false;
  schema.schema_version = 2u;
  if (microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true) != MICRODB_OK) return false;
  if (microdb_schema_seal(&schema) != MICRODB_OK) return false;
  if (microdb_table_create(&g_db, &schema) != MICRODB_OK) return false;

  MDB_CONSOLE.printf("[MIGRATE] calls=%lu old=%u new=%u\n", (unsigned long)g_migrate_calls, (unsigned)g_migrate_old,
                     (unsigned)g_migrate_new);
  return (g_migrate_calls == 1u && g_migrate_old == 1u && g_migrate_new == 2u);
}

static bool run_txn_check(void) {
  uint32_t v1 = 111u, v2 = 222u, out = 0u;
  if (microdb_txn_begin(&g_db) != MICRODB_OK) return false;
  if (microdb_kv_put(&g_db, "txn_a", &v1, sizeof(v1)) != MICRODB_OK) return false;
  if (microdb_txn_commit(&g_db) != MICRODB_OK) return false;
  if (microdb_kv_get(&g_db, "txn_a", &out, sizeof(out), NULL) != MICRODB_OK || out != v1) return false;

  if (microdb_txn_begin(&g_db) != MICRODB_OK) return false;
  if (microdb_kv_put(&g_db, "txn_a", &v2, sizeof(v2)) != MICRODB_OK) return false;
  if (microdb_txn_rollback(&g_db) != MICRODB_OK) return false;
  out = 0u;
  if (microdb_kv_get(&g_db, "txn_a", &out, sizeof(out), NULL) != MICRODB_OK || out != v1) return false;
  return true;
}

static void print_stats_snapshot(void) {
  microdb_stats_t st;
  if (microdb_inspect(&g_db, &st) != MICRODB_OK) {
    MDB_CONSOLE.println("[STATS] inspect failed");
    return;
  }
  MDB_CONSOLE.printf("[STATS] kv=%lu/%lu (%u%%) coll=%lu evict=%lu\n", (unsigned long)st.kv_entries_used,
                     (unsigned long)st.kv_entries_max, (unsigned)st.kv_fill_pct, (unsigned long)st.kv_collision_count,
                     (unsigned long)st.kv_eviction_count);
  MDB_CONSOLE.printf("[STATS] ts_streams=%lu ts_samples=%lu ts_fill=%u%%\n", (unsigned long)st.ts_streams_registered,
                     (unsigned long)st.ts_samples_total, (unsigned)st.ts_fill_pct);
  MDB_CONSOLE.printf("[STATS] wal=%lu/%lu (%u%%) rel_tables=%lu rel_rows=%lu\n", (unsigned long)st.wal_bytes_used,
                     (unsigned long)st.wal_bytes_total, (unsigned)st.wal_fill_pct, (unsigned long)st.rel_tables_count,
                     (unsigned long)st.rel_rows_total);
}

static void print_config(void) {
  MDB_CONSOLE.printf("[CONFIG] profile=%s storage=%luKB ram=%luKB split=%u/%u/%u wal_thr=%u%%\n", P()->name,
                     (unsigned long)(BENCH_STORAGE_BYTES / 1024u), (unsigned long)P()->ram_kb, (unsigned)P()->kv_pct,
                     (unsigned)P()->ts_pct, (unsigned)P()->rel_pct, (unsigned)P()->wal_threshold_pct);
  MDB_CONSOLE.printf("[CONFIG] target_kv=%lu target_ts=%lu target_rel=%lu wal_ops=%lu wal_key=%lu wal_val=%lu\n",
                     (unsigned long)P()->kv_ops, (unsigned long)P()->ts_ops, (unsigned long)P()->rel_rows,
                     (unsigned long)P()->wal_ops, (unsigned long)P()->wal_key_span, (unsigned long)P()->wal_val_bytes);
  MDB_CONSOLE.printf("[CONFIG] paced=%s pace_every=%lu pace_us=%lu flush_every=%lu\n", g_paced_mode ? "ON" : "OFF",
                     (unsigned long)P()->pace_every_ops, (unsigned long)P()->pace_us, (unsigned long)P()->flush_every_ops);
  print_effective_capacity();
}

static void print_profiles(void) {
  size_t i;
  MDB_CONSOLE.println("Profiles:");
  for (i = 0u; i < (sizeof(g_profiles) / sizeof(g_profiles[0])); ++i) {
    MDB_CONSOLE.printf("  %-13s ram=%lu kv=%lu ts=%lu rel=%lu wal=%lu%s\n", g_profiles[i].name,
                       (unsigned long)g_profiles[i].ram_kb, (unsigned long)g_profiles[i].kv_ops,
                       (unsigned long)g_profiles[i].ts_ops, (unsigned long)g_profiles[i].rel_rows,
                       (unsigned long)g_profiles[i].wal_ops, (i == g_profile_idx) ? " <active>" : "");
  }
}

static bool set_profile(const char *name) {
  size_t i;
  for (i = 0u; i < (sizeof(g_profiles) / sizeof(g_profiles[0])); ++i) {
    if (strcmp(name, g_profiles[i].name) == 0) {
      g_profile_idx = i;
      g_paced_mode = (strcmp(name, "deterministic") == 0);
      return true;
    }
  }
  return false;
}

static bool try_profile_shortcut(const char *cmd) {
  if (set_profile(cmd)) {
    MDB_CONSOLE.printf("[PROFILE] switched to %s paced=%s\n", P()->name, g_paced_mode ? "ON" : "OFF");
    reset_db_and_open(true);
    return true;
  }
  return false;
}

static void print_last_metrics(void) {
  size_t i;
  if (g_last_count == 0u) {
    MDB_CONSOLE.println("[METRICS] no metrics captured yet");
    return;
  }
  MDB_CONSOLE.printf("[METRICS] count=%lu\n", (unsigned long)g_last_count);
  for (i = 0u; i < g_last_count; ++i) {
    MDB_CONSOLE.printf("[METRIC] %s total=%.3fms avg=%.3fus p50=%lu p95=%lu max=%lu@%lu xmax/p50=%.1f spk>1ms=%lu@%lu spk>5ms=%lu@%lu ops/s=%.1f MB/s=%.3f heap_d=%ld\n", g_last[i].name,
                       (double)((float)g_last[i].total_us / 1000.0f), (double)g_last[i].avg_us,
                       (unsigned long)g_last[i].p50_us, (unsigned long)g_last[i].p95_us, (unsigned long)g_last[i].max_us,
                       (unsigned long)g_last[i].max_op_approx, (double)g_last[i].max_over_p50, (unsigned long)g_last[i].spike_gt_1ms,
                       (unsigned long)((g_last[i].first_spike_1ms_op == 0xFFFFFFFFu) ? 0u : g_last[i].first_spike_1ms_op),
                       (unsigned long)g_last[i].spike_gt_5ms,
                       (unsigned long)((g_last[i].first_spike_5ms_op == 0xFFFFFFFFu) ? 0u : g_last[i].first_spike_5ms_op), (double)g_last[i].ops_per_s,
                       (double)g_last[i].mb_per_s, (long)g_last[i].heap_delta);
  }
}

static bool run_real_data_suite(void) {
  const char *first_fail = NULL;
  microdb_table_t *table = NULL;
  microdb_schema_t schema;
  uint32_t u32 = 0u;
  uint32_t v_5000 = 5000u;
  uint32_t v_1 = 1u;
  uint32_t v_100 = 100u;
  uint32_t v_999 = 999u;
  uint8_t sev_3 = 3u;
  float tf1 = 18.5f;
  float tf2 = 19.2f;
  size_t out_len = 0u;
  microdb_ts_sample_t ts_last;
  size_t ts_count = 0u;
  uint32_t rel_count = 0u;
  uint32_t deleted = 0u;
  uint8_t row[64];
  uint8_t out_row[64];
  microdb_db_stats_t dbs;
  microdb_kv_stats_t kvs;
  microdb_ts_stats_t tss;
  microdb_rel_stats_t rs;
  microdb_effective_capacity_t ec;
  microdb_pressure_t p;
  microdb_admission_t adm;

#define RD_CHECK_REAL(label, expr)                                                                                                 \
  do {                                                                                                                             \
    uint32_t _t0 = micros();                                                                                                       \
    microdb_err_t _rc = (expr);                                                                                                    \
    uint32_t _dt = micros() - _t0;                                                                                                 \
    MDB_CONSOLE.printf("[RD][%-30s] rc=%s (%d) %lu us\n", (label), microdb_err_to_string(_rc), (int)_rc, (unsigned long)_dt);   \
    if (_rc != MICRODB_OK) {                                                                                                       \
      first_fail = (label);                                                                                                        \
      goto rd_fail;                                                                                                                \
    }                                                                                                                              \
  } while (0)

#define RD_EXPECT_REAL(label, cond)                                                                                                \
  do {                                                                                                                             \
    bool _ok = (cond);                                                                                                             \
    MDB_CONSOLE.printf("[RD][%-30s] expect=%s\n", (label), _ok ? "OK" : "FAIL");                                                 \
    if (!_ok) {                                                                                                                    \
      first_fail = (label);                                                                                                        \
      goto rd_fail;                                                                                                                \
    }                                                                                                                              \
  } while (0)

  RD_CHECK_REAL("kv_put/wifi.ssid", microdb_kv_put(&g_db, "wifi.ssid", "HomeNetwork_5G", 14u));
  RD_CHECK_REAL("kv_set/interval", microdb_kv_set(&g_db, "sensor.interval_ms", &v_5000, sizeof(uint32_t), 0u));
  RD_CHECK_REAL("kv_set/boot.count", microdb_kv_set(&g_db, "boot.count", &v_1, sizeof(uint32_t), 2u));
  RD_CHECK_REAL("kv_get/interval", microdb_kv_get(&g_db, "sensor.interval_ms", &u32, sizeof(u32), &out_len));
  RD_EXPECT_REAL("assert/interval", u32 == 5000u && out_len == sizeof(uint32_t));
  RD_CHECK_REAL("kv_del/boot.count", microdb_kv_del(&g_db, "boot.count"));
  RD_CHECK_REAL("admit_kv_set", microdb_admit_kv_set(&g_db, "wifi.ssid", 16u, &adm));

  RD_CHECK_REAL("ts_register/temp", microdb_ts_register(&g_db, "temperature", MICRODB_TS_F32, 0u));
  RD_CHECK_REAL("ts_insert/t1", microdb_ts_insert(&g_db, "temperature", 1700000000u, &tf1));
  RD_CHECK_REAL("ts_insert/t2", microdb_ts_insert(&g_db, "temperature", 1700000120u, &tf2));
  RD_CHECK_REAL("ts_last/temp", microdb_ts_last(&g_db, "temperature", &ts_last));
  RD_EXPECT_REAL("assert/ts_last", ts_last.ts == 1700000120u);
  RD_CHECK_REAL("ts_count/temp", microdb_ts_count(&g_db, "temperature", 0u, (microdb_timestamp_t)0xFFFFFFFFu, &ts_count));
  RD_EXPECT_REAL("assert/ts_count", ts_count >= 2u);

  memset(&schema, 0, sizeof(schema));
  RD_CHECK_REAL("rel_schema_init", microdb_schema_init(&schema, "event_log", 16u));
  RD_CHECK_REAL("rel_schema_add/id", microdb_schema_add(&schema, "id", MICRODB_COL_U32, sizeof(uint32_t), true));
  RD_CHECK_REAL("rel_schema_add/sev", microdb_schema_add(&schema, "severity", MICRODB_COL_U8, sizeof(uint8_t), false));
  RD_CHECK_REAL("rel_schema_seal", microdb_schema_seal(&schema));
  {
    microdb_err_t rc = microdb_table_create(&g_db, &schema);
    MDB_CONSOLE.printf("[RD][%-30s] rc=%s (%d)\n", "rel_table_create", microdb_err_to_string(rc), (int)rc);
    if (rc != MICRODB_OK && rc != MICRODB_ERR_EXISTS) {
      first_fail = "rel_table_create";
      goto rd_fail;
    }
  }
  RD_CHECK_REAL("rel_table_get", microdb_table_get(&g_db, "event_log", &table));
  RD_CHECK_REAL("rel_clear", microdb_rel_clear(&g_db, table));
  memset(row, 0, sizeof(row));
  RD_CHECK_REAL("rel_row_set/id", microdb_row_set(table, row, "id", &v_1));
  RD_CHECK_REAL("rel_row_set/sev", microdb_row_set(table, row, "severity", &sev_3));
  RD_CHECK_REAL("rel_insert", microdb_rel_insert(&g_db, table, row));
  RD_CHECK_REAL("rel_find_by/id", microdb_rel_find_by(&g_db, table, "id", &v_1, out_row));
  RD_CHECK_REAL("rel_count", microdb_rel_count(table, &rel_count));
  RD_EXPECT_REAL("assert/rel_count", rel_count == 1u);
  RD_CHECK_REAL("rel_delete/id", microdb_rel_delete(&g_db, table, &v_1, &deleted));
  RD_EXPECT_REAL("assert/rel_delete", deleted == 1u);
  RD_CHECK_REAL("admit_rel_insert", microdb_admit_rel_insert(&g_db, "event_log", microdb_table_row_size(table), &adm));

  RD_CHECK_REAL("txn_begin", microdb_txn_begin(&g_db));
  RD_CHECK_REAL("txn_set/a", microdb_kv_set(&g_db, "txn.a", &v_100, sizeof(uint32_t), 0u));
  RD_CHECK_REAL("txn_commit", microdb_txn_commit(&g_db));
  RD_CHECK_REAL("txn_begin2", microdb_txn_begin(&g_db));
  RD_CHECK_REAL("txn_set/undo", microdb_kv_set(&g_db, "txn.undo", &v_999, sizeof(uint32_t), 0u));
  RD_CHECK_REAL("txn_rollback", microdb_txn_rollback(&g_db));

  RD_CHECK_REAL("flush", microdb_flush(&g_db));
  RD_CHECK_REAL("deinit", microdb_deinit(&g_db));
  RD_CHECK_REAL("reinit", db_open(false, false));
  RD_CHECK_REAL("recover/kv_get", microdb_kv_get(&g_db, "wifi.ssid", row, sizeof(row), &out_len));
  RD_CHECK_REAL("recover/ts_count", microdb_ts_count(&g_db, "temperature", 0u, (microdb_timestamp_t)0xFFFFFFFFu, &ts_count));
  RD_CHECK_REAL("db_stats", microdb_get_db_stats(&g_db, &dbs));
  RD_CHECK_REAL("kv_stats", microdb_get_kv_stats(&g_db, &kvs));
  RD_CHECK_REAL("ts_stats", microdb_get_ts_stats(&g_db, &tss));
  RD_CHECK_REAL("rel_stats", microdb_get_rel_stats(&g_db, &rs));
  RD_CHECK_REAL("eff_cap", microdb_get_effective_capacity(&g_db, &ec));
  RD_CHECK_REAL("pressure", microdb_get_pressure(&g_db, &p));

  MDB_CONSOLE.println("[REAL_DATA] PASS");
  return true;

rd_fail:
  MDB_CONSOLE.printf("[REAL_DATA] FAIL: %s\n", first_fail != NULL ? first_fail : "unknown");
  return false;

#undef RD_CHECK_REAL
#undef RD_EXPECT_REAL
}

static void run_full_bench_once(void) {
  bool ok;
  bool stage_flush = g_paced_mode || is_deterministic_profile();
  microdb_err_t deinit_rc;
  microdb_err_t open_rc;
  clear_metrics();
  MDB_CONSOLE.println();
  MDB_CONSOLE.printf("=== microdb ESP32-S3 benchmark start (profile=%s) ===\n", P()->name);

  deinit_rc = microdb_deinit(&g_db);
  open_rc = db_open(true, false);
  if (deinit_rc != MICRODB_OK || open_rc != MICRODB_OK) {
    MDB_CONSOLE.printf("[ERR] pre-run reset/open failed: deinit=%s (%d), open=%s (%d)\n",
                       microdb_err_to_string(deinit_rc),
                       (int)deinit_rc,
                       microdb_err_to_string(open_rc),
                       (int)open_rc);
    return;
  }
  print_effective_capacity();

  ok = run_kv_bench();
  MDB_CONSOLE.printf("[CHECK] KV benchmark: %s\n", ok ? "PASS" : "FAIL");
  if (!ok) return;
  if (stage_flush) (void)microdb_flush(&g_db);
  ok = run_ts_bench();
  MDB_CONSOLE.printf("[CHECK] TS benchmark: %s\n", ok ? "PASS" : "FAIL");
  if (!ok) return;
  if (stage_flush) (void)microdb_flush(&g_db);
  ok = run_rel_bench();
  MDB_CONSOLE.printf("[CHECK] REL benchmark: %s\n", ok ? "PASS" : "FAIL");
  if (!ok) return;
  if (stage_flush) (void)microdb_flush(&g_db);
  ok = run_wal_compact_bench();
  MDB_CONSOLE.printf("[CHECK] WAL compact: %s\n", ok ? "PASS" : "FAIL");
  if (!ok) return;
  ok = run_reopen_check();
  MDB_CONSOLE.printf("[CHECK] Reopen integrity: %s\n", ok ? "PASS" : "FAIL");
  if (!ok) return;
  ok = run_migration_check();
  MDB_CONSOLE.printf("[CHECK] Migration callback: %s\n", ok ? "PASS" : "FAIL");
  if (!ok) return;
  ok = run_txn_check();
  MDB_CONSOLE.printf("[CHECK] TXN commit/rollback: %s\n", ok ? "PASS" : "FAIL");
  if (!ok) return;

  print_stats_snapshot();
  MDB_CONSOLE.println("=== microdb ESP32-S3 benchmark end ===");
}

static void print_help(void) {
  MDB_CONSOLE.println("Commands:");
  MDB_CONSOLE.println("  help      - show commands");
  MDB_CONSOLE.println("  run       - run full benchmark suite (fresh DB)");
  MDB_CONSOLE.println("  run_real  - run real-data integration smoke suite");
  MDB_CONSOLE.println("  kv/ts/rel/wal - run single benchmark stage");
  MDB_CONSOLE.println("  reopenchk - run reopen latency + integrity check");
  MDB_CONSOLE.println("  migrate   - run schema migration check");
  MDB_CONSOLE.println("  txn       - run txn check");
  MDB_CONSOLE.println("  stats     - print inspect snapshot");
  MDB_CONSOLE.println("  metrics   - print last captured metrics");
  MDB_CONSOLE.println("  config    - print active config");
  MDB_CONSOLE.println("  profiles  - list profiles");
  MDB_CONSOLE.println("  profile           - show active profile");
  MDB_CONSOLE.println("  profile <name>    - switch profile and reopen DB (wipe)");
  MDB_CONSOLE.println("  run_det   - deterministic profile + paced OFF + run (recommended)");
  MDB_CONSOLE.println("  run_det_paced - deterministic profile + paced ON + run");
  MDB_CONSOLE.println("  note: run_det validates deterministic profile latency, not all profiles/workloads");
  MDB_CONSOLE.println("  paced              - print paced mode");
  MDB_CONSOLE.println("  paced on|off       - toggle paced mode");
  MDB_CONSOLE.println("  resetdb   - wipe storage + reopen DB");
  MDB_CONSOLE.println("  reopen    - reopen DB without wipe");
}

static void prompt(void) { MDB_CONSOLE.print("microdb-bench> "); }

static void reset_db_and_open(bool wipe) {
  microdb_err_t d = microdb_deinit(&g_db);
  if (d != MICRODB_OK) MDB_CONSOLE.printf("[WARN] deinit returned %d\n", (int)d);
  {
    microdb_err_t o = db_open(wipe, false);
    if (o != MICRODB_OK)
      MDB_CONSOLE.printf("[ERR] db_open failed: %s (%d)\n", microdb_err_to_string(o), (int)o);
    else
      MDB_CONSOLE.printf("[OK] DB ready (wipe=%u, profile=%s)\n", wipe ? 1u : 0u, P()->name);
  }
}

static void execute_command(char *line) {
  char *cmd = strtok(line, " \t");
  char *arg = strtok(NULL, " \t");
  if (cmd == NULL) return;

  if (strcmp(cmd, "help") == 0) {
    print_help();
  } else if (try_profile_shortcut(cmd)) {
    return;
  } else if (strcmp(cmd, "run_det") == 0) {
    if (!set_profile("deterministic")) {
      MDB_CONSOLE.println("[ERR] deterministic profile is not available");
      return;
    }
    MDB_CONSOLE.println("[NOTE] run_det validates deterministic profile latency, not all profiles/workloads.");
    set_paced_mode(false);
    reset_db_and_open(true);
    run_full_bench_once();
  } else if (strcmp(cmd, "run_det_paced") == 0) {
    if (!set_profile("deterministic")) {
      MDB_CONSOLE.println("[ERR] deterministic profile is not available");
      return;
    }
    set_paced_mode(true);
    reset_db_and_open(true);
    run_full_bench_once();
  } else if (strcmp(cmd, "run") == 0) {
    run_full_bench_once();
  } else if (strcmp(cmd, "run_real") == 0) {
    (void)run_real_data_suite();
  } else if (strcmp(cmd, "kv") == 0) {
    clear_metrics();
    MDB_CONSOLE.printf("[CHECK] KV benchmark: %s\n", run_kv_bench() ? "PASS" : "FAIL");
  } else if (strcmp(cmd, "ts") == 0) {
    clear_metrics();
    MDB_CONSOLE.printf("[CHECK] TS benchmark: %s\n", run_ts_bench() ? "PASS" : "FAIL");
  } else if (strcmp(cmd, "rel") == 0) {
    clear_metrics();
    MDB_CONSOLE.printf("[CHECK] REL benchmark: %s\n", run_rel_bench() ? "PASS" : "FAIL");
  } else if (strcmp(cmd, "wal") == 0) {
    clear_metrics();
    MDB_CONSOLE.printf("[CHECK] WAL compact: %s\n", run_wal_compact_bench() ? "PASS" : "FAIL");
  } else if (strcmp(cmd, "reopenchk") == 0) {
    clear_metrics();
    MDB_CONSOLE.printf("[CHECK] Reopen integrity: %s\n", run_reopen_check() ? "PASS" : "FAIL");
  } else if (strcmp(cmd, "migrate") == 0) {
    MDB_CONSOLE.printf("[CHECK] Migration callback: %s\n", run_migration_check() ? "PASS" : "FAIL");
  } else if (strcmp(cmd, "txn") == 0) {
    MDB_CONSOLE.printf("[CHECK] TXN commit/rollback: %s\n", run_txn_check() ? "PASS" : "FAIL");
  } else if (strcmp(cmd, "stats") == 0) {
    print_stats_snapshot();
  } else if (strcmp(cmd, "metrics") == 0) {
    print_last_metrics();
  } else if (strcmp(cmd, "config") == 0) {
    print_config();
  } else if (strcmp(cmd, "profiles") == 0) {
    print_profiles();
  } else if (strcmp(cmd, "profile") == 0) {
    if (arg == NULL) {
      MDB_CONSOLE.printf("[PROFILE] active=%s paced=%s\n", P()->name, g_paced_mode ? "ON" : "OFF");
    } else if (set_profile(arg)) {
      MDB_CONSOLE.printf("[PROFILE] switched to %s paced=%s\n", P()->name, g_paced_mode ? "ON" : "OFF");
      reset_db_and_open(true);
    } else {
      MDB_CONSOLE.printf("[ERR] unknown profile: %s\n", arg);
      print_profiles();
    }
  } else if (strcmp(cmd, "paced") == 0) {
    if (arg == NULL) {
      MDB_CONSOLE.printf("[PACED] mode=%s\n", g_paced_mode ? "ON" : "OFF");
    } else if (strcmp(arg, "on") == 0) {
      set_paced_mode(true);
    } else if (strcmp(arg, "off") == 0) {
      set_paced_mode(false);
    } else {
      MDB_CONSOLE.printf("[ERR] unknown paced arg: %s (use on/off)\n", arg);
    }
  } else if (strcmp(cmd, "resetdb") == 0) {
    reset_db_and_open(true);
  } else if (strcmp(cmd, "reopen") == 0) {
    reset_db_and_open(false);
  } else {
    MDB_CONSOLE.printf("[ERR] unknown command: %s\n", cmd);
    MDB_CONSOLE.println("Type 'help' for available commands.");
  }
}

void setup(void) {
  microdb_err_t err;
  MDB_CONSOLE.begin(115200);
  delay(1200);

  memset(&g_store_ctx, 0, sizeof(g_store_ctx));
  if (!storage_alloc()) {
    MDB_CONSOLE.println("[FATAL] storage alloc failed");
    return;
  }
  storage_reset();
  err = db_open(true, false);
  if (err != MICRODB_OK) {
    MDB_CONSOLE.printf("[FATAL] microdb_init failed: %s (%d)\n", microdb_err_to_string(err), (int)err);
    return;
  }

  MDB_CONSOLE.println();
  MDB_CONSOLE.println("microdb ESP32-S3 terminal bench is ready.");
  MDB_CONSOLE.println("Tests do NOT run automatically at power-on.");
  print_config();
  print_help();
  prompt();
}

void loop(void) {
  static char line[96];
  static size_t line_len = 0u;
  while (MDB_CONSOLE.available() > 0) {
    char ch = (char)MDB_CONSOLE.read();
    if (ch == '\r') continue;
    if (ch == '\n') {
      line[line_len] = '\0';
      execute_command(line);
      line_len = 0u;
      prompt();
      continue;
    }
    if (line_len < (sizeof(line) - 1u)) line[line_len++] = ch;
  }
}
