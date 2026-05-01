#include <Arduino.h>
#include <SPI.h>
#include <SD_MMC.h>
#include <FS.h>
#include <string.h>

/* Increase core engine limits for large SD stress profile. */
#ifndef LOX_KV_MAX_KEYS
#define LOX_KV_MAX_KEYS 4096
#endif
#ifndef LOX_TS_MAX_STREAMS
#define LOX_TS_MAX_STREAMS 12
#endif
#ifndef LOX_REL_MAX_TABLES
#define LOX_REL_MAX_TABLES 12
#endif
#ifndef LOX_REL_MAX_COLS
#define LOX_REL_MAX_COLS 12
#endif

#if defined(ARDUINO_ARCH_ESP32)
#include <esp_heap_caps.h>
#endif

#ifndef SDSTRESS_LCD_ENABLE
#define SDSTRESS_LCD_ENABLE 1
#endif

#if SDSTRESS_LCD_ENABLE
#include <Adafruit_GFX.h>
#include <Adafruit_ST7735.h>
#endif

extern "C" {
#include "lox.h"
}

#define LOX_PROFILE_CORE_HIMEM 1

#ifndef SDMMC_PIN_CLK
#define SDMMC_PIN_CLK 17
#endif
#ifndef SDMMC_PIN_CMD
#define SDMMC_PIN_CMD 18
#endif
#ifndef SDMMC_PIN_D0
#define SDMMC_PIN_D0 16
#endif
#ifndef SDMMC_PIN_D3
#define SDMMC_PIN_D3 47
#endif

#ifndef LCD_PIN_SCLK
#define LCD_PIN_SCLK 10
#endif
#ifndef LCD_PIN_MOSI
#define LCD_PIN_MOSI 11
#endif
#ifndef LCD_PIN_CS
#define LCD_PIN_CS 12
#endif
#ifndef LCD_PIN_DC
#define LCD_PIN_DC 13
#endif
#ifndef LCD_PIN_RST
#define LCD_PIN_RST 14
#endif
#ifndef LCD_PIN_BL
#define LCD_PIN_BL -1
#endif

static const char *kStoragePath = "/loxdb_stress_store.bin";
/* Stable large-window mode for endurance on ESP32-S3 + PSRAM. */
static const uint32_t kStorageBytes = 64u * 1024u * 1024u;
static const uint32_t kEraseSize = 4096u;
static const uint32_t kReportEveryMs = 1000u;
/* Bench default: start from clean DB image on every boot. */
static const bool kFreshStartOnBoot = false;

static lox_t g_db;
static lox_storage_t g_storage;
static File g_store;
static bool g_sd_ready = false;
static bool g_db_ready = false;
static bool g_running = true;
static bool g_verify_enabled = true;

static uint8_t *g_erase_buf = NULL;
static uint32_t g_ops = 0u;
static uint32_t g_ts_seq[8] = {0};
static uint32_t g_rel_next_id = 1u;
static lox_table_t *g_rel_tables[4] = {0};
static uint8_t g_ts_rr = 0u;
static uint8_t g_rel_rr = 0u;
static uint8_t g_kv_rr = 0u;
static uint8_t g_lcd_page = 0u;
static uint32_t g_verify_ok = 0u;
static uint32_t g_verify_fail = 0u;
static uint32_t g_compact_count = 0u;
static uint32_t g_last_compact_ms = 0u;
typedef enum {
  MODE_ALL = 0,
  MODE_KV,
  MODE_TS,
  MODE_REL
} stress_mode_t;
static stress_mode_t g_mode = MODE_ALL;
typedef enum {
  PROFILE_SMOKE = 0,
  PROFILE_SOAK,
  PROFILE_STRESS
} bench_profile_t;
static bench_profile_t g_profile = PROFILE_SOAK;
static uint16_t g_w_kv = 35u;
static uint16_t g_w_ts = 35u;
static uint16_t g_w_rel = 30u;
static const char *kTsStreams[] = {
  "stress_ts_0", "stress_ts_1", "stress_ts_2", "stress_ts_3",
  "stress_ts_4", "stress_ts_5", "stress_ts_6", "stress_ts_7"
};
static const char *kRelNames[] = {
  "stress_rel_0", "stress_rel_1", "stress_rel_2", "stress_rel_3"
};
static const uint32_t kTsStreamCount = (uint32_t)(sizeof(kTsStreams) / sizeof(kTsStreams[0]));
static const uint32_t kRelTableCount = (uint32_t)(sizeof(kRelNames) / sizeof(kRelNames[0]));
static const uint32_t kKvKeySpace = 20000u;
/* 128MiB profile: start high, then downscale per-table on FULL/NO_MEM. */
static const uint32_t kRelMaxRows = 4096u;
static const uint32_t kRelMinRows = 64u;

#if SDSTRESS_LCD_ENABLE
static Adafruit_ST7735 g_tft(LCD_PIN_CS, LCD_PIN_DC, LCD_PIN_RST);
static bool g_lcd_ready = false;
#endif

static uint32_t rng_next(void) {
  static uint32_t s = 0x1234ABCDu;
  s = s * 1664525u + 1013904223u;
  return s;
}

static const char *mode_name(stress_mode_t m) {
  switch (m) {
    case MODE_KV: return "kv";
    case MODE_TS: return "ts";
    case MODE_REL: return "rel";
    default: return "all";
  }
}

static void log_line(const char *msg) {
  Serial.println(msg);
}

static bool mount_sd() {
  if (g_sd_ready) return true;
  if (!SD_MMC.setPins(SDMMC_PIN_CLK, SDMMC_PIN_CMD, SDMMC_PIN_D0, -1, -1, SDMMC_PIN_D3)) {
    Serial.println("[ERR] SD_MMC.setPins failed");
    return false;
  }
  if (!SD_MMC.begin("/sdcard", true, false)) {
    Serial.println("[ERR] SD_MMC.begin failed");
    return false;
  }
  if (SD_MMC.cardType() == CARD_NONE) {
    Serial.println("[ERR] no SD card");
    return false;
  }
  g_sd_ready = true;
  Serial.printf("[OK] SD mounted card=%lluMB\n", (unsigned long long)(SD_MMC.cardSize() / (1024ull * 1024ull)));
  return true;
}

static bool open_storage_file() {
  if (!mount_sd()) return false;

  if (SD_MMC.exists(kStoragePath)) {
    g_store = SD_MMC.open(kStoragePath, "r");
    if (!g_store) return false;
    size_t sz = (size_t)g_store.size();
    g_store.close();
    if (sz != kStorageBytes) {
      SD_MMC.remove(kStoragePath);
    }
  }

  if (!SD_MMC.exists(kStoragePath)) {
    /* Deterministic media image: fill full DB region with erased pattern (0xFF). */
    File f = SD_MMC.open(kStoragePath, FILE_WRITE);
    uint8_t *fmt_buf = NULL;
    const uint32_t fmt_chunk = 64u * 1024u;
    uint32_t off = 0u;
    if (!f) return false;
    fmt_buf = (uint8_t *)malloc(fmt_chunk);
    if (fmt_buf != NULL) memset(fmt_buf, 0xFF, fmt_chunk);
    while (off < kStorageBytes) {
      size_t chunk = (size_t)((kStorageBytes - off) > fmt_chunk ? fmt_chunk : (kStorageBytes - off));
      const uint8_t *src = (fmt_buf != NULL) ? fmt_buf : g_erase_buf;
      size_t src_len = (fmt_buf != NULL) ? chunk : (size_t)kEraseSize;
      if (fmt_buf == NULL && chunk > src_len) chunk = src_len;
      if (f.write(src, chunk) != chunk) {
        if (fmt_buf != NULL) free(fmt_buf);
        f.close();
        return false;
      }
      off += (uint32_t)chunk;
    }
    if (fmt_buf != NULL) free(fmt_buf);
    f.flush();
    f.close();
  }

  g_store = SD_MMC.open(kStoragePath, "r+");
  if (!g_store) return false;
  if ((uint32_t)g_store.size() != kStorageBytes) {
    g_store.close();
    return false;
  }
  return true;
}

static bool recreate_storage_file() {
  if (g_store) g_store.close();
  if (SD_MMC.exists(kStoragePath)) {
    if (!SD_MMC.remove(kStoragePath)) return false;
  }
  return open_storage_file();
}

static lox_err_t st_read(void *ctx, uint32_t off, void *buf, size_t len) {
  (void)ctx;
  if (!g_store || !buf || (uint64_t)off + (uint64_t)len > kStorageBytes) return LOX_ERR_INVALID;
  if (!g_store.seek(off)) return LOX_ERR_STORAGE;
  size_t got = g_store.read((uint8_t *)buf, len);
  return (got == len) ? LOX_OK : LOX_ERR_STORAGE;
}

static lox_err_t st_write(void *ctx, uint32_t off, const void *buf, size_t len) {
  (void)ctx;
  if (!g_store || !buf || (uint64_t)off + (uint64_t)len > kStorageBytes) return LOX_ERR_INVALID;
  if (!g_store.seek(off)) return LOX_ERR_STORAGE;
  size_t wr = g_store.write((const uint8_t *)buf, len);
  return (wr == len) ? LOX_OK : LOX_ERR_STORAGE;
}

static lox_err_t st_erase(void *ctx, uint32_t off) {
  (void)ctx;
  if (!g_store || off >= kStorageBytes || (off + kEraseSize) > kStorageBytes) return LOX_ERR_INVALID;
  if (!g_store.seek(off)) return LOX_ERR_STORAGE;
  size_t wr = g_store.write(g_erase_buf, kEraseSize);
  return (wr == kEraseSize) ? LOX_OK : LOX_ERR_STORAGE;
}

static lox_err_t st_sync(void *ctx) {
  (void)ctx;
  if (!g_store) return LOX_ERR_STORAGE;
  g_store.flush();
  return LOX_OK;
}

static bool setup_rel_table(uint32_t idx, const char *name) {
  lox_schema_t s;
  lox_err_t rc;
  uint32_t rows_try = kRelMaxRows;
  lox_table_t *tmp = NULL;
  rc = lox_table_get(&g_db, name, &tmp);
  if (rc == LOX_OK) return true;
  if (rc != LOX_ERR_NOT_FOUND) return false;

  while (rows_try >= kRelMinRows) {
    rc = lox_schema_init(&s, name, rows_try);
    if (rc != LOX_OK) {
      Serial.printf("[ERR] lox_schema_init(%s,%lu) rc=%d\n", name, (unsigned long)rows_try, (int)rc);
      return false;
    }
    rc = lox_schema_add(&s, "id", LOX_COL_U32, sizeof(uint32_t), true);
    if (rc != LOX_OK) {
      Serial.printf("[ERR] lox_schema_add(id) table=%s rc=%d\n", name, (int)rc);
      return false;
    }
    rc = lox_schema_add(&s, "v", LOX_COL_U32, sizeof(uint32_t), false);
    if (rc != LOX_OK) {
      Serial.printf("[ERR] lox_schema_add(v) table=%s rc=%d\n", name, (int)rc);
      return false;
    }
    if (idx % 2u == 0u) {
      rc = lox_schema_add(&s, "temp", LOX_COL_I32, sizeof(int32_t), false);
      if (rc != LOX_OK) {
        Serial.printf("[ERR] lox_schema_add(temp) table=%s rc=%d\n", name, (int)rc);
        return false;
      }
    } else {
      rc = lox_schema_add(&s, "flags", LOX_COL_U16, sizeof(uint16_t), false);
      if (rc != LOX_OK) {
        Serial.printf("[ERR] lox_schema_add(flags) table=%s rc=%d\n", name, (int)rc);
        return false;
      }
    }
    if (idx % 3u == 0u) {
      rc = lox_schema_add(&s, "ts", LOX_COL_U64, sizeof(uint64_t), false);
      if (rc != LOX_OK) {
        Serial.printf("[ERR] lox_schema_add(ts) table=%s rc=%d\n", name, (int)rc);
        return false;
      }
    }
    rc = lox_schema_seal(&s);
    if (rc != LOX_OK) {
      Serial.printf("[ERR] lox_schema_seal(%s) rc=%d\n", name, (int)rc);
      return false;
    }
    rc = lox_table_create(&g_db, &s);
    if (rc == LOX_OK || rc == LOX_ERR_EXISTS) {
      if (rows_try != kRelMaxRows) {
        Serial.printf("[WARN] table %s max_rows reduced to %lu\n", name, (unsigned long)rows_try);
      }
      break;
    }
    if (rc != LOX_ERR_FULL && rc != LOX_ERR_NO_MEM) {
      Serial.printf("[ERR] lox_table_create(%s) rc=%d\n", name, (int)rc);
      return false;
    }
    Serial.printf("[WARN] lox_table_create(%s) rc=%d at rows=%lu, retry smaller\n",
                  name, (int)rc, (unsigned long)rows_try);
    rows_try /= 2u;
  }
  if (rc != LOX_OK && rc != LOX_ERR_EXISTS) {
    Serial.printf("[ERR] lox_table_create(%s) exhausted retries down to min_rows=%lu\n",
                  name, (unsigned long)kRelMinRows);
    return false;
  }
  rc = lox_table_get(&g_db, name, &tmp);
  if (rc != LOX_OK) {
    Serial.printf("[ERR] lox_table_get(%s) rc=%d\n", name, (int)rc);
  }
  return rc == LOX_OK;
}

static bool setup_rel() {
  uint32_t i;
  for (i = 0u; i < kRelTableCount; ++i) {
    if (!setup_rel_table(i, kRelNames[i])) return false;
    if (lox_table_get(&g_db, kRelNames[i], &g_rel_tables[i]) != LOX_OK) return false;
  }
  return true;
}

static void lcd_init() {
#if SDSTRESS_LCD_ENABLE
  if (LCD_PIN_BL >= 0) {
    pinMode(LCD_PIN_BL, OUTPUT);
    digitalWrite(LCD_PIN_BL, HIGH);
  }
  SPI.begin(LCD_PIN_SCLK, -1, LCD_PIN_MOSI, LCD_PIN_CS);
#ifdef INITR_MINI160x80
  g_tft.initR(INITR_MINI160x80);
#else
  g_tft.initR(INITR_BLACKTAB);
#endif
  g_tft.setRotation(1);
  g_tft.fillScreen(ST77XX_BLACK);
  g_lcd_ready = true;
#endif
}

static void lcd_status_page0(uint8_t kv, uint8_t ts, uint8_t rel, uint8_t wal, uint8_t risk) {
#if SDSTRESS_LCD_ENABLE
  if (!g_lcd_ready) return;
  g_tft.fillScreen(ST77XX_BLACK);
  g_tft.setTextSize(1);
  g_tft.setTextColor(ST77XX_YELLOW);
  g_tft.setCursor(2, 2);
  g_tft.println("loxdb SD stress");
  g_tft.setTextColor(ST77XX_WHITE);
  g_tft.setCursor(2, 18);
  g_tft.printf("ops:%lu m:%s", (unsigned long)g_ops, mode_name(g_mode));
  g_tft.setCursor(2, 32);
  g_tft.printf("KV : %3u%%", (unsigned)kv);
  g_tft.setCursor(2, 44);
  g_tft.printf("TS : %3u%%", (unsigned)ts);
  g_tft.setCursor(2, 56);
  g_tft.printf("REL: %3u%%", (unsigned)rel);
  g_tft.setCursor(2, 68);
  g_tft.printf("WAL: %3u%% R:%3u%%", (unsigned)wal, (unsigned)risk);
#endif
}

static void lcd_status_page1(const lox_stats_t *st) {
#if SDSTRESS_LCD_ENABLE
  if (!g_lcd_ready || st == NULL) return;
  g_tft.fillScreen(ST77XX_BLACK);
  g_tft.setTextSize(1);
  g_tft.setTextColor(ST77XX_YELLOW);
  g_tft.setCursor(2, 2);
  g_tft.println("loxdb SD stress");
  g_tft.setTextColor(ST77XX_WHITE);
  g_tft.setCursor(2, 18);
  g_tft.printf("KV:%lu/%lu", (unsigned long)st->kv_entries_used, (unsigned long)st->kv_entries_max);
  g_tft.setCursor(2, 30);
  g_tft.printf("TS:%lu", (unsigned long)st->ts_samples_total);
  g_tft.setCursor(2, 42);
  g_tft.printf("REL rows:%lu", (unsigned long)st->rel_rows_total);
  g_tft.setCursor(2, 54);
  g_tft.printf("WAL:%lu/%lu", (unsigned long)st->wal_bytes_used, (unsigned long)st->wal_bytes_total);
  g_tft.setCursor(2, 66);
  g_tft.printf("str:%lu tbl:%lu", (unsigned long)st->ts_streams_registered, (unsigned long)st->rel_tables_count);
#endif
}

static void lcd_status(uint8_t kv, uint8_t ts, uint8_t rel, uint8_t wal, uint8_t risk, const lox_stats_t *st) {
  if (g_lcd_page == 0u) {
    lcd_status_page0(kv, ts, rel, wal, risk);
  } else {
    lcd_status_page1(st);
  }
}

static bool init_db() {
  lox_cfg_t cfg;
  lox_err_t rc;
  memset(&cfg, 0, sizeof(cfg));
  memset(&g_storage, 0, sizeof(g_storage));

  g_storage.read = st_read;
  g_storage.write = st_write;
  g_storage.erase = st_erase;
  g_storage.sync = st_sync;
  g_storage.capacity = kStorageBytes;
  g_storage.erase_size = kEraseSize;
  g_storage.write_size = 1u;
  g_storage.ctx = NULL;

  cfg.storage = &g_storage;
  cfg.ram_kb = 8192u;
  cfg.kv_pct = 45u;
  cfg.ts_pct = 20u;
  cfg.rel_pct = 35u;
  cfg.wal_compact_auto = 1u;
  cfg.wal_compact_threshold_pct = 75u;
  cfg.wal_sync_mode = LOX_WAL_SYNC_FLUSH_ONLY;

  rc = lox_init(&g_db, &cfg);
  if (rc == LOX_ERR_CORRUPT || rc == LOX_ERR_EXISTS || rc == LOX_ERR_SCHEMA) {
    Serial.printf("[WARN] lox_init rc=%d (%s), recreating storage file\n", (int)rc, lox_err_to_string(rc));
    (void)lox_deinit(&g_db);
    if (g_store) g_store.close();
    (void)SD_MMC.remove(kStoragePath);
    if (!open_storage_file()) {
      Serial.println("[ERR] recreate storage file failed");
      return false;
    }
    rc = lox_init(&g_db, &cfg);
  }
  if (rc != LOX_OK) {
    Serial.printf("[ERR] lox_init rc=%d cap=%lu erase=%lu write=%lu ram_kb=%u split=%u/%u/%u wal_th=%u\n",
                  (int)rc,
                  (unsigned long)g_storage.capacity,
                  (unsigned long)g_storage.erase_size,
                  (unsigned long)g_storage.write_size,
                  (unsigned)cfg.ram_kb,
                  (unsigned)cfg.kv_pct,
                  (unsigned)cfg.ts_pct,
                  (unsigned)cfg.rel_pct,
                  (unsigned)cfg.wal_compact_threshold_pct);
    return false;
  }
  {
    uint32_t i;
    for (i = 0u; i < kTsStreamCount; ++i) {
      rc = lox_ts_register(&g_db, kTsStreams[i], LOX_TS_U32, 0u);
      if (!(rc == LOX_OK || rc == LOX_ERR_EXISTS)) {
        Serial.printf("[ERR] lox_ts_register(%s) rc=%d\n", kTsStreams[i], (int)rc);
        return false;
      }
      g_ts_seq[i] = 0u;
    }
  }
  if (!setup_rel()) {
    Serial.println("[ERR] setup_rel failed");
    return false;
  }
  return true;
}

static const char *profile_name(bench_profile_t p) {
  switch (p) {
    case PROFILE_SMOKE: return "smoke";
    case PROFILE_STRESS: return "stress";
    default: return "soak";
  }
}

static void apply_profile(bench_profile_t p) {
  g_profile = p;
  if (p == PROFILE_SMOKE) {
    g_w_kv = 45u; g_w_ts = 30u; g_w_rel = 25u;
  } else if (p == PROFILE_STRESS) {
    g_w_kv = 25u; g_w_ts = 35u; g_w_rel = 40u;
  } else {
    g_w_kv = 35u; g_w_ts = 35u; g_w_rel = 30u;
  }
  Serial.printf("[OK] profile=%s mix=%u/%u/%u\n", profile_name(g_profile),
                (unsigned)g_w_kv, (unsigned)g_w_ts, (unsigned)g_w_rel);
}

static void maybe_compact(lox_err_t rc) {
  if (rc == LOX_ERR_STORAGE || rc == LOX_ERR_FULL) {
    uint32_t t0 = millis();
    (void)lox_compact(&g_db);
    g_compact_count++;
    g_last_compact_ms = millis() - t0;
  }
}

static void op_kv(void) {
  char key[32];
  uint32_t v = rng_next();
  uint32_t idx = (uint32_t)((g_kv_rr++) % kKvKeySpace);
  snprintf(key, sizeof(key), "bulk_%05lu", (unsigned long)idx);
  maybe_compact(lox_kv_put(&g_db, key, &v, sizeof(v)));
  if (g_verify_enabled && ((g_ops & 0x3Fu) == 0u)) {
    uint32_t out = 0u;
    size_t out_len = sizeof(out);
    lox_err_t rc = lox_kv_get(&g_db, key, &out, sizeof(out), &out_len);
    if (rc == LOX_OK && out_len == sizeof(out)) g_verify_ok++;
    else g_verify_fail++;
  }
}

static void op_ts(void) {
  uint8_t si = g_ts_rr;
  uint32_t v = rng_next();
  g_ts_seq[si]++;
  maybe_compact(lox_ts_insert(&g_db, kTsStreams[si], g_ts_seq[si], &v));
  if (g_verify_enabled && ((g_ops & 0x7Fu) == 0u)) {
    lox_ts_sample_t sample;
    lox_err_t rc = lox_ts_last(&g_db, kTsStreams[si], &sample);
    if (rc == LOX_OK) g_verify_ok++;
    else g_verify_fail++;
  }
  g_ts_rr = (uint8_t)((g_ts_rr + 1u) % kTsStreamCount);
}

static void op_rel(void) {
  uint8_t row[128];
  uint32_t v = rng_next();
  uint32_t ti = (uint32_t)g_rel_rr % kRelTableCount;
  lox_table_t *rel = g_rel_tables[ti];
  int32_t temp = (int32_t)(rng_next() & 0x7FFFu);
  uint16_t flags = (uint16_t)(rng_next() & 0x3FFu);
  uint64_t ts64 = (((uint64_t)rng_next()) << 32u) | (uint64_t)rng_next();
  memset(row, 0, sizeof(row));
  if (lox_row_set(rel, row, "id", &g_rel_next_id) != LOX_OK) return;
  if (lox_row_set(rel, row, "v", &v) != LOX_OK) return;
  if (ti % 2u == 0u) {
    if (lox_row_set(rel, row, "temp", &temp) != LOX_OK) return;
  } else {
    if (lox_row_set(rel, row, "flags", &flags) != LOX_OK) return;
  }
  if (ti % 3u == 0u) {
    if (lox_row_set(rel, row, "ts", &ts64) != LOX_OK) return;
  }
  {
    lox_err_t rc = lox_rel_insert(&g_db, rel, row);
    if (rc == LOX_OK) {
      g_rel_next_id++;
      g_rel_rr++;
      if (g_verify_enabled && ((g_ops & 0x7Fu) == 0u)) {
        uint32_t cnt = 0u;
        if (lox_rel_count(rel, &cnt) == LOX_OK) g_verify_ok++;
        else g_verify_fail++;
      }
    } else {
      maybe_compact(rc);
    }
  }
}

static void do_one_op() {
  if (g_mode == MODE_KV) op_kv();
  else if (g_mode == MODE_TS) op_ts();
  else if (g_mode == MODE_REL) op_rel();
  else {
    uint16_t roll = (uint16_t)(rng_next() % 100u);
    if (roll < g_w_kv) op_kv();
    else if (roll < (uint16_t)(g_w_kv + g_w_ts)) op_ts();
    else op_rel();
  }
  g_ops++;
}

static void print_usage() {
  Serial.println("Commands:");
  Serial.println("  run | pause | resume");
  Serial.println("  profile smoke|soak|stress");
  Serial.println("  verify on|off");
  Serial.println("  mode all|kv|ts|rel");
  Serial.println("  clear kv|ts|rel|all");
  Serial.println("  compact | stats | resetdb | formatdb");
}

static void set_profile_from_text(const String &arg) {
  if (arg == "smoke") apply_profile(PROFILE_SMOKE);
  else if (arg == "soak") apply_profile(PROFILE_SOAK);
  else if (arg == "stress") apply_profile(PROFILE_STRESS);
  else Serial.println("[ERR] profile must be smoke|soak|stress");
}

static void set_verify_from_text(const String &arg) {
  if (arg == "on") g_verify_enabled = true;
  else if (arg == "off") g_verify_enabled = false;
  else {
    Serial.println("[ERR] verify must be on|off");
    return;
  }
  Serial.printf("[OK] verify=%s\n", g_verify_enabled ? "on" : "off");
}

static void set_mode_from_text(const String &arg) {
  if (arg == "all") g_mode = MODE_ALL;
  else if (arg == "kv") g_mode = MODE_KV;
  else if (arg == "ts") g_mode = MODE_TS;
  else if (arg == "rel") g_mode = MODE_REL;
  else {
    Serial.println("[ERR] mode must be all|kv|ts|rel");
    return;
  }
  Serial.printf("[OK] mode=%s\n", mode_name(g_mode));
}

static void clear_engine(const String &arg) {
  lox_err_t rc = LOX_ERR_INVALID;
  if (arg == "kv") {
    rc = lox_kv_clear(&g_db);
  } else if (arg == "ts") {
    uint32_t i;
    rc = LOX_OK;
    for (i = 0u; i < kTsStreamCount; ++i) {
      lox_err_t s = lox_ts_clear(&g_db, kTsStreams[i]);
      if (s != LOX_OK) rc = s;
      g_ts_seq[i] = 0u;
    }
    g_ts_rr = 0u;
  } else if (arg == "rel") {
    uint32_t i;
    rc = LOX_OK;
    for (i = 0u; i < kRelTableCount; ++i) {
      lox_err_t t = (g_rel_tables[i] != NULL) ? lox_rel_clear(&g_db, g_rel_tables[i]) : LOX_ERR_INVALID;
      if (t != LOX_OK) rc = t;
    }
    if (rc == LOX_OK) {
      g_rel_next_id = 1u;
      g_rel_rr = 0u;
    }
  } else if (arg == "all") {
    uint32_t i;
    lox_err_t a = lox_kv_clear(&g_db);
    lox_err_t b = LOX_OK;
    lox_err_t c = LOX_OK;
    for (i = 0u; i < kTsStreamCount; ++i) {
      lox_err_t s = lox_ts_clear(&g_db, kTsStreams[i]);
      if (s != LOX_OK) b = s;
      g_ts_seq[i] = 0u;
    }
    g_ts_rr = 0u;
    for (i = 0u; i < kRelTableCount; ++i) {
      lox_err_t t = (g_rel_tables[i] != NULL) ? lox_rel_clear(&g_db, g_rel_tables[i]) : LOX_ERR_INVALID;
      if (t != LOX_OK) c = t;
    }
    if (a == LOX_OK && b == LOX_OK && c == LOX_OK) {
      g_rel_next_id = 1u;
      g_rel_rr = 0u;
      Serial.println("[OK] clear all");
      return;
    }
    Serial.printf("[ERR] clear all kv=%d ts=%d rel=%d\n", (int)a, (int)b, (int)c);
    return;
  } else {
    Serial.println("[ERR] clear must be kv|ts|rel|all");
    return;
  }

  if (rc == LOX_OK) Serial.printf("[OK] clear %s\n", arg.c_str());
  else Serial.printf("[ERR] clear %s rc=%d\n", arg.c_str(), (int)rc);
}

static void show_stats() {
  lox_pressure_t p;
  lox_stats_t st;
  if (lox_get_pressure(&g_db, &p) != LOX_OK) return;
  if (lox_inspect(&g_db, &st) != LOX_OK) return;
  Serial.printf("[PRESSURE] kv=%u ts=%u rel=%u wal=%u risk=%u ops=%lu\n",
                p.kv_fill_pct, p.ts_fill_pct, p.rel_fill_pct, p.wal_fill_pct,
                p.near_full_risk_pct, (unsigned long)g_ops);
  Serial.printf("[STATS] kv=%lu/%lu ts_samples=%lu rel_rows=%lu wal=%lu/%lu\n",
                (unsigned long)st.kv_entries_used,
                (unsigned long)st.kv_entries_max,
                (unsigned long)st.ts_samples_total,
                (unsigned long)st.rel_rows_total,
                (unsigned long)st.wal_bytes_used,
                (unsigned long)st.wal_bytes_total);
  Serial.printf("[BENCH] profile=%s mode=%s verify=%s ok=%lu fail=%lu compact=%lu last_compact_ms=%lu streams=%lu tables=%lu\n",
                profile_name(g_profile), mode_name(g_mode), g_verify_enabled ? "on" : "off",
                (unsigned long)g_verify_ok, (unsigned long)g_verify_fail,
                (unsigned long)g_compact_count, (unsigned long)g_last_compact_ms,
                (unsigned long)st.ts_streams_registered, (unsigned long)st.rel_tables_count);
  lcd_status(p.kv_fill_pct, p.ts_fill_pct, p.rel_fill_pct, p.wal_fill_pct, p.near_full_risk_pct, &st);
  g_lcd_page ^= 1u;
}

static void reset_db() {
  (void)lox_deinit(&g_db);
  if (!recreate_storage_file()) {
    Serial.println("[ERR] recreate storage file failed");
    Serial.println("[ERR] reset failed");
    return;
  }
  if (!init_db()) {
    Serial.println("[ERR] init_db failed after remove");
    Serial.println("[ERR] reset failed");
    return;
  }
  g_ops = 0u;
  memset(g_ts_seq, 0, sizeof(g_ts_seq));
  g_ts_rr = 0u;
  g_kv_rr = 0u;
  g_rel_next_id = 1u;
  g_rel_rr = 0u;
  g_verify_ok = 0u;
  g_verify_fail = 0u;
  g_compact_count = 0u;
  g_last_compact_ms = 0u;
  Serial.println("[OK] db reset complete");
}

static void format_db() {
  Serial.println("[INFO] formatting 128MiB image, this can take a while...");
  reset_db();
}

void setup() {
  Serial.begin(115200);
  delay(1200);
  lcd_init();

#if defined(ARDUINO_ARCH_ESP32)
  g_erase_buf = (uint8_t *)heap_caps_malloc(kEraseSize, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
  if (!g_erase_buf) g_erase_buf = (uint8_t *)heap_caps_malloc(kEraseSize, MALLOC_CAP_8BIT);
#else
  g_erase_buf = (uint8_t *)malloc(kEraseSize);
#endif
  if (!g_erase_buf) {
    Serial.println("[FATAL] no erase buffer");
    return;
  }
  memset(g_erase_buf, 0xFF, kEraseSize);

  if (!open_storage_file()) {
    Serial.println("[FATAL] SD storage file open failed");
    return;
  }
  if (kFreshStartOnBoot) {
    if (!recreate_storage_file()) {
      Serial.println("[FATAL] fresh-start recreate failed");
      return;
    }
    Serial.println("[OK] fresh-start storage image created");
  }
  if (!init_db()) {
    Serial.println("[FATAL] lox_init failed");
    return;
  }
  g_db_ready = true;
  apply_profile(PROFILE_SOAK);
  Serial.println("[OK] loxdb SD stress bench ready");
  Serial.printf("SD pins CLK=%d CMD=%d D0=%d D3=%d\n", SDMMC_PIN_CLK, SDMMC_PIN_CMD, SDMMC_PIN_D0, SDMMC_PIN_D3);
  Serial.printf("LCD pins SCLK=%d MOSI=%d CS=%d DC=%d RST=%d\n", LCD_PIN_SCLK, LCD_PIN_MOSI, LCD_PIN_CS, LCD_PIN_DC, LCD_PIN_RST);
  print_usage();
}

void loop() {
  static uint32_t last_report = 0u;
  static String cmd;

  while (Serial.available() > 0) {
    char c = (char)Serial.read();
    if (c == '\r') continue;
    if (c == '\n') {
      if (cmd == "run" || cmd == "resume") g_running = true;
      else if (cmd == "pause") g_running = false;
      else if (cmd.startsWith("profile ")) set_profile_from_text(cmd.substring(8));
      else if (cmd.startsWith("verify ")) set_verify_from_text(cmd.substring(7));
      else if (cmd.startsWith("mode ")) set_mode_from_text(cmd.substring(5));
      else if (cmd.startsWith("clear ")) clear_engine(cmd.substring(6));
      else if (cmd == "compact") {
        uint32_t t0 = millis();
        (void)lox_compact(&g_db);
        g_compact_count++;
        g_last_compact_ms = millis() - t0;
      }
      else if (cmd == "stats") show_stats();
      else if (cmd == "resetdb") reset_db();
      else if (cmd == "formatdb") format_db();
      else if (cmd.length() > 0) print_usage();
      cmd = "";
    } else {
      cmd += c;
      if (cmd.length() > 64) cmd = "";
    }
  }

  if (!g_db_ready) {
    delay(200);
    return;
  }

  if (g_running) {
    do_one_op();
  } else {
    delay(5);
  }

  uint32_t now = millis();
  if (now - last_report >= kReportEveryMs) {
    last_report = now;
    show_stats();
  }
}
