#include <Arduino.h>
#include <SPI.h>
#include <SD_MMC.h>
#include <FS.h>
#include <string.h>

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
static const uint32_t kStorageBytes = 16u * 1024u * 1024u;
static const uint32_t kEraseSize = 4096u;
static const uint32_t kReportEveryMs = 1000u;

static lox_t g_db;
static lox_storage_t g_storage;
static File g_store;
static bool g_sd_ready = false;
static bool g_db_ready = false;
static bool g_running = true;

static uint8_t *g_erase_buf = NULL;
static uint32_t g_ops = 0u;
static uint32_t g_ts = 0u;
static uint32_t g_rel_next_id = 1u;
static lox_table_t *g_rel = NULL;

#if SDSTRESS_LCD_ENABLE
static Adafruit_ST7735 g_tft(LCD_PIN_CS, LCD_PIN_DC, LCD_PIN_RST);
static bool g_lcd_ready = false;
#endif

static uint32_t rng_next(void) {
  static uint32_t s = 0x1234ABCDu;
  s = s * 1664525u + 1013904223u;
  return s;
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
    File f = SD_MMC.open(kStoragePath, FILE_WRITE);
    if (!f) return false;
    static uint8_t zero[1024];
    memset(zero, 0xFF, sizeof(zero));
    uint32_t written = 0u;
    while (written < kStorageBytes) {
      uint32_t n = (kStorageBytes - written > sizeof(zero)) ? sizeof(zero) : (kStorageBytes - written);
      if (f.write(zero, n) != n) {
        f.close();
        return false;
      }
      written += n;
    }
    f.flush();
    f.close();
  }

  g_store = SD_MMC.open(kStoragePath, "r+");
  if (!g_store) return false;
  return true;
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

static bool setup_rel() {
  lox_schema_t s;
  lox_err_t rc;
  rc = lox_table_get(&g_db, "stress_rel", &g_rel);
  if (rc == LOX_OK) return true;
  if (rc != LOX_ERR_NOT_FOUND) return false;

  rc = lox_schema_init(&s, "stress_rel", 4096u);
  if (rc != LOX_OK) return false;
  rc = lox_schema_add(&s, "id", LOX_COL_U32, sizeof(uint32_t), true);
  if (rc != LOX_OK) return false;
  rc = lox_schema_add(&s, "v", LOX_COL_U32, sizeof(uint32_t), false);
  if (rc != LOX_OK) return false;
  rc = lox_schema_seal(&s);
  if (rc != LOX_OK) return false;
  rc = lox_table_create(&g_db, &s);
  if (rc != LOX_OK && rc != LOX_ERR_EXISTS) return false;
  rc = lox_table_get(&g_db, "stress_rel", &g_rel);
  return rc == LOX_OK;
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

static void lcd_status(uint8_t kv, uint8_t ts, uint8_t rel, uint8_t wal) {
#if SDSTRESS_LCD_ENABLE
  if (!g_lcd_ready) return;
  g_tft.fillScreen(ST77XX_BLACK);
  g_tft.setTextSize(1);
  g_tft.setTextColor(ST77XX_YELLOW);
  g_tft.setCursor(2, 2);
  g_tft.println("loxdb SD stress");
  g_tft.setTextColor(ST77XX_WHITE);
  g_tft.setCursor(2, 18);
  g_tft.printf("ops: %lu", (unsigned long)g_ops);
  g_tft.setCursor(2, 32);
  g_tft.printf("KV : %3u%%", (unsigned)kv);
  g_tft.setCursor(2, 44);
  g_tft.printf("TS : %3u%%", (unsigned)ts);
  g_tft.setCursor(2, 56);
  g_tft.printf("REL: %3u%%", (unsigned)rel);
  g_tft.setCursor(2, 68);
  g_tft.printf("WAL: %3u%%", (unsigned)wal);
#endif
}

static bool init_db() {
  lox_cfg_t cfg;
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
  cfg.ram_kb = 2048u;
  cfg.kv_pct = 34u;
  cfg.ts_pct = 33u;
  cfg.rel_pct = 33u;
  cfg.wal_compact_auto = 1u;
  cfg.wal_compact_threshold_pct = 75u;
  cfg.wal_sync_mode = LOX_WAL_SYNC_FLUSH_ONLY;

  if (lox_init(&g_db, &cfg) != LOX_OK) return false;
  if (lox_ts_register(&g_db, "stress_ts", LOX_TS_U32, 0u) != LOX_OK &&
      lox_ts_register(&g_db, "stress_ts", LOX_TS_U32, 0u) != LOX_ERR_EXISTS) {
    return false;
  }
  if (!setup_rel()) return false;
  return true;
}

static void do_one_op() {
  uint32_t r = rng_next() % 3u;
  lox_err_t rc;
  if (r == 0u) {
    char key[24];
    uint32_t v = rng_next();
    snprintf(key, sizeof(key), "bulk_%06lu", (unsigned long)(g_ops % 100000u));
    rc = lox_kv_put(&g_db, key, &v, sizeof(v));
    if (rc == LOX_ERR_STORAGE || rc == LOX_ERR_FULL) {
      (void)lox_compact(&g_db);
    }
  } else if (r == 1u) {
    uint32_t v = rng_next();
    g_ts++;
    rc = lox_ts_insert(&g_db, "stress_ts", g_ts, &v);
    if (rc == LOX_ERR_STORAGE || rc == LOX_ERR_FULL) {
      (void)lox_compact(&g_db);
    }
  } else {
    uint8_t row[64];
    uint32_t v = rng_next();
    memset(row, 0, sizeof(row));
    rc = lox_row_set(g_rel, row, "id", &g_rel_next_id);
    if (rc != LOX_OK) return;
    rc = lox_row_set(g_rel, row, "v", &v);
    if (rc != LOX_OK) return;
    rc = lox_rel_insert(&g_db, g_rel, row);
    if (rc == LOX_OK) {
      g_rel_next_id++;
    } else if (rc == LOX_ERR_STORAGE || rc == LOX_ERR_FULL) {
      (void)lox_compact(&g_db);
    }
  }
  g_ops++;
}

static void print_usage() {
  Serial.println("Commands: run | pause | resume | compact | stats | resetdb");
}

static void show_stats() {
  lox_pressure_t p;
  if (lox_get_pressure(&g_db, &p) != LOX_OK) return;
  Serial.printf("[PRESSURE] kv=%u ts=%u rel=%u wal=%u risk=%u ops=%lu\n",
                p.kv_fill_pct, p.ts_fill_pct, p.rel_fill_pct, p.wal_fill_pct,
                p.near_full_risk_pct, (unsigned long)g_ops);
  lcd_status(p.kv_fill_pct, p.ts_fill_pct, p.rel_fill_pct, p.wal_fill_pct);
}

static void reset_db() {
  (void)lox_deinit(&g_db);
  if (g_store) g_store.close();
  SD_MMC.remove(kStoragePath);
  if (!open_storage_file() || !init_db()) {
    Serial.println("[ERR] reset failed");
    return;
  }
  g_ops = 0u;
  g_ts = 0u;
  g_rel_next_id = 1u;
  Serial.println("[OK] db reset complete");
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
  if (!init_db()) {
    Serial.println("[FATAL] lox_init failed");
    return;
  }
  g_db_ready = true;
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
      else if (cmd == "compact") (void)lox_compact(&g_db);
      else if (cmd == "stats") show_stats();
      else if (cmd == "resetdb") reset_db();
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
