# loxdb ESP32-S3 SD Stress Bench

Real-hardware stress benchmark for ESP32-S3 with SD_MMC (1-bit) storage backend and ST7735 LCD live utilization panel.

## Goal

- generate sustained real data writes on real hardware
- use SD card as persistent loxdb storage medium
- visualize live fill/pressure for all three engines (`KV`, `TS`, `REL`) plus `WAL`

## Hardware profile

Pin map is aligned to your `esp32_s3_sd_storage_smoketest` setup.

### SD_MMC (1-bit)

- `CLK=GPIO17`
- `CMD=GPIO18`
- `D0=GPIO16`
- `D3=GPIO47`

### ST7735 LCD

- `SCLK=GPIO10`
- `MOSI=GPIO11`
- `CS=GPIO12`
- `DC=GPIO13`
- `RST=GPIO14`
- `BL=-1` (set to GPIO if your board requires explicit backlight control)

## Runtime model

- persistent storage file on SD: `/loxdb_stress_store.bin`
- storage size: `16 MiB`
- erase size: `4096`
- write size: `1`
- loxdb RAM budget: `2048 KB` (uses OPI PSRAM when available)
- engine split: `KV 34% / TS 33% / REL 33%`
- WAL mode: `LOX_WAL_SYNC_FLUSH_ONLY` for higher ingest throughput

## What runs continuously

Main loop randomly mixes:

- KV writes (`lox_kv_put`)
- TS inserts (`lox_ts_insert`)
- REL inserts (`lox_rel_insert`)

On storage pressure (`LOX_ERR_STORAGE`/`LOX_ERR_FULL`) it triggers `lox_compact()` and continues.

## LCD output

Updated every second:

- total operation counter
- `KV` fill %
- `TS` fill %
- `REL` fill %
- `WAL` fill %

(values come from `lox_get_pressure(...)`)

## Serial commands

- `run` / `resume` -> continue stress loop
- `pause` -> pause writes
- `compact` -> force compact
- `stats` -> print pressure snapshot
- `resetdb` -> delete storage file and recreate database

## Arduino dependencies

- ESP32 core with `SD_MMC`
- `Adafruit GFX Library`
- `Adafruit ST7735 and ST7789 Library`

## Notes

- This bench is intentionally long-running and aggressive.
- For endurance runs, keep board power stable and use a good quality SD card.
- If your board uses different wiring, change the pin macros at the top of the `.ino`.
