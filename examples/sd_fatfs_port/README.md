# SD + FatFS Port Skeleton

This example shows practical glue for running `loxdb` on SD storage via FatFS.

What it is:
- a minimal `lox_storage_t` mapping over a single fixed-size file (`loxdb.bin`)
- intended as a bring-up template for ESP32/STM32-class SDMMC or SPI-SD stacks

What it is not:
- a complete SD driver
- a replacement for your board SD init/mount logic

Notes:
- `erase()` is modeled as writing `0xFF` over one erase block region in the file.
- `sync()` uses `f_sync`.
- geometry (`capacity/erase_size/write_size`) must match your chosen backing policy.

See `main.c` in this folder for the storage callbacks and init flow.
