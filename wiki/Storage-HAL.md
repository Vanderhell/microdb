# Storage HAL

microdb supports three storage modes.

## RAM-only

Set `cfg.storage = NULL`.
This is the simplest mode and avoids persistence entirely.

## POSIX

The repository includes a POSIX-backed port for tests and simulation:

- `port/posix/microdb_port_posix.c`
- `port/posix/microdb_port_posix.h`

This is suitable for local testing and host-side integration runs.

## ESP32

The repository includes an ESP32 port:

- `port/esp32/microdb_port_esp32.c`
- `port/esp32/microdb_port_esp32.h`

The CMake file also exposes an ESP-IDF component path when `IDF_TARGET` is defined.

## Layout

Persistent storage starts with a WAL region, followed by engine regions for:

- KV
- TS
- REL

Alignment follows the storage erase size.
