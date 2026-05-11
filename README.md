![loxdb](docs/banner.svg)

# loxdb

> Predictable-memory database for microcontrollers. KV + time-series + relational, one malloc, WAL recovery.

[![CI](https://github.com/Vanderhell/loxdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Vanderhell/loxdb/actions/workflows/ci.yml)
[![Language: C99](https://img.shields.io/badge/language-C99-blue)](https://en.wikipedia.org/wiki/C99)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Platform: MCU | Linux | Windows | macOS](https://img.shields.io/badge/platform-MCU%20%7C%20Linux%20%7C%20Windows%20%7C%20macOS-informational)](https://github.com/Vanderhell/loxdb)
[![Tests](https://img.shields.io/badge/tests-504%20microtests-brightgreen)](docs/TEST_SUITE_SIZE.md)
[![Release](https://img.shields.io/github/v/release/Vanderhell/loxdb)](https://github.com/Vanderhell/loxdb/releases)

## What is loxdb?

loxdb is a compact embedded database written in C99 for firmware and small edge runtimes.
It provides one unified API over three engines (KV, time-series, relational) and is designed around predictable memory behavior.
The library allocates once at `lox_init()` and runs without allocator churn during normal operation.
Persistence is optional via a small storage HAL (read/write/erase/sync), with WAL + recovery when enabled.

Test suite size: **504 microtest cases across 48 test files (+1 C++ wrapper test), organized into ~78 CTest entries including RAM-budget sweep matrices.**

## Why loxdb? (When to use / when not to)

| Use loxdb when you need... | Avoid loxdb when you need... |
|---|---|
| bounded RAM and predictable allocation behavior | unbounded queries / SQL flexibility |
| durability with WAL recovery on flash-like media | a full SQL database with complex query planning |
| KV + telemetry streams + small indexed tables in one library | multi-process concurrency / server database features |
| a small storage HAL integration | transparent large-object storage and advanced indexing |

## Quick start (RAM-backed)

```c
#include "lox.h"
#include "lox_port_ram.h"

int main(void) {
    lox_t db;
    lox_storage_t storage;
    lox_cfg_t cfg = {0};

    lox_port_ram_init(&storage, 64u * 1024u);
    cfg.storage = &storage;
    cfg.ram_kb = 32u;
    if (lox_init(&db, &cfg) != LOX_OK) return 1;

    uint8_t v = 7u, out = 0u;
    lox_kv_put(&db, "k", &v, 1u);
    lox_kv_get(&db, "k", &out, 1u);

    lox_deinit(&db);
    lox_port_ram_deinit(&storage);
    return 0;
}
```

## Build & test (desktop)

```bash
cmake --preset ci-debug-linux
cmake --build --preset ci-debug-linux
ctest --preset ci-debug-linux
```

## Three engines in 30 seconds

- **KV (key-value):** config/state, binary-safe values, optional TTL, bounded by compile-time limits.
- **TS (time-series):** typed telemetry streams (`F32/I32/U32/RAW`) with timestamp range queries and retention policies.
- **REL (relational):** small fixed-schema tables with one indexed column, designed for predictable memory use.

## Verified hardware

loxdb is written in portable C99 and works on any MCU with byte-write 
flash storage — including ESP32 family, STM32, RP2040, nRF52, and similar 
Cortex-M class hardware. Only platforms verified end-to-end on real 
hardware are listed below; ports to other targets are technically 
supported but not yet bench-validated.

### Verified on hardware

| Platform | Status | Benchmarks |
|---|---|---|
| ESP32-S3 N16R8 (16MB NOR flash, 8MB PSRAM) | Verified — KV/TS/REL engines, WAL recovery, power-loss scenarios | [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) |

Verified using the ESP32-S3 bench runners under [`bench/`](bench/). 
Published benchmark results live in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) 
(template until filled with real measurements).

## Project status & roadmap

- Current release line: `v1.4.0` (see `CHANGELOG.md`).
- Young project focused on embedded correctness and predictable memory behavior; production use-cases and feedback are welcome.

## loxdb vs loxdb_pro

This repository is the MIT-licensed OSS edition. A planned commercial edition (`loxdb_pro`) will live in a separate repository as additive modules on top of `loxdb` (it will not replace or relicense the MIT core). See `docs/EDITIONS.md`.

## Documentation

- Getting started: `docs/GETTING_STARTED_5_MIN.md`
- Programmer manual: `docs/PROGRAMMER_MANUAL.md`
- Backend integration: `docs/BACKEND_INTEGRATION_GUIDE.md`
- Port authoring (ESP32 reference): `docs/PORT_AUTHORING_GUIDE.md`
- Schema migration: `docs/SCHEMA_MIGRATION_GUIDE.md`
- Docs index: `docs/README.md`

## Contributing & support

- Contributing guide: `.github/CONTRIBUTING.md`
- Support policy: `.github/SUPPORT.md`
- Security policy: `.github/SECURITY.md`

## License

MIT (see `LICENSE`).

