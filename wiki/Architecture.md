# Architecture

loxdb exposes one API surface across three engines:

- KV engine
- Time-series engine
- Relational engine

## Memory model

loxdb allocates exactly once during `lox_init()`.
That heap block is partitioned into fixed slices for KV, TS, and REL.
There is no runtime allocator churn after initialization.

Default split is controlled by:

- `LOX_RAM_KV_PCT`
- `LOX_RAM_TS_PCT`
- `LOX_RAM_REL_PCT`

Runtime overrides are available in `lox_cfg_t`.

## Persistence model

loxdb can run:

- in RAM-only mode
- with a POSIX file-backed storage HAL
- with an ESP32 partition-backed storage HAL

When storage is present and WAL is enabled, writes are logged and replayed during recovery.

## Tradeoffs

- predictable memory instead of elastic allocation
- one index per relational table instead of secondary indexes
- single-threaded core instead of built-in synchronization
- embedded-first limits instead of general-purpose database behavior
