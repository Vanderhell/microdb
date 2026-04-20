# Port Authoring Guide

This guide shows how to author a real platform port for `microdb`.

Primary live reference:

- `port/esp32/microdb_port_esp32.c`
- `examples/freertos_port/main.c` (RTOS lock/storage skeleton)
- `examples/zephyr_port/main.c` (RTOS lock/storage skeleton)

Use it as a template for mapping your platform driver to `microdb_storage_t`.

## 1) What a port must provide

`microdb` expects a filled `microdb_storage_t` with:

- `read(ctx, offset, buf, len)`
- `write(ctx, offset, buf, len)`
- `erase(ctx, offset)` (erase exactly one erase block)
- `sync(ctx)` (durability barrier)
- `capacity`, `erase_size`, `write_size`
- `ctx` (driver context pointer)

The ESP32 port is a direct example of this mapping:

- `esp_partition_read` -> `storage.read`
- `esp_partition_write` -> `storage.write`
- `esp_partition_erase_range` -> `storage.erase`
- sync no-op for this backend -> `storage.sync`

## 2) Why ESP32 port is the reference example

`port/esp32/microdb_port_esp32.c` is not just a platform utility.
It is the canonical "real driver glue" example in this repository.

If you are writing a new port (STM32, custom RTOS, Zephyr, FreeRTOS platform BSP):

1. Copy the ESP32 structure.
2. Replace driver calls with your platform API.
3. Keep the same error mapping style (`MICRODB_OK` / `MICRODB_ERR_STORAGE` / `MICRODB_ERR_INVALID`).

## 3) Core storage contract and adapters

Core `microdb` storage contract is strict:

- `erase_size > 0`
- `write_size == 1`

If your medium cannot provide byte-write semantics (`write_size != 1`) directly:

- do not pass raw storage directly into `microdb_init`
- use optional backend adapter flow (`microdb_backend_open_prepare`) and aligned/managed adapter path

See:

- `docs/BACKEND_INTEGRATION_GUIDE.md`
- `docs/FS_BLOCK_ADAPTER_CONTRACT.md`

## 4) Async erase and sync semantics

Port authors must define clear behavior for:

- erase completion timing (sync vs async)
- what `sync()` guarantees (flush only vs durable persistence)

Rule of thumb:

- if your erase/write path is asynchronous, `sync()` must complete pending operations before returning `MICRODB_OK`
- returning `MICRODB_OK` from `sync()` means durability contract for your selected backend policy is satisfied

## 5) Thread safety hooks (RTOS integration)

Storage hooks are only one part of a production port.
For multithreaded environments, wire lock callbacks in `microdb_cfg_t`:

- `lock_create`
- `lock`
- `unlock`
- `lock_destroy`

Internal lock behavior is gated by `MICRODB_THREAD_SAFE` (see `src/microdb_lock.h`):

- `MICRODB_THREAD_SAFE=0`: lock macros are no-op
- `MICRODB_THREAD_SAFE=1`: callbacks are invoked around core operations

Minimal integration pattern:

```c
microdb_cfg_t cfg = {0};
cfg.storage = &storage;
cfg.lock_create = my_mutex_create;
cfg.lock = my_mutex_lock;
cfg.unlock = my_mutex_unlock;
cfg.lock_destroy = my_mutex_destroy;
```

## 6) Port bring-up checklist

1. Validate raw read/write/erase/sync behavior with a standalone driver test.
2. Fill `microdb_storage_t` and run `microdb_init` smoke test.
3. Verify reopen behavior (`init -> write -> deinit -> init -> read`).
4. Verify power-loss path and recovery behavior where applicable.
5. Run backend-open and recovery tests if adapter flow is used.

## 7) Common mistakes

- Treating backend stubs as real drivers.
- Setting `write_size` to natural media granularity and calling `microdb_init` directly.
- Returning success from `sync()` before pending operations are durable.
- Enabling thread-safe build without wiring lock callbacks.
