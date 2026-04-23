# Backend Integration Guide

This guide is the practical entry point for integrating `loxdb` storage backends.

It explains:

- how to connect a raw `lox_storage_t` directly to `lox_init`
- how to use optional backend-open orchestration (`descriptor -> decision -> adapter`)
- what backend stubs are (and are not)
- where to start when writing a real platform port

For real port authoring, see:

- `docs/PORT_AUTHORING_GUIDE.md`
- `port/esp32/lox_port_esp32.c` (live reference implementation)

## 1) Two integration modes

### A) Direct mode (minimal path)

Use this when your storage already satisfies core contract requirements:

- `erase_size > 0`
- `write_size == 1`
- stable `read/write/erase/sync` semantics

Flow:

1. Prepare `lox_storage_t` from your platform driver.
2. Put it into `lox_cfg_t.storage`.
3. Call `lox_init`.

### B) Backend-open mode (policy path)

Use this when you support multiple storage classes, or when media may need adapter wrapping.

Flow:

1. Reset session (`lox_backend_open_session_reset`).
2. Call `lox_backend_open_prepare(...)`.
3. Use returned `out_storage` in `lox_cfg_t.storage`.
4. Call `lox_init`.
5. On shutdown, call `lox_backend_open_release(...)`.

## 2) Reference backend-open integration

```c
#include "lox.h"
#include "lox_backend_open.h"

lox_t db;
lox_cfg_t cfg = {0};
lox_backend_open_session_t session;
lox_storage_t raw_storage;       /* filled by your platform */
lox_storage_t *opened = NULL;

lox_backend_open_session_reset(&session);

if (lox_backend_open_prepare("my_backend",
                                 &raw_storage,
                                 1u,   /* has_aligned_adapter */
                                 1u,   /* has_managed_adapter */
                                 &session,
                                 &opened) != LOX_OK) {
    /* handle open-classification/adapter failure */
}

cfg.storage = opened;
cfg.ram_kb = 32u;

if (lox_init(&db, &cfg) != LOX_OK) {
    lox_backend_open_release(&session);
    /* handle init failure */
}

/* ... use db ... */

lox_deinit(&db);
lox_backend_open_release(&session);
```

## 3) Important: backend stubs are not media drivers

Modules like `*_stub` are capability descriptors used by decision/orchestration and test matrices.

They do **not** provide:

- real hardware I/O implementation
- NAND ECC
- bad block management
- wear leveling

For production media integration, you still must provide real driver glue in `lox_storage_t` hooks.

Practical interpretation by medium:

- SD card / eMMC:
  - supported when you already have a real stack (for example FatFS/LittleFS + SDMMC/SPI block I/O, or vendor managed block API).
  - unsupported as "plug-and-play" from stubs alone.
- Raw NAND:
  - not a direct loxdb storage target.
  - requires a managed layer that provides ECC, bad-block handling, and wear leveling before mapping to `lox_storage_t`.

Reference glue example:

- `examples/sd_fatfs_port/main.c` (SD + FatFS file-backed storage hook skeleton)

## 4) Which optional adapter path to use

- Aligned-write media: use aligned adapter path (RMW/byte-write shim).
- Managed media with durable sync semantics: managed adapter path.
- Filesystem/block-like path with non-durable flush semantics: filesystem adapter policy path.

Detailed contract:

- `docs/FS_BLOCK_ADAPTER_CONTRACT.md`
- `docs/PROGRAMMER_MANUAL.md` (backend APIs and open wrapper sections)
- `examples/aligned_block_port/main.c` (reference aligned/block glue skeleton)

## 5) Common pitfalls

- Calling `lox_init` directly with unsupported geometry (`write_size != 1`) without adapter path.
- Assuming stubs are plug-and-play drivers.
- Forgetting `lox_backend_open_release` after backend-open session use.
- Mixing adapter policy assumptions without explicit expectations/checks.

## 6) Bring-up checklist

1. Validate raw storage hooks (`read/write/erase/sync`) with small smoke test.
2. Decide direct mode vs backend-open mode.
3. Verify open/reopen/power-loss behavior with corresponding backend tests.
4. Run CI lane including `test_backend_open`, recovery tests, and stress/matrix slices.
