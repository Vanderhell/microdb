# Aligned Block Port Skeleton

This example shows how to integrate `loxdb` on storage media that cannot do byte writes directly.

Scenario:

- raw medium has `write_size > 1` (for example 4 or 8 bytes)
- loxdb core still requires byte-write contract
- aligned adapter bridges the gap with bounce-buffer read-modify-write behavior

Files:

- `main.c` - raw storage hooks + aligned adapter lifecycle skeleton

Key lifecycle:

1. Fill raw `lox_storage_t` with native media geometry (`write_size > 1`).
2. Call `lox_backend_aligned_adapter_init(...)`.
3. Pass adapted storage into `lox_init(...)`.
4. On shutdown call `lox_backend_aligned_adapter_deinit(...)`.

Sync expectations:

- raw `sync()` must return success only when pending data is durable enough for your policy.
- if erase/write is asynchronous in the underlying driver, complete it before returning `LOX_OK`.

Reference docs:

- `docs/BACKEND_INTEGRATION_GUIDE.md`
- `docs/PORT_AUTHORING_GUIDE.md`
- `docs/FS_BLOCK_ADAPTER_CONTRACT.md`

