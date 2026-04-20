# Zephyr Port Skeleton

This folder is a template for integrating `microdb` on Zephyr-based targets.

File:

- `main.c` - storage hook and lock-hook wiring skeleton

What to replace:

- storage hook bodies (`read/write/erase/sync`)
- storage geometry (`capacity`, `erase_size`)
- lock mapping (`k_mutex_lock` / `k_mutex_unlock`)

Notes:

- direct core mode requires `write_size = 1`
- if your medium cannot do byte writes directly, use backend-open adapter path

Reference docs:

- `docs/PORT_AUTHORING_GUIDE.md`
- `docs/BACKEND_INTEGRATION_GUIDE.md`

