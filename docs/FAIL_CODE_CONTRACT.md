# Fail-Code Contract

This document defines the external error semantics for `loxdb` public API.

## Error String Mapping

- Use `lox_err_to_string(lox_err_t)` to convert return codes to stable symbolic names.
- The function always returns a non-null string.
- Unknown numeric values map to `LOX_ERR_UNKNOWN`.

## Core Error Codes

- `LOX_ERR_INVALID`
  - Invalid handle, null pointer, invalid argument, API misuse before init/after deinit.
  - Storage contract violation at init/open, including:
    - `erase_size == 0`
    - `write_size == 0`
    - `write_size != 1` (current releases support only `write_size = 1`)
- `LOX_ERR_NO_MEM`
  - RAM allocation/init budget failure.
- `LOX_ERR_FULL`
  - Capacity limit reached for bounded containers (for example REL table max rows).
- `LOX_ERR_NOT_FOUND`
  - Missing key/stream/row.
- `LOX_ERR_EXPIRED`
  - KV value exists but TTL expired.
- `LOX_ERR_STORAGE`
  - Storage backend I/O failure (`read/write/erase/sync`) or insufficient storage budget.
- `LOX_ERR_CORRUPT`
  - Corrupt persisted page/snapshot/WAL record detected in strict decode paths.
- `LOX_ERR_SEALED`
  - REL schema modification attempted after `schema_seal`.
- `LOX_ERR_EXISTS`
  - Duplicate registration/create for existing object.
- `LOX_ERR_DISABLED`
  - Feature disabled at compile time.
- `LOX_ERR_OVERFLOW`
  - User output buffer too small.
- `LOX_ERR_SCHEMA`
  - Schema mismatch or unsupported migration path.
- `LOX_ERR_TXN_ACTIVE`
  - Nested/conflicting transaction state.

## Recovery Contract Notes

- WAL header CRC corruption and WAL tail truncation are treated as **recoverable replay conditions**.
  - Behavior: init/open succeeds, invalid WAL tail is dropped, committed state is preserved.
  - These cases do not necessarily surface as immediate `LOX_ERR_CORRUPT` to the caller.
- Hard decode failures in strict snapshot/page paths return `LOX_ERR_CORRUPT`.

## Verified By Tests

- API contract matrix: `tests/test_api_contract_matrix.c`
- Fail-code and recovery contract: `tests/test_fail_code_contract.c`
- Error-code string mapping: `tests/test_error_strings.c`
- Additional durability/recovery coverage: `tests/test_wal.c`, `tests/test_durability_closure.c`
