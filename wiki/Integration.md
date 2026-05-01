# Integration

This page points to the practical integration docs in the repository.

## Backend and Storage Integration

- Backend integration guide:  
  `docs/BACKEND_INTEGRATION_GUIDE.md`
- Port authoring guide (ESP32 reference):  
  `docs/PORT_AUTHORING_GUIDE.md`
- FS/block adapter contract:  
  `docs/FS_BLOCK_ADAPTER_CONTRACT.md`

## Thread Safety / RTOS Lock Hooks

- Thread-safety contract:  
  `docs/THREAD_SAFETY.md`

Use `lox_cfg_t` lock callbacks for multithreaded builds:

- `lock_create`
- `lock`
- `unlock`
- `lock_destroy`

## Migration and Contracts

- Schema migration and compatibility entry points:  
  `docs/PROGRAMMER_MANUAL.md`
- Profile guarantees:  
  `docs/PROFILE_GUARANTEES.md`
- Fail-code contract:  
  `docs/FAIL_CODE_CONTRACT.md`

## Full Docs Map

- `docs/DOCS_MAP.md`

