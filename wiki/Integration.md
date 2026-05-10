# Integration

This page points to the practical integration docs in the repository.

## Backend and Storage Integration

- Backend integration guide:  
  `https://github.com/Vanderhell/loxdb/blob/master/docs/BACKEND_INTEGRATION_GUIDE.md`
- Port authoring guide (ESP32 reference):  
  `https://github.com/Vanderhell/loxdb/blob/master/docs/PORT_AUTHORING_GUIDE.md`
- FS/block adapter contract:  
  `https://github.com/Vanderhell/loxdb/blob/master/docs/FS_BLOCK_ADAPTER_CONTRACT.md`

## Thread Safety / RTOS Lock Hooks

- Thread-safety contract:  
  `https://github.com/Vanderhell/loxdb/blob/master/docs/THREAD_SAFETY.md`

Use `lox_cfg_t` lock callbacks for multithreaded builds:

- `lock_create`
- `lock`
- `unlock`
- `lock_destroy`

## Migration and Contracts

- Schema migration and compatibility entry points:  
  `https://github.com/Vanderhell/loxdb/blob/master/docs/PROGRAMMER_MANUAL.md`
- Profile guarantees:  
  `https://github.com/Vanderhell/loxdb/blob/master/docs/PROFILE_GUARANTEES.md`
- Fail-code contract:  
  `https://github.com/Vanderhell/loxdb/blob/master/docs/FAIL_CODE_CONTRACT.md`

## Full Docs Map

- `https://github.com/Vanderhell/loxdb/blob/master/docs/README.md`

