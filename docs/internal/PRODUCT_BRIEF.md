# loxdb Product Brief

## One-line positioning

loxdb is a deterministic embedded storage core for MCU/edge systems with KV, TS, and REL APIs in a compact C99 surface.

## Target users

- firmware teams building persistent embedded devices
- products that need bounded memory behavior
- systems that require reopen/recovery behavior after resets

## Core value

- one allocation model at init
- fixed resource budgeting through compile-time/runtime configuration
- optional WAL-based durability path with storage HAL
- explicit fail-code contract

## Product scope

- KV: configuration/state values, optional TTL
- TS: named streams with timestamped samples
- REL: fixed-schema tables with one indexed column per table
- diagnostics/capacity APIs for pressure and budget visibility

## Non-goals

- SQL query language
- dynamic schema planner/optimizer
- large multi-index relational workloads

## Trust assets in this repository

- fail-code contract: `docs/FAIL_CODE_CONTRACT.md`
- profile/limit contract: `docs/PROFILE_GUARANTEES.md`
- storage/recovery invariants: `docs/CORE_INVARIANTS.md`
- offline verifier contract: `docs/OFFLINE_VERIFIER.md`
- executable tests under `tests/`

## Adoption path

1. start RAM-only
2. integrate persistent storage HAL
3. select build/profile variant
4. validate with target workload and long-run reopen tests
