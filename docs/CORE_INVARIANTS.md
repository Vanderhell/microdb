# loxdb Core Invariants

1. Active snapshot bank is never overwritten in place.
2. Compact/flush writes inactive bank first, syncs, then switches superblock.
3. Boot selects only validated durable state (superblock+page CRCs and bounds).
4. WAL replay stops at first invalid entry and never fail-opens selected snapshot.
5. TXN_KV replay entries are ignored unless a durable TXN_COMMIT marker is present.
6. Public callback-based APIs invoke user callbacks outside internal DB lock.
7. Public DB-handle APIs acquire/release internal lock consistently.
8. Optional profile/perf hooks are compile-time only and must not alter durability order.
9. Persistent-storage contract is fail-fast at open/init: `erase_size > 0` and `write_size == 1`.
