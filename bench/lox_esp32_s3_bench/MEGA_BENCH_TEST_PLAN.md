# Mega Bench Test Plan (Draft)

Based on current core scope (KV, TS, REL, WAL/compact, migration, txn, reopen/reset/inspect), this is the long-horizon checklist for expanded coverage.

## 1. Boot / open / close / reopen

- open empty DB
- open after wipe
- reopen without changes
- reopen after KV writes
- reopen after TS writes
- reopen after REL writes
- reopen after mixed workload
- reopen after compact
- reopen after migration
- reopen after successful txn commit
- reopen after txn rollback
- open with corrupted header
- open with invalid format version
- open with invalid magic
- open with partially written header
- open with corrupted inspect/stats metadata
- repeated reopen cycles
- open when storage driver read fails
- open when storage driver write fails

## 2. KV basic

- put new key
- put existing key overwrite
- get existing key
- get missing key
- delete existing key
- delete missing key
- put key with min length
- put key with max length
- put zero-length value
- put max-length value
- put value one byte over limit
- put many distinct keys
- repeatedly overwrite one key
- get after repeated overwrite
- delete then get
- delete then re-put same key
- put binary payload with zero bytes
- put binary payload with `0xFF` pattern
- put random binary payload
- iterate all KV items

## 3. KV correctness / edge cases

- hash/index collision handling for distinct keys
- heavy collision bucket behavior
- eviction path preserves newest data
- eviction path does not corrupt old data
- near-full KV behavior
- full KV behavior
- put after full condition and reclaim/compact
- tombstone persistence after reopen
- repeated delete of same key
- put/delete sequence on same key
- iterator excludes deleted records
- iterator returns latest version
- same-prefix key handling
- case-sensitive key behavior
- long key + short value
- short key + long value
- KV integrity after reopen
- KV integrity after compact
- KV integrity after migration
- benchmark validation checks data, not only counts

## 4. TS basic

- create stream
- insert first sample
- insert multiple increasing timestamps
- query full stream
- query bounded range
- query empty range
- query missing stream
- insert min timestamp
- insert max timestamp
- insert zero-length payload (if supported)
- insert max-length payload
- insert payload over limit
- high sample count in one stream
- multi-stream inserts
- query after reopen
- query after compact
- query after migration
- query last sample
- query first sample
- sample count by range

## 5. TS ordering / retention / correctness

- out-of-order insert behavior
- duplicate timestamp insert behavior
- duplicate timestamp with different value
- query ordering correctness
- query strict-range correctness
- segment-full behavior
- rollover behavior
- retention drops oldest samples
- retention preserves newer samples
- stream metadata persistence after reopen
- stream metadata persistence after compact
- mixed KV+TS workload
- mixed REL+TS workload
- binary payload edge cases
- large-range query across segments
- one-sample stream query
- query after reset/wipe
- TS fill-percent consistency
- inspect sample counts consistency

## 6. REL basic

- create table
- create multiple tables
- insert one row
- insert many rows
- find by primary key
- find by index
- find missing row
- insert min-size row
- insert max-size row
- insert over-limit row
- update existing row
- update missing row
- delete existing row
- delete missing row
- full table scan
- empty table scan
- reopen and find row
- compact and find row
- migrate and find row
- mixed inserts across tables

## 7. REL indexes / constraints / correctness

- index creation correctness
- index lookup correctness
- deleted-row index exclusion
- index update after indexed-field change
- duplicate-key rejection
- duplicate-key rollback behavior
- index cleanup on delete
- index consistency after reopen
- index consistency after compact
- scan order deterministic/stable behavior
- nullable-like field behavior (if format allows)
- lookup after high insert volume
- lookup after delete+insert same key
- repeated updates on same row
- row count consistency under mixed workload
- table metadata after reopen
- table metadata after compact
- corrupted row record detection
- corrupted index record detection
- inspect stats consistency

## 8. WAL / journaling

- append one record
- append many records
- replay after reopen
- replay after simulated crash
- partial-write tail handling
- corrupted WAL header handling
- corrupted WAL tail handling
- corrupted record CRC handling
- compact on empty WAL
- compact on partially full WAL
- compact on nearly full WAL
- compact preserves valid state
- compact removes obsolete data
- WAL fill-percent before/after compact
- reopen after compact
- repeated compact cycles
- append after compact
- replay idempotence after repeated reopen
- benchmark validates post-compact content

## 9. Transactions

- begin/commit empty txn
- begin/rollback empty txn
- KV put in txn commit
- KV put in txn rollback
- KV delete in txn commit
- KV delete in txn rollback
- TS insert in txn commit
- TS insert in txn rollback
- REL insert in txn commit
- REL insert in txn rollback
- mixed KV+TS+REL commit
- mixed KV+TS+REL rollback
- repeated same-key operations in txn
- commit behavior after reopen
- rollback behavior after reopen
- crash before commit marker
- crash after commit marker before finalize
- crash during rollback path
- txn recovery idempotence

## 10. Migration / schema

- open old schema and migrate to new
- migration callback called exactly once
- no callback on equal version
- callback failure keeps DB consistent
- partial migration crash behavior
- recovery after partial migration
- KV data preserved across migration
- TS metadata preserved across migration
- REL tables preserved across migration
- migrate then reopen
- migrate then compact
- chained migrations across versions
- rejected downgrade behavior
- rejected unknown-future-version behavior
- migration modifies only intended fields

## 11. Corruption / recovery / power-fail

- power cut during header write
- power cut during KV append
- power cut during TS append
- power cut during REL insert
- power cut during WAL append
- power cut during compact
- power cut during metadata update
- recovery after single corrupted record
- recovery after segment corruption
- recovery after CRC corruption
- recovery after random tail bytes
- recovery after torn last write
- recovery excludes ghost records
- recovery preserves last committed state
- recovery after repeated crash loops

## 12. Capacity / limits / stress

- fill to limit with KV-only
- fill to limit with TS-only
- fill to limit with REL-only
- fill mixed workload to limit
- ENOSPC behavior validation
- reclaim behavior after frees/deletes
- long run 10k operations
- long run 100k operations
- periodic reopen every N ops
- periodic compact every N ops
- randomized mixed workload
- randomized mixed + reopen
- randomized mixed + compact
- randomized mixed + crash injection
- inspect stats after stress

## 13. API / contract / misuse

- null pointer rejection
- invalid handle rejection
- double close defined behavior
- API call before open rejection
- API call after close rejection
- too-small output buffer handling
- output length correctness
- unsupported mode rejection
- invalid stream/table name rejection
- stable error-code contract

## 14. Stats / inspect / observability

- stats on fresh DB
- stats after KV writes
- stats after TS writes
- stats after REL writes
- stats after delete
- stats after compact
- stats after reopen
- stats after migration
- stats after txn commit
- stats after txn rollback
- collision count consistency
- eviction count consistency
- WAL used/total consistency
- TS fill-percent consistency
- REL row-count consistency

## 15. Performance regression tests

- KV put median under target
- KV put tail under target
- TS insert median under target
- TS insert tail under target
- REL insert median under target
- REL lookup under target
- compact latency under target
- reopen latency under target
- migration latency under target
- recovery latency under target
