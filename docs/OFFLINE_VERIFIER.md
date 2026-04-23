# Offline Verifier (`lox_verify`)

`lox_verify` is a read-only verifier for a persisted loxdb storage image.

## What it checks

- superblock A/B validity (magic/version/header CRC/active bank)
- bank A/B page headers (KV/TS/REL magic/version/header CRC)
- bank A/B payload CRC for KV/TS/REL pages
- WAL header validity and WAL entry-chain integrity
- recovery root selection (`superblock` vs `bank_scan` fallback)

## KV decode checks

After KV page header + payload CRC validation, verifier decodes KV payload entries and reports:

- `live_keys`
- `tombstones`
- `value_bytes_used`
- `overlaps_detected`

Decode anomalies are surfaced as `WARN` lines and included in JSON `warnings`.

## TS decode checks

After TS page header + payload CRC validation, verifier decodes stream entries and reports:

- `stream_count`
- `retained_samples`
- `ring_anomalies`

Any truncated stream/sample decode path or invalid type is reported as `WARN`.

## REL decode checks

After REL page header + payload CRC validation, verifier decodes table metadata and reports:

- `table_count`
- `live_rows`
- `bitmap_mismatches`

Structural decode mismatches (truncation, inconsistent row span, invalid column metadata bounds) are reported as `WARN`.

## WAL semantic summary

WAL scan includes semantic counters in addition to structural validity:

- KV: `SET`, `DEL`, `CLEAR`
- TS: `INSERT`, `REGISTER`, `CLEAR`
- REL: `INSERT`, `DEL`, `CLEAR`, `CREATE`
- TXN: `txn_kv`, `txn_committed`, `txn_orphaned`

These counters are emitted in text output and JSON (`wal_semantic`).

## --check flag (CI mode)

Use `--check` for strict CI gating:

- Exit `0` only when verdict is exactly `ok` and no `WARN` lines were produced.
- Exit `4` on `corrupt`, `recoverable`, or any `WARN`.
- Exit `5` on `uninitialized`.

`--check` prints only:

- verdict line
- `WARN`/`ERROR` lines

## Output

- human-readable text by default
- JSON with `--json`
- includes verdict, recovery reason, selected bank/generation, WAL summary, and layout metadata

## Output format reference

Text mode includes:

- image/layout header
- superblock and selected bank summary
- KV/TS/REL decode summaries
- WAL validity + semantic op counts
- overall verdict + recovery reason
- appended `WARN` lines (if any)

JSON mode is always a single complete object and includes:

- base verdict/layout/super/bank fields
- `kv_decode`
- `ts_decode`
- `rel_decode`
- `wal_semantic`
- `warnings` array

## Exit codes

- `0` = valid/recoverable image (`ok`, `ok_with_wal_header_reset`, `ok_with_wal_tail_truncation`)
- `1` = usage error
- `2` = I/O error
- `3` = invalid verifier config (RAM/split/erase does not match image geometry)
- `4` = unrecoverable corruption
- `5` = uninitialized image (cold-start state)

On failure, stderr uses a stable format:

- `<operation> failed: <VERIFY_CODE_NAME> (<numeric_code>) - <detail>`

## Usage

```bash
lox_verify --image db.bin --json
lox_verify --image db.bin --ram-kb 32 --kv-pct 40 --ts-pct 40 --rel-pct 20 --erase-size 4096 --json
lox_verify --image db.bin --check
```

## Notes

- The verifier is read-only and does not mutate the image.
- Geometry is derived from build/runtime profile inputs (`ram_kb`, split, `erase_size`) and image size.
- If these inputs do not match how the image was produced, verifier returns exit code `3`.
