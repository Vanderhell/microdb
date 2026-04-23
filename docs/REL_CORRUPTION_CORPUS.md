# REL Corruption Corpus

This corpus defines deterministic REL persistence corruption fixtures used by `test_rel_corruption_replay`.

Location:

- `tests/corpus/rel/*.fixture`

Current fixtures:

- `flip_payload_crc.fixture`: flips REL page payload CRC bits in both banks.
- `overflow_table_name_len.fixture`: writes invalid table-name length.
- `overflow_col_count.fixture`: writes `col_count` above `LOX_REL_MAX_COLS`.
- `overflow_row_count.fixture`: writes `row_count` above persisted `max_rows`.

Expected test contract:

- Fixture expectation is read from each `.fixture` (`LOX_ERR_CORRUPT` or `LOX_OK_OR_CORRUPT`).
- `LOX_OK_OR_CORRUPT` is allowed when one bank remains recoverable.

Run locally:

- configure/build tests (for example `ci-debug-linux` preset)
- run: `ctest --preset ci-debug-linux -R test_rel_corruption_replay --output-on-failure`
