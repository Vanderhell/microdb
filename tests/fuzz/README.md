# Fuzzing (libFuzzer)

This directory contains libFuzzer harnesses for loxdb’s most safety-critical parsers and decoders.

## Requirements

- Linux (recommended) with clang/llvm installed
- libFuzzer (ships with clang)

## Harnesses

- `fuzz_wal_parser.cpp`: targets WAL entry/header parsing logic (via `tools/lox_verify.c` WAL inspector).

## How to add a new harness

1. Create a new `fuzz_*.cpp` file with the standard entry point:
   - `extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);`
2. Prefer targeting **pure parsing/decoding** functions (no network/IO), and keep runtime bounded:
   - cap input size
   - avoid unbounded loops
3. Add a build snippet to `tests/fuzz/build.sh` and a run snippet to `tests/fuzz/run_one.sh`.

## Local build + run (Linux)

```bash
./tests/fuzz/build.sh
./tests/fuzz/run_one.sh fuzz_wal_parser 600
```

The second argument is the max runtime in seconds.

