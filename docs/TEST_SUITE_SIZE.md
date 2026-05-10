# Test suite size (measured)

The repo uses CMake/CTest, but the most meaningful “test case” unit is the in-repo microtest harness (`tests/microtest.h`) executed via `MDB_RUN_TEST(...)`.

Use this exact phrasing in user-facing docs:

> **504 microtest cases across 48 test files (+1 C++ wrapper test), organized into ~78 CTest entries including RAM-budget sweep matrices.**

## Breakdown

- **Microtest cases (504):** exact count of `MDB_RUN_TEST(` call sites across `tests/*.c`.
- **Test files (48 + 1):**
  - `48` × `tests/test_*.c`
  - `+1` × `tests/test_*.cpp` (`tests/test_cpp_wrapper.cpp`)
- **CTest entries (~78):**
  - Root `CMakeLists.txt` contains `72` textual `add_test` tokens.
  - Two `foreach(RAM_KB 128 256 512 1024)` matrices generate `4 + 4` additional configured CTest entries:
    - `integration_${RAM_KB}kb`
    - `limits_${RAM_KB}kb`

## Re-measuring (quick)

- Microtest cases: count occurrences of `MDB_RUN_TEST(` under `tests/`.
- Effective configured CTest list for a preset: configure + `ctest -N` (e.g. `cmake --preset ci-debug-linux` then `ctest --preset ci-debug-linux -N`).

Notes:
- The microtest-case count is stable and meaningful.
- The effective CTest entry count may vary with optional/conditional targets enabled at configure time.

