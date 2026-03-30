# Testing

microdb uses CMake and CTest.

## Local workflow

```sh
cmake -S . -B build -DMICRODB_BUILD_TESTS=ON
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

## Coverage areas

The current test suite covers:

- KV behavior and overflow variants
- TS behavior and overflow variants
- REL schema and indexed access
- WAL recovery and disabled-WAL mode
- integration flows in RAM-only and persistent modes
- RAM budget variants from small to large configurations
- compile-fail validation for invalid percentage sums

## CI

The repository CI workflow builds and runs the suite on GitHub Actions for Linux and Windows.
