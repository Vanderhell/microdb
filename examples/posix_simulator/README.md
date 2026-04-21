## POSIX End-to-End Lifecycle Example

This example is a full persistence lifecycle on POSIX:

1. init database
2. write KV/TS/REL data
3. flush + deinit
4. reopen (simulated reboot)
5. verify recovered data

The example source is `examples/posix_simulator/main.c`.

### Build

From repository root:

```sh
cmake -S . -B build
cmake --build build --target example_posix_simulator
```

### Run

Windows:

```sh
.\build\Debug\example_posix_simulator.exe
```

Linux/macOS:

```sh
./build/example_posix_simulator
```

### Expected output shape

- `Reloaded KV entries:`
- `TS temp_last ...`
- `TS hum_last ...`
- `Reloaded REL rows:`

If these sections print without errors, init/write/reopen/recovery path is working.
