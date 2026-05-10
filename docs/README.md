# Documentation index

Start here:

- Getting started: `GETTING_STARTED_5_MIN.md`
- Programmer manual: `PROGRAMMER_MANUAL.md`
- Backend integration: `BACKEND_INTEGRATION_GUIDE.md`
- Port authoring (ESP32 reference): `PORT_AUTHORING_GUIDE.md`
- Schema migration: `SCHEMA_MIGRATION_GUIDE.md`

Other technical notes:

- Profiles: `PROFILES.md`
- Safety notes: `SAFETY_NOTES.md`
- Offline verifier: `OFFLINE_VERIFIER.md`
- WCET analysis: `WCET_ANALYSIS.md`
- Benchmarks (results publication): `BENCHMARKS.md`

Internal/process documents live in `docs/internal/`.

## Distribution (planned)

Publishing is maintainer-driven and not automated yet:

- PlatformIO Registry (`library.json`)
- Arduino Library Manager (`library.properties`)
- CMake install + `find_package(loxdb)` via installed config files (see `CMakeLists.txt` and `cmake/loxdbConfig.cmake.in`)
