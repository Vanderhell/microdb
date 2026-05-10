# Maintainer TODO (repo settings + follow-ups)

This file lists actions that require repository admin access or infrastructure decisions and therefore cannot be done from a docs-only PR.

## GitHub repository settings checklist

1. **Description (About)**
   - Suggested text:
     - Predictable-memory embedded database for microcontrollers. KV, time-series, and relational engines with WAL recovery. C99, zero dependencies, verified on ESP32-S3.

2. **Topics**
   - Suggested topics:
     - `embedded-database`, `wal`, `flash-storage`, `microcontroller`, `esp32`, `c99`
     - (optional) `littlefs-alternative`

3. **Wiki**
   - Disable GitHub Wiki in repo settings.
   - Keep `wiki/` (in-repo Markdown) as the single source of truth for wiki content.
   - Rationale: avoids double maintenance and drift.

4. **Discussions**
   - Keep enabled.
   - Add a short pinned “Welcome / How to ask for help” post pointing to:
     - the minimal repro expectations
     - target platform details (MCU, storage backend, erase/write sizes)

5. **Releases**
   - For `v1.4.0`, ensure GitHub Release notes are complete and not duplicated in repo-root process docs.

6. **Social preview image**
   - Create a 1280×640 social preview image: project name + one-line technical tagline (no marketing contract language).

## Documentation surface policy

- Keep `README.md` short and technical.
- Keep “process” and “status artifact” docs under `docs/internal/`.
- Keep `docs/` for technical docs users actually need to integrate and ship.
- Keep `docs/results/` as a working directory for tooling outputs, but avoid linking to it from top-level docs.

## CI roadmap (future; infrastructure decisions required)

Not implemented in this PR:

1. **Coverage lane**
   - Add a dedicated CMake preset `ci-coverage-linux` (`-O0 -g --coverage`) and a CI job that runs *only* that preset, then uploads coverage (Codecov/Coveralls).
   - Do not mix coverage flags into sanitizer lanes.
   - Optional (badge): enable Codecov for the repo, then add `codecov/codecov-action` upload step and a Codecov badge to `README.md`.

2. **Hardware-in-the-loop (HIL) or emulator lane**
   - Decide between:
     - ESP32-S3 hardware-in-the-loop runner (self-hosted or farm), or
     - a QEMU-based lane (if/when ESP32-S3 support is feasible), or
     - a hybrid approach (QEMU smoke + periodic HIL).
   - Define the gate: smoke-only, nightly, or release-only.
   - Determine how artifacts (serial logs, crash dumps, perf outputs) are captured and retained.
