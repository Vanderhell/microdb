# Changelog

All notable changes to this project are documented in this file.

The format is inspired by Keep a Changelog and follows semantic versioning intent where possible.

## [Unreleased]

### Added

- Repository governance documents: `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `SUPPORT.md`, `RELEASE_LOG.md`.
- GitHub issue forms and pull request template.

### Changed

- README expanded with optional wrappers/adapter modules and storage budget details.

## [1.2.0] - 2026-04-20

### Added

- Optional wrapper and backend adapter documentation in README.
- Automated GitHub Release workflow on tag push (`v*`).

### Fixed

- UBSAN misalignment issues in TS/REL and WAL-related paths.
- ASAN leak paths in multiple recovery/reopen test flows.

## [1.1.0] - 2026-04-12

### Added

- Read-only diagnostics and admission APIs.
- Managed stress baseline tooling and threshold recommendation scripts.

### Changed

- Preset-driven CI/release testing strategy (`ci-debug-*`, `release-*`).

## [1.0.0] - 2026-04-01

### Added

- Initial public release with KV, TS, and REL engines.
- WAL durability core and recovery flow.
- C and C++ wrapper surfaces.

