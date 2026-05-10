# Release Tag and Announcement Template

## Tag message template

```text
loxdb <version>

Embedded C99 database core for MCU/edge systems.

This release includes:
- updated source and tests
- release artifacts for supported platforms
- updated contracts/docs in docs/
```

## Short announcement template

```text
Released loxdb <version>.

loxdb provides KV, TS, and REL APIs for embedded systems with optional WAL-backed durability.

Start here:
- README.md
- docs/GETTING_STARTED_5_MIN.md
- docs/FAIL_CODE_CONTRACT.md
- docs/PROFILE_GUARANTEES.md
```

## Guardrails

- only claim what is validated by current code/tests
- avoid hardware-latency claims unless accompanied by dated benchmark context
- keep unsupported scope explicit
