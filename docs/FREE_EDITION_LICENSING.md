# Free Edition Licensing Policy

This repository is currently distributed as **MIT-licensed Free Edition**.

## Scope

- Core library code in this repository is MIT.
- Current wrappers/adapters in this repository are MIT.
- Current tooling/scripts in this repository are MIT.

## How licensing is expressed

- Full license text: `LICENSE`
- File-level markers: `SPDX-License-Identifier: MIT` headers in source/script files

## Maintenance commands

Apply missing SPDX headers:

```powershell
powershell -ExecutionPolicy Bypass -File tools/apply_spdx_headers.ps1
```

Verify SPDX headers:

```powershell
powershell -ExecutionPolicy Bypass -File tools/check_spdx_headers.ps1
```

## Why SPDX headers

SPDX identifiers make automated license scanning and compliance checks easier for users and integrators.

## Future commercial add-ons

If commercial modules are introduced later, keep them in separate packages/repositories,
with explicit license boundaries and clear dependency lines.

Recommended approach:

1. Keep `loxdb` core API open and stable.
2. Publish premium add-ons as separate modules with separate license terms.
3. Avoid mixing paid code into MIT core files.
