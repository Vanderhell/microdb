# SPDX-License-Identifier: MIT
param(
    [string]$HeadBench = "bench/loxdb_esp32_s3_bench_head",
    [string]$BaseBench = "bench/loxdb_esp32_s3_bench_base"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Assert-Exists {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Missing: $Path"
    }
}

function Get-Includes {
    param([Parameter(Mandatory = $true)][string]$Root)
    $includes = @()
    Get-ChildItem -LiteralPath $Root -Recurse -File -Include *.c,*.h,*.ino | ForEach-Object {
        $file = $_.FullName
        Get-Content -LiteralPath $file | ForEach-Object {
            if ($_ -match '^\s*#include\s+"([^"]+)"') {
                $includes += [pscustomobject]@{ file = $file; include = $Matches[1] }
            }
        }
    }
    return $includes
}

function Validate-Bench {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string[]]$SearchPaths
    )

    Write-Host "== $Name =="
    Assert-Exists $Root

    $missing = @()
    $includes = Get-Includes -Root $Root
    foreach ($inc in $includes) {
        # Only validate project-local headers; platform/toolchain headers may not exist in the repo.
        if ($inc.include -notmatch '^(lox|microdb)[^\\/]*\.h$') {
            continue
        }
        $found = $false
        foreach ($sp in $SearchPaths) {
            $cand = Join-Path $sp $inc.include
            if (Test-Path -LiteralPath $cand) { $found = $true; break }
        }
        if (-not $found) {
            $missing += [pscustomobject]@{ file = $inc.file; include = $inc.include }
        }
    }

    if ($missing.Count -gt 0) {
        Write-Host "Missing includes:"
        $missing | Select-Object -First 30 | ForEach-Object {
            Write-Host ("  {0} -> {1}" -f $_.file, $_.include)
        }
        if ($missing.Count -gt 30) {
            Write-Host "  ... ($($missing.Count-30) more)"
        }
        exit 2
    }

    Write-Host "OK: include graph resolves within search paths."
    Write-Host ""
}

Validate-Bench `
    -Name "HEAD bench (Arduino folder + repo helpers)" `
    -Root $HeadBench `
    -SearchPaths @(
        (Join-Path $HeadBench "lox_esp32_s3_bench"),
        (Join-Path $HeadBench "lox_esp32_s3_bench/src"),
        $HeadBench,
        (Join-Path $HeadBench "src")
    )

Validate-Bench `
    -Name "BASE bench" `
    -Root $BaseBench `
    -SearchPaths @(
        $BaseBench,
        (Join-Path $BaseBench "src")
    )

Write-Host "All bench layouts look consistent."
