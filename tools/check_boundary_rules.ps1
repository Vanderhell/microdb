Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$failed = $false

function Report-Fail([string]$msg) {
  Write-Host "[BOUNDARY][FAIL] $msg" -ForegroundColor Red
  $script:failed = $true
}

function Report-Ok([string]$msg) {
  Write-Host "[BOUNDARY][OK] $msg" -ForegroundColor Green
}

$coreDirs = @("include", "src", "port")
$coreFiles = Get-ChildItem $coreDirs -Recurse -File

# Rule 1: core code must not directly reference loxdb_pro.
$proRef = $coreFiles | Select-String -Pattern "loxdb_pro" -CaseSensitive:$false
if ($proRef) {
  $proRef | ForEach-Object { Report-Fail "core references loxdb_pro: $($_.Path):$($_.LineNumber)" }
} else {
  Report-Ok "no direct loxdb_pro references in core tree"
}

# Rule 2: core code must not include pro module headers.
$proHdrInc = $coreFiles | Select-String -Pattern '^\s*#\s*include\s*["<]loxdb_.*\.h[">]' -CaseSensitive:$false
if ($proHdrInc) {
  $proHdrInc | ForEach-Object { Report-Fail "core includes pro header: $($_.Path):$($_.LineNumber)" }
} else {
  Report-Ok "no pro module headers included by core tree"
}

# Rule 3: public core header must keep core API namespace only.
$publicHeader = "include/lox.h"
if (Test-Path $publicHeader) {
  $apiLeak = Select-String -Path $publicHeader -Pattern "\b(loxdb_|loxpro_|lox_media_)" -CaseSensitive:$false
  if ($apiLeak) {
    $apiLeak | ForEach-Object { Report-Fail "public core API leak in include/lox.h: line $($_.LineNumber)" }
  } else {
    Report-Ok "include/lox.h has no pro namespace leaks"
  }
} else {
  Report-Fail "missing include/lox.h"
}

if ($failed) {
  Write-Host "[BOUNDARY] rule check failed" -ForegroundColor Red
  exit 1
}

Write-Host "[BOUNDARY] all checks passed" -ForegroundColor Green
exit 0
