# SPDX-License-Identifier: MIT
param(
    [string]$RecommendationsJson,
    [string]$PresetsPath = "CMakePresets.json",
    [switch]$RequireCompleteMapping = $true,
    [int]$MinBudgetMs = 1,
    [int]$MaxBudgetMs = 600000,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($RecommendationsJson -eq "") {
    throw "RecommendationsJson is required."
}
if (-not (Test-Path $RecommendationsJson)) {
    throw "Recommendations file not found: $RecommendationsJson"
}
if (-not (Test-Path $PresetsPath)) {
    throw "Presets file not found: $PresetsPath"
}
if ($MinBudgetMs -lt 1) {
    throw "MinBudgetMs must be >= 1."
}
if ($MaxBudgetMs -lt $MinBudgetMs) {
    throw "MaxBudgetMs must be >= MinBudgetMs."
}

$rec = Get-Content $RecommendationsJson -Raw | ConvertFrom-Json
$presets = Get-Content $PresetsPath -Raw | ConvertFrom-Json

if ($null -eq $presets.configurePresets) {
    throw "Invalid presets file: missing configurePresets."
}
if ($null -eq $rec.recommendations -or $rec.recommendations.Count -eq 0) {
    throw "Invalid recommendations file: missing recommendations entries."
}

function Get-RecommendationKey {
    param(
        [string]$OsName,
        [string]$Lane
    )
    return ("{0}|{1}" -f $OsName.Trim().ToLowerInvariant(), $Lane.Trim().ToLowerInvariant())
}

$recMap = @{}
foreach ($entry in $rec.recommendations) {
    $osName = [string]$entry.os
    $lane = [string]$entry.lane
    if ([string]::IsNullOrWhiteSpace($osName) -or [string]::IsNullOrWhiteSpace($lane)) {
        throw "Invalid recommendations entry: each item must define non-empty 'os' and 'lane'."
    }
    $key = Get-RecommendationKey -OsName $osName -Lane $lane
    if ($recMap.ContainsKey($key)) {
        throw "Duplicate recommendation entry for os='$osName', lane='$lane'."
    }
    $recMap[$key] = $entry
}

function Get-RecBudget {
    param(
        [string]$OsName,
        [string]$Lane
    )
    $key = Get-RecommendationKey -OsName $OsName -Lane $Lane
    if (-not $recMap.ContainsKey($key)) {
        return $null
    }
    $entry = $recMap[$key]
    if ($null -eq $entry) {
        return $null
    }
    $budget = [int][math]::Ceiling([double]$entry.recommended_budget_ms)
    if ($budget -lt $MinBudgetMs -or $budget -gt $MaxBudgetMs) {
        throw "Recommended budget out of range for os='$OsName' lane='$Lane': $budget (allowed $MinBudgetMs..$MaxBudgetMs)."
    }
    return $budget
}

$map = @(
    @{ preset = "ci-debug-linux"; os = "linux-debug" },
    @{ preset = "ci-debug-windows"; os = "windows-debug" },
    @{ preset = "release-linux"; os = "linux-debug" },
    @{ preset = "release-windows"; os = "windows-debug" }
)

$changes = @()

foreach ($m in $map) {
    $preset = $presets.configurePresets | Where-Object { $_.name -eq $m.preset } | Select-Object -First 1
    if ($null -eq $preset) {
        if ($RequireCompleteMapping) {
            throw "Preset '$($m.preset)' not found in $PresetsPath."
        }
        continue
    }
    if ($null -eq $preset.cacheVariables) {
        $preset | Add-Member -NotePropertyName cacheVariables -NotePropertyValue (@{})
    }

    $smokeRec = Get-RecBudget -OsName $m.os -Lane "smoke"
    $longRec = Get-RecBudget -OsName $m.os -Lane "long"
    if ($null -eq $smokeRec -or $null -eq $longRec) {
        if ($RequireCompleteMapping) {
            throw "Missing recommendation mapping for os='$($m.os)' lanes smoke/long."
        }
        continue
    }

    $oldSmoke = [string]$preset.cacheVariables.MICRODB_FS_MATRIX_SMOKE_MAX_MS
    $oldLong = [string]$preset.cacheVariables.MICRODB_FS_MATRIX_LONG_MAX_MS
    $newSmoke = [string]$smokeRec
    $newLong = [string]$longRec

    if ($oldSmoke -ne $newSmoke) {
        $preset.cacheVariables.MICRODB_FS_MATRIX_SMOKE_MAX_MS = $newSmoke
    }
    if ($oldLong -ne $newLong) {
        $preset.cacheVariables.MICRODB_FS_MATRIX_LONG_MAX_MS = $newLong
    }

    $changes += [PSCustomObject]@{
        preset = $m.preset
        smoke_old = $oldSmoke
        smoke_new = $newSmoke
        long_old = $oldLong
        long_new = $newLong
    }
}

if ($changes.Count -eq 0) {
    Write-Output "No matching preset updates were applied."
    exit 0
}

Write-Output "FS matrix threshold updates:"
foreach ($c in $changes) {
    Write-Output ("- {0}: smoke {1} -> {2}, long {3} -> {4}" -f $c.preset, $c.smoke_old, $c.smoke_new, $c.long_old, $c.long_new)
}

if ($DryRun) {
    Write-Output "DryRun enabled: no file changes written."
    exit 0
}

$jsonOut = $presets | ConvertTo-Json -Depth 20
Set-Content -Path $PresetsPath -Value $jsonOut -Encoding ascii
Write-Output "Updated presets file: $PresetsPath"
