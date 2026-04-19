# SPDX-License-Identifier: MIT
param(
    [string]$ManagedRecommendationsJson,
    [string]$FsRecommendationsJson,
    [double]$WarnPct = 15.0,
    [string]$OutputMarkdown = "baseline-rollup.md"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($ManagedRecommendationsJson -eq "" -or -not (Test-Path $ManagedRecommendationsJson)) {
    throw "ManagedRecommendationsJson is required and must exist."
}
if ($FsRecommendationsJson -eq "" -or -not (Test-Path $FsRecommendationsJson)) {
    throw "FsRecommendationsJson is required and must exist."
}
if ($WarnPct -lt 0.0) {
    throw "WarnPct must be >= 0."
}

function Get-Key {
    param([string]$Suite, [string]$Os, [string]$Lane)
    return ("{0}|{1}|{2}" -f $Suite.Trim().ToLowerInvariant(), $Os.Trim().ToLowerInvariant(), $Lane.Trim().ToLowerInvariant())
}

function Add-Recommendations {
    param(
        [hashtable]$Map,
        [string]$SuiteName,
        [object]$Payload
    )

    if ($null -eq $Payload.recommendations -or $Payload.recommendations.Count -eq 0) {
        throw "Invalid recommendation payload for suite '$SuiteName': missing recommendations."
    }

    foreach ($r in $Payload.recommendations) {
        $os = [string]$r.os
        $lane = [string]$r.lane
        if ([string]::IsNullOrWhiteSpace($os) -or [string]::IsNullOrWhiteSpace($lane)) {
            throw "Invalid recommendation entry in suite '$SuiteName': os/lane must be non-empty."
        }

        $current = [double]$r.current_budget_median_ms
        $recommended = [double]$r.recommended_budget_ms
        if ($current -lt 1.0 -or $recommended -lt 1.0) {
            throw "Invalid budgets in suite '$SuiteName' for os='$os' lane='$lane'."
        }

        $deltaMs = $recommended - $current
        $deltaPct = ($deltaMs / $current) * 100.0
        $severity = "ok"
        if ([math]::Abs($deltaPct) -ge $WarnPct) {
            if ($deltaPct -gt 0.0) {
                $severity = "warn_up"
            } else {
                $severity = "warn_down"
            }
        }

        $key = Get-Key -Suite $SuiteName -Os $os -Lane $lane
        if ($Map.ContainsKey($key)) {
            throw "Duplicate rollup key detected: $key"
        }

        $Map[$key] = [PSCustomObject]@{
            suite = $SuiteName
            os = $os
            lane = $lane
            sample_count = [int]$r.sample_count
            max_ms = [double]$r.max_ms
            p95_ms = [double]$r.p95_ms
            current_budget_median_ms = $current
            recommended_budget_ms = $recommended
            delta_ms = $deltaMs
            delta_pct = $deltaPct
            severity = $severity
        }
    }
}

$managedPayload = Get-Content $ManagedRecommendationsJson -Raw | ConvertFrom-Json
$fsPayload = Get-Content $FsRecommendationsJson -Raw | ConvertFrom-Json

$rowsByKey = @{}
Add-Recommendations -Map $rowsByKey -SuiteName "managed" -Payload $managedPayload
Add-Recommendations -Map $rowsByKey -SuiteName "fs" -Payload $fsPayload

$rows = @($rowsByKey.Values | Sort-Object suite, os, lane)
$warnings = @($rows | Where-Object { $_.severity -ne "ok" })

$md = @()
$md += "# Baseline Drift Rollup"
$md += ""
$md += "- Generated UTC: $((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"))"
$md += "- Drift warning threshold: +/-$WarnPct %"
$md += "- Managed source: $ManagedRecommendationsJson"
$md += "- FS source: $FsRecommendationsJson"
$md += ""
$md += "| Suite | OS | Lane | Samples | p95 (ms) | Max (ms) | Current Median Budget (ms) | Recommended Budget (ms) | Delta (ms) | Delta (%) | Status |"
$md += "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|"

foreach ($r in $rows) {
    $status = "OK"
    if ($r.severity -eq "warn_up") { $status = "WARN-UP" }
    if ($r.severity -eq "warn_down") { $status = "WARN-DOWN" }

    $md += "| $($r.suite) | $($r.os) | $($r.lane) | $($r.sample_count) | $([math]::Round($r.p95_ms, 2)) | $([math]::Round($r.max_ms, 2)) | $([math]::Round($r.current_budget_median_ms, 2)) | $([math]::Round($r.recommended_budget_ms, 2)) | $([math]::Round($r.delta_ms, 2)) | $([math]::Round($r.delta_pct, 2)) | $status |"
}

$md += ""
if ($warnings.Count -eq 0) {
    $md += "## Drift Warnings"
    $md += "No threshold drift exceeded +/-$WarnPct %."
} else {
    $md += "## Drift Warnings"
    foreach ($w in $warnings) {
        $dir = "increase"
        if ($w.delta_pct -lt 0.0) { $dir = "decrease" }
        $md += "- [$($w.suite)] $($w.os) $($w.lane): $dir by $([math]::Round([math]::Abs($w.delta_pct), 2))% ($([math]::Round($w.current_budget_median_ms, 2)) -> $([math]::Round($w.recommended_budget_ms, 2)) ms)"
    }
}

$markdown = ($md -join [Environment]::NewLine)
Set-Content -Path $OutputMarkdown -Value $markdown -Encoding ascii
Write-Output $markdown
