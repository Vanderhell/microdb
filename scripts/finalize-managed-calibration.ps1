# SPDX-License-Identifier: MIT
param(
    [string]$InputDir,
    [string]$PresetsPath = "CMakePresets.json",
    [double]$Quantile = 0.95,
    [double]$MarginPct = 20.0,
    [double]$MinHeadroomMs = 50.0,
    [int]$MinSamplesPerOs = 8,
    [double]$MaxBudgetDecreasePct = 15.0,
    [string]$OutputDir = ".",
    [switch]$Apply
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($InputDir)) {
    throw "InputDir is required."
}
if (-not (Test-Path $InputDir)) {
    throw "InputDir not found: $InputDir"
}
if (-not (Test-Path $PresetsPath)) {
    throw "Presets file not found: $PresetsPath"
}
if ($MinSamplesPerOs -lt 1) {
    throw "MinSamplesPerOs must be >= 1."
}
if ($MaxBudgetDecreasePct -lt 0.0) {
    throw "MaxBudgetDecreasePct must be >= 0."
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$recommendJson = Join-Path $OutputDir "managed-baseline-recommendations.json"
$recommendMd = Join-Path $OutputDir "managed-baseline-recommendations.md"
$recommendLog = Join-Path $OutputDir "managed-baseline-recommendations.log"
$applyDryrunLog = Join-Path $OutputDir "managed-threshold-apply-dryrun.log"
$applyLog = Join-Path $OutputDir "managed-threshold-apply.log"
$candidatePresets = Join-Path $OutputDir "CMakePresets.candidate.json"
$candidateDiff = Join-Path $OutputDir "managed-threshold-presets.diff"
$finalSummary = Join-Path $OutputDir "managed-calibration-finalize-summary.md"

Write-Output "Generating recommendations from '$InputDir'..."
./scripts/recommend-managed-baselines.ps1 `
    -InputDir $InputDir `
    -Quantile $Quantile `
    -MarginPct $MarginPct `
    -MinHeadroomMs $MinHeadroomMs `
    -MinSamplesPerOs $MinSamplesPerOs `
    -OutputJson $recommendJson `
    -OutputMarkdown $recommendMd | Tee-Object -FilePath $recommendLog

Copy-Item $PresetsPath $candidatePresets -Force

Write-Output "Previewing threshold updates..."
./scripts/apply-managed-thresholds.ps1 `
    -RecommendationsJson $recommendJson `
    -PresetsPath $candidatePresets `
    -DryRun | Tee-Object -FilePath $applyDryrunLog

Write-Output "Applying updates into candidate presets..."
./scripts/apply-managed-thresholds.ps1 `
    -RecommendationsJson $recommendJson `
    -PresetsPath $candidatePresets | Tee-Object -FilePath $applyLog

& git diff --no-index -- $PresetsPath $candidatePresets > $candidateDiff
if ($LASTEXITCODE -gt 1) {
    throw "git diff failed with exit code $LASTEXITCODE"
}

$basePresets = Get-Content $PresetsPath -Raw | ConvertFrom-Json
$candPresets = Get-Content $candidatePresets -Raw | ConvertFrom-Json

function Get-Budget {
    param(
        [object]$Presets,
        [string]$PresetName,
        [string]$VarName
    )
    $p = $Presets.configurePresets | Where-Object { $_.name -eq $PresetName } | Select-Object -First 1
    if ($null -eq $p -or $null -eq $p.cacheVariables) {
        return $null
    }
    $v = [string]$p.cacheVariables.$VarName
    if ([string]::IsNullOrWhiteSpace($v)) {
        return $null
    }
    return [double]$v
}

$targets = @("ci-debug-linux", "ci-debug-windows", "release-linux", "release-windows")
$violations = @()
$changeRows = @()
foreach ($name in $targets) {
    $oldSmoke = Get-Budget -Presets $basePresets -PresetName $name -VarName "LOX_MANAGED_STRESS_SMOKE_MAX_MS"
    $oldLong = Get-Budget -Presets $basePresets -PresetName $name -VarName "LOX_MANAGED_STRESS_LONG_MAX_MS"
    $newSmoke = Get-Budget -Presets $candPresets -PresetName $name -VarName "LOX_MANAGED_STRESS_SMOKE_MAX_MS"
    $newLong = Get-Budget -Presets $candPresets -PresetName $name -VarName "LOX_MANAGED_STRESS_LONG_MAX_MS"
    if ($null -eq $oldSmoke -or $null -eq $oldLong -or $null -eq $newSmoke -or $null -eq $newLong) {
        continue
    }

    $smokeDeltaPct = if ($oldSmoke -eq 0.0) { 0.0 } else { (($newSmoke - $oldSmoke) / $oldSmoke) * 100.0 }
    $longDeltaPct = if ($oldLong -eq 0.0) { 0.0 } else { (($newLong - $oldLong) / $oldLong) * 100.0 }

    $changeRows += [PSCustomObject]@{
        preset = $name
        smoke_old = [int]$oldSmoke
        smoke_new = [int]$newSmoke
        smoke_delta_pct = [math]::Round($smokeDeltaPct, 2)
        long_old = [int]$oldLong
        long_new = [int]$newLong
        long_delta_pct = [math]::Round($longDeltaPct, 2)
    }

    if ($smokeDeltaPct -lt (-1.0 * $MaxBudgetDecreasePct)) {
        $violations += "$name smoke drop $([math]::Round($smokeDeltaPct, 2))% exceeds policy -$MaxBudgetDecreasePct%"
    }
    if ($longDeltaPct -lt (-1.0 * $MaxBudgetDecreasePct)) {
        $violations += "$name long drop $([math]::Round($longDeltaPct, 2))% exceeds policy -$MaxBudgetDecreasePct%"
    }
}

$summary = @()
$summary += "# Managed Calibration Finalize Summary"
$summary += ""
$summary += "- InputDir: $InputDir"
$summary += "- PresetsPath: $PresetsPath"
$summary += "- MinSamplesPerOs: $MinSamplesPerOs"
$summary += "- MaxBudgetDecreasePct: $MaxBudgetDecreasePct"
$summary += "- Apply requested: $([bool]$Apply)"
$summary += ""
$summary += "## Candidate Changes"
$summary += ""
$summary += "| Preset | Smoke old | Smoke new | Smoke delta % | Long old | Long new | Long delta % |"
$summary += "|---|---:|---:|---:|---:|---:|---:|"
foreach ($r in $changeRows) {
    $summary += "| $($r.preset) | $($r.smoke_old) | $($r.smoke_new) | $($r.smoke_delta_pct) | $($r.long_old) | $($r.long_new) | $($r.long_delta_pct) |"
}
$summary += ""
if ($violations.Count -eq 0) {
    $summary += "Policy gate: PASS"
} else {
    $summary += "Policy gate: FAIL"
    $summary += ""
    $summary += "Violations:"
    foreach ($v in $violations) {
        $summary += "- $v"
    }
}
Set-Content -Path $finalSummary -Value ($summary -join [Environment]::NewLine) -Encoding ascii
Write-Output (($summary -join [Environment]::NewLine))

if ($violations.Count -ne 0) {
    throw "Calibration policy gate failed; candidate presets were not applied."
}

if ($Apply) {
    Copy-Item $candidatePresets $PresetsPath -Force
    Write-Output "Applied candidate presets to: $PresetsPath"
} else {
    Write-Output "Apply not requested; candidate presets left at: $candidatePresets"
}
