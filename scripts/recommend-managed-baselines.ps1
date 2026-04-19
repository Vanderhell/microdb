# SPDX-License-Identifier: MIT
param(
    [string]$InputDir = ".",
    [double]$Quantile = 0.95,
    [double]$MarginPct = 20.0,
    [double]$MinHeadroomMs = 50.0,
    [int]$MinSamplesPerOs = 1,
    [string]$OutputJson = "",
    [string]$OutputMarkdown = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Quantile -le 0.0 -or $Quantile -gt 1.0) {
    throw "Quantile must be in range (0, 1]."
}
if ($MarginPct -lt 0.0) {
    throw "MarginPct must be >= 0."
}
if ($MinHeadroomMs -lt 0.0) {
    throw "MinHeadroomMs must be >= 0."
}
if ($MinSamplesPerOs -lt 1) {
    throw "MinSamplesPerOs must be >= 1."
}

function Test-IsFiniteNumber {
    param([double]$Value)
    return (-not [double]::IsNaN($Value)) -and (-not [double]::IsInfinity($Value))
}

function Get-PercentileValue {
    param(
        [double[]]$SortedValues,
        [double]$Q
    )
    if ($SortedValues.Count -eq 0) {
        throw "Cannot compute percentile on empty set."
    }
    $rank = [math]::Ceiling($Q * $SortedValues.Count)
    if ($rank -lt 1) {
        $rank = 1
    }
    if ($rank -gt $SortedValues.Count) {
        $rank = $SortedValues.Count
    }
    return [double]$SortedValues[$rank - 1]
}

$files = Get-ChildItem -Path $InputDir -Recurse -File -Filter "managed_stress_baseline_*.json"
if ($files.Count -eq 0) {
    throw "No baseline files found under '$InputDir' matching managed_stress_baseline_*.json"
}

$rows = @()
foreach ($f in $files) {
    $obj = Get-Content $f.FullName -Raw | ConvertFrom-Json
    $os = [string]$obj.os
    $preset = [string]$obj.preset
    $smokeRuntime = [double]$obj.smoke_runtime_ms
    $longRuntime = [double]$obj.long_runtime_ms
    $smokeBudget = [double]$obj.smoke_budget_ms
    $longBudget = [double]$obj.long_budget_ms

    if ([string]::IsNullOrWhiteSpace($os)) {
        throw "Invalid baseline file '$($f.FullName)': missing or empty 'os'."
    }
    if ([string]::IsNullOrWhiteSpace($preset)) {
        throw "Invalid baseline file '$($f.FullName)': missing or empty 'preset'."
    }
    if (
        -not (Test-IsFiniteNumber $smokeRuntime) -or $smokeRuntime -lt 0.0 -or
        -not (Test-IsFiniteNumber $longRuntime) -or $longRuntime -lt 0.0 -or
        -not (Test-IsFiniteNumber $smokeBudget) -or $smokeBudget -lt 1.0 -or
        -not (Test-IsFiniteNumber $longBudget) -or $longBudget -lt 1.0
    ) {
        throw "Invalid baseline file '$($f.FullName)': runtime/budget fields are missing or out of range."
    }

    $rows += [PSCustomObject]@{
        os = $os
        preset = $preset
        date_utc = [string]$obj.date_utc
        smoke_runtime_ms = $smokeRuntime
        long_runtime_ms = $longRuntime
        smoke_budget_ms = $smokeBudget
        long_budget_ms = $longBudget
        file = [string]$f.FullName
    }
}

$groups = $rows | Group-Object -Property os
$result = @()
$mdLines = @()
$mdLines += "# Managed Baseline Recommendations"
$mdLines += ""
$mdLines += "Source directory: $InputDir"
$mdLines += "Samples: $($rows.Count)"
$mdLines += "Quantile: $Quantile"
$mdLines += "Margin: $MarginPct %"
$mdLines += "Min headroom: $MinHeadroomMs ms"
$mdLines += ""
$mdLines += "| OS | Lane | Samples | Max (ms) | p95 (ms) | Recommended (ms) | Current Budget Median (ms) |"
$mdLines += "|---|---:|---:|---:|---:|---:|---:|"

foreach ($g in $groups) {
    $os = [string]$g.Name
    if ($g.Count -lt $MinSamplesPerOs) {
        throw "OS '$os' has only $($g.Count) samples, below MinSamplesPerOs=$MinSamplesPerOs."
    }

    $smokeVals = @($g.Group | ForEach-Object { [double]$_.smoke_runtime_ms } | Sort-Object)
    $longVals = @($g.Group | ForEach-Object { [double]$_.long_runtime_ms } | Sort-Object)
    $smokeBudgets = @($g.Group | ForEach-Object { [double]$_.smoke_budget_ms } | Sort-Object)
    $longBudgets = @($g.Group | ForEach-Object { [double]$_.long_budget_ms } | Sort-Object)

    $smokeP = Get-PercentileValue -SortedValues $smokeVals -Q $Quantile
    $longP = Get-PercentileValue -SortedValues $longVals -Q $Quantile
    $smokeMax = [double]$smokeVals[-1]
    $longMax = [double]$longVals[-1]
    $smokeCurrentMedian = Get-PercentileValue -SortedValues $smokeBudgets -Q 0.5
    $longCurrentMedian = Get-PercentileValue -SortedValues $longBudgets -Q 0.5

    $smokeRec = [math]::Ceiling([math]::Max($smokeP * (1.0 + ($MarginPct / 100.0)), $smokeMax + $MinHeadroomMs))
    $longRec = [math]::Ceiling([math]::Max($longP * (1.0 + ($MarginPct / 100.0)), $longMax + $MinHeadroomMs))

    $result += [PSCustomObject]@{
        os = $os
        lane = "smoke"
        sample_count = $smokeVals.Count
        max_ms = $smokeMax
        p95_ms = $smokeP
        current_budget_median_ms = $smokeCurrentMedian
        recommended_budget_ms = [double]$smokeRec
    }
    $result += [PSCustomObject]@{
        os = $os
        lane = "long"
        sample_count = $longVals.Count
        max_ms = $longMax
        p95_ms = $longP
        current_budget_median_ms = $longCurrentMedian
        recommended_budget_ms = [double]$longRec
    }

    $mdLines += "| $os | smoke | $($smokeVals.Count) | $([math]::Round($smokeMax, 2)) | $([math]::Round($smokeP, 2)) | $smokeRec | $([math]::Round($smokeCurrentMedian, 2)) |"
    $mdLines += "| $os | long | $($longVals.Count) | $([math]::Round($longMax, 2)) | $([math]::Round($longP, 2)) | $longRec | $([math]::Round($longCurrentMedian, 2)) |"
}

$jsonPayload = [PSCustomObject]@{
    generated_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    input_dir = $InputDir
    sample_count = $rows.Count
    quantile = $Quantile
    margin_pct = $MarginPct
    min_headroom_ms = $MinHeadroomMs
    recommendations = $result
}

$markdown = ($mdLines -join [Environment]::NewLine)
Write-Output $markdown

if ($OutputMarkdown -ne "") {
    Set-Content -Path $OutputMarkdown -Value $markdown -Encoding ascii
}
if ($OutputJson -ne "") {
    $json = $jsonPayload | ConvertTo-Json -Depth 8
    Set-Content -Path $OutputJson -Value $json -Encoding ascii
}
