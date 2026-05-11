# SPDX-License-Identifier: MIT
param(
    [Parameter(Mandatory = $true)][string]$DeterministicLog,
    [string]$BalancedLog = "",
    [string]$StressLog = "",
    [string]$OutPath = "docs/BENCHMARKS.md"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Parse-MetricsFromLog {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Log not found: $Path"
    }

    $text = Get-Content -LiteralPath $Path -Raw

    $rowsExpected = $null
    $tsRetained = $null
    foreach ($line in ($text -split "`r?`n")) {
        if ($null -eq $rowsExpected -and $line -match "^\[REL\]\s+rows_expected=(\d+)") {
            $rowsExpected = [int]$Matches[1]
        }
        if ($null -eq $tsRetained -and $line -match "^\[TS\]\s+target=\d+\s+retained=(\d+)") {
            $tsRetained = [int]$Matches[1]
        }
    }

    $metrics = @{}
    foreach ($line in ($text -split "`r?`n")) {
        if ($line -match "^\[METRIC\]\s+(?<op>.+?)\s+total=(?<total_ms>[0-9.]+)ms\s+avg=(?<avg_us>[0-9.]+)us\s+p50=(?<p50>\d+)\s+p95=(?<p95>\d+)\s+max=(?<max>\d+)@(?<max_i>\d+).*?\s+ops/s=(?<ops_s>[0-9.]+)\b") {
            $op = $Matches["op"].Trim()
            $metrics[$op] = [pscustomobject]@{
                op       = $op
                total_ms = [double]$Matches["total_ms"]
                avg_us   = [double]$Matches["avg_us"]
                p50_us   = [int]$Matches["p50"]
                p95_us   = [int]$Matches["p95"]
                max_us   = [int]$Matches["max"]
                ops_s    = [double]$Matches["ops_s"]
            }
        }
    }

    return [pscustomobject]@{
        text         = $text
        metrics      = $metrics
        rowsExpected = $rowsExpected
        tsRetained   = $tsRetained
    }
}

function Fmt-Num {
    param([Parameter(Mandatory = $true)]$Value)
    if ($Value -is [double]) {
        return ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, "{0:0.0}", $Value))
    }
    return "$Value"
}

function Require-Metric {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Metrics,
        [Parameter(Mandatory = $true)][string]$Op
    )
    if (-not $Metrics.ContainsKey($Op)) {
        $known = ($Metrics.Keys | Sort-Object) -join ", "
        throw "Metric '$Op' not found in log. Known: $known"
    }
    return $Metrics[$Op]
}

$det = Parse-MetricsFromLog -Path $DeterministicLog
$bal = $null
if (-not [string]::IsNullOrWhiteSpace($BalancedLog)) {
    $bal = Parse-MetricsFromLog -Path $BalancedLog
}
$stress = $null
if (-not [string]::IsNullOrWhiteSpace($StressLog)) {
    $stress = Parse-MetricsFromLog -Path $StressLog
}

$kv_put = Require-Metric -Metrics $det.metrics -Op "kv_put"
$kv_get = Require-Metric -Metrics $det.metrics -Op "kv_get"
$kv_del = Require-Metric -Metrics $det.metrics -Op "kv_del"
$ts_ins = Require-Metric -Metrics $det.metrics -Op "ts_insert"
$ts_q   = Require-Metric -Metrics $det.metrics -Op "ts_query_buf"
$rel_i  = Require-Metric -Metrics $det.metrics -Op "rel_insert"
$rel_f  = Require-Metric -Metrics $det.metrics -Op "rel_find(index)"
$wal_kv = Require-Metric -Metrics $det.metrics -Op "wal_kv_put"
$compact = Require-Metric -Metrics $det.metrics -Op "compact"
$reopen = Require-Metric -Metrics $det.metrics -Op "reopen"

$detName = Split-Path -Leaf $DeterministicLog
$balName = if ($bal) { Split-Path -Leaf $BalancedLog } else { "" }

$relRows = if ($null -ne $det.rowsExpected) { $det.rowsExpected } else { "TBD" }
$tsTypeNote = if ($null -ne $det.tsRetained) { "retained=$($det.tsRetained)" } else { "retained=TBD" }

$generatedLines = @()
$generatedLines += '## Results - KV engine (deterministic profile)'
$generatedLines += ''
$generatedLines += '| Operation | p50 (us) | p95 (us) | max (us) | throughput (ops/s) | Notes |'
$generatedLines += '|---|---:|---:|---:|---:|---|'
$generatedLines += ('| `kv_put` | {0} | {1} | {2} | {3} | `{4}` |' -f $kv_put.p50_us, $kv_put.p95_us, $kv_put.max_us, (Fmt-Num $kv_put.ops_s), $detName)
$generatedLines += ('| `kv_get` | {0} | {1} | {2} | {3} | `{4}` |' -f $kv_get.p50_us, $kv_get.p95_us, $kv_get.max_us, (Fmt-Num $kv_get.ops_s), $detName)
$generatedLines += ('| `kv_del` | {0} | {1} | {2} | {3} | `{4}` |' -f $kv_del.p50_us, $kv_del.p95_us, $kv_del.max_us, (Fmt-Num $kv_del.ops_s), $detName)
$generatedLines += ''
$generatedLines += 'WAL impact (KV):'
$generatedLines += ('- `wal_kv_put` p50/p95/max: {0}/{1}/{2} us (`{3}`)' -f $wal_kv.p50_us, $wal_kv.p95_us, $wal_kv.max_us, $detName)
$generatedLines += ''
$generatedLines += '## Results - TS engine (deterministic profile)'
$generatedLines += ''
$generatedLines += '| Stream type | insert rate (samples/s) | query p50 (us) | query p95 (us) | Notes |'
$generatedLines += '|---|---:|---:|---:|---|'
$generatedLines += ('| `F32` | {0} | {1} | {2} | `{3}` ({4}) |' -f (Fmt-Num $ts_ins.ops_s), $ts_q.p50_us, $ts_q.p95_us, $detName, $tsTypeNote)
$generatedLines += '| `I32` | TBD | TBD | TBD | <!-- TODO(maintainer): add I32 run --> |'
$generatedLines += '| `U32` | TBD | TBD | TBD | <!-- TODO(maintainer): add U32 run --> |'
$generatedLines += '| `RAW` | TBD | TBD | TBD | <!-- TODO(maintainer): add RAW run --> |'
$generatedLines += ''
$generatedLines += '## Results - REL engine (deterministic profile)'
$generatedLines += ''
$generatedLines += '| Rows (N) | insert p50 (us) | find_by_index p50 (us) | Notes |'
$generatedLines += '|---:|---:|---:|---|'
$generatedLines += ('| {0} | {1} | {2} | `{3}` |' -f $relRows, $rel_i.p50_us, $rel_f.p50_us, $detName)
$generatedLines += ''
$generatedLines += '## WAL / maintenance (deterministic profile)'
$generatedLines += ''
$generatedLines += '| Operation | total (ms) | Notes |'
$generatedLines += '|---|---:|---|'
$generatedLines += ('| `compact` | {0} | `{1}` |' -f ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, "{0:0.###}", $compact.total_ms)), $detName)
$generatedLines += ('| `reopen` | {0} | `{1}` |' -f ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, "{0:0.###}", $reopen.total_ms)), $detName)

$generated = ($generatedLines -join "`r`n")

if ($bal) {
    $b_kv_put = Require-Metric -Metrics $bal.metrics -Op "kv_put"
    $b_kv_get = Require-Metric -Metrics $bal.metrics -Op "kv_get"
    $b_kv_del = Require-Metric -Metrics $bal.metrics -Op "kv_del"
    $b_ts_ins = Require-Metric -Metrics $bal.metrics -Op "ts_insert"
    $b_rel_i  = Require-Metric -Metrics $bal.metrics -Op "rel_insert"
    $generated += "`r`n`r`n## Throughput reference - balanced profile`r`n`r`n"
    $generated += "| Operation | throughput (ops/s) | Notes |`r`n"
    $generated += "|---|---:|---|`r`n"
    $generated += ('| `kv_put` | {0} | `{1}` |' -f (Fmt-Num $b_kv_put.ops_s), $balName) + "`r`n"
    $generated += ('| `kv_get` | {0} | `{1}` |' -f (Fmt-Num $b_kv_get.ops_s), $balName) + "`r`n"
    $generated += ('| `kv_del` | {0} | `{1}` |' -f (Fmt-Num $b_kv_del.ops_s), $balName) + "`r`n"
    $generated += ('| `ts_insert` | {0} | `{1}` |' -f (Fmt-Num $b_ts_ins.ops_s), $balName) + "`r`n"
    $generated += ('| `rel_insert` | {0} | `{1}` |' -f (Fmt-Num $b_rel_i.ops_s), $balName) + "`r`n"
}

if ($stress) {
    $s_kv_put = Require-Metric -Metrics $stress.metrics -Op "kv_put"
    $s_kv_get = Require-Metric -Metrics $stress.metrics -Op "kv_get"
    $s_kv_del = Require-Metric -Metrics $stress.metrics -Op "kv_del"
    $s_ts_ins = Require-Metric -Metrics $stress.metrics -Op "ts_insert"
    $s_rel_i  = Require-Metric -Metrics $stress.metrics -Op "rel_insert"
    $s_wal_kv = Require-Metric -Metrics $stress.metrics -Op "wal_kv_put"
    $s_compact = Require-Metric -Metrics $stress.metrics -Op "compact"
    $s_reopen = Require-Metric -Metrics $stress.metrics -Op "reopen"
    $stressName = Split-Path -Leaf $StressLog

    $stressTsNote = if ($null -ne $stress.tsRetained) { "retained=$($stress.tsRetained)" } else { "retained=TBD" }
    $stressRelRows = if ($null -ne $stress.rowsExpected) { $stress.rowsExpected } else { "TBD" }

    $generated += "`r`n`r`n## Stress profile reference`r`n`r`n"
    $generated += "| Metric | Value | Notes |`r`n"
    $generated += "|---|---:|---|`r`n"
    $generated += ('| `kv_put` throughput (ops/s) | {0} | `{1}` |' -f (Fmt-Num $s_kv_put.ops_s), $stressName) + "`r`n"
    $generated += ('| `kv_get` throughput (ops/s) | {0} | `{1}` |' -f (Fmt-Num $s_kv_get.ops_s), $stressName) + "`r`n"
    $generated += ('| `kv_del` throughput (ops/s) | {0} | `{1}` |' -f (Fmt-Num $s_kv_del.ops_s), $stressName) + "`r`n"
    $generated += ('| `ts_insert` throughput (samples/s) | {0} | `{1}` ({2}) |' -f (Fmt-Num $s_ts_ins.ops_s), $stressName, $stressTsNote) + "`r`n"
    $generated += ('| `rel_insert` throughput (rows/s) | {0} | `{1}` (N={2}) |' -f (Fmt-Num $s_rel_i.ops_s), $stressName, $stressRelRows) + "`r`n"
    $generated += ('| `wal_kv_put` throughput (ops/s) | {0} | `{1}` |' -f (Fmt-Num $s_wal_kv.ops_s), $stressName) + "`r`n"
    $generated += ('| `compact` total (ms) | {0} | `{1}` |' -f ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, "{0:0.###}", $s_compact.total_ms)), $stressName) + "`r`n"
    $generated += ('| `reopen` total (ms) | {0} | `{1}` |' -f ([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, "{0:0.###}", $s_reopen.total_ms)), $stressName) + "`r`n"
}

$md = Get-Content -LiteralPath $OutPath -Raw
$begin = "<!-- BENCHMARKS:BEGIN -->"
$end = "<!-- BENCHMARKS:END -->"

$beginIndex = $md.IndexOf($begin)
$endIndex = $md.IndexOf($end)
if ($beginIndex -lt 0 -or $endIndex -lt 0 -or $endIndex -le $beginIndex) {
    throw "Markers not found or invalid in $OutPath. Expected $begin ... $end"
}

$before = $md.Substring(0, $beginIndex + $begin.Length)
$after = $md.Substring($endIndex)

$newMd = $before + "`r`n" + $generated.Trim() + "`r`n" + $after
$utf8Bom = New-Object System.Text.UTF8Encoding $true
[System.IO.File]::WriteAllText($OutPath, $newMd, $utf8Bom)

Write-Host "Updated: $OutPath"
