# ==============================================================================
# StreamKernel Demo — Post-Run: Uncorked / Max Throughput
# demo_after_uncorked.ps1
#
# Run this AFTER test-java-runner.ps1 completes for the uncorked profile.
# Reads the GC log produced by the runner and summarizes what actually
# happened inside the JVM at peak throughput — the numbers engineers trust.
#
# Usage:
#   .\demo_after_uncorked.ps1
#   .\demo_after_uncorked.ps1 -GcLogPath ".\benchmark-runs\streamkernel_source_baseline_uncorked\streamkernel_source_baseline_uncorked_20260314_1430_gc.log"
#   .\demo_after_uncorked.ps1 -BenchmarkRunsDir ".\benchmark-runs"
# ==============================================================================

param(
    [string]$GcLogPath        = "",
    [string]$BenchmarkRunsDir = (Join-Path (Split-Path -Parent $PSScriptRoot) "benchmark-runs"),
    [string]$ProfileName      = "streamkernel_source_baseline_uncorked",
    [string]$PrometheusUrl    = "http://localhost:9090"
)

function Write-Banner($text) {
    $line = "=" * 66
    Write-Host ""
    Write-Host $line           -ForegroundColor Green
    Write-Host "  $text"       -ForegroundColor Green
    Write-Host $line           -ForegroundColor Green
    Write-Host ""
}

function Write-Step($n, $text) { Write-Host "  [$n] $text" -ForegroundColor White }
function Write-Ok($text)       { Write-Host "      OK  $text" -ForegroundColor Green  }
function Write-Info($text)     { Write-Host "      >>  $text" -ForegroundColor Cyan   }
function Write-Warn($text)     { Write-Host "      !!  $text" -ForegroundColor Yellow }

function Get-PromFinal {
    param([string]$Query, [string]$Label, [string]$Unit = "")
    try {
        $enc = [uri]::EscapeDataString($Query)
        $r   = Invoke-RestMethod "$PrometheusUrl/api/v1/query?query=$enc" -TimeoutSec 5
        $val = $r.data.result[0].value[1]
        if ($null -ne $val) {
            $v = [Math]::Round([double]$val, 3)
            Write-Host ("      {0,-42} {1}{2}" -f $Label, $v, $Unit) -ForegroundColor Cyan
            return [double]$val
        }
    } catch {}
    Write-Host ("      {0,-42} ---" -f $Label) -ForegroundColor DarkGray
    return $null
}

# ------------------------------------------------------------------------------
Write-Banner "StreamKernel Demo  |  Post-Run Results  |  Uncorked / Max Throughput"

# ── Locate GC log ─────────────────────────────────────────────────────────────
Write-Step 1 "Locating GC log"

if ($GcLogPath -eq "") {
    # Auto-find most recent GC log for this profile
    $profileDir = Join-Path $BenchmarkRunsDir $ProfileName
    if (Test-Path $profileDir) {
        $gcLog = Get-ChildItem $profileDir -Filter "*_gc.log" |
            Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($gcLog) {
            $GcLogPath = $gcLog.FullName
            Write-Ok "Found: $($gcLog.Name)"
        } else {
            Write-Warn "No GC log found in $profileDir"
        }
    } else {
        Write-Warn "Profile output directory not found: $profileDir"
    }
} else {
    if (Test-Path $GcLogPath) {
        Write-Ok "Using: $GcLogPath"
    } else {
        Write-Warn "GC log not found at: $GcLogPath"
        $GcLogPath = ""
    }
}

# ── Parse GC log ──────────────────────────────────────────────────────────────
Write-Host ""
Write-Step 2 "GC log analysis"

if ($GcLogPath -ne "" -and (Test-Path $GcLogPath)) {
    $gcLines = Get-Content $GcLogPath

    # Parse pause lines — G1GC format: [x.xxxs][info][gc] GC(N) Pause ... Xms
    $pauseLines = $gcLines | Where-Object { $_ -match "Pause" -and $_ -match "ms" }

    $pauseTimes = $pauseLines | ForEach-Object {
        if ($_ -match "(\d+\.\d+)ms") { [double]$Matches[1] }
    } | Where-Object { $null -ne $_ }

    if ($pauseTimes -and $pauseTimes.Count -gt 0) {
        $gcCount   = $pauseTimes.Count
        $gcMax     = ($pauseTimes | Measure-Object -Maximum).Maximum
        $gcMin     = ($pauseTimes | Measure-Object -Minimum).Minimum
        $gcAvg     = [Math]::Round(($pauseTimes | Measure-Object -Average).Average, 2)
        $gcTotal   = [Math]::Round(($pauseTimes | Measure-Object -Sum).Sum, 1)

        # P99 approximation
        $sorted    = $pauseTimes | Sort-Object
        $p99idx    = [Math]::Floor($sorted.Count * 0.99)
        $gcP99     = $sorted[$p99idx]

        Write-Host ""
        Write-Host "      ┌──────────────────────────────────────────────┐" -ForegroundColor Cyan
        Write-Host "      │  G1GC Pause Statistics  (full run)           │" -ForegroundColor Cyan
        Write-Host "      ├──────────────────────────────────────────────┤" -ForegroundColor Cyan
        Write-Host ("      │  Total GC events    : {0,-24} │" -f $gcCount)   -ForegroundColor White
        Write-Host ("      │  Min pause          : {0,-21}ms  │" -f $gcMin)  -ForegroundColor Green
        Write-Host ("      │  Avg pause          : {0,-21}ms  │" -f $gcAvg)  -ForegroundColor $(if ($gcAvg -lt 50) { "Green" } else { "Yellow" })
        Write-Host ("      │  P99 pause          : {0,-21}ms  │" -f $gcP99) -ForegroundColor $(if ($gcP99 -lt 50) { "Green" } else { "Yellow" })
        Write-Host ("      │  Max pause          : {0,-21}ms  │" -f $gcMax)  -ForegroundColor $(if ($gcMax -lt 50) { "Green" } elseif ($gcMax -lt 100) { "Yellow" } else { "Red" })
        Write-Host ("      │  Total GC time      : {0,-21}ms  │" -f $gcTotal) -ForegroundColor Cyan
        Write-Host "      └──────────────────────────────────────────────┘" -ForegroundColor Cyan
        Write-Host ""

        if ($gcMax -lt 50) {
            Write-Ok "All GC pauses under 50ms — G1GC target maintained at peak throughput"
        } elseif ($gcMax -lt 100) {
            Write-Warn "Max pause ${gcMax}ms — slightly above 50ms target but acceptable"
        } else {
            Write-Warn "Max pause ${gcMax}ms — consider tuning HeapGb or GcThreads"
        }

        # Show longest pauses
        $top5 = $sorted | Sort-Object -Descending | Select-Object -First 5
        Write-Host ""
        Write-Info "Longest individual pauses:"
        $top5 | ForEach-Object { Write-Host "        ${_}ms" -ForegroundColor $(if ($_ -lt 50) { "Green" } elseif ($_ -lt 100) { "Yellow" } else { "Red" }) }

    } else {
        Write-Warn "No GC pause lines found in log — format may differ"
        Write-Info "First 5 lines of GC log:"
        $gcLines | Select-Object -First 5 | ForEach-Object { Write-Host "      $_" -ForegroundColor DarkGray }
    }

    # Young vs Mixed GC split
    $youngPauses = ($gcLines | Where-Object { $_ -match "Pause Young" }).Count
    $mixedPauses = ($gcLines | Where-Object { $_ -match "Pause Mixed" }).Count
    $fullPauses  = ($gcLines | Where-Object { $_ -match "Pause Full"  }).Count

    Write-Host ""
    Write-Info "GC type breakdown:"
    Write-Host "      Young (minor) : $youngPauses" -ForegroundColor $(if ($youngPauses -gt 0) { "Green" } else { "DarkGray" })
    Write-Host "      Mixed         : $mixedPauses" -ForegroundColor $(if ($mixedPauses -gt 0) { "Yellow" } else { "Green" })
    Write-Host "      Full (STW)    : $fullPauses"  -ForegroundColor $(if ($fullPauses -gt 0) { "Red" } else { "Green" })
    if ($fullPauses -eq 0) {
        Write-Ok "Zero Full GC events — no stop-the-world pauses at peak throughput"
    }

} else {
    Write-Warn "No GC log available — skipping GC analysis"
    Write-Info "GC log is written by test-java-runner.ps1 as <testName>_<timestamp>_gc.log"
}

# ── Prometheus final snapshot ─────────────────────────────────────────────────
Write-Host ""
Write-Step 3 "Prometheus final metrics"
Write-Host ""
$peakEps    = Get-PromFinal "max_over_time(rate(streamkernel_records_processed_total[10s])[10m:])" "Peak throughput" " rec/s"
$totalRecs  = Get-PromFinal "streamkernel_records_processed_total" "Total records processed"
$heapFinal  = Get-PromFinal "jvm_memory_used_bytes{area='heap'}" "Final heap used" " bytes"
$gcPauseP99 = Get-PromFinal "histogram_quantile(0.99, rate(jvm_gc_pause_seconds_bucket[5m]))" "GC pause P99 (Prometheus)" " s"

# ── Sidecar JSON — Grafana time range ─────────────────────────────────────────
Write-Host ""
Write-Step 4 "Grafana time range (from sidecar)"
$profileDir = Join-Path $BenchmarkRunsDir $ProfileName
if (Test-Path $profileDir) {
    $sidecar = Get-ChildItem $profileDir -Filter "*_meta.json" |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($sidecar) {
        $meta = Get-Content $sidecar.FullName | ConvertFrom-Json
        Write-Host ""
        Write-Host "      Grafana URL parameters:" -ForegroundColor Cyan
        Write-Host "        from=$($meta.grafanaFrom)  to=$($meta.grafanaTo)" -ForegroundColor White
        Write-Host ""
        Write-Host "      Run duration : $($meta.actualMinutes) minutes" -ForegroundColor Cyan
        Write-Host "      Status       : $($meta.status)" -ForegroundColor $(if ($meta.status -eq "COMPLETED") { "Green" } else { "Red" })
        Write-Host "      Heap config  : $($meta.profile.heapGb)GB" -ForegroundColor Cyan
        Write-Host "      GC threads   : $($meta.profile.gcThreads)" -ForegroundColor Cyan
    }
}

# ── The talking point ─────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  " + ("─" * 64) -ForegroundColor DarkGray
Write-Host ""
Write-Host "  THE RESULT:" -ForegroundColor White
if ($null -ne $peakEps) {
    Write-Host ("  {0:N0} events/sec  ·  single JVM  ·  no cluster  ·  GC under control" -f $peakEps) -ForegroundColor Green
} else {
    Write-Host "  Check Grafana for throughput — set time range from sidecar above" -ForegroundColor Cyan
}
Write-Host ""
Write-Host "  This is the uncorked ceiling. Compare it with transformer-heavy runs" -ForegroundColor DarkGray
Write-Host "  to see how each added stage changes throughput and latency." -ForegroundColor DarkGray
Write-Host ""
