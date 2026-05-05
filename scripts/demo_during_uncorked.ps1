# ==============================================================================
# StreamKernel Demo — During Run: Uncorked / Max Throughput
# demo_during_uncorked.ps1
#
# Run this in a SECOND TERMINAL while test-java-runner.ps1 is running.
# Shows a live rolling dashboard of throughput, JVM heap, GC activity,
# and CPU — the things that prove the framework is efficient under load.
#
# Usage:
#   .\demo_during_uncorked.ps1
#   .\demo_during_uncorked.ps1 -IntervalSec 3 -PrometheusUrl "http://localhost:9090"
# ==============================================================================

param(
    [int]   $IntervalSec    = 4,
    [string]$PrometheusUrl  = "http://localhost:9090"
)

function Get-PromVal {
    param([string]$Query)
    try {
        $enc = [uri]::EscapeDataString($Query)
        $r   = Invoke-RestMethod "$PrometheusUrl/api/v1/query?query=$enc" -TimeoutSec 3
        $val = $r.data.result[0].value[1]
        return if ($null -ne $val) { [double]$val } else { $null }
    } catch { return $null }
}

function Format-Num($val, $unit = "", $decimals = 0) {
    if ($null -eq $val) { return "---" }
    return "$([Math]::Round($val, $decimals))$unit"
}

function Get-Bar {
    param([double]$pct, [int]$width = 30)
    $filled = [Math]::Min($width, [int]($pct / 100 * $width))
    $empty  = $width - $filled
    $color  = if ($pct -lt 60) { "Green" } elseif ($pct -lt 85) { "Yellow" } else { "Red" }
    return @{ Bar = ("█" * $filled) + ("░" * $empty); Color = $color; Pct = $pct }
}

# Get Java PID for per-process CPU/mem
function Get-JavaPid {
    $p = Get-Process -Name java -ErrorAction SilentlyContinue | Select-Object -First 1
    return if ($p) { $p.Id } else { $null }
}

# Peak tracking
$peakEps     = 0.0
$peakHeapPct = 0.0
$iteration   = 0

Clear-Host
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     StreamKernel  ·  Uncorked  ·  Live Throughput Monitor        ║" -ForegroundColor Cyan
Write-Host "║     Ctrl+C to stop  ·  Refreshing every ${IntervalSec}s                    ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$startTime = Get-Date

while ($true) {
    $iteration++
    $elapsed = [Math]::Round(((Get-Date) - $startTime).TotalSeconds, 0)

    # ── Prometheus metrics ────────────────────────────────────────────────────
    $eps          = Get-PromVal "rate(streamkernel_records_processed_total[10s])"
    $heapUsed     = Get-PromVal "jvm_memory_used_bytes{area='heap'}"
    $heapMax      = Get-PromVal "jvm_memory_max_bytes{area='heap'}"
    $gcPauseRate  = Get-PromVal "rate(jvm_gc_pause_seconds_sum[30s])"
    $gcPauseMax   = Get-PromVal "max_over_time(jvm_gc_pause_seconds_max[60s])"
    $gcCount      = Get-PromVal "increase(jvm_gc_pause_seconds_count[60s])"
    $threadsLive  = Get-PromVal "jvm_threads_live"
    $cpuProcess   = Get-PromVal "process_cpu_usage"

    # ── Per-process stats (OS level) ──────────────────────────────────────────
    $pid      = Get-JavaPid
    $procMem  = $null
    $procCpu  = $null
    if ($pid) {
        $proc    = Get-Process -Id $pid -ErrorAction SilentlyContinue
        $procMem = if ($proc) { [Math]::Round($proc.WorkingSet64 / 1MB, 0) } else { $null }
    }

    # ── Derived values ────────────────────────────────────────────────────────
    $heapPct = if ($heapUsed -and $heapMax -and $heapMax -gt 0) {
        [Math]::Round(($heapUsed / $heapMax) * 100, 1)
    } else { $null }

    $heapUsedMB = if ($heapUsed) { [Math]::Round($heapUsed / 1MB, 0) } else { $null }
    $heapMaxMB  = if ($heapMax)  { [Math]::Round($heapMax  / 1MB, 0) } else { $null }

    $gcPauseMs  = if ($gcPauseMax)  { [Math]::Round($gcPauseMax  * 1000, 1) } else { $null }
    $cpuPct     = if ($cpuProcess)  { [Math]::Round($cpuProcess  * 100,  1) } else { $null }

    # Update peaks
    if ($eps      -and $eps      -gt $peakEps)     { $peakEps     = $eps }
    if ($heapPct  -and $heapPct  -gt $peakHeapPct) { $peakHeapPct = $heapPct }

    # ── Render ────────────────────────────────────────────────────────────────
    # Move cursor to row 6 to overwrite (avoids scroll)
    $pos   = $host.UI.RawUI.CursorPosition
    $pos.Y = 5
    $host.UI.RawUI.CursorPosition = $pos

    $ts = Get-Date -Format "HH:mm:ss"
    Write-Host ""
    Write-Host ("  $ts  |  elapsed: ${elapsed}s  |  sample #$iteration" + " " * 20) -ForegroundColor DarkGray
    Write-Host ""

    # ── Throughput ────────────────────────────────────────────────────────────
    $epsStr  = if ($eps) { "{0:N0} events/sec" -f $eps } else { "waiting for data..." }
    $peakStr = if ($peakEps -gt 0) { "  (peak: {0:N0})" -f $peakEps } else { "" }
    $epsColor = if ($eps -gt 100000) { "Green" } elseif ($eps -gt 0) { "Yellow" } else { "DarkGray" }
    Write-Host ("  THROUGHPUT    $epsStr$peakStr" + " " * 10) -ForegroundColor $epsColor

    Write-Host ""

    # ── JVM Heap bar ──────────────────────────────────────────────────────────
    if ($null -ne $heapPct) {
        $bar = Get-Bar $heapPct 36
        Write-Host ("  HEAP          [{0}] {1}%   {2}MB / {3}MB" -f `
            $bar.Bar, $heapPct, $heapUsedMB, $heapMaxMB) -ForegroundColor $bar.Color
    } else {
        Write-Host ("  HEAP          [" + ("░" * 36) + "] ---") -ForegroundColor DarkGray
    }

    Write-Host ""

    # ── CPU bar ───────────────────────────────────────────────────────────────
    if ($null -ne $cpuPct) {
        $cpuBar = Get-Bar $cpuPct 36
        Write-Host ("  CPU (process) [{0}] {1}%" -f $cpuBar.Bar, $cpuPct + " " * 10) -ForegroundColor $cpuBar.Color
    } elseif ($null -ne $procMem) {
        Write-Host ("  JVM Process   RSS: ${procMem}MB   PID: $pid" + " " * 10) -ForegroundColor Cyan
    } else {
        Write-Host ("  CPU           [" + ("░" * 36) + "] ---") -ForegroundColor DarkGray
    }

    Write-Host ""

    # ── GC stats ──────────────────────────────────────────────────────────────
    $gcColor = if ($gcPauseMs -ne $null -and $gcPauseMs -lt 50) { "Green" } `
               elseif ($gcPauseMs -ne $null -and $gcPauseMs -lt 100) { "Yellow" } `
               else { "DarkGray" }
    $gcStr = if ($gcPauseMs -ne $null) {
        "max pause: ${gcPauseMs}ms   count (60s): $([Math]::Round($gcCount, 0))"
    } else { "waiting for GC data..." }
    Write-Host ("  G1GC          $gcStr" + " " * 10) -ForegroundColor $gcColor

    Write-Host ""

    # ── Thread count ──────────────────────────────────────────────────────────
    $thrStr = if ($threadsLive) { "$([int]$threadsLive) live threads" } else { "---" }
    Write-Host ("  THREADS       $thrStr" + " " * 20) -ForegroundColor Cyan

    Write-Host ""
    Write-Host ("  " + ("─" * 64)) -ForegroundColor DarkGray

    # ── The talking point ─────────────────────────────────────────────────────
    Write-Host ""
    if ($eps -gt 0 -and $heapPct -ne $null) {
        if ($heapPct -lt 75 -and ($gcPauseMs -eq $null -or $gcPauseMs -lt 50)) {
            Write-Host ("  Single JAR  ·  {0:N0} events/sec  ·  heap stable  ·  GC under 50ms" -f $eps) -ForegroundColor Green
        } elseif ($heapPct -ge 75) {
            Write-Host "  Heap climbing — GC will trigger. Watch for pause." -ForegroundColor Yellow
        }
    } else {
        Write-Host "  Pipeline starting up..." -ForegroundColor DarkGray
    }
    Write-Host ""

    Start-Sleep -Seconds $IntervalSec
}
