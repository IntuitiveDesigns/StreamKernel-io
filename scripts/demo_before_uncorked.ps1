# ==============================================================================
# StreamKernel Demo — Pre-Run: Uncorked / Max Throughput
# demo_before_uncorked.ps1
#
# Run this BEFORE test-java-runner.ps1 for the uncorked/devnull-max profiles.
# Establishes a clean system baseline so the audience can see the contrast
# once the pipeline is running at full throughput.
#
# Usage:
#   .\demo_before_uncorked.ps1
# ==============================================================================

function Write-Banner($text) {
    $line = "=" * 66
    Write-Host ""
    Write-Host $line           -ForegroundColor Cyan
    Write-Host "  $text"       -ForegroundColor Cyan
    Write-Host $line           -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step($n, $text) { Write-Host "  [$n] $text" -ForegroundColor White }
function Write-Ok($text)       { Write-Host "      OK  $text" -ForegroundColor Green  }
function Write-Info($text)     { Write-Host "      >>  $text" -ForegroundColor Cyan   }
function Write-Warn($text)     { Write-Host "      !!  $text" -ForegroundColor Yellow }

# ------------------------------------------------------------------------------
Write-Banner "StreamKernel Demo  |  Pre-Run Baseline  |  Uncorked / Max Throughput"

# ── Step 1: No existing Java process ─────────────────────────────────────────
Write-Step 1 "Checking for existing JVM processes"
$javaProcs = Get-Process -Name java -ErrorAction SilentlyContinue
if ($javaProcs) {
    Write-Warn "$($javaProcs.Count) Java process(es) already running:"
    $javaProcs | ForEach-Object {
        Write-Host "      PID $($_.Id)  Mem: $([Math]::Round($_.WorkingSet64/1MB,0))MB  $($_.MainWindowTitle)" -ForegroundColor Yellow
    }
    Write-Info "Stop them before the demo or the throughput numbers will be shared"
} else {
    Write-Ok "No existing Java processes — clean JVM start confirmed"
}

# ── Step 2: CPU baseline ──────────────────────────────────────────────────────
Write-Host ""
Write-Step 2 "CPU baseline (should be idle)"

# Cross-platform CPU check
$cpuLoad = $null
try {
    # Windows
    $cpuLoad = (Get-CimInstance -ClassName Win32_Processor -ErrorAction Stop |
        Measure-Object -Property LoadPercentage -Average).Average
} catch {
    # Linux/Mac via ps
    try {
        $cpuLine = & ps -A -o %cpu 2>/dev/null | tail -n +2 |
            ForEach-Object { [double]$_.Trim() } |
            Measure-Object -Sum
        $cpuLoad = [Math]::Round($cpuLine.Sum, 1)
    } catch {}
}

if ($null -ne $cpuLoad) {
    $cpuColor = if ($cpuLoad -lt 20) { "Green" } elseif ($cpuLoad -lt 50) { "Yellow" } else { "Red" }
    Write-Host "      CPU load: ${cpuLoad}%" -ForegroundColor $cpuColor
    if ($cpuLoad -gt 30) {
        Write-Warn "CPU already loaded at ${cpuLoad}% — throughput numbers will be depressed"
    } else {
        Write-Ok "CPU is idle — baseline established"
    }
}

# Logical core count
$cores = $null
try {
    $cores = (Get-CimInstance Win32_Processor -ErrorAction Stop |
        Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum
} catch {
    $cores = [int](& nproc 2>/dev/null)
}
if ($cores) { Write-Info "Logical CPUs available: $cores" }

# ── Step 3: Available RAM ─────────────────────────────────────────────────────
Write-Host ""
Write-Step 3 "Available memory"
try {
    $os      = Get-CimInstance Win32_OperatingSystem -ErrorAction Stop
    $freeGB  = [Math]::Round($os.FreePhysicalMemory / 1MB, 1)
    $totalGB = [Math]::Round($os.TotalVisibleMemorySize / 1MB, 1)
    $usedPct = [Math]::Round((1 - $os.FreePhysicalMemory / $os.TotalVisibleMemorySize) * 100, 0)
    $ramColor = if ($freeGB -gt 4) { "Green" } elseif ($freeGB -gt 2) { "Yellow" } else { "Red" }
    Write-Host "      RAM: ${freeGB}GB free of ${totalGB}GB total (${usedPct}% used)" -ForegroundColor $ramColor
    if ($freeGB -lt 3) {
        Write-Warn "Low free RAM — JVM heap allocation may be constrained"
    } else {
        Write-Ok "Sufficient RAM for uncorked profile"
    }
} catch {
    # Linux/Mac fallback
    $freeLine = & free -m 2>/dev/null | Select-String "Mem:"
    if ($freeLine) { Write-Info "Memory: $freeLine" }
}

# ── Step 4: Docker / Kafka (source still needs broker) ───────────────────────
Write-Host ""
Write-Step 4 "Kafka broker (source for synthetic events)"
$brokerCheck = docker exec broker kafka-broker-api-versions `
    --bootstrap-server localhost:9092 2>&1 | Select-String "localhost:9092"
if ($brokerCheck) {
    Write-Ok "Kafka broker responding — synthetic source will connect"
} else {
    Write-Warn "Kafka broker not responding — check Docker container 'broker'"
}

# ── Step 5: What to watch ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "  " + ("─" * 64) -ForegroundColor DarkGray
Write-Host ""
Write-Host "  BASELINE ESTABLISHED" -ForegroundColor Green
Write-Host ""
Write-Host "  While the pipeline runs, watch for:" -ForegroundColor White
Write-Host "    - Throughput climbing in Grafana / Prometheus" -ForegroundColor Cyan
Write-Host "    - JVM heap staying FLAT (G1GC doing its job)" -ForegroundColor Cyan
Write-Host "    - GC pauses staying under 50ms" -ForegroundColor Cyan
Write-Host "    - CPU pinned to available cores — single JAR, no cluster" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Run demo_during_uncorked.ps1 in a second terminal once the pipeline starts." -ForegroundColor DarkGray
Write-Host "  Run demo_after_uncorked.ps1 once test-java-runner.ps1 completes." -ForegroundColor DarkGray
Write-Host ""
