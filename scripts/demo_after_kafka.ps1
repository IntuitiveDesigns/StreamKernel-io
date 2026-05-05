# ==============================================================================
# StreamKernel Demo — Post-Run: Kafka Pipeline
# demo_after_kafka.ps1
#
# Run this AFTER test-java-runner.ps1 completes.
# Shows record count, partition distribution, and a sample of actual data.
#
# Usage:
#   .\demo_after_kafka.ps1
#   .\demo_after_kafka.ps1 -Topic "arena-bench-test" -Container "broker" -SampleCount 3
# ==============================================================================

param(
    [string]$Topic       = "arena-bench-test",
    [string]$Container   = "broker",
    [int]   $SampleCount = 3
)

function Write-Banner($text) {
    $line = "=" * 66
    Write-Host ""
    Write-Host $line                          -ForegroundColor Green
    Write-Host "  $text"                      -ForegroundColor Green
    Write-Host $line                          -ForegroundColor Green
    Write-Host ""
}

function Write-Step($n, $text) {
    Write-Host "  [$n] $text" -ForegroundColor White
}

function Write-Ok($text)   { Write-Host "      OK  $text" -ForegroundColor Green  }
function Write-Info($text) { Write-Host "      >>  $text" -ForegroundColor Cyan   }
function Write-Warn($text) { Write-Host "      !!  $text" -ForegroundColor Yellow }

# ------------------------------------------------------------------------------
Write-Banner "StreamKernel Demo  |  Post-Run Results  |  Kafka Pipeline"

# ── Step 1: Topic exists? ─────────────────────────────────────────────────────
Write-Step 1 "Verifying topic '$Topic' was created"
$topicList = docker exec $Container kafka-topics `
    --bootstrap-server localhost:9092 --list 2>&1
$topicExists = ($topicList -split "`n") | Where-Object { $_.Trim() -eq $Topic }

if (-not $topicExists) {
    Write-Warn "Topic '$Topic' not found — pipeline may not have started correctly"
    exit 1
}
Write-Ok "Topic '$Topic' exists"

# ── Step 2: Record count per partition ────────────────────────────────────────
Write-Host ""
Write-Step 2 "Record count"

$offsets = docker exec $Container kafka-run-class `
    kafka.tools.GetOffsetShell `
    --bootstrap-server localhost:9092 `
    --topic $Topic --time -1 2>&1

$partitionCounts = @{}
$offsets -split "`n" | Where-Object { $_ -match ":(\d+):(\d+)$" } | ForEach-Object {
    if ($_ -match ":(\d+):(\d+)$") {
        $partitionCounts[[int]$Matches[1]] = [long]$Matches[2]
    }
}

$total = ($partitionCounts.Values | Measure-Object -Sum).Sum

Write-Host ""
Write-Host "      ┌─────────────┬──────────────────┐" -ForegroundColor Cyan
Write-Host "      │  Partition  │   Record Count   │" -ForegroundColor Cyan
Write-Host "      ├─────────────┼──────────────────┤" -ForegroundColor Cyan
foreach ($p in ($partitionCounts.Keys | Sort-Object)) {
    $cnt = $partitionCounts[$p]
    Write-Host ("      │     {0,-7}  │   {1,-14}  │" -f $p, $cnt) -ForegroundColor White
}
Write-Host "      ├─────────────┼──────────────────┤" -ForegroundColor Cyan
Write-Host ("      │   TOTAL     │   {0,-14}  │" -f $total) -ForegroundColor Green
Write-Host "      └─────────────┴──────────────────┘" -ForegroundColor Cyan
Write-Host ""

# ── Step 3: Consumer lag ──────────────────────────────────────────────────────
Write-Step 3 "Consumer lag (should be zero or near-zero)"
$groups = docker exec $Container kafka-consumer-groups `
    --bootstrap-server localhost:9092 --list 2>&1
$skGroups = ($groups -split "`n") | Where-Object { $_ -match "streamkernel" }

if ($skGroups) {
    foreach ($g in $skGroups) {
        $lag = docker exec $Container kafka-consumer-groups `
            --bootstrap-server localhost:9092 --describe --group $g 2>&1
        $totalLag = ($lag -split "`n" |
            Where-Object { $_ -match "\d+\s+\d+\s+\d+" } |
            ForEach-Object {
                $cols = ($_ -split "\s+") | Where-Object { $_ -ne "" }
                if ($cols.Count -ge 6) { [long]$cols[5] } else { 0 }
            } | Measure-Object -Sum).Sum
        $lagColor = if ($totalLag -eq 0) { "Green" } elseif ($totalLag -lt 100) { "Yellow" } else { "Red" }
        Write-Host "      Group '$g' — final lag: $totalLag" -ForegroundColor $lagColor
    }
} else {
    Write-Info "No consumer groups found (devnull/NOOP profiles produce no consumer group)"
}

# ── Step 4: Sample messages ───────────────────────────────────────────────────
Write-Host ""
Write-Step 4 "Sample messages from '$Topic' (showing $SampleCount)"
Write-Host ""

# Use kafka-console-consumer with timeout to grab a sample
# --from-beginning --max-messages pulls the first N records off partition 0
$sample = docker exec $Container kafka-console-consumer `
    --bootstrap-server localhost:9092 `
    --topic $Topic `
    --from-beginning `
    --max-messages $SampleCount `
    --timeout-ms 8000 2>&1 |
    Where-Object { $_ -ne "" -and $_ -notmatch "^Processed" -and $_ -notmatch "^WARN" }

if ($sample) {
    $i = 1
    foreach ($msg in $sample | Select-Object -First $SampleCount) {
        Write-Host "      ── Record $i ─────────────────────────────────────────" -ForegroundColor DarkGray
        # Try to pretty-print if JSON
        try {
            $parsed = $msg | ConvertFrom-Json -ErrorAction Stop
            $pretty = $parsed | ConvertTo-Json -Depth 5
            $pretty -split "`n" | ForEach-Object { Write-Host "      $_" -ForegroundColor White }
        } catch {
            # Not JSON — print raw
            Write-Host "      $msg" -ForegroundColor White
        }
        Write-Host ""
        $i++
    }
} else {
    Write-Warn "Could not retrieve sample messages (timeout or empty partition 0)"
    Write-Info "Try: docker exec $Container kafka-console-consumer --bootstrap-server localhost:9092 --topic $Topic --from-beginning --max-messages 1"
}

# ── Step 5: Summary ───────────────────────────────────────────────────────────
Write-Host "  " + ("─" * 64) -ForegroundColor DarkGray
Write-Host ""
Write-Host ("  PIPELINE RESULT: {0:N0} records written to topic '{1}'" -f $total, $Topic) -ForegroundColor Green
Write-Host ""
