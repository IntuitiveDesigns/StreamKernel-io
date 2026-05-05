# ==============================================================================
# StreamKernel Demo — Pre-Run: Kafka Pipeline
# demo_before_kafka.ps1
#
# Run this BEFORE test-java-runner.ps1 for any Kafka sink profile.
# Shows the audience the topic does not exist / is empty, then hands off.
#
# Usage:
#   .\demo_before_kafka.ps1
#   .\demo_before_kafka.ps1 -Topic "arena-bench-test" -Container "broker"
# ==============================================================================

param(
    [string]$Topic     = "arena-bench-test",
    [string]$Container = "broker"
)

function Write-Banner($text) {
    $line = "=" * 66
    Write-Host ""
    Write-Host $line                          -ForegroundColor Cyan
    Write-Host "  $text"                      -ForegroundColor Cyan
    Write-Host $line                          -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step($n, $text) {
    Write-Host "  [$n] $text" -ForegroundColor White
}

function Write-Ok($text)   { Write-Host "      OK  $text" -ForegroundColor Green  }
function Write-Info($text) { Write-Host "      >>  $text" -ForegroundColor Cyan   }
function Write-Warn($text) { Write-Host "      !!  $text" -ForegroundColor Yellow }

# ------------------------------------------------------------------------------
Write-Banner "StreamKernel Demo  |  Pre-Run State  |  Kafka Pipeline"

# ── Step 1: Kafka broker health ───────────────────────────────────────────────
Write-Step 1 "Kafka broker health"
$brokerCheck = docker exec $Container kafka-broker-api-versions `
    --bootstrap-server localhost:9092 2>&1 | Select-String "localhost:9092"
if ($brokerCheck) {
    Write-Ok "Broker responding at localhost:9092"
} else {
    Write-Warn "Broker not responding — check Docker container '$Container'"
}

# ── Step 2: Topic existence check ─────────────────────────────────────────────
Write-Host ""
Write-Step 2 "Topic state: '$Topic'"
$topicList = docker exec $Container kafka-topics `
    --bootstrap-server localhost:9092 --list 2>&1
$topicExists = ($topicList -split "`n") | Where-Object { $_.Trim() -eq $Topic }

if (-not $topicExists) {
    Write-Ok "Topic '$Topic' does NOT exist"
    Write-Info "It will be created fresh by the runner before the pipeline starts"
} else {
    # Topic exists — show current offset (message count)
    Write-Warn "Topic '$Topic' already exists — showing current state:"
    $offsets = docker exec $Container kafka-run-class `
        kafka.tools.GetOffsetShell `
        --bootstrap-server localhost:9092 `
        --topic $Topic --time -1 2>&1
    $total = ($offsets -split "`n" |
        Where-Object { $_ -match ":\d+:\d+$" } |
        ForEach-Object { [long]($_ -split ":")[-1] } |
        Measure-Object -Sum).Sum
    if ($total -eq 0) {
        Write-Ok "Topic exists but contains 0 messages (clean)"
    } else {
        Write-Warn "Topic has $total existing messages"
        Write-Info "The runner will delete and recreate it — this is expected"
    }

    # Show partition layout
    $desc = docker exec $Container kafka-topics `
        --bootstrap-server localhost:9092 --describe --topic $Topic 2>&1
    Write-Host ""
    Write-Host "      Current partition layout:" -ForegroundColor DarkGray
    $desc -split "`n" | Where-Object { $_ -match "Partition:" } |
        ForEach-Object { Write-Host "        $_" -ForegroundColor DarkGray }
}

# ── Step 3: Consumer group state ──────────────────────────────────────────────
Write-Host ""
Write-Step 3 "Consumer group state"
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
        Write-Info "Group '$g' — lag: $totalLag"
    }
} else {
    Write-Ok "No StreamKernel consumer groups registered yet"
}

# ── Step 4: Confirm ready ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "  " + ("─" * 64) -ForegroundColor DarkGray
Write-Host ""
Write-Host "  PRE-RUN STATE CONFIRMED" -ForegroundColor Green
Write-Host "  Topic '$Topic' is absent or clean — starting from zero." -ForegroundColor Green
Write-Host ""
Write-Host "  Next: run test-java-runner.ps1, then demo_after_kafka.ps1" -ForegroundColor DarkGray
Write-Host ""
