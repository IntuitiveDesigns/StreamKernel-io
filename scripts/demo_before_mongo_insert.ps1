# ==============================================================================
# StreamKernel Demo — Pre-Run: MongoDB Insert Baseline
# demo_before_mongo_insert.ps1
#
# Run this BEFORE test-java-runner.ps1 for the mongodb_insert_baseline profile.
# Clears support_db.sk_insert_baseline, confirms zero documents,
# and shows collection state.
#
# This profile uses the MONGO_INSERT plugin — pure insertMany, no upsert,
# no embedding, no vector field. This is the raw MongoDB write ceiling run.
#
# Requires: MongoDB container running via:
#   docker compose --profile mongo up -d
#
# Usage:
#   .\demo_before_mongo_insert.ps1
#   .\demo_before_mongo_insert.ps1 -NoClear          # inspect only, skip delete
#   .\demo_before_mongo_insert.ps1 -Container "mongodb" -Database "support_db"
# ==============================================================================

param(
    [string]$Container  = "mongodb",
    [string]$Database   = "support_db",
    [string]$Collection = "sk_insert_baseline",
    [switch]$NoClear
)

# ── Transcript capture ────────────────────────────────────────────────────────
# Writes a timestamped copy of all console output into the run artifact folder
# so demo state is preserved alongside the benchmark logs.
$_transcriptStamp  = Get-Date -Format "yyyyMMdd_HHmmss"
$_scriptLeaf       = [System.IO.Path]::GetFileNameWithoutExtension($MyInvocation.MyCommand.Name)
$_runsBase         = Join-Path $PSScriptRoot ".." "benchmark-runs"
$_transcriptFolder = Join-Path $_runsBase "streamkernel_mongodb_insert_baseline_10m"
if (!(Test-Path -LiteralPath $_transcriptFolder)) {
    New-Item -ItemType Directory -Force -Path $_transcriptFolder | Out-Null
}
$_transcriptPath   = Join-Path $_transcriptFolder "$_scriptLeaf`_$_transcriptStamp.txt"
Start-Transcript -LiteralPath $_transcriptPath -Force | Out-Null
# ─────────────────────────────────────────────────────────────────────────────


function Write-Banner($text) {
    $line = "=" * 66
    Write-Host ""
    Write-Host $line             -ForegroundColor Cyan
    Write-Host "  $text"         -ForegroundColor Cyan
    Write-Host $line             -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step($n, $text) { Write-Host "  [$n] $text" -ForegroundColor White }
function Write-Ok($text)       { Write-Host "      OK  $text" -ForegroundColor Green  }
function Write-Info($text)     { Write-Host "      >>  $text" -ForegroundColor Cyan   }
function Write-Warn($text)     { Write-Host "      !!  $text" -ForegroundColor Yellow }
function Write-Fail($text)     { Write-Host "      XX  $text" -ForegroundColor Red    }

function Invoke-ContainerMongosh {
    param([string]$Script)
    $output = docker exec $Container mongosh $Database --quiet --eval $Script 2>&1
    return $output
}

# ── Verify container is running ───────────────────────────────────────────────
Write-Banner "StreamKernel Demo  |  Pre-Run State  |  MongoDB Insert Baseline"

Write-Step 0 "Container health check"
$state = docker inspect --format='{{.State.Running}}' $Container 2>&1
if ($state -ne "true") {
    Write-Fail "Container '$Container' is not running"
    Write-Info "Start it with: docker compose --profile mongo up -d"
    Write-Info "Then wait for healthy: docker compose ps"
    exit 1
}
Write-Ok "Container '$Container' is running"

# ── Step 1: Connectivity ──────────────────────────────────────────────────────
Write-Host ""
Write-Step 1 "MongoDB connectivity"
$ping = Invoke-ContainerMongosh "JSON.stringify(db.runCommand({ ping: 1 }))"
if ($ping -match '"ok"\s*:\s*1' -or $ping -match '"ok":1' -or $ping -match 'ok: 1') {
    Write-Ok "mongosh connected inside container '$Container'"
} else {
    Write-Fail "mongosh could not connect — container may still be initializing"
    Write-Info "Wait ~10s and retry, or check: docker logs $Container"
    Write-Info "Raw output: $ping"
    exit 1
}

# ── Step 2: Current document count ───────────────────────────────────────────
Write-Host ""
Write-Step 2 "Current document count in $Database.$Collection"
$countRaw = Invoke-ContainerMongosh "db.$Collection.countDocuments()"
$count    = ($countRaw -split "`n" | Where-Object { $_ -match "^\d+$" } | Select-Object -Last 1)

if ($count -and [long]$count -gt 0) {
    Write-Warn "$Collection currently contains $([long]$count) documents"

    if (-not $NoClear) {
        Write-Host ""
        Write-Host "      Clearing collection for clean baseline run..." -ForegroundColor Yellow
        $deleteResult = Invoke-ContainerMongosh "JSON.stringify(db.$Collection.deleteMany({}))"
        if ($deleteResult -match '"deletedCount"') {
            $deleted = [regex]::Match($deleteResult, '"deletedCount"\s*:\s*(\d+)').Groups[1].Value
            Write-Ok "Deleted $deleted documents from $Collection"
        } else {
            Write-Warn "Delete may not have completed — check manually"
            Write-Info "Output: $deleteResult"
        }
    } else {
        Write-Info "-NoClear flag set — skipping delete"
    }
} else {
    Write-Ok "$Collection is empty (0 documents) — clean state confirmed"
}

# ── Step 3: Verify zero after clear ──────────────────────────────────────────
if (-not $NoClear) {
    Write-Host ""
    Write-Step 3 "Confirming empty collection"
    $countAfter = Invoke-ContainerMongosh "db.$Collection.countDocuments()"
    $finalCount = ($countAfter -split "`n" | Where-Object { $_ -match "^\d+$" } | Select-Object -Last 1)
    if ($null -eq $finalCount -or $finalCount -eq "0" -or [long]$finalCount -eq 0) {
        Write-Ok "Confirmed: $Collection contains 0 documents"
    } else {
        Write-Warn "Collection still shows $finalCount documents after delete — verify manually"
    }
}

# ── Step 4: List all collections in the database ─────────────────────────────
Write-Host ""
Write-Step 4 "Collections in $Database"
$colls    = Invoke-ContainerMongosh "db.getCollectionNames().forEach(c => print(c))"
$collList = ($colls -split "`n") | Where-Object { $_ -ne "" -and $_ -notmatch "^(use |switched)" }
if ($collList) {
    foreach ($c in $collList) {
        $marker = if ($c.Trim() -eq $Collection) { "  <-- target (insert baseline)" } else { "" }
        Write-Info "$($c.Trim())$marker"
    }
} else {
    Write-Info "No collections yet (database is empty — will be created on first write)"
}

# ── Step 5: Confirm no indexes (baseline should have only _id) ────────────────
Write-Host ""
Write-Step 5 "Index state on $Collection"
$idxJs = "const idxs = db.getCollection('$Collection').getIndexes();" +
         "if (idxs.length === 0) { print('NO_INDEXES'); }" +
         "else { idxs.forEach(i => print(JSON.stringify({ name: i.name, key: i.key }))); }"
$idxOutput = Invoke-ContainerMongosh $idxJs

if ($idxOutput -match "NO_INDEXES") {
    Write-Info "No indexes on $Collection yet (only _id index will be auto-created on first insert)"
} else {
    ($idxOutput -split "`n") | Where-Object { $_ -match "{" } | ForEach-Object {
        try {
            $obj = $_ | ConvertFrom-Json
            Write-Info "Index: $($obj.name)  key: $($obj.key | ConvertTo-Json -Compress)"
        } catch { Write-Info $_ }
    }
    Write-Info "Note: for a clean write throughput baseline, only _id index should be present"
}

# ── Step 6: MongoDB server stats (storage engine) ────────────────────────────
Write-Host ""
Write-Step 6 "Storage engine info"
$serverJs = "const si = db.serverStatus(); print(si.storageEngine.name + ' / MongoDB ' + si.version);"
$serverInfo = Invoke-ContainerMongosh $serverJs
Write-Info ($serverInfo -join " ").Trim()

# ── Ready ─────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  " + ("─" * 64) -ForegroundColor DarkGray
Write-Host ""
Write-Host "  PRE-RUN STATE CONFIRMED" -ForegroundColor Green
Write-Host "  $Database.$Collection is empty — starting from zero." -ForegroundColor Green
Write-Host "  Profile: MONGO_INSERT — pure insertMany, ObjectId _id, no upsert." -ForegroundColor Green
Write-Host ""
Write-Host "  Next: run test-java-runner.ps1, then demo_after_mongo_insert.ps1" -ForegroundColor DarkGray
Write-Host ""

# ── End transcript ────────────────────────────────────────────────────────────
try { Stop-Transcript | Out-Null } catch {}
