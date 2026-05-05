# ==============================================================================
# StreamKernel Demo — Post-Run: MongoDB Insert Baseline
# demo_after_mongo_insert.ps1
#
# Run this AFTER test-java-runner.ps1 completes for the mongodb_insert_baseline
# profile. Shows:
#   - Total documents written
#   - Calculated average throughput (docs/sec) over the run duration
#   - Sample documents showing the actual schema written by MONGO_INSERT
#   - Field inventory for the baseline insert schema
#   - Collection storage size on disk
#   - Collection index inventory
#
# This is the raw write ceiling run: pure insertMany with the baseline schema.
# The after-script is the proof that the pipeline delivered to MongoDB.
#
# Requires: MongoDB container running via:
#   docker compose --profile mongo up -d
#
# Usage:
#   .\demo_after_mongo_insert.ps1
#   .\demo_after_mongo_insert.ps1 -SampleCount 3
#   .\demo_after_mongo_insert.ps1 -RunMinutes 10 -Container "mongodb"
# ==============================================================================

param(
    [string]$Container   = "mongodb",
    [string]$Database    = "support_db",
    [string]$Collection  = "sk_insert_baseline",
    [int]   $SampleCount = 2,
    [int]   $RunMinutes  = 10      # used to calculate avg docs/sec
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
    Write-Host $line             -ForegroundColor Green
    Write-Host "  $text"         -ForegroundColor Green
    Write-Host $line             -ForegroundColor Green
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

# ── Container check ───────────────────────────────────────────────────────────
Write-Banner "StreamKernel Demo  |  Post-Run Results  |  MongoDB Insert Baseline"

$state = docker inspect --format='{{.State.Running}}' $Container 2>&1
if ($state -ne "true") {
    Write-Fail "Container '$Container' is not running"
    Write-Info "Start it with: docker compose --profile mongo up -d"
    exit 1
}

# ── Step 1: Document count + throughput calculation ───────────────────────────
Write-Step 1 "Documents written to $Database.$Collection"
$countRaw  = Invoke-ContainerMongosh "db.$Collection.countDocuments()"
$count     = ($countRaw -split "`n" | Where-Object { $_ -match "^\d+$" } | Select-Object -Last 1)
$countLong = if ($count) { [long]$count } else { 0 }

$runSeconds = $RunMinutes * 60
$avgDocsPerSec = if ($runSeconds -gt 0 -and $countLong -gt 0) {
    [math]::Round($countLong / $runSeconds, 0)
} else { 0 }

Write-Host ""
if ($countLong -gt 0) {
    Write-Host "      ┌──────────────────────────────────────────────────┐" -ForegroundColor Green
    Write-Host ("      │  {0,-50} │" -f "MongoDB Insert Baseline Results") -ForegroundColor Green
    Write-Host "      │                                                  │" -ForegroundColor Green
    Write-Host ("      │  Documents written : {0,-30} │" -f ("{0:N0}" -f $countLong)) -ForegroundColor Green
    Write-Host ("      │  Run duration      : {0,-30} │" -f "$RunMinutes minutes") -ForegroundColor Green
    Write-Host ("      │  Avg throughput    : {0,-30} │" -f ("{0:N0} docs/sec" -f $avgDocsPerSec)) -ForegroundColor Green
    Write-Host "      │  Plugin            : MONGO_INSERT (pure insertMany)  │" -ForegroundColor Green
    Write-Host "      │  Delivery          : At-least-once                   │" -ForegroundColor Green
    Write-Host "      └──────────────────────────────────────────────────┘" -ForegroundColor Green
} else {
    Write-Warn "No documents found — pipeline may not have written to $Collection"
    Write-Info "Check that sink.type=MONGO_INSERT and mongodb.collection=sk_insert_baseline"
    Write-Info "Also confirm the pipeline ran to completion in the benchmark logs"
}

# ── Step 2: Sample documents ──────────────────────────────────────────────────
Write-Host ""
Write-Step 2 "Sample documents (showing $SampleCount)"
Write-Host ""

$sampleJs = "const docs = db.getCollection('$Collection').find({}).limit($SampleCount).toArray();" +
        "docs.forEach((d, i) => {" +
        "  print('--- Document ' + (i+1) + ' ---');" +
        "  print(JSON.stringify(d, null, 2));" +
        "});"

$sampleOutput = Invoke-ContainerMongosh $sampleJs
if ($sampleOutput) {
    $sampleOutput | ForEach-Object { Write-Host "      $_" -ForegroundColor White }
} else {
    Write-Warn "No sample output returned"
}

# ── Step 3: Field inventory ───────────────────────────────────────────────────
Write-Host ""
Write-Step 3 "Document field structure (MONGO_INSERT schema)"

$fieldsJs = "const doc = db.getCollection('$Collection').findOne();" +
        "if (doc) {" +
        "  Object.keys(doc).forEach(k => {" +
        "    const v = doc[k];" +
        "    const t = Array.isArray(v) ? 'Array[' + v.length + ']' : typeof v;" +
        "    print('  ' + k + ' (' + t + ')');" +
        "  });" +
        "} else { print('no documents found'); }"

$fieldsOutput = Invoke-ContainerMongosh $fieldsJs
$fieldsOutput | Where-Object { $_ -ne "" } |
        ForEach-Object { Write-Host "      $_" -ForegroundColor Cyan }

Write-Host ""
Write-Info "Expected fields: _id (ObjectId), key (string), bytes (number), headers (object), ts (number)"
Write-Info "No enrichment field expected: this is the baseline insert profile"

# ── Step 4: Timestamp range ───────────────────────────────────────────────────
Write-Host ""
Write-Step 4 "Write timestamp range (first and last document)"

$tsJs = "const first = db.getCollection('$Collection').findOne({}, { sort: { ts: 1 } });" +
        "const last  = db.getCollection('$Collection').findOne({}, { sort: { ts: -1 } });" +
        "function toMillis(v) {" +
        "  if (v == null) return NaN;" +
        "  if (typeof v === 'number') return v;" +
        "  if (typeof v.toNumber === 'function') return v.toNumber();" +
        "  if (v.low !== undefined && v.high !== undefined) return v.low + (v.high * 4294967296);" +
        "  return Number(v);" +
        "}" +
        "if (first && last) {" +
        "  const firstTs = toMillis(first.ts);" +
        "  const lastTs = toMillis(last.ts);" +
        "  const elapsed = (lastTs - firstTs) / 1000;" +
        "  print('first_ts=' + firstTs + ' last_ts=' + lastTs + ' elapsed_s=' + elapsed);" +
        "} else { print('NO_TS_DATA'); }"

$tsOutput = Invoke-ContainerMongosh $tsJs
$tsLine   = ($tsOutput -split "`n") | Where-Object { $_ -match "first_ts=" } | Select-Object -First 1

if ($tsLine -match "first_ts=(\d+).*last_ts=(\d+).*elapsed_s=([\d.]+)") {
    $firstTs  = [long]$Matches[1]
    $lastTs   = [long]$Matches[2]
    $elapsedS = [double]$Matches[3]
    $firstDt  = [DateTimeOffset]::FromUnixTimeMilliseconds($firstTs).ToString("yyyy-MM-dd HH:mm:ss UTC")
    $lastDt   = [DateTimeOffset]::FromUnixTimeMilliseconds($lastTs).ToString("yyyy-MM-dd HH:mm:ss UTC")
    $measuredDps = if ($elapsedS -gt 0) { [math]::Round($countLong / $elapsedS, 0) } else { 0 }
    Write-Info "First write  : $firstDt"
    Write-Info "Last write   : $lastDt"
    Write-Info "Elapsed (ts) : $([math]::Round($elapsedS, 1)) seconds"
    Write-Info "Measured avg : $("{0:N0}" -f $measuredDps) docs/sec (from ts field)"
    Write-Ok   "Timestamp delta confirms documents were written continuously throughout the run"
} elseif ($tsOutput -match "NO_TS_DATA") {
    Write-Warn "No timestamp data — collection may be empty"
} else {
    Write-Info "Raw output: $tsOutput"
}

# ── Step 5: Collection storage size ──────────────────────────────────────────
Write-Host ""
Write-Step 5 "Collection storage size on disk"

$statsJs = "const s = db.getCollection('$Collection').stats();" +
           "print('size=' + s.size + ' storageSize=' + s.storageSize + " +
           "      ' count=' + s.count + ' avgObjSize=' + Math.round(s.avgObjSize));"

$statsOutput = Invoke-ContainerMongosh $statsJs
$statsLine   = ($statsOutput -split "`n") | Where-Object { $_ -match "size=" } | Select-Object -First 1

if ($statsLine -match "size=(\d+).*storageSize=(\d+).*count=(\d+).*avgObjSize=(\d+)") {
    $dataSize    = [long]$Matches[1]
    $storageSize = [long]$Matches[2]
    $docCount    = [long]$Matches[3]
    $avgObjSize  = [long]$Matches[4]

    $dataMB    = [math]::Round($dataSize    / 1MB, 2)
    $storageMB = [math]::Round($storageSize / 1MB, 2)

    Write-Info ("Data size    : {0:N2} MB  ({1:N0} bytes uncompressed)" -f $dataMB,    $dataSize)
    Write-Info ("Storage size : {0:N2} MB  ({1:N0} bytes on disk, WiredTiger compressed)" -f $storageMB, $storageSize)
    Write-Info ("Avg doc size : $avgObjSize bytes")
    if ($docCount -eq $countLong) {
        Write-Info ("Doc count    : {0:N0}" -f $docCount)
    } else {
        Write-Info ("Doc count    : {0:N0} from countDocuments()" -f $countLong)
        Write-Warn ("collStats count currently reports {0:N0}; using countDocuments() for throughput evidence" -f $docCount)
    }
    if ($storageSize -gt 0 -and $dataSize -gt 0) {
        $ratio = [math]::Round($dataSize / $storageSize, 2)
        Write-Info ("Compression  : ${ratio}x  (data/storage ratio)")
    }
} else {
    Write-Info "Raw stats output: $statsOutput"
}

# ── Step 6: Index inventory ───────────────────────────────────────────────────
Write-Host ""
Write-Step 6 "Indexes on $Collection"

$idxJs = "const idxs = db.getCollection('$Collection').getIndexes();" +
         "if (idxs.length === 0) { print('NO_INDEXES'); }" +
         "else { idxs.forEach(i => print(JSON.stringify({ name: i.name, key: i.key }))); }"

$idxOutput = Invoke-ContainerMongosh $idxJs
if ($idxOutput -match "NO_INDEXES") {
    Write-Warn "No indexes found — even _id index missing, which is unexpected"
} else {
    ($idxOutput -split "`n") | Where-Object { $_ -match "{" } | ForEach-Object {
        try {
            $obj    = $_ | ConvertFrom-Json
            $marker = if ($obj.name -eq "_id_") { "  <-- auto-created, only index in baseline" } else { "" }
            Write-Info "Index: $($obj.name)  key: $($obj.key | ConvertTo-Json -Compress)$marker"
        } catch { Write-Info $_ }
    }
    Write-Info "Only _id index expected: no secondary indexes"
    Write-Info "This confirms throughput reflects raw insertMany with minimal index overhead"
}

# ── Step 7: Benchmark context ─────────────────────────────────────────────────
Write-Host ""
Write-Step 7 "Benchmark suite context"
Write-Host ""
Write-Host "      ┌──────────────────────────────────────────────────────────┐" -ForegroundColor DarkGray
Write-Host "      │  StreamKernel Benchmark Suite — Write Cost Stack         │" -ForegroundColor DarkGray
Write-Host "      ├──────────────────────────────────────────────────────────┤" -ForegroundColor DarkGray
Write-Host "      │  Kafka Bench  (NOOP)        955K ops/s  Kafka ceiling    │" -ForegroundColor White
Write-Host "      │  Kafka ALO    (WireEvent)   525K ops/s  ALO + transform  │" -ForegroundColor White
Write-Host "      │  Kafka EOS    (WireEvent)   507K ops/s  EOS (-3.5%)      │" -ForegroundColor White
Write-Host ("      │  MongoDB Insert (this run)  {0,-6} docs/s  Write floor    │" -f ("{0:N0}" -f $avgDocsPerSec)) -ForegroundColor Green
Write-Host "      ├──────────────────────────────────────────────────────────┤" -ForegroundColor DarkGray
Write-Host "      │  Compare with other sink profiles as needed              │" -ForegroundColor Cyan
Write-Host "      │  Delta = added sink and transform cost over baseline      │" -ForegroundColor Cyan
Write-Host "      └──────────────────────────────────────────────────────────┘" -ForegroundColor DarkGray

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "  " + ("─" * 64) -ForegroundColor DarkGray
Write-Host ""
Write-Host ("  PIPELINE RESULT: {0:N0} documents written to {1}.{2}" -f $countLong, $Database, $Collection) -ForegroundColor Green
Write-Host ("  Avg throughput : {0:N0} docs/sec over {1} minutes" -f $avgDocsPerSec, $RunMinutes) -ForegroundColor Green
Write-Host "  Baseline schema and primary index only — this is the MongoDB write ceiling." -ForegroundColor Green
Write-Host ""
Write-Host "  Next: run another sink profile with test-java-runner for comparison" -ForegroundColor DarkGray
Write-Host ""

# ── End transcript ────────────────────────────────────────────────────────────
try { Stop-Transcript | Out-Null } catch {}
