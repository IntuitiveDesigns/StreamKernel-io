# ==============================================================================
# StreamKernel Demo - Post-Run: Pulsar Source + Kafka Sink
# demo_after_pulsar_source_kafka.ps1
#
# Run this AFTER test-java-runner.ps1 completes for:
#   config/pipelines/streamkernel_pulsar_source_kafka.properties
#
# This script:
# - verifies Pulsar subscription drain state
# - verifies the Kafka output topic has records
# - samples Kafka output records
# - captures Docker logs and a transcript in benchmark-runs/<RunFolder>
# ==============================================================================

param(
    [string]$Topic            = "persistent://public/default/streamkernel-bench-in",
    [string]$Subscription     = "streamkernel-pulsar-kafka",
    [string]$KafkaTopic       = "streamkernel-pulsar-out",
    [string]$PulsarContainer  = "pulsar",
    [string]$KafkaContainer   = "broker",
    [string]$RunFolder        = "streamkernel_pulsar_source_kafka_10m",
    [int]$SampleCount         = 5,
    [int]$SampleTimeoutMs     = 60000,
    [int]$DockerLogTail       = 400
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path

$_transcriptStamp  = Get-Date -Format "yyyyMMdd_HHmmss"
$_scriptLeaf       = [System.IO.Path]::GetFileNameWithoutExtension($MyInvocation.MyCommand.Name)
$_runsBase         = Join-Path $ProjectRoot "benchmark-runs"
$_transcriptFolder = Join-Path $_runsBase $RunFolder
if (!(Test-Path -LiteralPath $_transcriptFolder)) {
    New-Item -ItemType Directory -Force -Path $_transcriptFolder | Out-Null
}
$_transcriptPath = Join-Path $_transcriptFolder "$_scriptLeaf`_$_transcriptStamp.txt"
Start-Transcript -LiteralPath $_transcriptPath -Force | Out-Null

function Write-Banner($Text) {
    $line = "=" * 74
    Write-Host ""
    Write-Host $line     -ForegroundColor Green
    Write-Host "  $Text" -ForegroundColor Green
    Write-Host $line     -ForegroundColor Green
    Write-Host ""
}

function Write-Step($Number, $Text) { Write-Host "  [$Number] $Text" -ForegroundColor White }
function Write-Ok($Text)            { Write-Host "      OK  $Text" -ForegroundColor Green }
function Write-Info($Text)          { Write-Host "      >>  $Text" -ForegroundColor Cyan }
function Write-Warn($Text)          { Write-Host "      !!  $Text" -ForegroundColor Yellow }
function Write-Fail($Text)          { Write-Host "      XX  $Text" -ForegroundColor Red }

function Test-RunningContainer {
    param([string]$Name)

    $state = & docker inspect --format='{{.State.Running}}' $Name 2>&1
    return ($LASTEXITCODE -eq 0 -and "$state" -eq "true")
}

function Get-PulsarTopicStats {
    param(
        [string]$Container,
        [string]$TargetTopic
    )

    $raw = & docker exec $Container bin/pulsar-admin topics stats $TargetTopic 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw (($raw | ForEach-Object { "$_" }) -join [Environment]::NewLine)
    }
    return ($raw -join [Environment]::NewLine) | ConvertFrom-Json
}

function Get-PulsarSubscriptionStats {
    param(
        $TopicStats,
        [string]$SubscriptionName
    )

    if ($null -eq $TopicStats -or $null -eq $TopicStats.subscriptions) {
        return $null
    }

    $property = $TopicStats.subscriptions.PSObject.Properties[$SubscriptionName]
    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function Get-KafkaTopicTotal {
    param(
        [string]$Container,
        [string]$TargetTopic
    )

    $offsets = & docker exec $Container kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic $TargetTopic --time -1 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Could not read offsets for Kafka topic '$TargetTopic'. $($offsets -join ' ')"
    }

    $partitionCounts = @{}
    $offsets | Where-Object { $_ -match ":(\d+):(\d+)$" } | ForEach-Object {
        if ($_ -match ":(\d+):(\d+)$") {
            $partitionCounts[[int]$Matches[1]] = [long]$Matches[2]
        }
    }

    $total = ($partitionCounts.Values | Measure-Object -Sum).Sum
    return [pscustomobject]@{
        Total = [long]$total
        Partitions = $partitionCounts
    }
}

function Save-DockerContext {
    param([string]$Folder)

    & docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>&1 |
        Set-Content -LiteralPath (Join-Path $Folder "docker_ps_after.txt") -Encoding UTF8

    foreach ($name in @($KafkaContainer, $PulsarContainer)) {
        & docker logs --tail $DockerLogTail $name 2>&1 |
            Set-Content -LiteralPath (Join-Path $Folder "docker_${name}_after.log") -Encoding UTF8
    }
}

function Get-LatestRunArtifacts {
    param([string]$Folder)

    $log = Get-ChildItem -LiteralPath $Folder -File -Filter "*.log" -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -notlike "*_gc.log" -and $_.Name -notlike "*_control.log" -and $_.Name -notlike "docker_*" } |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
    $meta = Get-ChildItem -LiteralPath $Folder -File -Filter "*_meta.json" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
    $metrics = Get-ChildItem -LiteralPath $Folder -File -Filter "*_metrics.prom" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1

    return [pscustomobject]@{
        Log = $log
        Meta = $meta
        Metrics = $metrics
    }
}

function Read-KafkaSample {
    param(
        [string]$Container,
        [string]$TargetTopic,
        [int]$MaxMessages,
        [int]$TimeoutMs,
        [int]$WallClockTimeoutSeconds
    )

    $job = Start-Job -ScriptBlock {
        param([string]$container_, [string]$topic_, [int]$maxMessages_, [int]$timeoutMs_)
        & docker exec $container_ kafka-console-consumer `
            --bootstrap-server localhost:9092 `
            --topic $topic_ `
            --from-beginning `
            --max-messages $maxMessages_ `
            --timeout-ms $timeoutMs_ 2>&1
    } -ArgumentList $Container, $TargetTopic, $MaxMessages, $TimeoutMs

    $completed = Wait-Job -Id $job.Id -Timeout $WallClockTimeoutSeconds
    if ($null -eq $completed) {
        Stop-Job -Id $job.Id -ErrorAction SilentlyContinue
        Remove-Job -Id $job.Id -Force -ErrorAction SilentlyContinue
        throw "Kafka sample capture exceeded ${WallClockTimeoutSeconds}s."
    }

    try {
        return Receive-Job -Id $job.Id -ErrorAction SilentlyContinue
    } finally {
        Remove-Job -Id $job.Id -Force -ErrorAction SilentlyContinue
    }
}

try {
    Write-Banner "StreamKernel Demo  |  Post-Run Results  |  Pulsar Source + Kafka Sink"

    Write-Step 1 "Container health"
    if (-not (Test-RunningContainer -Name $KafkaContainer)) {
        throw "Container '$KafkaContainer' is not running."
    }
    if (-not (Test-RunningContainer -Name $PulsarContainer)) {
        throw "Container '$PulsarContainer' is not running."
    }
    Write-Ok "Containers are running: $KafkaContainer, $PulsarContainer"

    Write-Host ""
    Write-Step 2 "Pulsar subscription state"
    $stats = Get-PulsarTopicStats -Container $PulsarContainer -TargetTopic $Topic
    $subStats = Get-PulsarSubscriptionStats -TopicStats $stats -SubscriptionName $Subscription
    if ($stats.msgInCounter -ne $null) {
        Write-Info ("Publish counter : {0:N0}" -f [double]$stats.msgInCounter)
    }
    if ($stats.msgOutCounter -ne $null) {
        Write-Info ("Deliver counter : {0:N0}" -f [double]$stats.msgOutCounter)
    }
    if ($subStats) {
        $backlog = [double]$subStats.msgBacklog
        $unacked = [double]$subStats.unackedMessages
        Write-Info ("Subscription    : {0}" -f $Subscription)
        Write-Info ("Message backlog : {0:N0}" -f $backlog)
        Write-Info ("Unacked         : {0:N0}" -f $unacked)

        if ($backlog -eq 0 -and $unacked -eq 0) {
            Write-Ok "Pulsar subscription drained cleanly"
        } else {
            Write-Warn "Pulsar still has backlog or unacked messages"
        }
    } else {
        Write-Warn "Subscription '$Subscription' was not present in Pulsar stats"
    }

    Write-Host ""
    Write-Step 3 "Kafka output topic '$KafkaTopic'"
    $topicList = & docker exec $KafkaContainer kafka-topics --bootstrap-server localhost:9092 --list 2>&1
    $topicExists = ($topicList -split "`n") | Where-Object { $_.Trim() -eq $KafkaTopic }
    if (-not $topicExists) {
        throw "Kafka output topic '$KafkaTopic' was not found."
    }
    Write-Ok "Kafka output topic exists"

    $topicTotals = Get-KafkaTopicTotal -Container $KafkaContainer -TargetTopic $KafkaTopic
    Write-Host ""
    Write-Host "      Partition record counts" -ForegroundColor Cyan
    foreach ($p in ($topicTotals.Partitions.Keys | Sort-Object)) {
        Write-Host ("      partition {0,-3} {1,12:N0}" -f $p, $topicTotals.Partitions[$p]) -ForegroundColor White
    }
    Write-Info ("Total records   : {0:N0}" -f $topicTotals.Total)
    if ($topicTotals.Total -gt 0) {
        Write-Ok "Kafka sink wrote records"
    } else {
        throw "Kafka output topic exists but has zero records."
    }

    Write-Host ""
    Write-Step 4 "Sample Kafka records"
    $sampleRaw = Read-KafkaSample `
        -Container $KafkaContainer `
        -TargetTopic $KafkaTopic `
        -MaxMessages $SampleCount `
        -TimeoutMs $SampleTimeoutMs `
        -WallClockTimeoutSeconds ([Math]::Max(15, [int][Math]::Ceiling($SampleTimeoutMs / 1000.0) + 15))

    $sample = $sampleRaw |
        Where-Object {
            $_ -ne "" `
                -and $_ -notmatch "^Processed" `
                -and $_ -notmatch "^\[" `
                -and $_ -notmatch "^(org|java|javax|kafka)\." `
                -and $_ -notmatch "^\s+at " `
                -and $_ -notmatch "^Caused by:"
        }

    if ($sample) {
        $samplePath = Join-Path $_transcriptFolder "kafka_${KafkaTopic}_sample_after.txt"
        $sample | Set-Content -LiteralPath $samplePath -Encoding UTF8
        $i = 1
        foreach ($msg in ($sample | Select-Object -First $SampleCount)) {
            Write-Host "      -- Record $i ----------------------------------------" -ForegroundColor DarkGray
            try {
                $parsed = $msg | ConvertFrom-Json -ErrorAction Stop
                ($parsed | ConvertTo-Json -Depth 8) -split "`n" | ForEach-Object {
                    Write-Host "      $_" -ForegroundColor White
                }
            } catch {
                Write-Host "      $msg" -ForegroundColor White
            }
            $i++
        }
        Write-Info "Sample saved    : $samplePath"
    } else {
        Write-Warn "Could not retrieve Kafka samples even though offsets are non-zero"
    }

    Write-Host ""
    Write-Step 5 "Runner artifacts and Docker logs"
    Save-DockerContext -Folder $_transcriptFolder
    $artifacts = Get-LatestRunArtifacts -Folder $_transcriptFolder
    if ($artifacts.Log)     { Write-Info ("Pipeline log   : {0}" -f $artifacts.Log.FullName) }
    if ($artifacts.Meta)    { Write-Info ("Run metadata   : {0}" -f $artifacts.Meta.FullName) }
    if ($artifacts.Metrics) { Write-Info ("Metrics snapshot: {0}" -f $artifacts.Metrics.FullName) }
    Write-Info ("Docker logs    : {0}" -f $_transcriptFolder)
    Write-Info ("Transcript     : {0}" -f $_transcriptPath)

    Write-Host ""
    Write-Host ("  " + ("-" * 72)) -ForegroundColor DarkGray
    Write-Host ""
    Write-Host ("  PIPELINE RESULT: {0:N0} records written to Kafka topic '{1}'" -f $topicTotals.Total, $KafkaTopic) -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Fail $_.Exception.Message
    try { Save-DockerContext -Folder $_transcriptFolder } catch {}
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}

try { Stop-Transcript | Out-Null } catch {}
exit 0
