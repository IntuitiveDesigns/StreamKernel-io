# ==============================================================================
# StreamKernel Demo - Pre-Run: Pulsar Source + Kafka Sink
# demo_before_pulsar_source_kafka.ps1
#
# Run this BEFORE test-java-runner.ps1 for:
#   config/pipelines/streamkernel_pulsar_source_kafka.properties
#
# This script:
# - starts the Docker Compose services required by the profile
# - waits for Kafka and Pulsar readiness
# - resets and seeds the Pulsar input topic
# - creates the benchmark subscription at earliest
# - captures a transcript in benchmark-runs/<RunFolder>
# ==============================================================================

param(
    [string]$Topic            = "persistent://public/default/streamkernel-bench-in",
    [string]$Subscription     = "streamkernel-pulsar-kafka",
    [string]$KafkaTopic       = "streamkernel-pulsar-out",
    [string]$PulsarContainer  = "pulsar",
    [string]$KafkaContainer   = "broker",
    [int]$MessageCount        = 250000,
    [int]$ProduceChunkSize    = 1000,
    [int]$SeedRate            = 50000,
    [int]$SeedProducerCount   = 4,
    [int]$SeedProducerThreads = 2,
    [string]$MessageText      = "Customer reports repeated login failures after MFA reset. Preserve session trace and keep sentiment neutral for triage.",
    [string]$PayloadPath      = "/tmp/streamkernel-pulsar-source-kafka-payload.txt",
    [string]$RunFolder        = "streamkernel_pulsar_source_kafka_10m",
    [switch]$ResetPulsarVolumeOnLedgerError,
    [switch]$SkipComposeUp,
    [Parameter(ValueFromRemainingArguments = $true)]
    [object[]]$RemainingArgs
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
    Write-Host $line     -ForegroundColor Cyan
    Write-Host "  $Text" -ForegroundColor Cyan
    Write-Host $line     -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step($Number, $Text) { Write-Host "  [$Number] $Text" -ForegroundColor White }
function Write-Ok($Text)            { Write-Host "      OK  $Text" -ForegroundColor Green }
function Write-Info($Text)          { Write-Host "      >>  $Text" -ForegroundColor Cyan }
function Write-Warn($Text)          { Write-Host "      !!  $Text" -ForegroundColor Yellow }
function Write-Fail($Text)          { Write-Host "      XX  $Text" -ForegroundColor Red }

function Invoke-ProjectCompose {
    param([Parameter(Mandatory = $true)][string[]]$ComposeArgs)

    Push-Location -LiteralPath $ProjectRoot
    try {
        $output = & docker compose @ComposeArgs 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw (($output | ForEach-Object { "$_" }) -join [Environment]::NewLine)
        }
        return $output
    } finally {
        Pop-Location
    }
}

function Test-KafkaSecretsPresent {
    $required = @(
        "kafka.server.keystore.p12",
        "kafka.truststore.p12",
        "keystore_creds",
        "key_creds",
        "truststore_creds"
    )

    $missing = @()
    foreach ($leaf in $required) {
        $candidate = Join-Path (Join-Path $ProjectRoot "secrets") $leaf
        if (!(Test-Path -LiteralPath $candidate)) {
            $missing += $leaf
        }
    }

    return $missing
}

function Test-RunningContainer {
    param([string]$Name)

    $state = & docker inspect --format='{{.State.Running}}' $Name 2>&1
    return ($LASTEXITCODE -eq 0 -and "$state" -eq "true")
}

function ConvertTo-ShellSingleQuoted {
    param([string]$Value)

    if ($null -eq $Value) {
        return "''"
    }

    return "'" + $Value.Replace("'", "'""'""'") + "'"
}

function Invoke-ContainerShell {
    param(
        [string]$Container,
        [string]$Script
    )

    $output = & docker exec $Container sh -lc $Script 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw (($output | ForEach-Object { "$_" }) -join [Environment]::NewLine)
    }
    return $output
}

function Reset-PulsarVolume {
    param([string]$Container)

    Write-Warn "BookKeeper ledger recovery failed. Resetting Pulsar standalone data volume."
    Invoke-ProjectCompose -ComposeArgs @("--profile", "pulsar", "stop", $Container) | Out-Null
    Invoke-ProjectCompose -ComposeArgs @("--profile", "pulsar", "rm", "-f", $Container) | Out-Null
    Start-Sleep -Seconds 3

    $volumeNames = & docker volume ls --format "{{.Name}}" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Could not list Docker volumes while resetting Pulsar."
    }

    $projectLeaf = Split-Path -Leaf $ProjectRoot
    $volumeName = $volumeNames |
        Where-Object {
            $_ -match "pulsar-data-volume$" -and
            ($_ -match "streamkernel" -or $_ -match [regex]::Escape($projectLeaf))
        } |
        Select-Object -First 1

    if ($volumeName) {
        $removeOutput = & docker volume rm $volumeName 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "Could not remove Pulsar volume '$volumeName'. $($removeOutput -join ' ')"
        }
        Write-Ok "Removed stale Pulsar volume '$volumeName'"
    } else {
        Write-Warn "No Pulsar data volume was found; restarting container only"
    }

    Invoke-ProjectCompose -ComposeArgs @("--profile", "pulsar", "up", "-d", $Container) | Out-Null
    Wait-PulsarReady -Container $Container -TimeoutSeconds 180
    Write-Ok "Pulsar restarted with clean local storage"
}

function Wait-KafkaReady {
    param(
        [string]$Container,
        [int]$TimeoutSeconds = 180
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $output = & docker exec $Container kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)

    throw "Kafka broker '$Container' did not become ready within ${TimeoutSeconds}s."
}

function Wait-PulsarReady {
    param(
        [string]$Container,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $output = & docker exec $Container bin/pulsar-admin topics list public/default 2>&1
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)

    throw "Pulsar broker '$Container' did not become ready within ${TimeoutSeconds}s."
}

function Reset-PulsarTopic {
    param(
        [string]$Container,
        [string]$TargetTopic,
        [string]$TargetSubscription
    )

    & docker exec $Container bin/pulsar-admin topics unsubscribe $TargetTopic --subscription $TargetSubscription --force 2>$null | Out-Null
    & docker exec $Container bin/pulsar-admin topics delete $TargetTopic --force 2>$null | Out-Null
    Start-Sleep -Seconds 3

    $maxAttempts = 8
    $volumeResetDone = $false
    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        $createOut = & docker exec $Container bin/pulsar-admin topics create $TargetTopic 2>&1
        $createText = ($createOut | ForEach-Object { "$_" }) -join " "
        if ($LASTEXITCODE -eq 0 -or $createText -match "already exists") {
            return
        }
        if ($createText -match "error code: -10|ManagedLedgerException|recovering ledger") {
            if ($ResetPulsarVolumeOnLedgerError -and -not $volumeResetDone) {
                Reset-PulsarVolume -Container $Container
                $volumeResetDone = $true
                continue
            }
            throw "Pulsar ledger recovery failed for '$TargetTopic'. Re-run with -ResetPulsarVolumeOnLedgerError to clear the local Pulsar data volume. $createText"
        }
        if ($attempt -lt $maxAttempts -and $createText -match "not ready|ServiceUnitNotReady|recovering ledger|ManagedLedgerException") {
            Write-Info ("Pulsar topic create not ready yet, waiting 10s... (attempt {0}/{1})" -f $attempt, $maxAttempts)
            Start-Sleep -Seconds 10
            continue
        }
        throw "Could not create Pulsar topic '$TargetTopic'. $createText"
    }
}

function Publish-PulsarBacklog {
    param(
        [string]$Container,
        [string]$TargetTopic,
        [string]$Payload,
        [int]$TotalMessages,
        [int]$ChunkSize,
        [int]$Rate,
        [int]$ProducerCount,
        [int]$ProducerThreads,
        [string]$TargetPayloadPath
    )

    if ($TotalMessages -le 0) {
        return
    }
    if ($Rate -le 0) {
        throw "SeedRate must be greater than zero."
    }
    if ($ProducerCount -le 0) {
        throw "SeedProducerCount must be greater than zero."
    }
    if ($ProducerThreads -le 0) {
        throw "SeedProducerThreads must be greater than zero."
    }

    $payloadPathQuoted = ConvertTo-ShellSingleQuoted -Value $TargetPayloadPath
    $payloadQuoted = ConvertTo-ShellSingleQuoted -Value $Payload
    Invoke-ContainerShell -Container $Container -Script ('printf %s\\n {0} > {1}' -f $payloadQuoted, $payloadPathQuoted) | Out-Null

    $produceArgs = @(
        "exec", $Container,
        "bin/pulsar-perf", "produce",
        "-u", "pulsar://localhost:6650",
        "-m", $TotalMessages,
        "-r", $Rate,
        "-n", $ProducerCount,
        "-threads", $ProducerThreads,
        "-i", "5",
        "-f", $TargetPayloadPath,
        $TargetTopic
    )

    Write-Info ("Seed target    : {0:N0} messages" -f $TotalMessages)
    Write-Info ("Seed rate cap  : {0:N0} msg/s" -f $Rate)
    Write-Info ("Producers      : {0} producer(s), {1} thread(s)" -f $ProducerCount, $ProducerThreads)

    $produceOutput = & docker @produceArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        $text = ($produceOutput | ForEach-Object { "$_" }) -join [Environment]::NewLine

        if ($ChunkSize -gt 0) {
            Write-Warn "pulsar-perf seeding failed; falling back to slower pulsar-client chunked producer"
            $sent = 0
            $chunkIndex = 0
            while ($sent -lt $TotalMessages) {
                $chunkIndex++
                $remaining = $TotalMessages - $sent
                $thisChunk = [Math]::Min($ChunkSize, $remaining)
                $clientArgs = @("exec", $Container, "bin/pulsar-client", "produce", $TargetTopic, "-m", $Payload, "-n", $thisChunk)
                $clientOutput = & docker @clientArgs 2>&1
                if ($LASTEXITCODE -ne 0) {
                    throw (($clientOutput | ForEach-Object { "$_" }) -join [Environment]::NewLine)
                }

                $sent += $thisChunk
                if (($chunkIndex -eq 1) -or ($sent -eq $TotalMessages) -or (($chunkIndex % 10) -eq 0)) {
                    Write-Info ("Seed progress   : {0:N0}/{1:N0}" -f $sent, $TotalMessages)
                }
            }
            return
        }

        throw $text
    }

    $produceOutput | Select-Object -Last 12 | ForEach-Object {
        if ($_ -and "$_".Trim().Length -gt 0) {
            Write-Host "      $_" -ForegroundColor DarkGray
        }
    }
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

function Save-DockerContext {
    param([string]$Folder)

    & docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>&1 |
        Set-Content -LiteralPath (Join-Path $Folder "docker_ps_before.txt") -Encoding UTF8

    foreach ($name in @($KafkaContainer, $PulsarContainer)) {
        & docker logs --tail 200 $name 2>&1 |
            Set-Content -LiteralPath (Join-Path $Folder "docker_${name}_before.log") -Encoding UTF8
    }
}

try {
    Write-Banner "StreamKernel Demo  |  Pre-Run State  |  Pulsar Source + Kafka Sink"

    Write-Step 1 "Docker Compose services"
    $missingSecrets = @(Test-KafkaSecretsPresent)
    if ($missingSecrets.Count -gt 0) {
        $missingText = $missingSecrets -join ", "
        throw "Kafka compose secrets are missing: $missingText. Generate them with: bash scripts/gen-certs.sh"
    }

    if (-not $SkipComposeUp) {
        Invoke-ProjectCompose -ComposeArgs @("--profile", "pulsar", "up", "-d", $KafkaContainer, $PulsarContainer) | Out-Null
        Write-Ok "Requested services are up: $KafkaContainer, $PulsarContainer"
    } else {
        Write-Info "Skipping docker compose up because -SkipComposeUp was provided"
    }

    Write-Host ""
    Write-Step 2 "Kafka broker readiness"
    if (-not (Test-RunningContainer -Name $KafkaContainer)) {
        throw "Container '$KafkaContainer' is not running."
    }
    Wait-KafkaReady -Container $KafkaContainer
    Write-Ok "Kafka broker is responding at localhost:9092"

    Write-Host ""
    Write-Step 3 "Pulsar broker readiness"
    if (-not (Test-RunningContainer -Name $PulsarContainer)) {
        throw "Container '$PulsarContainer' is not running."
    }
    Wait-PulsarReady -Container $PulsarContainer
    Write-Ok "Pulsar broker is ready"

    Write-Host ""
    Write-Step 4 "Resetting Pulsar topic '$Topic'"
    & docker exec $PulsarContainer sh -c "pkill -f 'pulsar-client.*produce' 2>/dev/null || true; pkill -f 'pulsar-perf.*produce' 2>/dev/null || true" 2>$null | Out-Null
    Reset-PulsarTopic -Container $PulsarContainer -TargetTopic $Topic -TargetSubscription $Subscription
    Write-Ok "Input topic reset complete"

    Write-Host ""
    Write-Step 5 "Seeding Pulsar input backlog"
    Publish-PulsarBacklog `
        -Container $PulsarContainer `
        -TargetTopic $Topic `
        -Payload $MessageText `
        -TotalMessages $MessageCount `
        -ChunkSize $ProduceChunkSize `
        -Rate $SeedRate `
        -ProducerCount $SeedProducerCount `
        -ProducerThreads $SeedProducerThreads `
        -TargetPayloadPath $PayloadPath
    Write-Ok ("Produced {0:N0} messages into {1}" -f $MessageCount, $Topic)

    Write-Host ""
    Write-Step 6 "Creating benchmark subscription '$Subscription'"
    $subOutput = & docker exec $PulsarContainer bin/pulsar-admin topics create-subscription $Topic -s $Subscription -m earliest 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create Pulsar subscription '$Subscription'. $($subOutput -join ' ')"
    }
    Write-Ok "Subscription created at earliest position"

    Write-Host ""
    Write-Step 7 "Pulsar topic state"
    $stats = Get-PulsarTopicStats -Container $PulsarContainer -TargetTopic $Topic
    $subStats = Get-PulsarSubscriptionStats -TopicStats $stats -SubscriptionName $Subscription
    if ($stats.msgInCounter -ne $null) {
        Write-Info ("Publish counter : {0:N0}" -f [double]$stats.msgInCounter)
    }
    if ($subStats -and $subStats.msgBacklog -ne $null) {
        Write-Info ("Subscription    : {0}" -f $Subscription)
        Write-Info ("Message backlog : {0:N0}" -f [double]$subStats.msgBacklog)
        Write-Info ("Unacked         : {0:N0}" -f [double]$subStats.unackedMessages)
    }

    Write-Host ""
    Write-Step 8 "Kafka output topic handoff"
    Write-Info "Runner matrix will reset '$KafkaTopic' before starting the JVM."
    Write-Info "Run next: .\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_pulsar.csv"
    Write-Info "Then run: .\scripts\demo_after_pulsar_source_kafka.ps1"

    Save-DockerContext -Folder $_transcriptFolder

    Write-Host ""
    Write-Host ("  " + ("-" * 72)) -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "  PRE-RUN STATE CONFIRMED" -ForegroundColor Green
    Write-Host "  Pulsar input backlog is ready for the Pulsar source to Kafka sink proof." -ForegroundColor Green
    Write-Host ""
    Write-Info "Transcript      : $_transcriptPath"
    Write-Info "Artifact folder : $_transcriptFolder"
    Write-Host ""
} catch {
    Write-Fail $_.Exception.Message
    try { Save-DockerContext -Folder $_transcriptFolder } catch {}
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}

try { Stop-Transcript | Out-Null } catch {}
exit 0
