# ==============================================================================
# StreamKernel Benchmark Runner (v2026.03.05.Final-2)
# ==============================================================================
#
# USAGE:
#   .\test-java-runner.ps1
#   .\test-java-runner.ps1 -SingleTest streamkernel_source_baseline_uncorked
#   .\test-java-runner.ps1 -MatrixFile benchmark-runs\tests.csv
#
# CSV COLUMNS (tests.csv) - all required unless marked optional:
#
#   LogName              Short identifier. Used as output folder name and log prefix.
#   ConfigPath           Path relative to $root (e.g. config\pipelines\foo.properties)
#   Minutes              How long to run this test.
#   KafkaPartitions      Partitions to create on the output topic before the run.
#   RunId                Injected as -Dsk.run.id and metrics.tag.run_id.
#   HeapGb               JVM heap size in GB. -Xms and -Xmx set to same value.
#   GcThreads            -XX:ParallelGCThreads. Match to available cores minus workers.
#   LatencySampleMask    Bitmask for latency sampling when latency is enabled.
#                        0=every record, 127=1/128, 1023=1/1024.
#                        Use 0 for NOOP/high-EPS and latency-disabled perf rows.
#   InflightMax          streamkernel.sink.inflight.max. 0 = no ceiling (omit flag).
#                        Set to parallelism x batchSize x 3 for transform-heavy profiles.
#   OutbatchCapacity     streamkernel.outbatch.capacity. Set equal to pipeline.batch.size.
#   ExecutorMode         FIXED (CPU-bound/local work) or VIRTUAL (I/O-bound HTTP).
#   CacheDisabled        true = disable Caffeine. false = enable it.
#   SinkBatchCopy        true = defensive ArrayList copy before sink. false = skip.
#                        false is safe for all current sink plugins.
#   DuringRunScript      Optional PowerShell script invoked while the benchmark is
#                        still running. Output is captured to a per-run control log.
#   DuringRunDelaySeconds Optional delay before DuringRunScript runs.
#
# SYSTEM PROPERTY NOTE:
#   StreamKernel.main() now bridges many profile values into JVM system properties,
#   but this runner still passes benchmark-critical settings as explicit -D flags so
#   every run is deterministic and the effective values are captured in metadata.
#   These are the main benchmark-controlled keys:
#
#     streamkernel.latency.sample.mask     streamkernel.latency.enabled
#     streamkernel.latency.buffer.size     streamkernel.metrics.latency.enabled
#     streamkernel.metrics.latency.max.seconds
#     streamkernel.sink.batch.copy         streamkernel.executor.mode
#     streamkernel.cache.force.disabled    streamkernel.sink.inflight.max
#     streamkernel.outbatch.capacity
#
#   In benchmark mode, explicit runner flags intentionally win over profile values.
#
# PIPELINE QUICK REFERENCE:
#   NOOP / high-EPS     LatencySampleMask=0  InflightMax=0  HeapGb=4-8
#   HTTP transformer    ExecutorMode=VIRTUAL  LatencySampleMask=31
#   MongoDB Vector perf LatencySampleMask=0   InflightMax=p*b*4
# ==============================================================================

param(
    [string]$Root = $PSScriptRoot,
    [string]$MatrixFile = "",
    [string]$BaseOutputDir = "",
    [string]$SingleTest = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ------------------------------------------------------------------------------
# GLOBAL CONFIGURATION
# ------------------------------------------------------------------------------
$root           = (Resolve-Path -LiteralPath $Root).Path
$matrixFile     = if ($MatrixFile -ne "") {
    if ([System.IO.Path]::IsPathRooted($MatrixFile)) { $MatrixFile } else { Join-Path $root $MatrixFile }
} else {
    Join-Path $root "benchmark-runs\tests.csv"
}
$kafkaContainer = "broker"
$kafkaTopic     = "arena-bench-test"
$baseOutputDir  = if ($BaseOutputDir -ne "") {
    if ([System.IO.Path]::IsPathRooted($BaseOutputDir)) { $BaseOutputDir } else { Join-Path $root $BaseOutputDir }
} else {
    Join-Path $root "benchmark-runs"
}
$libsDir        = Join-Path $root "streamkernel-app\build\libs"
$cpuJarPath     = ""

function Resolve-LatestShadowJar {
    param(
        [Parameter(Mandatory = $true)][string]$LibsDirectory
    )

    if (!(Test-Path -LiteralPath $LibsDirectory)) {
        return ""
    }

    $candidates = Get-ChildItem -LiteralPath $LibsDirectory -File -Filter "streamkernel-app-*-all.jar" -ErrorAction SilentlyContinue

    $selected = $candidates | Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
    if ($null -eq $selected) {
        return ""
    }

    return $selected.FullName
}

function Test-StreamKernelLogHasPipelineStop {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (!(Test-Path -LiteralPath $Path)) {
        return $false
    }

    $tail = Get-Content -LiteralPath $Path -Tail 120 -ErrorAction SilentlyContinue
    return (($tail | Where-Object { $_ -match "Pipeline Stopped\." }) | Measure-Object).Count -gt 0
}

$cpuJarPath = Resolve-LatestShadowJar -LibsDirectory $libsDir

# ------------------------------------------------------------------------------
# FIXED JVM OPTIONS - same for every test unless the matrix explicitly requests
# a different GC mode. HeapGb and GcThreads vary per test.
# ------------------------------------------------------------------------------
$jvmCommonOpts = @(
    "-XX:+AlwaysPreTouch"
    "-Dfile.encoding=UTF-8"
)

# Baseline system properties. These preserve profile intent rather than
# force a benchmark shape that may not match the selected pipeline.
$skFixedProps = @()

function Read-JavaPropertiesFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    $map = @{}
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($null -eq $line) { continue }
        $trimmed = $line.Trim()
        if ($trimmed.Length -eq 0) { continue }
        if ($trimmed.StartsWith("#") -or $trimmed.StartsWith("!")) { continue }

        $idxEq = $trimmed.IndexOf("=")
        $idxColon = $trimmed.IndexOf(":")
        $idx = if ($idxEq -lt 0) { $idxColon } elseif ($idxColon -lt 0) { $idxEq } else { [Math]::Min($idxEq, $idxColon) }
        if ($idx -lt 0) { continue }

        $key = $trimmed.Substring(0, $idx).Trim()
        if ($key.Length -eq 0) { continue }

        $value = $trimmed.Substring($idx + 1).Trim()
        $map[$key] = $value
    }

    return $map
}

function Get-CsvValueOrDefault {
    param(
        [Parameter(Mandatory = $true)]$Row,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)]$Default
    )

    if ($Row.PSObject.Properties[$Name] -and $null -ne $Row.$Name -and $Row.$Name -ne "") {
        return $Row.$Name
    }

    return $Default
}

function Resolve-KafkaTopicForTest {
    param(
        [Parameter(Mandatory = $true)]$Row,
        [Parameter(Mandatory = $true)][string]$DefaultTopic
    )

    $rawTopic = [string](Get-CsvValueOrDefault -Row $Row -Name "KafkaTopic" -Default $DefaultTopic)
    $trimmed = if ($null -eq $rawTopic) { "" } else { $rawTopic.Trim() }
    if ($trimmed -eq "") {
        return $DefaultTopic
    }

    switch ($trimmed.ToLowerInvariant()) {
        "n/a"  { return $null }
        "na"   { return $null }
        "none" { return $null }
        "skip" { return $null }
        "-"    { return $null }
        default { return $trimmed }
    }
}

function Resolve-OptionalScriptPath {
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [AllowNull()][AllowEmptyString()][string]$Candidate = ""
    )

    if ([string]::IsNullOrWhiteSpace($Candidate)) {
        return $null
    }

    if ([System.IO.Path]::IsPathRooted($Candidate)) {
        return $Candidate
    }

    return (Join-Path $RootPath $Candidate)
}

function Parse-BootstrapServers {
    param([string]$BootstrapServers)

    $endpoints = [System.Collections.Generic.List[object]]::new()
    foreach ($raw in ($BootstrapServers -split ",")) {
        $endpoint = $raw.Trim()
        if ([string]::IsNullOrWhiteSpace($endpoint)) {
            continue
        }

        $parts = $endpoint.Split(":", 2)
        if ($parts.Count -ne 2) {
            continue
        }

        $port = 0
        if (-not [int]::TryParse($parts[1], [ref]$port)) {
            continue
        }

        $endpoints.Add([pscustomobject]@{
            Host = $parts[0]
            Port = $port
        })
    }

    return $endpoints
}

function Test-TcpEndpoint {
    param(
        [Parameter(Mandatory = $true)][string]$TargetHost,
        [Parameter(Mandatory = $true)][int]$Port,
        [int]$TimeoutMs = 3000
    )

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $connect = $client.BeginConnect($TargetHost, $Port, $null, $null)
        if (-not $connect.AsyncWaitHandle.WaitOne($TimeoutMs, $false)) {
            return $false
        }

        $client.EndConnect($connect)
        return $true
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

function Assert-BootstrapReachable {
    param(
        [Parameter(Mandatory = $true)][string]$BootstrapServers,
        [Parameter(Mandatory = $true)][string]$Context
    )

    foreach ($endpoint in (Parse-BootstrapServers -BootstrapServers $BootstrapServers)) {
        if (Test-TcpEndpoint -TargetHost $endpoint.Host -Port $endpoint.Port) {
            return
        }
    }

    throw "$Context bootstrap servers are not reachable from this host: '$BootstrapServers'. Start or repair the broker listener before running this benchmark."
}

function Resolve-KafkaContainerBootstrap {
    param(
        [Parameter(Mandatory = $true)][string]$Container,
        [int]$TimeoutSeconds = 45,
        [int]$PollSeconds = 3
    )

    $candidates = @("host.docker.internal:9092", "broker:29092", "localhost:9092", "localhost:29092") | Select-Object -Unique
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastFailures = @{}

    do {
        foreach ($candidate in $candidates) {
            $probeOutput = & docker exec $Container kafka-broker-api-versions --bootstrap-server $candidate 2>&1
            if ($LASTEXITCODE -eq 0) {
                return $candidate
            }

            $snippet = (($probeOutput | Select-Object -First 4) -join " ").Trim()
            if ([string]::IsNullOrWhiteSpace($snippet)) {
                $snippet = "no output"
            }
            $lastFailures[$candidate] = $snippet
        }

        Start-Sleep -Seconds $PollSeconds
    } while ((Get-Date) -lt $deadline)

    $details = ($candidates | ForEach-Object {
        $message = [string]$lastFailures[$_]
        if ([string]::IsNullOrWhiteSpace($message)) {
            $message = "no output"
        }
        "  $_ => $message"
    }) -join "`n"

    throw "Kafka broker in container '$Container' did not become ready for topic admin within ${TimeoutSeconds}s.`n$details"
}

function Reset-KafkaTopicInContainer {
    param(
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Topic,
        [Parameter(Mandatory = $true)][int]$Partitions
    )

    $containerBootstrap = Resolve-KafkaContainerBootstrap -Container $Container

    & docker exec $Container kafka-topics --bootstrap-server $containerBootstrap --delete --topic $Topic 2>$null | Out-Null
    Start-Sleep -Seconds 3

    $createOutput = & docker exec $Container kafka-topics --bootstrap-server $containerBootstrap --create --topic $Topic --partitions $Partitions --replication-factor 1 2>&1
    $createExitCode = $LASTEXITCODE
    if ($createExitCode -ne 0) {
        throw "Kafka topic create failed for '$Topic'.`n$($createOutput -join "`n")"
    }

    $describeOutput = & docker exec $Container kafka-topics --bootstrap-server $containerBootstrap --describe --topic $Topic 2>&1
    $describeExitCode = $LASTEXITCODE
    $describeText = $describeOutput -join "`n"
    if ($describeExitCode -ne 0 -or $describeText -notmatch ("Topic:\s*" + [regex]::Escape($Topic))) {
        throw "Kafka topic verification failed for '$Topic'.`n$describeText"
    }
}

function Parse-JobExitCode {
    param(
        [Parameter(Mandatory = $true)]$JobOutput,
        [Parameter(Mandatory = $true)][string]$MarkerPrefix
    )

    $line = $JobOutput |
        Where-Object { $_ -match ("^" + [regex]::Escape($MarkerPrefix) + "(-?\d+)$") } |
        Select-Object -Last 1

    if ($line -and $line -match ("^" + [regex]::Escape($MarkerPrefix) + "(-?\d+)$")) {
        return [int]$Matches[1]
    }

    return $null
}

function Get-JarBuildHint {
    return ".\gradlew.bat --no-daemon :streamkernel-app:shadowJar"
}

function Get-GcJvmOpts {
    param([Parameter(Mandatory = $true)][string]$GcMode)

    switch ($GcMode.Trim().ToUpper()) {
        "G1" {
            return @(
                "-XX:+UseG1GC"
                "-XX:MaxGCPauseMillis=50"
                "-XX:G1HeapRegionSize=16m"
            )
        }
        "ZGC" {
            return @(
                "-XX:+UseZGC"
                "-XX:SoftMaxHeapSize=0"
            )
        }
        default {
            throw "Unsupported GcMode '$GcMode'. Expected G1 or ZGC."
        }
    }
}

function Convert-ArgListToPropertyMap {
    param([Parameter(Mandatory = $true)][string[]]$Args)

    $props = [ordered]@{}
    foreach ($arg in $Args) {
        if ($null -eq $arg) { continue }
        if (-not $arg.StartsWith("-D")) { continue }

        $body = $arg.Substring(2)
        $idx = $body.IndexOf("=")
        if ($idx -lt 0) {
            $props[$body] = ""
            continue
        }

        $props[$body.Substring(0, $idx)] = $body.Substring($idx + 1)
    }

    return $props
}

function Get-EffectiveSetting {
    param(
        [string]$Key,
        [hashtable]$ProfileProps,
        [hashtable]$SystemProps,
        $Default = $null
    )

    if ($SystemProps.Contains($Key)) { return $SystemProps[$Key] }
    if ($ProfileProps.Contains($Key)) { return $ProfileProps[$Key] }
    return $Default
}

function Validate-TestMatrixRow {
    param(
        [Parameter(Mandatory = $true)]$Test,
        [Parameter(Mandatory = $true)][hashtable]$ProfileProps
    )

    $requiredColumns = @("LogName", "ConfigPath", "Minutes", "RunId", "HeapGb", "GcThreads", "GcMode")
    foreach ($col in $requiredColumns) {
        if (-not $Test.PSObject.Properties[$col] -or $null -eq $Test.$col -or $Test.$col -eq "") {
            throw "tests.csv row is missing required column '$col' for LogName='$($Test.LogName)'"
        }
    }

    $minutes = [int](Get-CsvValueOrDefault -Row $Test -Name "Minutes" -Default 0)
    if ($minutes -le 0) {
        throw "Minutes must be > 0 for LogName='$($Test.LogName)'"
    }

    $batchSize = if ($ProfileProps.ContainsKey("pipeline.batch.size")) { [int]$ProfileProps["pipeline.batch.size"] } else { 0 }
    $outbatchCapRaw = Get-CsvValueOrDefault -Row $Test -Name "OutbatchCapacity" -Default ""
    if ($outbatchCapRaw -ne "") {
        $outbatchCap = [int]$outbatchCapRaw
        if ($outbatchCap -le 0) {
            throw "OutbatchCapacity must be > 0 for LogName='$($Test.LogName)'"
        }
        # PipelineOrchestrator clamps outbatch capacity to at least pipeline.batch.size,
        # so smaller requested values are not an actual runtime hazard.
    }
}

function Get-LatestBuildInputWriteTimeUtc {
    param(
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$ConfigPath
    )

    $candidateFiles = New-Object System.Collections.Generic.List[System.IO.FileInfo]

    foreach ($relativePath in @(
        "build.gradle",
        "settings.gradle",
        "gradle.properties"
    )) {
        $absolutePath = Join-Path $RootPath $relativePath
        if (Test-Path -LiteralPath $absolutePath) {
            $candidateFiles.Add((Get-Item -LiteralPath $absolutePath))
        }
    }

    if (Test-Path -LiteralPath $ConfigPath) {
        $candidateFiles.Add((Get-Item -LiteralPath $ConfigPath))
    }

    foreach ($relativeDir in @(
        "streamkernel-app",
        "streamkernel-core",
        "streamkernel-spi",
        "streamkernel-metrics",
        "streamkernel-plugins"
    )) {
        $absoluteDir = Join-Path $RootPath $relativeDir
        if (!(Test-Path -LiteralPath $absoluteDir)) {
            continue
        }

        Get-ChildItem -LiteralPath $absoluteDir -Recurse -File -ErrorAction SilentlyContinue |
            Where-Object {
                $_.Extension -in @(".java", ".kt", ".gradle", ".kts")
            } |
            ForEach-Object {
                $candidateFiles.Add($_)
            }
    }

    if ($candidateFiles.Count -eq 0) {
        return $null
    }

    return ($candidateFiles |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1).LastWriteTimeUtc
}

function Warn-IfJarMayBeStale {
    param(
        [Parameter(Mandatory = $true)][string]$JarPath,
        [Parameter(Mandatory = $true)][string]$RootPath,
        [Parameter(Mandatory = $true)][string]$ConfigPath
    )

    if (!(Test-Path -LiteralPath $JarPath)) {
        return
    }

    $jarItem = Get-Item -LiteralPath $JarPath
    $latestInputUtc = Get-LatestBuildInputWriteTimeUtc -RootPath $RootPath -ConfigPath $ConfigPath
    if ($null -eq $latestInputUtc) {
        return
    }

    if ($jarItem.LastWriteTimeUtc -lt $latestInputUtc) {
        $buildHint = Get-JarBuildHint
        $jarStamp = $jarItem.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
        $inputStamp = $latestInputUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss")
        Write-Host "    WARN: selected JAR may be stale. jar_ts=$jarStamp latest_input_ts=$inputStamp" -ForegroundColor Yellow
        Write-Host "          Rebuild hint: $buildHint" -ForegroundColor Yellow
    }
}

# ------------------------------------------------------------------------------
# SANITY CHECKS
# ------------------------------------------------------------------------------
if (!(Test-Path $matrixFile)) {
    Write-Host "FATAL: Test matrix not found: $matrixFile" -ForegroundColor Red; exit 1
}

$allTests = Import-Csv $matrixFile
if ($allTests.Count -eq 0) {
    Write-Host "FATAL: tests.csv is empty: $matrixFile" -ForegroundColor Red; exit 1
}

$testSuite = if ($singleTest -ne "") {
    $filtered = $allTests | Where-Object { $_.LogName -eq $singleTest }
    if ($filtered.Count -eq 0) { Write-Host "FATAL: No test with LogName='$singleTest'" -ForegroundColor Red; exit 1 }
    $filtered
} else { $allTests }

Write-Host ""
Write-Host "=== StreamKernel Automated Benchmark Suite ===" -ForegroundColor Cyan
Write-Host "    Matrix       : $matrixFile" -ForegroundColor Cyan
Write-Host "    Tests        : $($allTests.Count) total, $($testSuite.Count) selected" -ForegroundColor Cyan
Write-Host "    Output dir   : $baseOutputDir" -ForegroundColor Cyan
Write-Host ""

$summaryRows = @()

# ==============================================================================
# TEST LOOP
# ==============================================================================
foreach ($test in $testSuite) {
    $testName        = $test.LogName
    $configPath      = Join-Path $root $test.ConfigPath

    if (!(Test-Path $configPath)) {
        Write-Host "SKIP: Config not found for '$testName'" -ForegroundColor Yellow
        Write-Host "      Expected: $configPath" -ForegroundColor Yellow
        continue
    }

    $profileProps = Read-JavaPropertiesFile -Path $configPath
    Validate-TestMatrixRow -Test $test -ProfileProps $profileProps
    $jarPathForTest = $cpuJarPath

    if (!(Test-Path $jarPathForTest)) {
        Write-Host "FATAL: shadow JAR not found: $jarPathForTest" -ForegroundColor Red
        Write-Host ("       Run: {0}" -f (Get-JarBuildHint)) -ForegroundColor Yellow
        exit 1
    }

    # Read all CSV columns with safe defaults after profile inspection so defaults can
    # align to the selected pipeline where appropriate.
    $durationMinutes = [int](Get-CsvValueOrDefault -Row $test -Name "Minutes" -Default 0)
    $partitions      = [int](Get-CsvValueOrDefault -Row $test -Name "KafkaPartitions" -Default 12)
    $topicForTest    = Resolve-KafkaTopicForTest -Row $test -DefaultTopic $kafkaTopic
    $runId           = [string](Get-CsvValueOrDefault -Row $test -Name "RunId" -Default $testName)
    $heapGb          = [int](Get-CsvValueOrDefault -Row $test -Name "HeapGb" -Default 4)
    $gcThreads       = [int](Get-CsvValueOrDefault -Row $test -Name "GcThreads" -Default 2)
    $gcMode          = [string](Get-CsvValueOrDefault -Row $test -Name "GcMode" -Default "G1")
    $latencyMask     = [int](Get-CsvValueOrDefault -Row $test -Name "LatencySampleMask" -Default 127)
    $inflightMax     = [int](Get-CsvValueOrDefault -Row $test -Name "InflightMax" -Default 0)
    $requestedOutbatchCap = [int](Get-CsvValueOrDefault -Row $test -Name "OutbatchCapacity" -Default $(if ($profileProps.ContainsKey("pipeline.batch.size")) { [int]$profileProps["pipeline.batch.size"] } else { 64 }))
    $executorMode    = [string](Get-CsvValueOrDefault -Row $test -Name "ExecutorMode" -Default "FIXED")
    $cacheDisabled   = [string](Get-CsvValueOrDefault -Row $test -Name "CacheDisabled" -Default "true")
    $sinkBatchCopy   = [string](Get-CsvValueOrDefault -Row $test -Name "SinkBatchCopy" -Default "false")
    $jvmGcOpts       = Get-GcJvmOpts -GcMode $gcMode

    $profileParallelism = Get-EffectiveSetting -Key "pipeline.parallelism" -ProfileProps $profileProps -SystemProps @{} -Default ""
    $profileBatchSize   = Get-EffectiveSetting -Key "pipeline.batch.size" -ProfileProps $profileProps -SystemProps @{} -Default ""
    $profileBatchSizeInt = if ($profileBatchSize -ne "") { [int]$profileBatchSize } else { 0 }
    $outbatchCap = if ($profileBatchSizeInt -gt 0) { [Math]::Max($requestedOutbatchCap, $profileBatchSizeInt) } else { $requestedOutbatchCap }
    $drainTimeoutMs = if ($profileProps.ContainsKey("pipeline.drain.timeout.ms")) { [int]$profileProps["pipeline.drain.timeout.ms"] } else { 15000 }
    $autoStopSeconds = $durationMinutes * 60

    $outputDir   = Join-Path $baseOutputDir $testName
    if (!(Test-Path $outputDir)) { New-Item -ItemType Directory -Force -Path $outputDir | Out-Null }
    $timestamp   = Get-Date -Format "yyyyMMdd_HHmm"
    $logFile     = Join-Path $outputDir "${testName}_${timestamp}.log"
    $gcLogFile   = Join-Path $outputDir "${testName}_${timestamp}_gc.log"
    $sidecarFile = Join-Path $outputDir "${testName}_${timestamp}_meta.json"
    $metricsFile = Join-Path $outputDir "${testName}_${timestamp}_metrics.prom"
    $duringRunScript = Resolve-OptionalScriptPath -RootPath $root -Candidate ([string](Get-CsvValueOrDefault -Row $test -Name "DuringRunScript" -Default ""))
    $duringRunDelaySeconds = [int](Get-CsvValueOrDefault -Row $test -Name "DuringRunDelaySeconds" -Default 0)
    $controlLogFile = if ($duringRunScript) { Join-Path $outputDir "${testName}_${timestamp}_control.log" } else { "" }

    if ($duringRunScript -and !(Test-Path -LiteralPath $duringRunScript)) {
        throw "DuringRunScript not found for LogName='$testName': $duringRunScript"
    }

    Write-Host ">>> $testName" -ForegroundColor Magenta
    Write-Host "    $durationMinutes min | heap=${heapGb}g gc=$gcMode gc_threads=$gcThreads mask=$latencyMask inflight=$inflightMax executor=$executorMode cache_disabled=$cacheDisabled" -ForegroundColor Gray
    if ($profileParallelism -ne "" -or $profileBatchSize -ne "") {
        Write-Host "    profile parallelism=$profileParallelism batchSize=$profileBatchSize outbatchCap=$outbatchCap" -ForegroundColor Gray
    }
    Write-Host "    jar=$jarPathForTest" -ForegroundColor DarkGray
    Warn-IfJarMayBeStale -JarPath $jarPathForTest -RootPath $root -ConfigPath $configPath
    if ($requestedOutbatchCap -ne $outbatchCap) {
        Write-Host "    note: requested outbatchCap=$requestedOutbatchCap clamped to effective $outbatchCap to match pipeline batching" -ForegroundColor DarkGray
    }
    if ($duringRunScript) {
        Write-Host "    during-run hook=$duringRunScript delay=${duringRunDelaySeconds}s" -ForegroundColor DarkGray
    }

    foreach ($bootstrapKey in @("source.kafka.bootstrap.servers", "sink.kafka.bootstrap.servers", "dlq.kafka.bootstrap.servers")) {
        $bootstrapValue = [string](Get-EffectiveSetting -Key $bootstrapKey -ProfileProps $profileProps -SystemProps @{} -Default "")
        if (-not [string]::IsNullOrWhiteSpace($bootstrapValue)) {
            Assert-BootstrapReachable -BootstrapServers $bootstrapValue -Context $bootstrapKey
        }
    }

    # --------------------------------------------------------------------------
    # 1. KAFKA RESET
    # --------------------------------------------------------------------------
    if ($null -eq $topicForTest) {
        Write-Host "    Skipping Kafka topic reset for this profile." -ForegroundColor Gray
    } else {
        if ($partitions -le 0) {
            throw "KafkaPartitions must be greater than 0 for LogName='$testName' when KafkaTopic='$topicForTest'"
        }

        Write-Host "    Resetting '$topicForTest' ($partitions partitions)..." -ForegroundColor Gray
        Reset-KafkaTopicInContainer -Container $kafkaContainer -Topic $topicForTest -Partitions $partitions
        Write-Host "    Topic ready." -ForegroundColor Gray
    }

    # --------------------------------------------------------------------------
    # 2. BUILD JVM ARG LIST
    #
    # Per-test values from CSV are applied here. Array is joined to a newline-
    # delimited string before Start-Job and split back inside the job - required
    # because PSSerializer collapses string arrays into one space-joined string,
    # causing Java to receive all flags as a single argument and crash with
    # "Invalid initial heap size: -Xms3g -Xmx3g -XX:..."
    # --------------------------------------------------------------------------
    $gcLogArg = "-Xlog:gc*:file=${gcLogFile}:time,uptime,level,tags:filecount=3,filesize=10m"

    $resolvedLatencyEnabled = [string](Get-EffectiveSetting -Key "streamkernel.latency.enabled" -ProfileProps $profileProps -SystemProps @{} -Default "")
    if ($resolvedLatencyEnabled -eq "") {
        $resolvedMetricsLatencyEnabled = [string](Get-EffectiveSetting -Key "streamkernel.metrics.latency.enabled" -ProfileProps $profileProps -SystemProps @{} -Default "")
        if ($resolvedMetricsLatencyEnabled -ne "") {
            $resolvedLatencyEnabled = $resolvedMetricsLatencyEnabled
        }
    }

    $resolvedLatencyBufferSize = [string](Get-EffectiveSetting -Key "streamkernel.latency.buffer.size" -ProfileProps $profileProps -SystemProps @{} -Default "16384")
    $resolvedMetricsLatencyMaxSeconds = [string](Get-EffectiveSetting -Key "streamkernel.metrics.latency.max.seconds" -ProfileProps $profileProps -SystemProps @{} -Default "10")

    $skResolvedProps = @(
        "-Dstreamkernel.latency.buffer.size=$resolvedLatencyBufferSize"
        "-Dstreamkernel.metrics.latency.max.seconds=$resolvedMetricsLatencyMaxSeconds"
    )
    if ($resolvedLatencyEnabled -ne "") {
        $skResolvedProps += "-Dstreamkernel.latency.enabled=$resolvedLatencyEnabled"
    }

    $skPerTestProps = @(
        "-Dstreamkernel.latency.sample.mask=$latencyMask"
        "-Dstreamkernel.sink.batch.copy=$sinkBatchCopy"
        "-Dstreamkernel.executor.mode=$executorMode"
        "-Dstreamkernel.cache.force.disabled=$cacheDisabled"
        "-Dstreamkernel.outbatch.capacity=$outbatchCap"
        "-Dstreamkernel.bench.auto.stop.after.seconds=$autoStopSeconds"
        "-Dstreamkernel.prometheus.snapshot.path=$metricsFile"
    )
    # InflightMax=0 means disabled - omit entirely so Integer.MAX_VALUE applies
    if ($inflightMax -gt 0) { $skPerTestProps += "-Dstreamkernel.sink.inflight.max=$inflightMax" }

    $javaArgs = @() `
        + @("-Xms${heapGb}g", "-Xmx${heapGb}g") `
        + @("-XX:ParallelGCThreads=$gcThreads") `
        + $jvmGcOpts `
        + $jvmCommonOpts `
        + @($gcLogArg) `
        + $skResolvedProps `
        + $skPerTestProps `
        + @("-Dsk.run.id=$runId", "-Dsk.config.path=$configPath", "-jar", $jarPathForTest)

    $effectiveSystemProps = Convert-ArgListToPropertyMap -Args ($skResolvedProps + $skPerTestProps + @("-Dsk.run.id=$runId", "-Dsk.config.path=$configPath"))
    $metricsProvider = [string](Get-EffectiveSetting -Key "metrics.provider" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default "")
    $prometheusEnabledRaw = [string](Get-EffectiveSetting -Key "metrics.prometheus.enabled" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default "")
    $prometheusEnabled = ($prometheusEnabledRaw -eq "") -or ($prometheusEnabledRaw.Trim().ToLowerInvariant() -eq "true")
    $prometheusPortRaw = [string](Get-EffectiveSetting -Key "metrics.prometheus.port" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default "8080")
    $prometheusPort = if ($prometheusPortRaw -ne "") { [int]$prometheusPortRaw } else { 8080 }
    $metricsSnapshotCaptured = $false
    $metricsSnapshotError = ""

    # --------------------------------------------------------------------------
    # 3. START TIME
    # --------------------------------------------------------------------------
    $startTime   = Get-Date
    $startUnixMs = [long]($startTime - [datetime]"1970-01-01T00:00:00Z").TotalMilliseconds

    # --------------------------------------------------------------------------
    # 4. LAUNCH JVM
    # --------------------------------------------------------------------------
    $javaArgsFlat = $javaArgs -join "`n"
    $job = Start-Job -ScriptBlock {
        param([string]$argsFlat_, [string]$log_)
        [string[]]$tokens = $argsFlat_ -split "`n"
        & java @tokens 2>&1 | Tee-Object -FilePath $log_
        $exitCode = $LASTEXITCODE
        "__STREAMKERNEL_EXIT_CODE=$exitCode"
    } -ArgumentList $javaArgsFlat, $logFile

    $controlJob = $null
    if ($duringRunScript) {
        $controlJob = Start-Job -ScriptBlock {
            param(
                [string]$scriptPath_,
                [int]$delaySeconds_,
                [string]$controlLog_,
                [string]$runId_,
                [string]$testName_,
                [string]$configPath_,
                [string]$outputDir_,
                [string]$pipelineLogFile_,
                [string]$metricsFile_,
                [string]$kafkaTopic_,
                [int]$kafkaPartitions_
            )

            try {
                if ($delaySeconds_ -gt 0) {
                    Start-Sleep -Seconds $delaySeconds_
                }

                & $scriptPath_ `
                    -RunId $runId_ `
                    -TestName $testName_ `
                    -ConfigPath $configPath_ `
                    -OutputDir $outputDir_ `
                    -PipelineLogFile $pipelineLogFile_ `
                    -MetricsFile $metricsFile_ `
                    -KafkaTopic $kafkaTopic_ `
                    -KafkaPartitions $kafkaPartitions_ *>&1 |
                    Tee-Object -FilePath $controlLog_

                "__STREAMKERNEL_CONTROL_EXIT_CODE=0"
            } catch {
                $_ | Out-String | Tee-Object -FilePath $controlLog_ -Append | Out-Null
                "__STREAMKERNEL_CONTROL_EXIT_CODE=1"
            }
        } -ArgumentList $duringRunScript, $duringRunDelaySeconds, $controlLogFile, $runId, $testName, $configPath, $outputDir, $logFile, $metricsFile, $(if ($null -ne $topicForTest) { $topicForTest } else { "" }), $partitions
    }

    Write-Host "    Running... (watchdog every 10s)" -ForegroundColor Gray

    # --------------------------------------------------------------------------
    # 5. WATCHDOG LOOP
    # --------------------------------------------------------------------------
    $endTime = $startTime.AddMinutes($durationMinutes)
    $crashed = $false
    $completedNaturally = $false

    while ((Get-Date) -lt $endTime) {
        if ((Get-Job -Id $job.Id).State -ne "Running") {
            $elapsedMinutes = ((Get-Date) - $startTime).TotalMinutes
            if ($elapsedMinutes + 0.2 -ge $durationMinutes) {
                $completedNaturally = $true
                break
            }
            Write-Host ""
            Write-Host "FATAL: '$testName' exited before timeout!" -ForegroundColor Red
            Receive-Job -Job $job -Keep | Select-Object -Last 40 | ForEach-Object { Write-Host "  $_" }
            $crashed = $true
            break
        }
        $elapsed = [math]::Round(((Get-Date) - $startTime).TotalMinutes, 1)
        Write-Progress -Activity "StreamKernel: $testName" `
            -Status "$elapsed / $durationMinutes min" `
            -PercentComplete ([math]::Min(100, [int](($elapsed / $durationMinutes) * 100)))
        Start-Sleep -Seconds 10
    }
    Write-Progress -Activity "StreamKernel: $testName" -Completed

    # --------------------------------------------------------------------------
    # 6. STOP AND CLEAN UP
    # --------------------------------------------------------------------------
    $graceSeconds = [Math]::Max(5, [int][Math]::Ceiling(($drainTimeoutMs + 10000) / 1000.0))

    if (-not $crashed -and -not $completedNaturally) {
        $waited = Wait-Job -Id $job.Id -Timeout $graceSeconds
        $completedNaturally = $null -ne $waited
    }

    $forcedStop = $false
    $pipelineStoppedBeforeCleanup = $false
    if (-not $crashed -and -not $completedNaturally) {
        $pipelineStoppedBeforeCleanup = Test-StreamKernelLogHasPipelineStop -Path $logFile
        if ($pipelineStoppedBeforeCleanup) {
            Write-Host "NOTE: '$testName' logged a clean pipeline stop but the wrapper job was still alive after ${graceSeconds}s; collecting final metrics and cleaning up the lingering process." -ForegroundColor Yellow
        } else {
            Write-Host "WARN: '$testName' did not exit after ${graceSeconds}s grace period; collecting any final metrics and forcing shutdown." -ForegroundColor Yellow
        }
        if ($metricsProvider.Trim().ToUpperInvariant() -eq "PROMETHEUS" -and $prometheusEnabled -and !(Test-Path $metricsFile)) {
            try {
                $metricsResponse = Invoke-WebRequest -Uri "http://127.0.0.1:$prometheusPort/metrics" -UseBasicParsing -TimeoutSec 5
                Set-Content -LiteralPath $metricsFile -Value $metricsResponse.Content -Encoding UTF8
            }
            catch {
                $metricsSnapshotError = $_.Exception.Message
            }
        }
        Stop-Job -Id $job.Id -ErrorAction SilentlyContinue
        Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" -ErrorAction SilentlyContinue |
            Where-Object { $_.CommandLine -like "*-Dsk.run.id=$runId*" } |
            ForEach-Object { Invoke-CimMethod -InputObject $_ -MethodName Terminate | Out-Null }
        $forcedStop = $true
    }

    $jobOutput = Receive-Job -Id $job.Id -Keep -ErrorAction SilentlyContinue
    $exitCode = Parse-JobExitCode -JobOutput $jobOutput -MarkerPrefix "__STREAMKERNEL_EXIT_CODE="

    if (Test-Path $metricsFile) {
        $metricsSnapshotCaptured = $true
    } elseif ($metricsProvider.Trim().ToUpperInvariant() -eq "PROMETHEUS" -and $prometheusEnabled) {
        $metricsSnapshotError = "Snapshot file was not produced before shutdown."
    }

    Remove-Job -Id $job.Id -Force -ErrorAction SilentlyContinue

    $controlExitCode = $null
    $controlTimedOut = $false
    if ($null -ne $controlJob) {
        $controlWait = Wait-Job -Id $controlJob.Id -Timeout 30
        if ($null -eq $controlWait -and (Get-Job -Id $controlJob.Id).State -eq "Running") {
            Stop-Job -Id $controlJob.Id -ErrorAction SilentlyContinue
            $controlTimedOut = $true
        }

        $controlOutput = Receive-Job -Id $controlJob.Id -Keep -ErrorAction SilentlyContinue
        $controlExitCode = Parse-JobExitCode -JobOutput $controlOutput -MarkerPrefix "__STREAMKERNEL_CONTROL_EXIT_CODE="
        Remove-Job -Id $controlJob.Id -Force -ErrorAction SilentlyContinue
    }

    Start-Sleep -Seconds 2

    # --------------------------------------------------------------------------
    # 7. WRITE GRAFANA SIDECAR JSON
    # --------------------------------------------------------------------------
    $endActual  = Get-Date
    $endUnixMs  = [long]($endActual - [datetime]"1970-01-01T00:00:00Z").TotalMilliseconds
    $actualMins = [math]::Round(($endActual - $startTime).TotalMinutes, 2)
    $controlFailed = ($null -ne $duringRunScript) -and ($controlTimedOut -or ($controlExitCode -ne $null -and $controlExitCode -ne 0))

    $finalStatus =
        if ($crashed -or ($exitCode -ne $null -and $exitCode -ne 0)) { "CRASHED" }
        elseif ($controlFailed) { "CONTROL_FAILED" }
        elseif ($forcedStop -and -not $pipelineStoppedBeforeCleanup) { "FORCED_STOP" }
        else { "COMPLETED" }

    [ordered]@{
        testName         = $testName
        runId            = $runId
        configPath       = $configPath
        logFile          = $logFile
        gcLogFile        = $gcLogFile
        status           = $finalStatus
        startUtc         = $startTime.ToUniversalTime().ToString("o")
        endUtc           = $endActual.ToUniversalTime().ToString("o")
        startUnixMs      = $startUnixMs
        endUnixMs        = $endUnixMs
        requestedMinutes = $durationMinutes
        actualMinutes    = $actualMins
        grafanaFrom      = $startUnixMs
        grafanaTo        = $endUnixMs
        profile          = [ordered]@{
            heapGb          = $heapGb
            gcMode          = $gcMode
            gcThreads       = $gcThreads
            latencyMask     = $latencyMask
            inflightMax     = $inflightMax
            outbatchCap     = $outbatchCap
            executorMode    = $executorMode
            cacheDisabled   = $cacheDisabled
            sinkBatchCopy   = $sinkBatchCopy
            kafkaPartitions = $partitions
        }
        profileConfig = [ordered]@{
            pipelineId      = Get-EffectiveSetting -Key "pipeline.id" -ProfileProps $profileProps -SystemProps @{} -Default ""
            parallelism     = $profileParallelism
            batchSize       = $profileBatchSize
            transformChain  = Get-EffectiveSetting -Key "transform.chain" -ProfileProps $profileProps -SystemProps @{} -Default ""
            sinkType        = Get-EffectiveSetting -Key "sink.type" -ProfileProps $profileProps -SystemProps @{} -Default ""
            securityType    = Get-EffectiveSetting -Key "security.type" -ProfileProps $profileProps -SystemProps @{} -Default ""
        }
        metricsSnapshot = [ordered]@{
            provider  = $metricsProvider
            captured  = $metricsSnapshotCaptured
            file      = if ($metricsSnapshotCaptured) { $metricsFile } else { "" }
            port      = $prometheusPort
            error     = $metricsSnapshotError
        }
        controlHook = [ordered]@{
            script       = if ($duringRunScript) { $duringRunScript } else { "" }
            delaySeconds = $duringRunDelaySeconds
            logFile      = $controlLogFile
            exitCode     = if ($null -ne $controlExitCode) { $controlExitCode } else { "" }
            timedOut     = $controlTimedOut
        }
        processCleanup = [ordered]@{
            forcedStop                    = $forcedStop
            pipelineStoppedBeforeCleanup  = $pipelineStoppedBeforeCleanup
        }
        runtimeArtifact = [ordered]@{
            jarPath     = $jarPathForTest
        }
        effectiveSettings = [ordered]@{
            gcMode                         = $gcMode
            latencySampleMask              = Get-EffectiveSetting -Key "streamkernel.latency.sample.mask" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
            latencyEnabled                 = Get-EffectiveSetting -Key "streamkernel.latency.enabled" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
            metricsLatencyEnabled          = Get-EffectiveSetting -Key "streamkernel.metrics.latency.enabled" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
            sinkInflightMax                = Get-EffectiveSetting -Key "streamkernel.sink.inflight.max" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
            outbatchCapacity               = Get-EffectiveSetting -Key "streamkernel.outbatch.capacity" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
            executorMode                   = Get-EffectiveSetting -Key "streamkernel.executor.mode" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
            cacheForceDisabled             = Get-EffectiveSetting -Key "streamkernel.cache.force.disabled" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
            sinkBatchCopy                  = Get-EffectiveSetting -Key "streamkernel.sink.batch.copy" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
            sourceFetchLock                = Get-EffectiveSetting -Key "streamkernel.source.fetch.lock" -ProfileProps $profileProps -SystemProps $effectiveSystemProps -Default ""
        }
        systemProperties = $effectiveSystemProps
        jvmArgs = ($javaArgs -join " ")
    } | ConvertTo-Json -Depth 4 | Set-Content -Path $sidecarFile -Encoding UTF8

    # --------------------------------------------------------------------------
    # 8. ACCUMULATE SUMMARY
    # --------------------------------------------------------------------------
    $summaryRows += [PSCustomObject]@{
        TestName      = $testName
        RunId         = $runId
        Status        = $finalStatus
        StartUtc      = $startTime.ToUniversalTime().ToString("o")
        EndUtc        = $endActual.ToUniversalTime().ToString("o")
        ActualMinutes = $actualMins
        HeapGb        = $heapGb
        GcMode        = $gcMode
        ExecutorMode  = $executorMode
        LatencyMask   = $latencyMask
        InflightMax   = $inflightMax
        GrafanaFrom   = $startUnixMs
        GrafanaTo     = $endUnixMs
        LogFile       = $logFile
        SidecarFile   = $sidecarFile
        ControlLogFile = $controlLogFile
    }

    $c = if ($finalStatus -eq "COMPLETED") { "Green" } elseif ($finalStatus -eq "FORCED_STOP") { "Yellow" } else { "Red" }
    Write-Host "    $finalStatus ($actualMins min) - Log: $logFile" -ForegroundColor $c
    Write-Host ""

    if ($test -ne $testSuite[-1]) {
        Write-Host "    Cooling down 15s..." -ForegroundColor Gray
        Start-Sleep -Seconds 15
        Write-Host ""
    }
}

# ==============================================================================
# RESULTS SUMMARY
# ==============================================================================
if ($summaryRows.Count -gt 0) {
    $summaryFile = Join-Path $baseOutputDir "results_$(Get-Date -Format 'yyyyMMdd_HHmm').csv"
    $summaryRows | Export-Csv -Path $summaryFile -NoTypeInformation -Encoding UTF8

    Write-Host "=== Suite Complete ===" -ForegroundColor Cyan
    Write-Host "    Tests run   : $($summaryRows.Count)" -ForegroundColor Cyan
    Write-Host "    Summary CSV : $summaryFile" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "--- Grafana Time Ranges ---" -ForegroundColor Yellow
    foreach ($row in $summaryRows) {
        $c = if ($row.Status -eq "CRASHED") { "Red" } else { "Green" }
        Write-Host ("  {0,-60} {1,9}  from={2}  to={3}" -f $row.TestName, $row.Status, $row.GrafanaFrom, $row.GrafanaTo) -ForegroundColor $c
    }
    Write-Host ""
} else {
    Write-Host "=== No tests completed ===" -ForegroundColor Yellow
}
