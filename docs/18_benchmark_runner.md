# 18 - Benchmark Runner

`test-java-runner.ps1` is the benchmark harness for published StreamKernel performance runs.

The important split is:

- A pipeline `.properties` file defines pipeline behavior.
- A benchmark CSV row defines the JVM, timing, sink, and runtime overrides used to produce a repeatable result.

That split is what lets someone clone the repo, pick a published row, and reproduce the run conditions instead of guessing which knobs were applied by hand.

## Canonical Reproduction Flow

CPU rows:

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_kafka_at_least_once_baseline_10m
```

OIDC/security rows:

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_oidc.csv
```

## CSV Contract

The runner expects these benchmark columns:

| Column | Meaning |
|---|---|
| `LogName` | Stable row identifier. Also becomes the output folder name. |
| `ConfigPath` | Pipeline profile under `config/pipelines/`. |
| `Minutes` | Target run length. The runner converts this to `streamkernel.bench.auto.stop.after.seconds`. |
| `KafkaTopic` | Topic to create/reset when the benchmark row writes to Kafka. |
| `KafkaPartitions` | Partition count for the benchmark topic. |
| `RunId` | Injected as `-Dsk.run.id`. Also captured in run metadata. |
| `HeapGb` | Sets `-Xms` and `-Xmx`. |
| `GcThreads` | JVM GC worker count override. |
| `GcMode` | GC family such as `G1` or `ZGC`. |
| `LatencySampleMask` | Sampling mask for latency capture. |
| `InflightMax` | Optional ceiling for `streamkernel.sink.inflight.max`. |
| `OutbatchCapacity` | Requested `streamkernel.outbatch.capacity`. The runner clamps this up to `pipeline.batch.size` when needed. |
| `ExecutorMode` | Injected as `streamkernel.executor.mode`. |
| `CacheDisabled` | Injected as `streamkernel.cache.force.disabled`. |
| `SinkBatchCopy` | Injected as `streamkernel.sink.batch.copy`. |
| `JarFlavor` | Optional selector for alternate JAR variants where a matrix mixes them. |
| `DuringRunScript` | Optional PowerShell script launched while the benchmark is running for control-plane actions. |
| `DuringRunDelaySeconds` | Optional delay before `DuringRunScript` is invoked. |

## Runner-Owned Overrides

These settings are intentionally controlled by the runner so published rows are reproducible:

- `sk.run.id`
- `sk.config.path`
- `streamkernel.metrics.latency.max.seconds`
- `streamkernel.executor.mode`
- `streamkernel.cache.force.disabled`
- `streamkernel.outbatch.capacity`
- `streamkernel.bench.auto.stop.after.seconds`
- `streamkernel.prometheus.snapshot.path`
- `streamkernel.sink.batch.copy`
- `streamkernel.latency.sample.mask`
- `streamkernel.sink.inflight.max` when `InflightMax > 0`

The pipeline file remains the source of truth for pipeline wiring. The runner owns the benchmark envelope around that wiring.

## Artifacts Produced Per Run

Each row produces a benchmark folder with:

- main application log
- GC log
- effective settings snapshot
- Prometheus snapshot
- `meta.json` with JVM args, effective system properties, resolved config path, and benchmark row values
- optional `*_control.log` when a row uses `DuringRunScript`

That `meta.json` is the paper trail for validating a published number on another machine.

## Smoke Test vs Benchmark

Use a direct Java command when you want a short smoke test:

```powershell
$cfg = (Resolve-Path .\config\pipelines\streamkernel_kafka_at_least_once_baseline.properties).Path
$jar = (Get-ChildItem .\streamkernel-app\build\libs\streamkernel-app-*-all.jar | Select-Object -First 1).FullName
$javaArgs = @(
  '-Xms2g'
  '-Xmx2g'
  '-Dsk.run.id=manual-smoke'
  "-Dsk.config.path=$cfg"
  '-Dstreamkernel.bench.auto.stop.after.seconds=60'
  '-jar'
  $jar
)
& "C:\Program Files\Eclipse Adoptium\jdk-21.0.10.7-hotspot\bin\java.exe" @javaArgs
```

Use `test-java-runner.ps1` when you want a result someone else can audit and replay.

## Related Docs

- [../BENCHMARK_SUITE.md](../BENCHMARK_SUITE.md)
- [19_configuration_reference.md](19_configuration_reference.md)
