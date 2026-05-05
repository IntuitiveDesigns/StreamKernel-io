## Profiles

Active StreamKernel profiles live under `config/pipelines/`.

Use them in one of two ways:

1. a short manual smoke test
2. a benchmark reproduction run through `test-java-runner.ps1`

The key distinction is:

- a profile file describes pipeline behavior
- a benchmark row describes the execution settings that produced a published result

Kafka examples are used below because they are the quickest local reproduction
path. They do not define the runtime boundary; the same profile model also
covers Pulsar, REST, Delta Lake, Snowflake, MongoDB, and DevNull plugin paths.

## Canonical References

- [docs/19_configuration_reference.md](docs/19_configuration_reference.md)
- [docs/18_benchmark_runner.md](docs/18_benchmark_runner.md)
- [benchmark-runs/tests.csv](benchmark-runs/tests.csv)

## Original Benchmark Matrix

The public benchmark matrix lives at:

- `benchmark-runs/tests.csv`
- `benchmark-runs/tests_lineage.csv`
- `benchmark-runs/tests_pulsar.csv`
- `benchmark-runs/tests_pulsar_live.csv`
- `benchmark-runs/tests_snowflake.csv`

Use it like this:

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_kafka_at_least_once_baseline_10m
```

Additional public use cases:

```powershell
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_mongodb_insert_baseline_10m
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_delta_spark_local_5m
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_lineage.csv -SingleTest streamkernel_lineage_audit_10m
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_pulsar.csv
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_pulsar_live.csv
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_snowflake.csv
```

## Manual Smoke Test

When you only want to confirm a profile boots, run the fat jar directly with an absolute config path:

```powershell
$cfg = (Resolve-Path .\config\pipelines\streamkernel_kafka_at_least_once_baseline.properties).Path
& "C:\Program Files\Eclipse Adoptium\jdk-21.0.10.7-hotspot\bin\java.exe" `
  -Xms2g -Xmx2g `
  -Dsk.run.id=manual-smoke `
  -Dsk.config.path=$cfg `
  -Dstreamkernel.bench.auto.stop.after.seconds=60 `
  -jar .\streamkernel-app\build\libs\streamkernel-app-0.0.1-SNAPSHOT-all.jar
```

## Why This Split Exists

This keeps published benchmark results reproducible:

- the profile remains readable and source-controlled
- the benchmark row captures JVM and runtime tuning
- the runner emits `meta.json`, metrics, and logs that others can inspect
