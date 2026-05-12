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

## Local Docker Prerequisite

Run the local cert generator before starting Kafka-backed profiles:

```bash
bash scripts/gen-certs.sh
```

On Windows with Git Bash:

```powershell
$env:MSYS_NO_PATHCONV = "1"
& "C:\Program Files\Git\bin\bash.exe" scripts/gen-certs.sh
```

The compose broker always configures SSL listeners, so these generated
keystores and Confluent `*_creds` files are needed by multiple profile families:
plain Kafka sink/source rows, mTLS rows, OIDC rows that start the shared broker,
and Pulsar source rows that write to Kafka.

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

For `benchmark-runs/tests_pulsar.csv`, prefer the paired demo scripts:

```powershell
.\scripts\demo_before_pulsar_source_kafka.ps1 -ResetPulsarVolumeOnLedgerError
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_pulsar.csv
.\scripts\demo_after_pulsar_source_kafka.ps1
```

That row is a seeded backlog burst-drain profile. It establishes how quickly
StreamKernel drains Pulsar into Kafka through `STRING_TO_WIREEVENT`; it is not a
full-duration sustained-throughput profile.

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
