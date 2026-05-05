# Optional: Profile Loading (Java)

This note reflects the current public layout.

## Current Pattern

StreamKernel now uses explicit pipeline files under `config/pipelines/` and passes the selected file through:

- `-Dsk.config.path=<absolute-or-repo-relative-path>`

For published benchmark runs, that profile path is paired with a benchmark row in `benchmark-runs/*.csv` and launched through `test-java-runner.ps1`.

## Recommended Entry Points

- [../PROFILES.md](../PROFILES.md)
- [18_benchmark_runner.md](18_benchmark_runner.md)
- [19_configuration_reference.md](19_configuration_reference.md)

## Direct Java Example

```powershell
$cfg = (Resolve-Path .\config\pipelines\streamkernel_kafka_at_least_once_baseline.properties).Path
$jar = (Resolve-Path .\streamkernel-app\build\libs\streamkernel-app-0.0.1-SNAPSHOT-all.jar).Path
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
