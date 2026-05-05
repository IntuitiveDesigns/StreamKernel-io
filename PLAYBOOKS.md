# Playbooks

## Local Build

```powershell
.\gradlew.bat --no-daemon clean test
```

Use Java 21. The Gradle toolchain is configured in the root build.

## Run A Pipeline

```powershell
$env:SK_CONFIG_PATH = "config/pipelines/streamkernel_synthetic-devnull-max.properties"
.\gradlew.bat --no-daemon :streamkernel-app:run
```

Every public profile should be runnable from a single `.properties` file in `config/pipelines`.

## Benchmark Evidence

Preserve benchmark rows in `benchmark-runs/*.csv`. Those CSV files are the public evidence index for run IDs, profiles, record counts, throughput, and environment notes. Large logs, metrics snapshots, and private artifacts should remain outside the public tree unless intentionally published.

The primary public reproduction suite is:

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_kafka_at_least_once_baseline_10m
```

See [BENCHMARK_SUITE.md](BENCHMARK_SUITE.md) for the full suite contract.

## Release Checklist

1. Confirm `git status --short --untracked-files=all` has no unintended files.
2. Confirm `secrets/`, local logs, crash artifacts, caches, and private legal work product are not tracked.
3. Run the focused Gradle test/build command for changed modules.
4. Confirm `README.md`, `ARCHITECTURE.md`, `MODULES.md`, `LICENSE-HISTORY.md`, `NOTICE`, and `THIRD-PARTY-NOTICES.md` agree on module boundaries.
5. Tag only from a clean tree.
