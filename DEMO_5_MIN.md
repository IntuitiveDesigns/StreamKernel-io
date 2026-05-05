# Five-Minute Demo Script

## Goal

Show that StreamKernel is a single-JVM event intelligence runtime with reproducible benchmarks, clean plugin boundaries, and a commercial source-available core.

## 0:00 - Category

Say:

> StreamKernel is a source-available event intelligence runtime. It sits between event transport and analytical platforms, where teams usually hand-build policy, inference, cache, DLQ, metrics, and sink logic.

Point to `README.md`.

## 0:45 - Pain

Say:

> Kafka moves events, Spark and Databricks analyze data, and Flink runs cluster stream jobs. The pain is the operational glue around them: model calls, OPA checks, semantic cache, DLQ, retries, metrics, provenance, and destination writers.

Point to `COMPARISON.md`.

## 1:30 - Architecture

Open `ARCHITECTURE.md` and show the diagram.

Say:

> The runtime owns batching, backpressure, lifecycle, policy flow, DLQ handoff, metrics, and plugin discovery. The source, transform, sink, security, cache, and metrics implementations are replaceable plugins.

## 2:15 - Reproducible Benchmark

Run or show:

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_kafka_at_least_once_baseline_10m
```

If time is short, open `benchmark-runs/tests.csv` and `BENCHMARK_SUITE.md` instead.

Say:

> A benchmark is not just a number. It is the pipeline config plus a matrix row plus runner artifacts with JVM flags, effective settings, logs, GC output, and metadata.

## 3:30 - SPI Moat

Open `docs/plugin-example.md`.

Say:

> The SDK contracts are Apache 2.0. Authors can build source, transform, sink, cache, security, or metrics plugins and keep their plugin ownership. StreamKernel keeps the runtime behavior consistent.

## 4:30 - Commercial Close

Open `COMMERCIAL.md`.

Say:

> The core runtime and first-party modules are source-available. Custom plugins remain author-owned. Commercial licensing covers redistribution, OEM embedding, managed service use, support, and negotiated patent terms.

## Backup Smoke Command

Use this if a full benchmark is too long for the room:

```powershell
$env:SK_CONFIG_PATH = "config/pipelines/streamkernel_synthetic-devnull-max.properties"
.\gradlew.bat --no-daemon :streamkernel-app:run
```
