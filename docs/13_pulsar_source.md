# 13 - Pulsar Source Portability

These public profiles prove the source SPI can swap Kafka-origin assumptions for Apache Pulsar while keeping the StreamKernel runtime, transformer chain, benchmark runner, and Kafka sink evidence path.

## Start Services

```powershell
docker compose --profile pulsar up -d pulsar
docker compose up -d broker
```

## Seed A Backlog

```powershell
docker exec pulsar bin/pulsar-admin topics create persistent://public/default/streamkernel-bench-in
docker exec pulsar bin/pulsar-perf produce persistent://public/default/streamkernel-bench-in -r 5000 -n 100000 -m 0 -s 256
```

## Run Seeded Backlog

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_pulsar.csv
```

## Run Live Producer

Start a producer for `persistent://public/default/streamkernel-bench-live`, then run:

```powershell
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_pulsar_live.csv
```

Profiles:

- `config/pipelines/streamkernel_pulsar_source_kafka.properties`
- `config/pipelines/streamkernel_pulsar_live_kafka.properties`
