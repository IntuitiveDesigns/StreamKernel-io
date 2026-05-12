# 13 - Pulsar Source Portability

These public profiles prove the source SPI can swap Kafka-origin assumptions for
Apache Pulsar while keeping the StreamKernel runtime, transformer chain,
benchmark runner, and Kafka sink evidence path.

There are two different stories:

- `streamkernel_pulsar_source_kafka`: seeded backlog burst drain. This is best
  read as "how fast can StreamKernel empty a Pulsar backlog into Kafka?"
- `streamkernel_pulsar_live_kafka`: live producer pressure. Use this when you
  want sustained speedometer windows across the run.

## Prerequisites

Generate local Kafka development certs before starting the broker:

```bash
bash scripts/gen-certs.sh
```

On Windows with Git Bash:

```powershell
$env:MSYS_NO_PATHCONV = "1"
& "C:\Program Files\Git\bin\bash.exe" scripts/gen-certs.sh
```

The compose broker enables SSL listeners even when this Pulsar profile writes to
Kafka over PLAINTEXT. Without `secrets/kafka.server.keystore.p12`,
`secrets/kafka.truststore.p12`, and the Confluent `*_creds` files, the broker
will restart before the benchmark can run.

## Start Services

```powershell
docker compose --profile pulsar up -d broker pulsar
```

On Apple Silicon Macs, the compose file defaults local services to
`linux/amd64` through `STREAMKERNEL_DOCKER_PLATFORM`. Docker Desktop runs those
images under emulation. Override that variable only after confirming each image
in the selected profile has a native ARM64 variant.

## Burst Drain Run

Use the before/after scripts so the topic reset, finite Pulsar seed, Kafka
verification, and log capture stay consistent:

```powershell
.\scripts\demo_before_pulsar_source_kafka.ps1 -ResetPulsarVolumeOnLedgerError
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_pulsar.csv
.\scripts\demo_after_pulsar_source_kafka.ps1
```

The before script seeds a finite backlog with `pulsar-perf produce` rather than
slow `pulsar-client` chunks. The default target is `250000` messages, and
`pulsar-perf` can slightly overshoot because of batching.

May 12, 2026 local evidence:

- Pulsar published and delivered `253235` records.
- Kafka topic `streamkernel-pulsar-out` contained `253235` records across 12
  partitions.
- Speedometer windows reached `15646.6` records/sec and drained the backlog in
  roughly 20 seconds.
- Final connector state: `readTotal=253235`, `errorTotal=0`, `DROPPED=0`, clean
  graceful shutdown.

This is not a 10-minute sustained-throughput claim. After the burst drains, the
runner idles until the 10-minute auto-stop so the evidence still captures idle
stability and graceful shutdown. A fully saturated 10-minute run at the observed
rate would need millions of messages or a live producer.

Kafka may log metadata refresh lines such as partition epoch resets or
`Node -1 disconnected` during the idle tail. For this burst profile, those are
post-work metadata refresh artifacts, not data loss. They matter more for a
pipeline that resumes producing after a long idle.

## Live Producer Run

For sustained pressure, use the live profile:

```powershell
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_pulsar_live.csv
```

Profiles:

- `config/pipelines/streamkernel_pulsar_source_kafka.properties`
- `config/pipelines/streamkernel_pulsar_live_kafka.properties`
