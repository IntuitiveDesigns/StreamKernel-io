# 01 - SYNTHETIC -> Kafka Bench Test

Validates:

- `source.type=SYNTHETIC` ring-buffer batches
- high-throughput orchestration
- Kafka sink write path

## 1) Generate Local Kafka Certs

The compose broker configures SSL listeners even when this profile uses the
PLAINTEXT listener. Run this once before starting `broker`:

```bash
bash scripts/gen-certs.sh
```

On Windows with Git Bash:

```powershell
$env:MSYS_NO_PATHCONV = "1"
& "C:\Program Files\Git\bin\bash.exe" scripts/gen-certs.sh
```

## 2) Start Infra

```powershell
docker compose up -d broker
docker compose ps
```

## 3) Create Topics

```powershell
docker exec -it broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists `
  --topic arena-bench-test --partitions 6 --replication-factor 1 --config max.message.bytes=10485760

docker exec -it broker kafka-topics --bootstrap-server broker:29092 --create --if-not-exists `
  --topic streamkernel-dlq --partitions 6 --replication-factor 1
```

## 4) Configure `pipeline.properties`

```properties
source.type=SYNTHETIC
source.synthetic.payload.size=1024
source.synthetic.high.entropy=false

sink.type=KAFKA
sink.topic=arena-bench-test

dlq.type=KAFKA_DLQ
dlq.topic=streamkernel-dlq
```

## 5) Build + Run

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_kafka_at_least_once_baseline_10m
```

## 6) Validate Messages

```powershell
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```

If OPA denies, validate DLQ instead:

```powershell
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic streamkernel-dlq --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```
