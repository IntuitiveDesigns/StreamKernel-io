# 12 - Delta/Spark Local Lakehouse

This public profile writes deterministic enriched rows into a MinIO-backed Delta table and validates the table with Spark. It avoids private model artifacts and uses `DETERMINISTIC_ENRICHMENT` to produce the `EnrichedTicket` row contract.

## Start Services

```powershell
docker compose --profile delta-spark up -d minio minio-init spark
```

## Run

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_delta_spark_local_5m
```

## Validate

Use the Spark helper scripts in `tools/spark/` against:

```text
s3a://streamkernel-delta/enriched-tickets-public
```

The profile is `config/pipelines/streamkernel_delta_spark_local.properties`.
