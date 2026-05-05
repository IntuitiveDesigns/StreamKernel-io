# 17 - Lineage Audit

The public lineage profile stamps provenance headers on records without relying on private model bootstrap or inference logic. It uses `STRING_TO_WIREEVENT`, Kafka output, and the runtime provenance layer.

## Run

```powershell
docker compose up -d broker
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_lineage.csv -SingleTest streamkernel_lineage_audit_10m
```

Profile: `config/pipelines/streamkernel_lineage_audit.properties`.

The resulting Kafka records carry `streamkernel.provenance.*` headers for pipeline id, run id, source, sink, transform version, feature version, model contract labels, config hash, and security mode.
