## Local Kafka OIDC

This path gives StreamKernel a local OIDC benchmark lane without changing the main `tests.csv` matrix. It uses the existing Kafka source and sink plugins, a Keycloak service in `docker-compose.yaml`, and a dedicated matrix file at `benchmark-runs/tests_oidc.csv`.

### What it covers

- `config/pipelines/streamkernel_synthetic_to_kafka_sink_oidc.properties`
  Synthetic -> Kafka sink over `SASL_PLAINTEXT` + `OAUTHBEARER`
- `config/pipelines/streamkernel_kafka_source_oidc_devnull.properties`
  Kafka source over `SASL_PLAINTEXT` + `OAUTHBEARER` -> DEVNULL
- `streamkernel.provenance.enabled=true`
  Provenance stays on so the same path can back the source-and-sink OIDC/provenance story

### Docker Compose services

`docker-compose.yaml` now includes:

- `keycloak` on `http://localhost:8085`
- an opt-in Kafka OIDC listener on `localhost:9095`

Keycloak imports `tools/keycloak/streamkernel-realm.json` with two confidential clients:

- `streamkernel-kafka-sink` / `streamkernel-sink-secret`
- `streamkernel-kafka-source` / `streamkernel-source-secret`

### Start the local stack

```powershell
docker compose --profile oidc --env-file .\config\kafka\oidc.compose.env up -d keycloak broker schema-registry
```

Wait for Keycloak and the broker to go healthy:

```powershell
docker compose ps
```

### Build StreamKernel

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
```

### Run the OIDC matrix

```powershell
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_oidc.csv
```

The first row writes to the `streamkernel-oidc` topic through the OIDC-enabled sink. The second row consumes that topic through the OIDC-enabled source.

### Run profiles directly

```powershell
$env:SK_RUN_ID = "manual-oidc-sink"
$cfg = (Resolve-Path .\config\pipelines\streamkernel_synthetic_to_kafka_sink_oidc.properties).Path
$jar = (Resolve-Path .\streamkernel-app\build\libs\streamkernel-app-0.0.1-SNAPSHOT-all.jar).Path
& "C:\Program Files\Eclipse Adoptium\jdk-21.0.10.7-hotspot\bin\java.exe" `
  -Xms2g -Xmx2g `
  -Dsk.config.path=$cfg `
  -Dstreamkernel.bench.auto.stop.after.seconds=60 `
  -jar $jar
```

```powershell
$env:SK_RUN_ID = "manual-oidc-source"
$cfg = (Resolve-Path .\config\pipelines\streamkernel_kafka_source_oidc_devnull.properties).Path
$jar = (Resolve-Path .\streamkernel-app\build\libs\streamkernel-app-0.0.1-SNAPSHOT-all.jar).Path
& "C:\Program Files\Eclipse Adoptium\jdk-21.0.10.7-hotspot\bin\java.exe" `
  -Xms2g -Xmx2g `
  -Dsk.config.path=$cfg `
  -Dstreamkernel.bench.auto.stop.after.seconds=60 `
  -jar $jar
```

### Notes

- The default local broker no longer enables the OIDC listener. This keeps the
  plain/mTLS stack healthy when Keycloak is not part of the run. Use
  `config/kafka/oidc.compose.env` whenever you want the OIDC lane.
- This local lane uses `SASL_PLAINTEXT` to keep the OIDC path easy to run inside Docker Compose. Your existing `PLAINTEXT` and mTLS listeners stay in place.
- Keycloak is behind the `oidc` compose profile so the rest of the local stack does not depend on it unless you explicitly enable the OIDC lane.
- The main benchmark matrix does not need to change unless you want these rows in the default suite. Keeping them in `tests_oidc.csv` makes it easy to run or ignore them as needed.
- For a stricter production-style deployment, move the OIDC listener to `SASL_SSL`, add issuer and audience validation, and replace the local Keycloak secrets with mounted secret files.
