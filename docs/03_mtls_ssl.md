# 03 - mTLS (SSL) Kafka Connectivity

Validates:

- Broker listens on `localhost:9093`.
- Client keystore/truststore files are readable.
- StreamKernel can produce through the SSL listener.

## 1) Generate Local Kafka Certs

The Docker Compose broker always configures SSL listeners. Generate the local
development keystores before starting `broker`, even for profiles that use the
PLAINTEXT listener:

```bash
bash scripts/gen-certs.sh
```

On Windows with Git Bash:

```powershell
$env:MSYS_NO_PATHCONV = "1"
& "C:\Program Files\Git\bin\bash.exe" scripts/gen-certs.sh
```

The script creates:

- `secrets/kafka.server.keystore.p12`
- `secrets/kafka.client.keystore.p12`
- `secrets/kafka.truststore.p12`
- `secrets/keystore_creds`
- `secrets/key_creds`
- `secrets/truststore_creds`

The password is `changeit` for local development only. The generated files are
ignored by git.

## 2) Restart Broker

```powershell
docker compose up -d --force-recreate broker
docker logs broker --tail 80
```

## 3) Verify Port

```powershell
Test-NetConnection localhost -Port 9093
```

## 4) Verify Keystores

```powershell
keytool -list -storetype PKCS12 -keystore .\secrets\kafka.client.keystore.p12 -storepass changeit
keytool -list -storetype PKCS12 -keystore .\secrets\kafka.truststore.p12 -storepass changeit
```

## 5) Configure StreamKernel

```properties
kafka.broker=localhost:9093
kafka.security.protocol=SSL

kafka.ssl.keystore.location=secrets/kafka.client.keystore.p12
kafka.ssl.truststore.location=secrets/kafka.truststore.p12
kafka.ssl.keystore.type=PKCS12
kafka.ssl.truststore.type=PKCS12
kafka.ssl.keystore.password=changeit
kafka.ssl.truststore.password=changeit
kafka.ssl.key.password=changeit

# For local CN=localhost dev
kafka.ssl.endpoint.identification.algorithm=
```

## 6) Run And Validate

Run StreamKernel, then consume from the primary topic through the PLAINTEXT
listener inside Docker:

```powershell
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```
