# Common Prereqs & Conventions

## Assumptions

- StreamKernel runs on the host (Windows PowerShell, PowerShell 7 on macOS, or
  a POSIX shell for build-only commands).
- Infra runs via Docker Compose in the repo root.
- Container names match `docker-compose.yaml` (for example `broker`, `pulsar`,
  `opa`, `mongodb`).

## Local Kafka Certs

Run this once before starting Kafka-backed profiles:

```bash
bash scripts/gen-certs.sh
```

On Windows with Git Bash:

```powershell
$env:MSYS_NO_PATHCONV = "1"
& "C:\Program Files\Git\bin\bash.exe" scripts/gen-certs.sh
```

The script creates local development keystores plus the Confluent `*_creds`
files required by the compose broker. This is needed by mTLS rows and by
PLAINTEXT rows too, because the shared broker service still starts SSL
listeners.

## macOS / Apple Silicon

`docker-compose.yaml` defaults `STREAMKERNEL_DOCKER_PLATFORM` to `linux/amd64`
so Docker Desktop can run the local benchmark images on Apple Silicon through
emulation. Native ARM64 can be tested by overriding the variable, but only after
verifying every enabled image supports it.

## Common Ports

- Kafka PLAINTEXT (host): `localhost:9092`
- Kafka SSL/mTLS (host): `localhost:9093`
- Kafka inside Docker: `broker:29092`
- OPA API: `http://localhost:8181`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Schema Registry: `http://localhost:8081`
- MongoDB: `mongodb://localhost:27017`

## Verify Containers

```powershell
docker compose ps
docker logs broker --tail 50
docker logs opa --tail 50
```

## Topic Commands

List topics:
```powershell
docker exec -it broker kafka-topics --bootstrap-server broker:29092 --list
```

Describe a topic:
```powershell
docker exec -it broker kafka-topics --bootstrap-server broker:29092 --describe --topic arena-bench-test
```

Consume from a topic:
```powershell
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```

## PowerShell curl

Use `curl.exe` for consistent behavior:

```powershell
@'
{"input":{"user":"service-account-1","action":"write","resource":"arena-bench-test"}}
'@ | Out-File -Encoding ascii .\opa_input.json

curl.exe -s -X POST "http://localhost:8181/v1/data/streamkernel/authz/allow" `
  -H "Content-Type: application/json" `
  --data-binary "@opa_input.json"
```

## Secrets Handling

Do not commit private keys/keystores. Add to `.gitignore`:
- `secrets/`
- `*.jks`
- `*.p12`
- `*_creds`
- `password.txt`
