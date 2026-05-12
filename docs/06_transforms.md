# 06 — Transformers

Validates transformer selection.

Before starting the local Kafka stack, run `bash scripts/gen-certs.sh`. The
broker configures SSL listeners for several profiles, so the generated keystore
and credential files need to exist even when this walkthrough uses PLAINTEXT.

## NOOP

```properties
transform.type=NOOP
```

## UPPER

```properties
transform.type=UPPER
```

Validate by consuming from the sink topic:

```powershell
docker exec -it broker kafka-console-consumer --bootstrap-server broker:29092 `
  --topic arena-bench-test --from-beginning --max-messages 5 `
  --property print.key=true --property key.separator=" | "
```
