# 05 - Prometheus

Validates:
- StreamKernel exports Prometheus metrics
- Prometheus scrapes

## 1) Run infra

```powershell
docker compose up -d
```

## 2) Run StreamKernel with Prometheus

```properties
metrics.provider=PROMETHEUS
metrics.prometheus.enabled=true
metrics.prometheus.bind.address=127.0.0.1
metrics.prometheus.port=8080
```

Run:
```powershell
java -Xms4g -Xmx4g -XX:+UseZGC -XX:+ZGenerational -jar .\build\libs\StreamKernel-0.0.1-SNAPSHOT-all.jar
```

## 3) Verify metrics endpoint

```powershell
curl.exe -s http://localhost:8080/metrics | Select-String streamkernel
```

For shared environments, protect the scrape endpoint:

```properties
metrics.prometheus.auth.enabled=true
metrics.prometheus.auth.bearer.token=${env:STREAMKERNEL_PROMETHEUS_BEARER_TOKEN}
metrics.prometheus.allowed.remote.addresses=127.0.0.1
```

Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: streamkernel
    metrics_path: /metrics
    scheme: http
    bearer_token: ${STREAMKERNEL_PROMETHEUS_BEARER_TOKEN}
    static_configs:
      - targets: ["localhost:8080"]
```

For HTTPS or mTLS, configure:

```properties
metrics.prometheus.tls.enabled=true
metrics.prometheus.tls.keystore.path=/etc/streamkernel/tls/metrics.p12
metrics.prometheus.tls.keystore.password=${env:STREAMKERNEL_PROMETHEUS_TLS_KEYSTORE_PASSWORD}
metrics.prometheus.tls.key.password=${env:STREAMKERNEL_PROMETHEUS_TLS_KEY_PASSWORD}
metrics.prometheus.tls.client.auth=NEED
metrics.prometheus.tls.truststore.path=/etc/streamkernel/tls/prometheus-clients.p12
metrics.prometheus.tls.truststore.password=${env:STREAMKERNEL_PROMETHEUS_TLS_TRUSTSTORE_PASSWORD}
```

## 4) UI

- Prometheus: http://localhost:9090
