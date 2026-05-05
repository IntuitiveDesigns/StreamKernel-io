# 19 - Configuration Reference

This is the operator reference for the public StreamKernel pipeline set.

There are two configuration layers:

1. `config/pipelines/*.properties` defines the pipeline itself.
2. `test-java-runner.ps1` can overlay benchmark-only JVM and runtime settings from a CSV row.

## Core Pipeline Keys

| Key | Purpose |
|---|---|
| `pipeline.id` | Human-readable pipeline identifier. |
| `pipeline.parallelism` | Worker parallelism for the orchestrator. |
| `pipeline.batch.size` | Batch size pulled from the source and passed downstream. |
| `pipeline.drain.timeout.ms` | Shutdown drain timeout. |
| `pipeline.source.backoff.initial.ms` | Initial backoff for source retry loops. |
| `pipeline.source.backoff.max.ms` | Max backoff for source retry loops. |
| `pipeline.source.fail.fast` | Fail immediately instead of retrying source startup. |

## Sources

| Key | Purpose |
|---|---|
| `source.type` | Source plugin selector such as `SYNTHETIC`, `KAFKA`, or `PULSAR`. |
| `source.unsafe.reuse.batch` | Enables batch object reuse for high-throughput scenarios. |
| `source.synthetic.payload.size` | Payload size for generated events. |
| `source.kafka.bootstrap.servers` | Kafka bootstrap servers. |
| `source.kafka.topic` | Input topic. |
| `source.kafka.group.id` | Consumer group id. |
| `source.kafka.oidc.enabled` | Enables OIDC auth for Kafka source. |
| `source.pulsar.service.url` | Pulsar broker URL. |
| `source.pulsar.topic` | Pulsar topic. |
| `source.pulsar.subscription` | Subscription name. |

## Transforms

| Key | Purpose |
|---|---|
| `transform.chain` | Comma-separated transformer chain. |
| `transform.type` | Legacy single-transform selector. |
| `transform.version` | Version label for transform lineage. |
| `transform.string_to_wireevent.trim` | Trims input text. |
| `transform.string_to_wireevent.default.key` | Default record key. |
| `transform.http.url` | HTTP transformer endpoint. |
| `transform.deterministic_enrichment.dimension` | Public deterministic vector dimension for Delta/Snowflake demos. |

## Sinks

| Key | Purpose |
|---|---|
| `sink.type` | Sink selector such as `KAFKA`, `DEVNULL`, `MONGODB_INSERT`, `DELTA`, or `SNOWFLAKE_SNOWPIPE_STREAMING`. |
| `sink.plugin.id` | Explicit sink plugin id when needed. |
| `sink.kafka.bootstrap.servers` | Kafka bootstrap servers. |
| `sink.kafka.topic` | Output topic. |
| `sink.kafka.acks` | Kafka durability mode. |
| `sink.kafka.enable.idempotence` | Idempotent producer toggle. |
| `sink.kafka.security.protocol` | Kafka security protocol. |
| `sink.kafka.oidc.enabled` | Enables OIDC auth for Kafka sink. |
| `mongo.uri` / `mongodb.uri` | MongoDB connection string. |
| `mongo.database` / `mongodb.database` | Database name. |
| `mongo.collection` / `mongodb.collection` | Collection name. |
| `delta.table.path` | Delta table path. |
| `delta.s3.endpoint` | S3-compatible endpoint for Delta object storage. |
| `delta.s3.access.key` | S3-compatible access key. |
| `delta.s3.secret.key` | S3-compatible secret key. |
| `snowflake.account` | Snowflake account id. |
| `snowflake.database` | Database name. |
| `snowflake.schema` | Schema name. |
| `snowflake.pipe` | Snowpipe Streaming pipe. |

## Security And Provenance

| Key | Purpose |
|---|---|
| `security.type` | Security provider such as `PERMIT_ALL` or `OPA_SIDECAR`. |
| `security.auth.ttl.ms` | Auth cache TTL. |
| `security.opa.url` | OPA sidecar URL. |
| `security.opa.principal` | Principal claim or identity. |
| `security.opa.action` | Action name for authorization. |
| `security.opa.resource` | Protected resource name. |
| `security.opa.fail.open` | Fail-open toggle. |
| `security.opa.cache.ttl.ms` | OPA decision cache TTL. |
| `streamkernel.provenance.enabled` | Enables per-event provenance stamping; default `false`. |

## Metrics And Benchmarks

| Key | Purpose |
|---|---|
| `metrics.provider` | Metrics backend selector. |
| `metrics.prometheus.enabled` | Prometheus exporter toggle. |
| `metrics.prometheus.port` | Prometheus scrape port. |
| `metrics.prometheus.bind.address` | Bind address for the embedded Prometheus HTTP endpoint. |
| `metrics.prometheus.snapshot.path` | Optional on-close scrape snapshot file path. |
| `metrics.tag.pipeline.id` | Metrics tag for pipeline id. |
| `metrics.tag.profile` | Metrics tag for named profile. |
| `metrics.tag.run_id` | Run identifier tag. |
| `streamkernel.metrics.latency.enabled` | Latency metric toggle. |
| `streamkernel.latency.sample.mask` | Latency sampling mask. |
| `streamkernel.speedometer.enabled` | Console speedometer toggle. |
| `streamkernel.bench.enabled` | Enables benchmark mode. |
| `streamkernel.bench.auto.stop.after.seconds` | Auto-stop duration for benchmark runs. |

## Runner Overrides

When you use `test-java-runner.ps1`, these CSV columns map to runtime overrides:

| CSV Column | Runtime Setting |
|---|---|
| `RunId` | `-Dsk.run.id` |
| `ConfigPath` | `-Dsk.config.path` |
| `Minutes` | `-Dstreamkernel.bench.auto.stop.after.seconds` |
| `LatencySampleMask` | `-Dstreamkernel.latency.sample.mask` |
| `InflightMax` | `-Dstreamkernel.sink.inflight.max` |
| `OutbatchCapacity` | `-Dstreamkernel.outbatch.capacity` |
| `ExecutorMode` | `-Dstreamkernel.executor.mode` |
| `CacheDisabled` | `-Dstreamkernel.cache.force.disabled` |
| `SinkBatchCopy` | `-Dstreamkernel.sink.batch.copy` |

For any published result, keep the pipeline `.properties` file, benchmark CSV
row, and generated benchmark artifacts together.
