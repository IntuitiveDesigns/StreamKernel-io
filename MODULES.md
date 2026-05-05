# Modules

## Public SDK

- `streamkernel-api`: shared event and contract model types.
- `streamkernel-spi`: plugin interfaces, registry contracts, cache/security/DLQ abstractions, and pipeline configuration contracts.
- `streamkernel-metrics/metrics-api`: metrics provider contracts and helpers.

These modules are intended as the stable extension boundary for custom plugins.

## Runtime

- `streamkernel-core`: orchestration, batching, backpressure, provenance, security context, DLQ handoff, and pipeline factory logic.
- `streamkernel-app`: executable bootstrap, configuration preflight, metrics publishing, and lifecycle wiring.
- `streamkernel-kafka`: Kafka transport support and benchmark harness classes.
- `streamkernel-avro`: Avro schema support.

## First-Party Plugins

- `streamkernel-plugins/source-synthetic`: synthetic, Kafka, Pulsar, REST, and Salesforce source plugins.
- `streamkernel-plugins/sink-kafka`: Kafka and Kafka Avro sinks.
- `streamkernel-plugins/sink-dlq-kafka`: Kafka DLQ sinks and DLQ serializers.
- `streamkernel-plugins/sink-mongo-insert`: MongoDB insert sink.
- `streamkernel-plugins/sink-mongo-vector`: MongoDB vector sink.
- `streamkernel-plugins/sink-delta`: Delta Lake sink.
- `streamkernel-plugins/sink-snowflake`: Snowflake Snowpipe Streaming sink.
- `streamkernel-plugins/transform-etl`: ETL and enrichment transforms.
- `streamkernel-plugins/transformer-string-to-wireevent`: string-to-WireEvent adapter.
- `streamkernel-plugins/http-embedding-stub`: HTTP embedding transformer stub for comparisons.
- `streamkernel-plugins/cache-local`, `cache-redis`, and `cache-noop`: cache implementations.
- `streamkernel-plugins/security-opa`: OPA security integration.

## Configuration And Operations

- `config/pipelines`: runnable profile properties.
- `config/opa` and `policies`: Rego policy examples.
- `config/prometheus`: observability examples.
- `scripts` and `tools`: benchmark, demo, Spark, Snowflake, and local support utilities.
- `benchmark-runs`: preserved CSV evidence for published benchmark runs.
