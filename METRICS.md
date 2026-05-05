# Metrics

Last reviewed: 2026-04-29.

This file documents metrics emitted by the current StreamKernel codebase. Code
call sites are the source of truth.

## Naming Rules

- Counters are monotonic totals. Use `rate(metric[window])` in Prometheus for
  per-second rates.
- Gauges are current values. Use the raw sample value in Prometheus/Grafana.
- Timers are recorded in milliseconds through `MetricsRuntime.timer(...)`.
  Micrometer Prometheus exports timers with `_seconds_count`,
  `_seconds_sum`, and `_seconds_max` suffixes.
- Dotted Micrometer names are exported by Prometheus with underscores. For
  example, `streamkernel.mongo.sink.records.total` is scraped as
  `streamkernel_mongo_sink_records_total`.

## Pipeline Runtime

Emitted by `streamkernel-app`.

Counters:

- `streamkernel_pipeline_processed_total`
- `streamkernel_pipeline_out_total`
- `streamkernel_pipeline_in_total`
- `streamkernel_pipeline_dropped_total`
- `streamkernel_pipeline_denied_total`
- `streamkernel_pipeline_dlq_total`
- `streamkernel_pipeline_empty_batch_total`
- `streamkernel_pipeline_source_errors_total`
- `streamkernel_pipeline_auth_errors_total`
- `streamkernel_pipeline_dlq_errors_total`

Gauges:

- `streamkernel_pipeline_inflight_batches`
- `streamkernel_pipeline_inflight_records`
- `streamkernel_pipeline_load_percent`
- `streamkernel_pipeline_latency_p50_ms`
- `streamkernel_pipeline_latency_p95_ms`
- `streamkernel_pipeline_latency_p99_ms`
- `streamkernel_pipeline_latency_p999_ms`
- `streamkernel_pipeline_latency_max_ms`
- `streamkernel_pipeline_latency_samples`
- `streamkernel_jvm_heap_used_mb`
- `streamkernel_pipeline_up_<sanitized_pipeline_id>`

## Prometheus Provider

The Prometheus metrics provider also registers JVM binders and scrape endpoint
health metrics.

Provider metrics:

- `streamkernel_process_start_time_seconds`
- `streamkernel_metrics_scrape_total`
- `streamkernel_metrics_scrape_failures_total`
- `streamkernel_metrics_scrape_duration_ms` timer
- `streamkernel_metrics_scrape_last_body_bytes`
- `streamkernel_metrics_auth_failures_total`
- `streamkernel_metrics_ip_denied_total`
- `streamkernel_metrics_gauge_invalid_total`

JVM binders emit the standard Micrometer JVM families, including
`jvm_memory_*`, `jvm_threads_*`, `jvm_gc_*`, and `jvm_classes_*`.

## Security / OPA

Emitted by the OPA sidecar security provider.

Counters:

- `opa_cache_hit_total`
- `opa_cache_miss_total`
- `opa_call_error_total`
- `opa_call_success_total`
- `opa_allow_total`
- `opa_deny_total`
- `opa_cache_expired_evictions_total`
- `opa_cache_flush_total`
- `opa_http_status_total`
- `opa_parse_error_total`
- `opa_error_log_suppressed_total`

Timers:

- `opa_request_latency_ms`

## Sinks

### Kafka Sink

Code names:

- `streamkernel.kafka.sink.sent.ok.total`
- `streamkernel.kafka.sink.sent.fail.total`
- `streamkernel.kafka.sink.send.ms` timer
- `streamkernel.kafka.sink.inflight`
- `streamkernel.kafka.sink.inflight.acquire.timeout.total`
- `streamkernel.kafka.sink.buffer.available.bytes`
- `streamkernel.kafka.sink.buffer.total.bytes`
- `streamkernel.kafka.sink.record.queue.time.avg.ms`
- `streamkernel.kafka.sink.request.latency.avg.ms`
- `streamkernel.kafka.sink.throttle.time.avg.ms`

Kafka Avro sink defaults:

- `sink.kafka.messages.written`
- `sink.kafka.errors`
- `sink.kafka.serialization.errors`

### Mongo Vector Sink

Code names:

- `streamkernel.mongo.sink.latency.ms` timer
- `streamkernel.mongo.sink.batches.total`
- `streamkernel.mongo.sink.records.seen.total`
- `streamkernel.mongo.sink.records.attempted.total`
- `streamkernel.mongo.sink.records.persistable.total`
- `streamkernel.mongo.sink.records.unsupported.total`
- `streamkernel.mongo.sink.batches.empty.total`
- `streamkernel.mongo.sink.batches.ok.total`
- `streamkernel.mongo.sink.records.total`
- `streamkernel.mongo.sink.flushes.total`
- `streamkernel.mongo.sink.ops.total`
- `streamkernel.mongo.sink.errors.total`
- `streamkernel.mongo.sink.records.dropped.total`
- `streamkernel.mongo.sink.flush.ms` timer
- `streamkernel.mongo.sink.payload.unsupported.total`
- `streamkernel.mongo.sink.payload.invalid.total`

### Mongo Insert Sink

Code names:

- `streamkernel.mongo.insert.sink.latency.ms` timer
- `streamkernel.mongo.insert.sink.batches.total`
- `streamkernel.mongo.insert.sink.records.seen.total`
- `streamkernel.mongo.insert.sink.records.skipped.total`
- `streamkernel.mongo.insert.sink.batches.ok.total`
- `streamkernel.mongo.insert.sink.batches.empty.total`
- `streamkernel.mongo.insert.sink.flushes.total`
- `streamkernel.mongo.insert.sink.records.total`
- `streamkernel.mongo.insert.sink.errors.total`
- `streamkernel.mongo.insert.sink.flush.ms` timer
- `streamkernel.mongo.insert.sink.records.skipped.type.<type>.total`

### Delta Sink

Code names:

- `streamkernel.delta.sink.rows.total`
- `streamkernel.delta.sink.batches.total`
- `streamkernel.delta.sink.batches.empty.total`
- `streamkernel.delta.sink.batches.skipped.total`
- `streamkernel.delta.sink.payload.unsupported.total`
- `streamkernel.delta.sink.commit.ms` timer

### Snowflake Sink

Code names:

- `streamkernel.snowflake.sink.batches.total`
- `streamkernel.snowflake.sink.batches.empty.total`
- `streamkernel.snowflake.sink.rows.total`
- `streamkernel.snowflake.sink.commit.ms` timer
- `streamkernel.snowflake.sink.error.total`

### Postgres Sink

Code names:

- `sink.postgres.written`
- `sink.postgres.latency` timer
- `sink.postgres.errors`

## Sources And Transformers

Source metrics:

- `streamkernel.source.pulsar.read.total`
- `streamkernel.source.pulsar.error.total`
- `source.salesforce.records`
- `source.salesforce.errors`

Transformer metrics:

- `sk_string_to_wireevent_dropped_null_total`
- `sk_wireevent_string_encode_ms` timer
- `sk_wireevent_string_encode_bytes`
- `sk_wireevent_vector_encode_ms` timer
- `sk_wireevent_vector_encode_bytes`
- `sk_wireevent_vector_dims`
- `etl.records.processed`
- `etl.pii.masked`
- `ai.enrichment.errors`
- `ai.enrichment.latency` timer

## Dashboard Notes

Known dashboard caveats as of 2026-04-29:

- `streamkernel_pipeline_dlq_routed_total` is not emitted; use
  `streamkernel_pipeline_dlq_total`.
- `streamkernel_sink_snowflake_write_total` and
  `streamkernel_sink_snowflake_error_total` are old dashboard names; the
  Snowflake sink emits the `streamkernel.snowflake.sink.*` family, exported to
  Prometheus as `streamkernel_snowflake_sink_*`.
- `streamkernel_sink_databricks_*` appears only as a placeholder in older
  dashboards; the current codebase uses the Delta sink for local lakehouse
  workflows.
