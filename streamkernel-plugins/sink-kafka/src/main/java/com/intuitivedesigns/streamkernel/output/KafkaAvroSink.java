/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.PipelineProvenance;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

/**
 * Kafka sink that maps {@link PipelinePayload} string payloads into Avro {@link GenericRecord}s
 * and publishes them through Schema Registry.
 *
 * Delivery guarantee:
 * writes are synchronous at the sink boundary and throw on terminal producer failures, which lets
 * the StreamKernel orchestrator apply its normal retry and DLQ policy. With the default producer
 * settings this is an at-least-once sink with idempotence enabled inside a producer session; it is
 * not a transactional exactly-once sink.
 *
 * Broker compatibility:
 * the default {@code max.in.flight.requests.per.connection=5} assumes modern Kafka brokers.
 * Operators targeting older brokers should lower that value to {@code 1}.
 *
 * Thread-safety:
 * this class is thread-safe when the configured mapper is stateless or otherwise thread-safe. The
 * default mapper created by {@link #fromConfig(PipelineConfig, String, MetricsRuntime)} is stateless.
 */
public final class KafkaAvroSink implements OutputSink<String>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaAvroSink.class);

    // ---- Config keys (sink-scoped) ----
    private static final String CFG_SCHEMA_PATH = "sink.avro.schema.path";
    private static final String CFG_SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static final String CFG_METRIC_WRITTEN = "sink.kafka.messages.written";
    private static final String CFG_METRIC_WRITE_ERRORS = "sink.kafka.write.errors";
    private static final String CFG_METRIC_SERIALIZE_ERRORS = "sink.kafka.serialize.errors";
    private static final String CFG_ERROR_LOG_INTERVAL_MS = "sink.kafka.error.log.interval.ms";

    // Kafka producer tuning (sink-scoped)
    private static final String CFG_PRODUCER_COMPRESSION = "sink.kafka.producer.compression";
    private static final String CFG_PRODUCER_BATCH_SIZE = "sink.kafka.producer.batch.size";
    private static final String CFG_PRODUCER_LINGER_MS = "sink.kafka.producer.linger.ms";
    private static final String CFG_PRODUCER_BUFFER_MEMORY = "sink.kafka.producer.buffer.memory";
    private static final String CFG_PRODUCER_DELIVERY_TIMEOUT_MS = "sink.kafka.producer.delivery.timeout.ms";
    private static final String CFG_PRODUCER_MAX_IN_FLIGHT = "sink.kafka.producer.max.in.flight";
    private static final String CFG_PRODUCER_CLIENT_ID = "sink.kafka.producer.client.id";

    // Kafka common
    private static final String CFG_KAFKA_BROKER = "kafka.broker";
    private static final String CFG_PIPELINE_NAME = "pipeline.name";

    // Defaults
    private static final String DEFAULT_SCHEMA_PATH = "schema.avsc";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final long DEFAULT_ERROR_LOG_INTERVAL_MS = 1000L;

    private static final String DEFAULT_COMPRESSION = "lz4";
    private static final int DEFAULT_BATCH_SIZE = 65_536;
    private static final int DEFAULT_LINGER_MS = 5;
    private static final long DEFAULT_BUFFER_MEMORY = 33_554_432L;
    private static final int DEFAULT_DELIVERY_TIMEOUT_MS = 120_000;
    private static final int DEFAULT_MAX_IN_FLIGHT = 5;

    // ---- State ----
    private final Producer<String, GenericRecord> producer;
    private final String topic;
    private final Schema schema;
    private final MetricsRuntime metrics;
    private final BiConsumer<PipelinePayload<String>, GenericRecord> mapper;

    // Rate-limited error logging
    private final long errorLogIntervalMs;
    private final AtomicLong lastErrorLogMs = new AtomicLong(0);
    private final LongAdder suppressedErrors = new LongAdder();

    private final String metricWritten;
    private final String metricWriteErrors;
    private final String metricSerializeErrors;

    private KafkaAvroSink(String topic,
                          Properties props,
                          Schema schema,
                          MetricsRuntime metrics,
                          BiConsumer<PipelinePayload<String>, GenericRecord> mapper,
                          long errorLogIntervalMs,
                          String metricWritten,
                          String metricWriteErrors,
                          String metricSerializeErrors) {
        this(
                new KafkaProducer<>(Objects.requireNonNull(props, "props")),
                topic,
                schema,
                metrics,
                mapper,
                errorLogIntervalMs,
                metricWritten,
                metricWriteErrors,
                metricSerializeErrors
        );
    }

    KafkaAvroSink(Producer<String, GenericRecord> producer,
                  String topic,
                  Schema schema,
                  MetricsRuntime metrics,
                  BiConsumer<PipelinePayload<String>, GenericRecord> mapper,
                  long errorLogIntervalMs,
                  String metricWritten,
                  String metricWriteErrors,
                  String metricSerializeErrors) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.producer = Objects.requireNonNull(producer, "producer");
        this.errorLogIntervalMs = Math.max(100L, errorLogIntervalMs);

        this.metricWritten = Objects.requireNonNull(metricWritten, "metricWritten");
        this.metricWriteErrors = Objects.requireNonNull(metricWriteErrors, "metricWriteErrors");
        this.metricSerializeErrors = Objects.requireNonNull(metricSerializeErrors, "metricSerializeErrors");
        log.info("KafkaAvroSink active. topic='{}' schema='{}'", this.topic, this.schema.getFullName());
    }

    public static KafkaAvroSink fromConfig(PipelineConfig config, String topic, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(metrics, "metrics");

        final String schemaPath = config.getString(CFG_SCHEMA_PATH, DEFAULT_SCHEMA_PATH);
        final Schema schema = loadSchema(schemaPath);
        validateDefaultMapperSchema(schema);

        final Properties props = buildProducerProps(config);

        final long logIntervalMs = parseLong(
                config.getString(CFG_ERROR_LOG_INTERVAL_MS, Long.toString(DEFAULT_ERROR_LOG_INTERVAL_MS)),
                DEFAULT_ERROR_LOG_INTERVAL_MS
        );

        final String metricWritten = config.getString(CFG_METRIC_WRITTEN, "sink.kafka.messages.written");
        final String metricWriteErrors = config.getString(CFG_METRIC_WRITE_ERRORS, "sink.kafka.errors");
        final String metricSerializeErrors = config.getString(CFG_METRIC_SERIALIZE_ERRORS, "sink.kafka.serialization.errors");

        // Default mapping: id->customerId, data->name, timestamp->timestamp (if present)
        final BiConsumer<PipelinePayload<String>, GenericRecord> defaultMapper = (payload, record) -> {
            if (payload == null || record == null) return;
            Schema s = record.getSchema();

            if (s.getField("customerId") != null) {
                record.put("customerId", payload.id());
            }
            if (s.getField("name") != null) {
                record.put("name", payload.data());
            }
            if (s.getField("timestamp") != null && payload.timestamp() != null) {
                record.put("timestamp", payload.timestamp().toEpochMilli());
            }
        };

        return new KafkaAvroSink(
                topic,
                props,
                schema,
                metrics,
                defaultMapper,
                logIntervalMs,
                metricWritten,
                metricWriteErrors,
                metricSerializeErrors
        );
    }

    static Properties buildProducerProps(PipelineConfig config) {
        final Properties props = new Properties();

        // Connection + Schema Registry
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(CFG_KAFKA_BROKER, "localhost:9092"));
        props.put(CFG_SCHEMA_REGISTRY_URL,
                validateSchemaRegistryUrl(config.getString(CFG_SCHEMA_REGISTRY_URL, DEFAULT_SCHEMA_REGISTRY_URL)));

        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Client id
        final String pipelineName = config.getString(CFG_PIPELINE_NAME, "StreamKernel");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                config.getString(CFG_PRODUCER_CLIENT_ID, pipelineName + "-KAFKA-AVRO"));

        // Durability-by-default
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                config.getString(CFG_PRODUCER_DELIVERY_TIMEOUT_MS, Integer.toString(DEFAULT_DELIVERY_TIMEOUT_MS)));
        final int maxInFlight = parseInt(
                config.getString(CFG_PRODUCER_MAX_IN_FLIGHT, Integer.toString(DEFAULT_MAX_IN_FLIGHT)),
                DEFAULT_MAX_IN_FLIGHT
        );
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(maxInFlight));

        // Throughput knobs (sink-scoped)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getString(CFG_PRODUCER_COMPRESSION, DEFAULT_COMPRESSION));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getString(CFG_PRODUCER_BATCH_SIZE, Integer.toString(DEFAULT_BATCH_SIZE)));
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.getString(CFG_PRODUCER_LINGER_MS, Integer.toString(DEFAULT_LINGER_MS)));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getString(CFG_PRODUCER_BUFFER_MEMORY, Long.toString(DEFAULT_BUFFER_MEMORY)));

        // Security passthrough: kafka.ssl.*, kafka.security.*, kafka.sasl.* -> strip "kafka."
        copySecurityProps(config, props);
        validateProducerProps(props);

        return props;
    }

    @Override
    public void write(PipelinePayload<String> payload) throws Exception {
        if (payload == null) return;

        final GenericRecord record;
        try {
            record = new GenericData.Record(schema);
            mapper.accept(payload, record);
        } catch (Exception e) {
            metrics.counter(metricSerializeErrors, 1.0);
            logRateLimited("Avro mapping/serialization failed id=" + safeId(payload), e);
            throw new RuntimeException("Avro mapping/serialization failed", e);
        }

        final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topic, payload.id(), record);
        for (Map.Entry<String, String> entry : PipelineProvenance.extractProvenanceHeaders(payload.metadata()).entrySet()) {
            producerRecord.headers().add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
        }

        try {
            producer.send(producerRecord).get();
            metrics.counter(metricWritten, 1.0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.counter(metricWriteErrors, 1.0);
            logRateLimited("KafkaAvroSink write interrupted id=" + safeId(payload), e);
            throw e;
        } catch (ExecutionException e) {
            metrics.counter(metricWriteErrors, 1.0);
            final Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            logRateLimited("KafkaAvroSink write failed id=" + safeId(payload), cause);
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException("KafkaAvroSink write failed", cause);
        } catch (RuntimeException e) {
            metrics.counter(metricWriteErrors, 1.0);
            logRateLimited("KafkaAvroSink write failed id=" + safeId(payload), e);
            throw e;
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        try {
            flush();
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            if (containsInterruptedCause(e)) {
                Thread.currentThread().interrupt();
            }
            log.warn("KafkaAvroSink close failed topic={}", topic, e);
        }
    }

    // ---- Helpers ----

    private void logRateLimited(String context, Throwable ex) {
        final long now = System.currentTimeMillis();
        final long last = lastErrorLogMs.get();

        if (now - last >= errorLogIntervalMs && lastErrorLogMs.compareAndSet(last, now)) {
            final long sup = suppressedErrors.sumThenReset();
            if (sup > 0) {
                log.error("{} (suppressed {} similar errors): {}", context, sup, ex.getMessage());
            } else {
                log.error("{}: {}", context, ex.getMessage());
            }
        } else {
            suppressedErrors.increment();
        }
    }

    private static String safeId(PipelinePayload<?> p) {
        try { return (p == null) ? "null" : String.valueOf(p.id()); } catch (Exception e) { return "unknown"; }
    }

    static Schema loadSchema(String resourcePath) {
        try (InputStream in = openSchemaStream(resourcePath)) {
            return new Schema.Parser().parse(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Avro schema: " + resourcePath, e);
        }
    }

    private static void copySecurityProps(PipelineConfig src, Properties dst) {
        for (String key : src.keys()) {
            if (key == null) continue;
            if (key.startsWith("kafka.ssl.") || key.startsWith("kafka.sasl.") || key.startsWith("kafka.security.")) {
                String v = src.getString(key, null);
                if (v == null) continue;
                String realKey = key.substring("kafka.".length());
                dst.put(realKey, v);
            }
        }
    }

    static void validateDefaultMapperSchema(Schema schema) {
        final boolean hasCustomerId = schema.getField("customerId") != null;
        final boolean hasName = schema.getField("name") != null;
        final boolean hasTimestamp = schema.getField("timestamp") != null;

        if (!hasCustomerId && !hasName && !hasTimestamp) {
            throw new IllegalArgumentException(
                    "KafkaAvroSink default mapper expects at least one of [customerId, name, timestamp] " +
                            "in schema '" + schema.getFullName() + "'");
        }

        final List<String> missing = new ArrayList<>(2);
        if (!hasCustomerId) missing.add("customerId");
        if (!hasName) missing.add("name");
        if (!missing.isEmpty()) {
            log.warn("KafkaAvroSink default mapper schema '{}' is missing field(s) {}; unmatched values will be unset",
                    schema.getFullName(), missing);
        }
    }

    private static String validateSchemaRegistryUrl(String raw) {
        final String value = (raw == null) ? "" : raw.trim();
        final String normalized = value.toLowerCase(Locale.ROOT);
        if (value.isEmpty()
                || "null".equalsIgnoreCase(value)
                || (!normalized.startsWith("http://")
                && !normalized.startsWith("https://")
                && !normalized.startsWith("mock://"))) {
            throw new IllegalArgumentException(
                    "Invalid schema.registry.url: '" + value + "'. Expected http://, https://, or mock:// URL.");
        }
        return value;
    }

    private static void validateProducerProps(Properties props) {
        if (!props.containsKey(CFG_SCHEMA_REGISTRY_URL)) {
            throw new IllegalArgumentException("KafkaAvroSink requires '" + CFG_SCHEMA_REGISTRY_URL + "'");
        }
        final boolean idempotenceEnabled = Boolean.parseBoolean(
                props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false"));
        final int maxInFlight = parseInt(
                props.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                        Integer.toString(DEFAULT_MAX_IN_FLIGHT)),
                DEFAULT_MAX_IN_FLIGHT
        );
        if (idempotenceEnabled && maxInFlight > 5) {
            throw new IllegalArgumentException(
                    "max.in.flight.requests.per.connection must be <= 5 when idempotence is enabled");
        }
    }

    private static InputStream openSchemaStream(String resourcePath) throws Exception {
        final String value = (resourcePath == null) ? "" : resourcePath.trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Schema path must not be blank");
        }
        if (value.regionMatches(true, 0, "classpath:", 0, "classpath:".length())) {
            final String classpathResource = value.substring("classpath:".length()).replaceFirst("^/+", "");
            return openClasspathSchema(classpathResource, resourcePath);
        }
        if (value.regionMatches(true, 0, "file:", 0, "file:".length())) {
            final Path filePath = resolveFileUri(value);
            if (!Files.exists(filePath)) {
                throw new IllegalArgumentException("Schema file not found: " + filePath);
            }
            return Files.newInputStream(filePath);
        }

        final Path filePath = Paths.get(value).toAbsolutePath().normalize();
        if (Files.exists(filePath)) {
            return Files.newInputStream(filePath);
        }

        return openClasspathSchema(value.replaceFirst("^/+", ""), resourcePath);
    }

    private static InputStream openClasspathSchema(String resourcePath, String originalValue) {
        final InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
        if (in == null) {
            throw new IllegalArgumentException(
                    "Schema file not found on filesystem or classpath: " + originalValue);
        }
        return in;
    }

    private static Path resolveFileUri(String fileUri) {
        try {
            return Paths.get(URI.create(fileUri)).toAbsolutePath().normalize();
        } catch (Exception ignored) {
            return Paths.get(URI.create(
                    fileUri.replaceFirst("(?i)^file:(?!///)", "file:///")
            )).toAbsolutePath().normalize();
        }
    }

    private static boolean containsInterruptedCause(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof InterruptedException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static int parseInt(String s, int def) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return def; }
    }

    private static long parseLong(String s, long def) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return def; }
    }
}
