/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaAvroSinkTest {

    private static final MetricsRuntime NOOP_METRICS = new MetricsRuntime() {
        @Override
        public Object registry() {
            return this;
        }

        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public void counter(String name, double increment) {
        }

        @Override
        public void timer(String name, long durationMillis) {
        }

        @Override
        public void gauge(String name, double value) {
        }

        @Override
        public void close() {
        }
    };

    @Test
    void buildProducerPropsRejectsInvalidSchemaRegistryUrl() {
        final Properties props = new Properties();
        props.setProperty("schema.registry.url", "   ");

        assertThrows(
                IllegalArgumentException.class,
                () -> KafkaAvroSink.buildProducerProps(PipelineConfig.from(props, "inline"))
        );
    }

    @Test
    void buildProducerPropsRejectsIdempotentMaxInFlightAboveFive() {
        final Properties props = new Properties();
        props.setProperty("schema.registry.url", "http://localhost:8081");
        props.setProperty("sink.kafka.producer.max.in.flight", "6");

        assertThrows(
                IllegalArgumentException.class,
                () -> KafkaAvroSink.buildProducerProps(PipelineConfig.from(props, "inline"))
        );
    }

    @Test
    void loadSchemaSupportsFilesystemPath() throws Exception {
        final Path schemaFile = Files.createTempFile("kafka-avro-sink-", ".avsc");
        Files.writeString(schemaFile, customerEventSchemaJson(), StandardCharsets.UTF_8);

        final Schema schema = KafkaAvroSink.loadSchema(schemaFile.toString());

        assertEquals("CustomerEvent", schema.getName());
        Files.deleteIfExists(schemaFile);
    }

    @Test
    void validateDefaultMapperSchemaRejectsUnmappedSchema() {
        final Schema unrelated = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "UnrelatedEvent",
                  "fields": [
                    { "name": "foo", "type": "string" }
                  ]
                }
                """);

        assertThrows(IllegalArgumentException.class, () -> KafkaAvroSink.validateDefaultMapperSchema(unrelated));
    }

    @Test
    void writeThrowsWhenProducerFutureFails() throws Exception {
        final AtomicInteger sendAttempts = new AtomicInteger();
        final Producer<String, GenericRecord> producer = newProducerProxy((record, callback) -> {
            sendAttempts.incrementAndGet();
            final CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(new KafkaException("broker down"));
            return future;
        });
        final KafkaAvroSink sink = newSink(producer);

        assertThrows(KafkaException.class, () -> sink.write(PipelinePayload.of("hello")));
        assertEquals(1, sendAttempts.get());
    }

    @Test
    void writeAddsProvenanceHeaders() throws Exception {
        final AtomicReference<ProducerRecord<String, GenericRecord>> captured = new AtomicReference<>();
        final Producer<String, GenericRecord> producer = newProducerProxy((record, callback) -> {
            captured.set(record);
            return CompletableFuture.completedFuture(null);
        });
        final KafkaAvroSink sink = newSink(producer);

        sink.write(new PipelinePayload<>(
                "evt-1",
                "customer-name",
                Map.of(
                        "streamkernel.provenance.model.name", "risk-model",
                        "streamkernel.provenance.inference.timestamp", "2026-04-18T12:00:00Z",
                        "streamkernel.source.text", "do not promote"
                )
        ));

        final ProducerRecord<String, GenericRecord> record = captured.get();
        assertEquals("risk-model", headerValue(record, "streamkernel.provenance.model.name"));
        assertEquals("2026-04-18T12:00:00Z", headerValue(record, "streamkernel.provenance.inference.timestamp"));
        assertEquals(null, headerValue(record, "streamkernel.source.text"));
    }

    @Test
    void closeFlushesBeforeCloseAndRestoresInterruptFlag() {
        final AtomicReference<List<String>> callOrder = new AtomicReference<>(new java.util.ArrayList<>());
        final Producer<String, GenericRecord> producer = newProducerProxy((record, callback) ->
                CompletableFuture.completedFuture(null), callOrder);
        final KafkaAvroSink sink = newSink(producer);

        assertDoesNotThrow(sink::close);

        assertEquals(List.of("flush", "close"), callOrder.get());
        assertTrue(Thread.currentThread().isInterrupted());
        Thread.interrupted();
    }

    @SuppressWarnings("unchecked")
    private static Producer<String, GenericRecord> newProducerProxy(SendBehavior behavior) {
        return newProducerProxy(behavior, new AtomicReference<>(new java.util.ArrayList<>()));
    }

    @SuppressWarnings("unchecked")
    private static Producer<String, GenericRecord> newProducerProxy(
            SendBehavior behavior,
            AtomicReference<List<String>> closeOrder) {
        return (Producer<String, GenericRecord>) Proxy.newProxyInstance(
                KafkaProducer.class.getClassLoader(),
                new Class<?>[]{Producer.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "send" -> behavior.send(
                            (ProducerRecord<String, GenericRecord>) args[0],
                            (args.length > 1) ? (Callback) args[1] : null
                    );
                    case "flush" -> {
                        closeOrder.get().add("flush");
                        yield null;
                    }
                    case "close" -> {
                        closeOrder.get().add("close");
                        if (args != null && args.length == 1) {
                            throw new KafkaException("interrupted", new InterruptedException("stop"));
                        }
                        yield null;
                    }
                    case "metrics" -> Map.of();
                    case "partitionsFor" -> List.of();
                    default -> throw new UnsupportedOperationException(method.getName());
                }
        );
    }

    private static KafkaAvroSink newSink(Producer<String, GenericRecord> producer) {
        return new KafkaAvroSink(
                producer,
                "tickets",
                customerEventSchema(),
                NOOP_METRICS,
                defaultMapper(),
                1_000L,
                "sink.kafka.messages.written",
                "sink.kafka.errors",
                "sink.kafka.serialization.errors"
        );
    }

    private static BiConsumer<PipelinePayload<String>, GenericRecord> defaultMapper() {
        return (payload, record) -> {
            if (payload == null || record == null) return;
            record.put("customerId", payload.id());
            record.put("name", payload.data());
            if (payload.timestamp() != null) {
                record.put("timestamp", payload.timestamp().toEpochMilli());
            }
        };
    }

    private static Schema customerEventSchema() {
        return new Schema.Parser().parse(customerEventSchemaJson());
    }

    private static String customerEventSchemaJson() {
        return """
                {
                  "type": "record",
                  "name": "CustomerEvent",
                  "namespace": "com.intuitivedesigns.streamkernel.test",
                  "fields": [
                    { "name": "customerId", "type": ["null", "string"], "default": null },
                    { "name": "name", "type": ["null", "string"], "default": null },
                    { "name": "timestamp", "type": ["null", "long"], "default": null }
                  ]
                }
                """;
    }

    private static String headerValue(ProducerRecord<String, GenericRecord> record, String key) {
        if (record == null || record.headers() == null) {
            return null;
        }
        final var header = record.headers().lastHeader(key);
        return (header == null) ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface SendBehavior {
        Future<RecordMetadata> send(ProducerRecord<String, GenericRecord> record, Callback callback) throws Exception;
    }
}
