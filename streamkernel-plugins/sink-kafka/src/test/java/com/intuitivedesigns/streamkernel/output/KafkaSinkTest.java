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
import com.intuitivedesigns.streamkernel.model.WireEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaSinkTest {

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
    void buildProducerPropertiesPrefersCanonicalSinkKafkaKeys() {
        final Properties props = new Properties();
        props.setProperty("sink.kafka.topic", "tickets");
        props.setProperty("sink.kafka.bootstrap.servers", "localhost:9092");
        props.setProperty("sink.kafka.acks", "all");
        props.setProperty("sink.kafka.linger.ms", "25");
        props.setProperty("sink.kafka.transactional.id", "tx-bench-1");
        props.setProperty("kafka.producer.acks", "1");
        props.setProperty("kafka.producer.linger.ms", "5");

        final Properties built = KafkaSink.buildProducerProperties(PipelineConfig.from(props, "inline"));

        assertEquals("all", built.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals("25", built.getProperty(ProducerConfig.LINGER_MS_CONFIG));
        assertEquals("tx-bench-1", built.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
    }

    @Test
    void buildProducerPropertiesMapsSinkScopedOidcAliases() {
        final Properties props = new Properties();
        props.setProperty("sink.kafka.topic", "tickets");
        props.setProperty("sink.kafka.bootstrap.servers", "localhost:9092");
        props.setProperty("sink.kafka.oidc.enabled", "true");
        props.setProperty("sink.kafka.oidc.token.endpoint.url", "https://issuer.example/token");
        props.setProperty("sink.kafka.oidc.client.id", "producer-client");
        props.setProperty("sink.kafka.oidc.client.secret", "producer-secret");
        props.setProperty("sink.kafka.oidc.scope", "kafka.write");

        final Properties built = KafkaSink.buildProducerProperties(PipelineConfig.from(props, "inline"));

        assertEquals("SASL_SSL", built.getProperty("security.protocol"));
        assertEquals("OAUTHBEARER", built.getProperty("sasl.mechanism"));
        assertEquals("https://issuer.example/token", built.getProperty("sasl.oauthbearer.token.endpoint.url"));
        assertEquals(
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
                built.getProperty("sasl.login.callback.handler.class")
        );
        assertTrue(built.getProperty("sasl.jaas.config").contains("clientId=\"producer-client\""));
        assertTrue(built.getProperty("sasl.jaas.config").contains("clientSecret=\"producer-secret\""));
        assertTrue(built.getProperty("sasl.jaas.config").contains("scope=\"kafka.write\""));
    }

    @Test
    void asyncWriteReleasesPermitWhenSendThrowsSynchronously() throws Exception {
        final AtomicInteger sendAttempts = new AtomicInteger();
        final Producer<String, byte[]> producer = newProducerProxy((record, callback) -> {
            sendAttempts.incrementAndGet();
            throw new KafkaException("boom");
        }, new TransactionCallbacks());
        final KafkaSink sink = newSink(producer, false, false, 1L, 1L, 0L, NOOP_METRICS);

        assertThrows(KafkaException.class, () -> sink.write(PipelinePayload.of("first")));
        assertThrows(KafkaException.class, () -> sink.write(PipelinePayload.of("second")));

        assertEquals(2, sendAttempts.get());
        assertEquals(2L, sink.sentFailTotal());
        assertEquals(0L, sink.inFlightTotal());
    }

    @Test
    void asyncWriteReleasesPermitWhenMetricsBindThrowsAfterAcquire() throws Exception {
        final AtomicInteger sendAttempts = new AtomicInteger();
        final Producer<String, byte[]> producer = newProducerProxy((record, callback) -> {
            sendAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }, new TransactionCallbacks());
        final MetricsRuntime metrics = new MetricsRuntime() {
            @Override
            public Object registry() {
                return this;
            }

            @Override
            public boolean enabled() {
                return true;
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
            public void bind(Object binder) {
                throw new IllegalStateException("bind failed");
            }

            @Override
            public void close() {
            }
        };
        final KafkaSink sink = newSink(producer, false, false, 1L, 10L, 0L, metrics);

        assertThrows(IllegalStateException.class, () -> sink.write(PipelinePayload.of("first")));
        assertThrows(IllegalStateException.class, () -> sink.write(PipelinePayload.of("second")));

        assertEquals(0, sendAttempts.get());
        assertEquals(2L, sink.sentFailTotal());
        assertEquals(0L, sink.inFlightTotal());
    }

    @Test
    void writeUsesWireEventBytes() throws Exception {
        final byte[] expected = new byte[]{1, 2, 3, 4};
        final AtomicReference<byte[]> captured = new AtomicReference<>();
        final Producer<String, byte[]> producer = newProducerProxy((record, callback) -> {
            captured.set(record.value());
            if (callback != null) {
                callback.onCompletion(null, null);
            }
            return CompletableFuture.completedFuture(null);
        }, new TransactionCallbacks());
        final KafkaSink sink = newSink(producer, false, false, 1L, 10L, 0L, NOOP_METRICS);

        sink.write(PipelinePayload.of(WireEvent.bytes(expected, Map.of())));

        assertArrayEquals(expected, captured.get());
        assertEquals(1L, sink.sentOkTotal());
        assertEquals(0L, sink.inFlightTotal());
    }

    @Test
    void writeAddsProvenanceHeadersWithoutCopyingSourceTextMetadata() throws Exception {
        final AtomicReference<ProducerRecord<String, byte[]>> captured = new AtomicReference<>();
        final Producer<String, byte[]> producer = newProducerProxy((record, callback) -> {
            captured.set(record);
            if (callback != null) {
                callback.onCompletion(null, null);
            }
            return CompletableFuture.completedFuture(null);
        }, new TransactionCallbacks());
        final KafkaSink sink = newSink(producer, false, false, 1L, 10L, 0L, NOOP_METRICS);

        sink.write(new PipelinePayload<>(
                "evt-1",
                WireEvent.bytes(new byte[]{1, 2, 3}, Map.of("wire.header", "present"), "ticket-17"),
                Map.of(
                        "streamkernel.provenance.model.name", "risk-model",
                        "streamkernel.provenance.inference.timestamp", "2026-04-17T12:00:00Z",
                        "streamkernel.source.text", "do not promote"
                )
        ));

        final ProducerRecord<String, byte[]> record = captured.get();
        assertEquals("present", headerValue(record, "wire.header"));
        assertEquals("risk-model", headerValue(record, "streamkernel.provenance.model.name"));
        assertEquals("2026-04-17T12:00:00Z", headerValue(record, "streamkernel.provenance.inference.timestamp"));
        assertEquals(null, headerValue(record, "streamkernel.source.text"));
    }

    @Test
    void writeIgnoresNullPayloadAndNullData() throws Exception {
        final AtomicInteger sendAttempts = new AtomicInteger();
        final Producer<String, byte[]> producer = newProducerProxy((record, callback) -> {
            sendAttempts.incrementAndGet();
            if (callback != null) {
                callback.onCompletion(null, null);
            }
            return CompletableFuture.completedFuture(null);
        }, new TransactionCallbacks());
        final KafkaSink sink = newSink(producer, false, false, 1L, 10L, 0L, NOOP_METRICS);

        assertDoesNotThrow(() -> sink.write(null));
        assertDoesNotThrow(() -> sink.write(new PipelinePayload<>("null-data", null)));

        assertEquals(0, sendAttempts.get());
        assertEquals(0L, sink.inFlightTotal());
    }

    @Test
    void transactionalBatchBeginsAndCommitsOneTransaction() throws Exception {
        final TransactionCallbacks tx = new TransactionCallbacks();
        final AtomicInteger sendAttempts = new AtomicInteger();
        final Producer<String, byte[]> producer = newProducerProxy((record, callback) -> {
            sendAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }, tx);
        final KafkaSink sink = newSink(producer, false, true, 10L, 10L, 0L, NOOP_METRICS);

        sink.writeBatch(List.of(PipelinePayload.of("first"), PipelinePayload.of("second")));

        assertEquals(1, tx.beginCalls.get());
        assertEquals(1, tx.commitCalls.get());
        assertEquals(0, tx.abortCalls.get());
        assertEquals(2, sendAttempts.get());
        assertEquals(2L, sink.sentOkTotal());
        assertEquals(0L, sink.inFlightTotal());
    }

    @Test
    void transactionalBatchCountsOnlySentRecordsAsSuccessful() throws Exception {
        final TransactionCallbacks tx = new TransactionCallbacks();
        final AtomicInteger sendAttempts = new AtomicInteger();
        final Producer<String, byte[]> producer = newProducerProxy((record, callback) -> {
            sendAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }, tx);
        final KafkaSink sink = newSink(producer, false, true, 10L, 10L, 0L, NOOP_METRICS);

        sink.writeBatch(Arrays.asList(
                PipelinePayload.of("first"),
                null,
                new PipelinePayload<>("null-data", null),
                PipelinePayload.of("second")
        ));

        assertEquals(1, tx.beginCalls.get());
        assertEquals(1, tx.commitCalls.get());
        assertEquals(0, tx.abortCalls.get());
        assertEquals(2, sendAttempts.get());
        assertEquals(2L, sink.sentOkTotal());
        assertEquals(0L, sink.inFlightTotal());
    }

    @Test
    void transactionalBatchAbortsWhenSendFails() throws Exception {
        final TransactionCallbacks tx = new TransactionCallbacks();
        final AtomicInteger sendAttempts = new AtomicInteger();
        final Producer<String, byte[]> producer = newProducerProxy((record, callback) -> {
            if (sendAttempts.incrementAndGet() == 2) {
                throw new KafkaException("send failed");
            }
            return CompletableFuture.completedFuture(null);
        }, tx);
        final KafkaSink sink = newSink(producer, false, true, 10L, 10L, 0L, NOOP_METRICS);

        assertThrows(
                KafkaException.class,
                () -> sink.writeBatch(List.of(PipelinePayload.of("first"), PipelinePayload.of("second")))
        );

        assertEquals(1, tx.beginCalls.get());
        assertEquals(0, tx.commitCalls.get());
        assertEquals(1, tx.abortCalls.get());
        assertEquals(2L, sink.sentFailTotal());
        assertEquals(0L, sink.inFlightTotal());
    }

    @SuppressWarnings("unchecked")
    private static Producer<String, byte[]> newProducerProxy(SendBehavior behavior, TransactionCallbacks tx) {
        return (Producer<String, byte[]>) Proxy.newProxyInstance(
                KafkaProducer.class.getClassLoader(),
                new Class<?>[]{Producer.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "send" -> behavior.send(
                            (ProducerRecord<String, byte[]>) args[0],
                            (args.length > 1) ? (Callback) args[1] : null
                    );
                    case "metrics" -> Map.of();
                    case "partitionsFor" -> List.of();
                    case "flush", "close", "sendOffsetsToTransaction" -> null;
                    case "initTransactions" -> {
                        tx.initCalls.incrementAndGet();
                        yield null;
                    }
                    case "beginTransaction" -> {
                        tx.beginCalls.incrementAndGet();
                        yield null;
                    }
                    case "commitTransaction" -> {
                        tx.commitCalls.incrementAndGet();
                        yield null;
                    }
                    case "abortTransaction" -> {
                        tx.abortCalls.incrementAndGet();
                        yield null;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                }
        );
    }

    private static KafkaSink newSink(Producer<String, byte[]> producer,
                                     boolean sync,
                                     boolean transactional,
                                     long inflightLimit,
                                     long inflightAcquireTimeoutMs,
                                     long inflightGaugeEveryN,
                                     MetricsRuntime metrics) throws Exception {
        final Constructor<KafkaSink> ctor = KafkaSink.class.getDeclaredConstructor(
                Producer.class,
                String.class,
                boolean.class,
                boolean.class,
                long.class,
                long.class,
                long.class,
                MetricsRuntime.class
        );
        ctor.setAccessible(true);
        return ctor.newInstance(
                producer,
                "tickets",
                sync,
                transactional,
                inflightLimit,
                inflightAcquireTimeoutMs,
                inflightGaugeEveryN,
                metrics
        );
    }

    private static String headerValue(ProducerRecord<String, byte[]> record, String key) {
        final Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface SendBehavior {
        Future<RecordMetadata> send(ProducerRecord<String, byte[]> record, Callback callback) throws Exception;
    }

    private static final class TransactionCallbacks {
        private final AtomicInteger initCalls = new AtomicInteger();
        private final AtomicInteger beginCalls = new AtomicInteger();
        private final AtomicInteger commitCalls = new AtomicInteger();
        private final AtomicInteger abortCalls = new AtomicInteger();
    }
}
