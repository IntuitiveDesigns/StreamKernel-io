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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoInsertSinkTest {

    @Test
    void initFailureRemainsRetryable() throws Exception {
        final MongoInsertSink sink = newSink(Map.of(
                "mongodb.uri", "mongodb://127.0.0.1:1",
                "mongodb.fail.fast", "true",
                "mongodb.disable.on.connect.failure", "false",
                "mongodb.server.selection.timeout.ms", "250",
                "mongodb.socket.connect.timeout.ms", "250",
                "mongodb.pool.max.wait.ms", "250"
        ));

        assertThrows(RuntimeException.class, sink::init);
        assertNull(readField(sink, "client"));
        assertNull(readField(sink, "collection"));
        assertFalse((Boolean) readField(sink, "disabled"));

        assertThrows(RuntimeException.class, sink::init);
        assertNull(readField(sink, "client"));
        assertNull(readField(sink, "collection"));
    }

    @Test
    void buildHeadersDocReturnsFreshEmptyDocument() throws Exception {
        final Document first = (Document) invokePrivateStatic(
                "buildHeadersDoc",
                new Class<?>[]{Map.class},
                (Object) null
        );
        final Document second = (Document) invokePrivateStatic(
                "buildHeadersDoc",
                new Class<?>[]{Map.class},
                Map.of()
        );

        assertNotSame(first, second);
        assertTrue(first.isEmpty());
        assertTrue(second.isEmpty());
    }

    @Test
    void safeUriRedactsEncodedCredentials() throws Exception {
        final String sanitized = (String) invokePrivateStatic(
                "safeUri",
                new Class<?>[]{String.class},
                "mongodb+srv://user:p%40ss@cluster0.example.mongodb.net/app?retryWrites=true"
        );

        assertEquals("mongodb+srv://***@cluster0.example.mongodb.net/app?retryWrites=true", sanitized);
    }

    @Test
    void flushReturnsPartialSuccessWhenBulkWriteFailsAndFailFastDisabled() throws Exception {
        final RecordingMetricsRuntime metrics = new RecordingMetricsRuntime();
        final MongoInsertSink sink = newSink(Map.of("mongodb.fail.fast", "false"), metrics);
        final ArrayList<Document> docs = new ArrayList<>(List.of(
                new Document("key", "a"),
                new Document("key", "b"),
                new Document("key", "c")
        ));

        final int inserted = (int) invokePrivate(
                sink,
                "flush",
                new Class<?>[]{MongoCollection.class, List.class},
                partialFailureCollection(1),
                docs
        );

        assertEquals(1, inserted);
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.insert.sink.flushes.total"));
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.insert.sink.records.total"));
        assertEquals(2.0, metrics.counterValue("streamkernel.mongo.insert.sink.errors.total"));
    }

    @Test
    void closeClearsReferencesAndClosesClient() throws Exception {
        final MongoInsertSink sink = newSink(Map.of());
        final AtomicBoolean closed = new AtomicBoolean(false);

        setField(sink, "client", closingClient(closed));
        setField(sink, "collection", throwingCollection("insertMany"));

        sink.close();

        assertTrue(closed.get());
        assertNull(readField(sink, "client"));
        assertNull(readField(sink, "collection"));
        assertTrue((Boolean) readField(sink, "disabled"));
    }

    @Test
    void writeBatchRecordsSkippedTypeAndClearsThreadLocalOnFailure() throws Exception {
        final RecordingMetricsRuntime metrics = new RecordingMetricsRuntime();
        final MongoInsertSink sink = newSink(Map.of("mongodb.fail.fast", "true"), metrics);
        setField(sink, "collection", throwingCollection("insertMany"));

        final List<PipelinePayload<Object>> batch = List.of(
                new PipelinePayload<>("evt-skip", (Object) new Object(), Instant.parse("2026-04-18T12:00:00Z"), Map.of()),
                new PipelinePayload<>(
                        "evt-wire",
                        (Object) WireEvent.vector(new byte[]{1, 2}, new float[]{0.25f}, Map.of("wire", "yes"), "ticket-1"),
                        Instant.parse("2026-04-18T12:00:01Z"),
                        Map.of("streamkernel.provenance.model.name", "demo")
                )
        );

        final RuntimeException error = assertThrows(RuntimeException.class, () -> sink.writeBatch(batch));

        assertEquals("MongoInsertSink flush failed", error.getMessage());
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.insert.sink.records.skipped.type.object.total"));

        @SuppressWarnings("unchecked")
        final ThreadLocal<ArrayList<Document>> tlDocs =
                (ThreadLocal<ArrayList<Document>>) readField(sink, "tlDocs");
        assertTrue(tlDocs.get().isEmpty());
    }

    private static MongoInsertSink newSink(Map<String, String> overrides) {
        return newSink(overrides, new RecordingMetricsRuntime());
    }

    private static MongoInsertSink newSink(Map<String, String> overrides, RecordingMetricsRuntime metrics) {
        final Properties props = new Properties();
        props.setProperty("mongodb.uri", "mongodb://localhost:27017");
        props.setProperty("mongodb.database", "test_db");
        props.setProperty("mongodb.collection", "test_collection");
        for (Map.Entry<String, String> entry : overrides.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
        return MongoInsertSink.fromConfig(PipelineConfig.from(props, "test"), metrics);
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> partialFailureCollection(int insertedCount) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if ("insertMany".equals(method.getName())) {
                        throw new MongoBulkWriteException(
                                BulkWriteResult.acknowledged(insertedCount, 0, 0, 0, List.of(), List.of()),
                                List.of(new BulkWriteError(11000, "duplicate key", new BsonDocument(), insertedCount)),
                                null,
                                new ServerAddress("localhost"),
                                Set.of()
                        );
                    }
                    if ("toString".equals(method.getName())) {
                        return "PartialFailureMongoCollection";
                    }
                    if ("hashCode".equals(method.getName())) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(method.getName())) {
                        return proxy == args[0];
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> throwingCollection(String throwingMethod) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if (throwingMethod.equals(method.getName())) {
                        throw new RuntimeException("boom");
                    }
                    if ("toString".equals(method.getName())) {
                        return "ThrowingMongoCollection";
                    }
                    if ("hashCode".equals(method.getName())) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(method.getName())) {
                        return proxy == args[0];
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    @SuppressWarnings("unchecked")
    private static MongoClient closingClient(AtomicBoolean closed) {
        return (MongoClient) Proxy.newProxyInstance(
                MongoClient.class.getClassLoader(),
                new Class<?>[]{MongoClient.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closed.set(true);
                        return null;
                    }
                    if ("toString".equals(method.getName())) {
                        return "ClosingMongoClient";
                    }
                    if ("hashCode".equals(method.getName())) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(method.getName())) {
                        return proxy == args[0];
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0.0f;
        }
        if (type == double.class) {
            return 0.0d;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }

    private static Object readField(Object target, String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        final Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(cause);
        }
    }

    private static Object invokePrivateStatic(String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        final Method method = MongoInsertSink.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        try {
            return method.invoke(null, args);
        } catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(cause);
        }
    }

    private static final class RecordingMetricsRuntime implements MetricsRuntime {
        private final Map<String, Double> counters = new HashMap<>();
        private final Map<String, Double> gauges = new HashMap<>();

        @Override
        public Object registry() {
            return this;
        }

        @Override
        public void counter(String name, double increment) {
            counters.merge(name, increment, Double::sum);
        }

        @Override
        public double counterValue(String name) {
            return counters.getOrDefault(name, 0.0d);
        }

        @Override
        public void timer(String name, long durationMillis) {
            // no-op
        }

        @Override
        public void gauge(String name, double value) {
            gauges.put(name, value);
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
