/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.avro.EnrichedTicket;
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
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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

class MongoVectorSinkTest {

    @Test
    void initFailureRemainsRetryable() throws Exception {
        final MongoVectorSink sink = newSink(Map.of(
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
                (Object) null);
        final Document second = (Document) invokePrivateStatic(
                "buildHeadersDoc",
                new Class<?>[]{Map.class},
                Map.of());

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
    void shortMongoAliasesAreAcceptedForConfiguredKeys() throws Exception {
        final MongoVectorSink sink = newSink(Map.ofEntries(
                Map.entry("mongodb.uri", ""),
                Map.entry("mongo.uri", "mongodb://alias-host:27017"),
                Map.entry("mongodb.database", ""),
                Map.entry("mongo.database", "alias_db"),
                Map.entry("mongodb.collection", ""),
                Map.entry("mongo.collection", "alias_collection"),
                Map.entry("mongo.upsert.mode", "insert"),
                Map.entry("mongo.business.key.field", "customerId"),
                Map.entry("mongo.vector.field", "embedding"),
                Map.entry("mongo.updated.field", "changedAt"),
                Map.entry("mongo.pool.max.size", "7"),
                Map.entry("mongo.pool.max.wait.ms", "1234"),
                Map.entry("mongo.server.selection.timeout.ms", "4321"),
                Map.entry("mongo.socket.connect.timeout.ms", "321"),
                Map.entry("mongo.socket.read.timeout.ms", "654"),
                Map.entry("mongo.fail.fast", "false"),
                Map.entry("mongo.disable.on.connect.failure", "true"),
                Map.entry("mongo.bulk.max.ops", "17"),
                Map.entry("mongo.bulk.ordered", "true"),
                Map.entry("mongo.strict.payload.type", "true"),
                Map.entry("mongo.use.key.as.document.id", "true"),
                Map.entry("mongo.retry.writes", "true"),
                Map.entry("mongo.ensure.business.key.index", "true")
        ));

        assertEquals("mongodb://alias-host:27017", readField(sink, "uri"));
        assertEquals("alias_db", readField(sink, "dbName"));
        assertEquals("alias_collection", readField(sink, "collectionName"));
        assertEquals("insert", readField(sink, "upsertMode"));
        assertEquals("customerId", readField(sink, "businessKeyField"));
        assertEquals("_id", readField(sink, "identityField"));
        assertEquals("embedding", readField(sink, "vectorField"));
        assertEquals("changedAt", readField(sink, "updatedField"));
        assertEquals(7, (Integer) readField(sink, "poolMaxSize"));
        assertEquals(1234L, (Long) readField(sink, "poolMaxWaitMs"));
        assertEquals(4321L, (Long) readField(sink, "serverSelectionTimeoutMs"));
        assertEquals(321, (Integer) readField(sink, "connectTimeoutMs"));
        assertEquals(654, (Integer) readField(sink, "readTimeoutMs"));
        assertFalse((Boolean) readField(sink, "failFast"));
        assertTrue((Boolean) readField(sink, "disableOnConnectFailure"));
        assertEquals(17, (Integer) readField(sink, "bulkMaxOps"));
        assertTrue((Boolean) readField(sink, "bulkOrdered"));
        assertTrue((Boolean) readField(sink, "strictPayloadType"));
        assertTrue((Boolean) readField(sink, "useKeyAsDocumentId"));
        assertTrue((Boolean) readField(sink, "retryWrites"));
        assertTrue((Boolean) readField(sink, "ensureBusinessKeyIndex"));
    }

    @Test
    void toDoubleListFromFloatListRejectsNullElements() {
        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> invokePrivateStatic(
                "toDoubleListFromFloatList",
                new Class<?>[]{List.class},
                Arrays.asList(1.0f, null, 2.0f)));

        assertEquals("Embedding list contains null element at index 1", error.getMessage());
    }

    @Test
    void flushAndClearOpsClearsThreadLocalBufferOnFailure() throws Exception {
        final MongoVectorSink sink = newSink(Map.of());
        final ArrayList<WriteModel<Document>> ops = new ArrayList<>();
        ops.add(new InsertOneModel<>(new Document("ticketId", "id-1")));

        final MongoCollection<Document> collection = throwingCollection("bulkWrite");

        final RuntimeException error = assertThrows(RuntimeException.class, () -> invokePrivate(
                sink,
                "flushAndClearOps",
                new Class<?>[]{MongoCollection.class, ArrayList.class},
                collection,
                ops));

        assertEquals("MongoVectorSink flush failed", error.getMessage());
        assertTrue(ops.isEmpty());
    }

    @Test
    void flushReturnsPartialSuccessWhenBulkWriteFailsAndFailFastDisabled() throws Exception {
        final RecordingMetricsRuntime metrics = new RecordingMetricsRuntime();
        final MongoVectorSink sink = newSink(Map.of("mongodb.fail.fast", "false"), metrics);
        final ArrayList<WriteModel<Document>> ops = new ArrayList<>();
        ops.add(new InsertOneModel<>(new Document("ticketId", "id-1")));
        ops.add(new InsertOneModel<>(new Document("ticketId", "id-2")));
        ops.add(new InsertOneModel<>(new Document("ticketId", "id-3")));

        final int succeeded = (int) invokePrivate(
                sink,
                "flush",
                new Class<?>[]{MongoCollection.class, List.class},
                partialFailureCollection("bulkWrite", 2),
                ops
        );

        assertEquals(2, succeeded);
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.sink.flushes.total"));
        assertEquals(2.0, metrics.counterValue("streamkernel.mongo.sink.ops.total"));
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.sink.errors.total"));
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.sink.records.dropped.total"));
    }

    @Test
    void flushAndClearDocumentsClearsThreadLocalBufferOnFailure() throws Exception {
        final MongoVectorSink sink = newSink(Map.of());
        final ArrayList<Document> docs = new ArrayList<>();
        docs.add(new Document("ticketId", "id-1"));

        final MongoCollection<Document> collection = throwingCollection("insertMany");

        final RuntimeException error = assertThrows(RuntimeException.class, () -> invokePrivate(
                sink,
                "flushAndClearDocuments",
                new Class<?>[]{MongoCollection.class, ArrayList.class},
                collection,
                docs));

        assertEquals("MongoVectorSink flush failed", error.getMessage());
        assertTrue(docs.isEmpty());
    }

    @Test
    void flushDocumentsReturnsPartialSuccessWhenBulkWriteFailsAndFailFastDisabled() throws Exception {
        final RecordingMetricsRuntime metrics = new RecordingMetricsRuntime();
        final MongoVectorSink sink = newSink(Map.of("mongodb.fail.fast", "false"), metrics);
        final ArrayList<Document> docs = new ArrayList<>(List.of(
                new Document("ticketId", "id-1"),
                new Document("ticketId", "id-2"),
                new Document("ticketId", "id-3")
        ));

        final int succeeded = (int) invokePrivate(
                sink,
                "flushDocuments",
                new Class<?>[]{MongoCollection.class, List.class},
                partialFailureCollection("insertMany", 1),
                docs
        );

        assertEquals(1, succeeded);
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.sink.flushes.total"));
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.sink.ops.total"));
        assertEquals(2.0, metrics.counterValue("streamkernel.mongo.sink.errors.total"));
        assertEquals(2.0, metrics.counterValue("streamkernel.mongo.sink.records.dropped.total"));
    }

    @Test
    void addEnrichedTicketInsertDocPersistsLineageMetadata() throws Exception {
        final MongoVectorSink sink = newSink(Map.of());
        final ArrayList<Document> docs = new ArrayList<>();
        final EnrichedTicket ticket = new EnrichedTicket();
        ticket.setTicketId("ticket-1");
        ticket.setDescription("hello");
        ticket.setSentiment("NEUTRAL");
        ticket.setEmbedding(List.of(1.0f, 2.0f));

        final boolean added = (boolean) invokePrivate(
                sink,
                "addEnrichedTicketInsertDoc",
                new Class<?>[]{List.class, PipelinePayload.class, EnrichedTicket.class, long.class},
                docs,
                new PipelinePayload<>(
                        "evt-1",
                        (Object) ticket,
                        Map.of(
                                "streamkernel.provenance.model.name", "risk-model",
                                "streamkernel.provenance.inference.timestamp", "2026-04-17T12:00:00Z",
                                "streamkernel.source.text", "do not copy"
                        )
                ),
                ticket,
                Instant.parse("2026-04-18T12:00:00Z").toEpochMilli()
        );

        assertTrue(added);
        final Document lineage = docs.get(0).get("lineage", Document.class);
        assertEquals("risk-model", lineage.getString("streamkernel.provenance.model.name"));
        assertEquals("2026-04-17T12:00:00Z", lineage.getString("streamkernel.provenance.inference.timestamp"));
        assertNull(lineage.get("streamkernel.source.text"));
    }

    @Test
    void addWireEventInsertDocMergesLineageIntoHeaders() throws Exception {
        final MongoVectorSink sink = newSink(Map.of());
        final ArrayList<Document> docs = new ArrayList<>();
        final WireEvent wireEvent = WireEvent.vector(
                new byte[]{1, 2, 3},
                new float[]{0.25f, 0.75f},
                Map.of("wire.header", "present"),
                "ticket-2"
        );

        final boolean added = (boolean) invokePrivate(
                sink,
                "addWireEventInsertDoc",
                new Class<?>[]{List.class, PipelinePayload.class, WireEvent.class, long.class},
                docs,
                new PipelinePayload<>(
                        "evt-2",
                        (Object) wireEvent,
                        Map.of("streamkernel.provenance.model.version", "42")
                ),
                wireEvent,
                Instant.parse("2026-04-18T12:00:00Z").toEpochMilli()
        );

        assertTrue(added);
        final Document headers = docs.get(0).get("headers", Document.class);
        assertEquals("present", headers.getString("wire.header"));
        assertEquals("42", headers.getString("streamkernel.provenance.model.version"));
    }

    @Test
    void writeBatchSkipsInvalidEmbeddingAndRecordsMetric() throws Exception {
        final RecordingMetricsRuntime metrics = new RecordingMetricsRuntime();
        final MongoVectorSink sink = newSink(Map.of("mongodb.upsert.mode", "insert"), metrics);
        setField(sink, "collection", noopCollection());

        final EnrichedTicket ticket = new EnrichedTicket();
        ticket.setTicketId("ticket-1");
        ticket.setEmbedding(Arrays.asList(1.0f, null, 2.0f));

        sink.writeBatch(List.of(new PipelinePayload<>("evt-3", (Object) ticket, Map.of())));

        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.sink.payload.invalid.total"));
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.sink.records.unsupported.total"));
        assertEquals(1.0, metrics.counterValue("streamkernel.mongo.sink.batches.empty.total"));
    }

    @Test
    void closeClearsReferencesAndRemovesThreadLocals() throws Exception {
        final MongoVectorSink sink = newSink(Map.of());
        final AtomicBoolean closed = new AtomicBoolean(false);

        @SuppressWarnings("unchecked")
        final ThreadLocal<ArrayList<WriteModel<Document>>> tlOps =
                (ThreadLocal<ArrayList<WriteModel<Document>>>) readField(sink, "tlOps");
        @SuppressWarnings("unchecked")
        final ThreadLocal<ArrayList<Document>> tlDocs =
                (ThreadLocal<ArrayList<Document>>) readField(sink, "tlDocs");

        final ArrayList<WriteModel<Document>> beforeOps = tlOps.get();
        final ArrayList<Document> beforeDocs = tlDocs.get();

        setField(sink, "client", closingClient(closed));
        setField(sink, "collection", noopCollection());

        sink.close();

        assertTrue(closed.get());
        assertNull(readField(sink, "client"));
        assertNull(readField(sink, "collection"));
        assertTrue((Boolean) readField(sink, "disabled"));
        assertNotSame(beforeOps, tlOps.get());
        assertNotSame(beforeDocs, tlDocs.get());
    }

    private static MongoVectorSink newSink(Map<String, String> overrides) {
        return newSink(overrides, new RecordingMetricsRuntime());
    }

    private static MongoVectorSink newSink(Map<String, String> overrides, RecordingMetricsRuntime metrics) {
        final Properties props = new Properties();
        props.setProperty("mongodb.uri", "mongodb://localhost:27017");
        props.setProperty("mongodb.database", "test_db");
        props.setProperty("mongodb.collection", "test_collection");
        for (Map.Entry<String, String> entry : overrides.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
        return MongoVectorSink.fromConfig(PipelineConfig.from(props, "test"), metrics);
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> partialFailureCollection(String throwingMethod, int succeededCount) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if (throwingMethod.equals(method.getName())) {
                        throw new MongoBulkWriteException(
                                BulkWriteResult.acknowledged(succeededCount, 0, 0, 0, List.of(), List.of()),
                                List.of(new BulkWriteError(11000, "duplicate key", new BsonDocument(), succeededCount)),
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
    private static MongoCollection<Document> noopCollection() {
        return (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if ("toString".equals(method.getName())) {
                        return "NoopMongoCollection";
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
        final Method method = MongoVectorSink.class.getDeclaredMethod(methodName, parameterTypes);
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
