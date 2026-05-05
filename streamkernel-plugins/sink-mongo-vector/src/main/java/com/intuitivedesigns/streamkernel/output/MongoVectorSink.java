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
import com.intuitivedesigns.streamkernel.core.BatchOutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.PipelineProvenance;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class MongoVectorSink implements BatchOutputSink<Object> {

    private static final Logger log = LoggerFactory.getLogger(MongoVectorSink.class);

    // -------------------------------------------------------------------------
    // Config keys
    // -------------------------------------------------------------------------
    private static final String KEY_URI                         = "mongodb.uri";
    private static final String KEY_DB                         = "mongodb.database";
    private static final String KEY_COLLECTION                 = "mongodb.collection";
    private static final String KEY_UPSERT_MODE                = "mongodb.upsert.mode";
    private static final String KEY_BUSINESS_KEY_FIELD         = "mongodb.business.key.field";
    private static final String KEY_VECTOR_FIELD               = "mongodb.vector.field";
    private static final String KEY_DOC_UPDATED_FIELD          = "mongodb.updated.field";
    private static final String KEY_POOL_MAX_SIZE              = "mongodb.pool.max.size";
    private static final String KEY_POOL_MAX_WAIT_MS           = "mongodb.pool.max.wait.ms";
    private static final String KEY_SERVER_SELECTION_TIMEOUT_MS = "mongodb.server.selection.timeout.ms";
    private static final String KEY_CONNECT_TIMEOUT_MS         = "mongodb.socket.connect.timeout.ms";
    private static final String KEY_READ_TIMEOUT_MS            = "mongodb.socket.read.timeout.ms";
    private static final String KEY_FAIL_FAST                  = "mongodb.fail.fast";
    private static final String KEY_DISABLE_ON_CONNECT_FAILURE = "mongodb.disable.on.connect.failure";
    private static final String KEY_BULK_BATCH_MAX             = "mongodb.bulk.max.ops";
    private static final String KEY_BULK_ORDERED               = "mongodb.bulk.ordered";
    private static final String KEY_STRICT_PAYLOAD_TYPE        = "mongodb.strict.payload.type";
    private static final String KEY_USE_KEY_AS_DOCUMENT_ID     = "mongodb.use.key.as.document.id";
    private static final String KEY_RETRY_WRITES               = "mongodb.retry.writes";
    private static final String KEY_ENSURE_BUSINESS_KEY_INDEX  = "mongodb.ensure.business.key.index";

    // -------------------------------------------------------------------------
    // Defaults
    // -------------------------------------------------------------------------
    private static final String  DEFAULT_URI                         = "mongodb://localhost:27017";
    private static final String  DEFAULT_DB                         = "support_db";
    private static final String  DEFAULT_COLLECTION                 = "tickets_vectorized";
    private static final String  DEFAULT_UPSERT_MODE                = "replace";
    private static final String  DEFAULT_BUSINESS_KEY_FIELD         = "ticketId";
    private static final String  DEFAULT_VECTOR_FIELD               = "vector_embedding";
    private static final String  DEFAULT_UPDATED_FIELD              = "updatedAt";
    private static final int     DEFAULT_POOL_MAX_SIZE              = 100;
    private static final long    DEFAULT_POOL_MAX_WAIT_MS           = 2_000L;
    private static final long    DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 2_000L;
    private static final int     DEFAULT_CONNECT_TIMEOUT_MS         = 2_000;
    private static final int     DEFAULT_READ_TIMEOUT_MS            = 0;
    private static final boolean DEFAULT_FAIL_FAST                  = true;
    private static final boolean DEFAULT_DISABLE_ON_CONNECT_FAILURE = false;
    private static final int     DEFAULT_BULK_BATCH_MAX             = 100;
    private static final boolean DEFAULT_BULK_ORDERED               = false;
    private static final boolean DEFAULT_STRICT_PAYLOAD_TYPE        = false;
    private static final boolean DEFAULT_USE_KEY_AS_DOCUMENT_ID     = false;
    private static final boolean DEFAULT_RETRY_WRITES               = false;
    private static final boolean DEFAULT_ENSURE_BUSINESS_KEY_INDEX  = false;

    // -------------------------------------------------------------------------
    // Instance fields
    // -------------------------------------------------------------------------
    private final MetricsRuntime metrics;

    private final String uri;
    private final String dbName;
    private final String collectionName;

    private final String  upsertMode;
    private final String  businessKeyField;
    private final String  identityField;
    private final String  vectorField;
    private final String  updatedField;

    private final int  poolMaxSize;
    private final long poolMaxWaitMs;
    private final long serverSelectionTimeoutMs;
    private final int  connectTimeoutMs;
    private final int  readTimeoutMs;

    private final boolean failFast;
    private final boolean disableOnConnectFailure;
    private final int     bulkMaxOps;
    private final boolean bulkOrdered;
    private final boolean strictPayloadType;
    private final boolean useKeyAsDocumentId;
    private final boolean retryWrites;
    private final boolean ensureBusinessKeyIndex;

    // -------------------------------------------------------------------------
    // Precomputed write options — allocated once, reused on every flush
    // -------------------------------------------------------------------------

    /** Reused for every UpdateOneModel — upsert=true, constructed once. */
    private final UpdateOptions  updateOptions;
    /** Reused for every ReplaceOneModel — upsert=true, constructed once. */
    private final ReplaceOptions replaceOptions;
    /** Reused for every bulkWrite call — ordered flag set once. */
    private final BulkWriteOptions bulkWriteOptions;
    /** Reused for every insertMany call on the append-only fast path. */
    private final InsertManyOptions insertOptions;

    // -------------------------------------------------------------------------
    // Thread-local buffers
    // -------------------------------------------------------------------------

    /**
     * Per-thread WriteModel buffer for update/replace mode.
     */
    private final ThreadLocal<ArrayList<WriteModel<Document>>> tlOps;
    /**
     * Per-thread Document buffer for append-only insert mode.
     */
    private final ThreadLocal<ArrayList<Document>> tlDocs;

    // -------------------------------------------------------------------------
    // Volatile state — written once at init, read frequently
    // -------------------------------------------------------------------------
    private volatile MongoClient client;
    private volatile MongoCollection<Document> collection;
    private volatile boolean disabled = false;

    // -------------------------------------------------------------------------
    // Constructor (private — use fromConfig)
    // -------------------------------------------------------------------------
    private MongoVectorSink(
            MetricsRuntime metrics,
            String uri, String dbName, String collectionName,
            String upsertMode, String businessKeyField, String vectorField, String updatedField,
            int poolMaxSize, long poolMaxWaitMs, long serverSelectionTimeoutMs, int connectTimeoutMs,
            int readTimeoutMs,
            boolean failFast, boolean disableOnConnectFailure,
            int bulkMaxOps, boolean bulkOrdered, boolean strictPayloadType,
            boolean useKeyAsDocumentId, boolean retryWrites, boolean ensureBusinessKeyIndex
    ) {
        this.metrics                  = metrics;
        this.uri                      = uri;
        this.dbName                   = dbName;
        this.collectionName           = collectionName;
        this.upsertMode               = upsertMode;
        this.businessKeyField         = businessKeyField;
        this.identityField            = useKeyAsDocumentId ? "_id" : businessKeyField;
        this.vectorField              = vectorField;
        this.updatedField             = updatedField;
        this.poolMaxSize              = poolMaxSize;
        this.poolMaxWaitMs            = poolMaxWaitMs;
        this.serverSelectionTimeoutMs = serverSelectionTimeoutMs;
        this.connectTimeoutMs         = connectTimeoutMs;
        this.readTimeoutMs            = readTimeoutMs;
        this.failFast                 = failFast;
        this.disableOnConnectFailure  = disableOnConnectFailure;
        this.bulkMaxOps               = bulkMaxOps;
        this.bulkOrdered              = bulkOrdered;
        this.strictPayloadType        = strictPayloadType;
        this.useKeyAsDocumentId       = useKeyAsDocumentId;
        this.retryWrites              = retryWrites;
        this.ensureBusinessKeyIndex   = ensureBusinessKeyIndex;

        // Precompute write options — these never change after construction
        this.updateOptions    = new UpdateOptions().upsert(true);
        this.replaceOptions   = new ReplaceOptions().upsert(true);
        this.bulkWriteOptions = new BulkWriteOptions().ordered(bulkOrdered);
        this.insertOptions    = new InsertManyOptions().ordered(bulkOrdered);

        // ThreadLocal buffers pre-sized to the configured bulk batch limit
        final int opsCapacity = bulkMaxOps;
        this.tlOps = ThreadLocal.withInitial(() -> new ArrayList<>(opsCapacity));
        this.tlDocs = ThreadLocal.withInitial(() -> new ArrayList<>(opsCapacity));
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------
    public static MongoVectorSink fromConfig(PipelineConfig cfg, MetricsRuntime metrics) {
        Objects.requireNonNull(cfg,     "cfg");
        Objects.requireNonNull(metrics, "metrics");

        // Accept both "mongodb.*" (canonical) and "mongo.*" (short alias) key prefixes.
        // Canonical takes precedence; short alias is the fallback.
        final String  uri     = getString(cfg, KEY_URI,                 DEFAULT_URI);
        final String  db      = getString(cfg, KEY_DB,                  DEFAULT_DB);
        final String  col     = getString(cfg, KEY_COLLECTION,          DEFAULT_COLLECTION);
        final String  mode    = normalizeMode(getString(cfg, KEY_UPSERT_MODE, DEFAULT_UPSERT_MODE));
        final String  keyFld  = getString(cfg, KEY_BUSINESS_KEY_FIELD,  DEFAULT_BUSINESS_KEY_FIELD);
        final String  vecFld  = getString(cfg, KEY_VECTOR_FIELD,        DEFAULT_VECTOR_FIELD);
        final String  updFld  = getString(cfg, KEY_DOC_UPDATED_FIELD,   DEFAULT_UPDATED_FIELD);

        final int  poolMaxSize   = clampInt( getInt( cfg, KEY_POOL_MAX_SIZE,               DEFAULT_POOL_MAX_SIZE),              1,       10_000);
        final long poolMaxWaitMs = clampLong(getLong(cfg, KEY_POOL_MAX_WAIT_MS,             DEFAULT_POOL_MAX_WAIT_MS),           1L,     120_000L);
        final long selTimeoutMs  = clampLong(getLong(cfg, KEY_SERVER_SELECTION_TIMEOUT_MS,  DEFAULT_SERVER_SELECTION_TIMEOUT_MS), 250L,   120_000L);
        final int  connTimeoutMs = clampInt( getInt( cfg, KEY_CONNECT_TIMEOUT_MS,           DEFAULT_CONNECT_TIMEOUT_MS),         250,    120_000);
        final int  readTimeoutMs = clampInt( getInt( cfg, KEY_READ_TIMEOUT_MS,              DEFAULT_READ_TIMEOUT_MS),            0,      120_000);

        final boolean failFast              = getBool(cfg, KEY_FAIL_FAST,                  DEFAULT_FAIL_FAST);
        final boolean disableOnConnectFail  = getBool(cfg, KEY_DISABLE_ON_CONNECT_FAILURE, DEFAULT_DISABLE_ON_CONNECT_FAILURE);
        final int     bulkMaxOps            = clampInt(getInt(cfg, KEY_BULK_BATCH_MAX,     DEFAULT_BULK_BATCH_MAX), 1, 100_000);
        final boolean bulkOrdered           = getBool(cfg, KEY_BULK_ORDERED,               DEFAULT_BULK_ORDERED);
        final boolean strictPayloadType     = getBool(cfg, KEY_STRICT_PAYLOAD_TYPE,        DEFAULT_STRICT_PAYLOAD_TYPE);
        final boolean useKeyAsDocumentId    = getBool(cfg, KEY_USE_KEY_AS_DOCUMENT_ID,     DEFAULT_USE_KEY_AS_DOCUMENT_ID);
        final boolean retryWrites           = getBool(cfg, KEY_RETRY_WRITES,               DEFAULT_RETRY_WRITES);
        final boolean ensureBusinessKeyIndex = getBool(cfg, KEY_ENSURE_BUSINESS_KEY_INDEX, DEFAULT_ENSURE_BUSINESS_KEY_INDEX);

        log.info("MongoVectorSink configured. db='{}' collection='{}' mode='{}' businessKey='{}' " +
                        "vectorField='{}' bulkMaxOps={} ordered={} strictPayloadType={} useKeyAsDocumentId={} retryWrites={} " +
                        "ensureBusinessKeyIndex={} poolMaxSize={} poolMaxWaitMs={} serverSelectionTimeoutMs={} connectTimeoutMs={} readTimeoutMs={}",
                db, col, mode, keyFld, vecFld, bulkMaxOps, bulkOrdered, strictPayloadType,
                useKeyAsDocumentId, retryWrites, ensureBusinessKeyIndex,
                poolMaxSize, poolMaxWaitMs, selTimeoutMs, connTimeoutMs, readTimeoutMs);

        return new MongoVectorSink(
                metrics, uri, db, col, mode, keyFld, vecFld, updFld,
                poolMaxSize, poolMaxWaitMs, selTimeoutMs, connTimeoutMs, readTimeoutMs,
                failFast, disableOnConnectFail, bulkMaxOps, bulkOrdered, strictPayloadType,
                useKeyAsDocumentId, retryWrites, ensureBusinessKeyIndex
        );
    }

    public void init() throws Exception {
        ensureInitialized();
    }

    // -------------------------------------------------------------------------
    // BatchOutputSink contract
    // -------------------------------------------------------------------------

    @Override
    public void write(PipelinePayload<Object> payload) throws Exception {
        if (payload == null || payload.data() == null) return;
        writeBatch(List.of(payload));
    }

    @Override
    public void writeBatch(List<PipelinePayload<Object>> batch) throws Exception {
        if (batch == null || batch.isEmpty()) return;
        if (disabled) return;
        final int batchSize = batch.size();

        if (log.isDebugEnabled()) {
            final PipelinePayload<Object> firstPayload = batch.get(0);
            final Object first = firstPayload == null ? null : firstPayload.data();
            log.debug("MongoVectorSink.writeBatch batchSize={} firstPayloadClass={}",
                    batchSize, first == null ? "null" : first.getClass().getSimpleName());
        }

        ensureInitialized();
        // Re-check after init because the sink may have been disabled by
        // disableOnConnectFailure or closed while the init path was running.
        if (disabled) return;

        final MongoCollection<Document> col = this.collection;
        if (col == null) {
            log.warn("MongoVectorSink: collection is null after init — batch of {} dropped", batchSize);
            return;
        }

        if ("insert".equals(upsertMode)) {
            writeInsertBatch(col, batch, batchSize);
            return;
        }

        writeUpsertBatch(col, batch, batchSize);
    }

    private void writeUpsertBatch(MongoCollection<Document> col,
                                  List<PipelinePayload<Object>> batch,
                                  int batchSize) throws Exception {
        final long startNs = System.nanoTime();
        final long batchTimestampMs = System.currentTimeMillis();

        final ArrayList<WriteModel<Document>> ops = tlOps.get();
        ops.clear();

        int attempted = 0;
        int persistable = 0;
        int persisted = 0;
        int unsupported = 0;

        try {
            for (final PipelinePayload<Object> p : batch) {
                if (p == null) continue;

                final Object v = p.data();
                if (v == null) continue;

                attempted++;

                final boolean added;
                try {
                    if (v instanceof WireEvent w) {
                        added = addWireEventOp(ops, p, w, batchTimestampMs);
                    } else if (v instanceof EnrichedTicket t) {
                        added = addEnrichedTicketOp(ops, p, t, batchTimestampMs);
                    } else {
                        unsupported++;
                        handleUnsupportedPayload(v);
                        continue;
                    }
                } catch (IllegalArgumentException e) {
                    unsupported++;
                    handleInvalidPayload(v, e);
                    continue;
                }

                if (added) {
                    persistable++;
                }

                if (ops.size() >= bulkMaxOps) {
                    persisted += flushAndClearOps(col, ops);
                }
            }

            if (!ops.isEmpty()) {
                persisted += flushAndClearOps(col, ops);
            }

            recordBatchOutcome(System.nanoTime() - startNs, batchSize, attempted, persistable, persisted, unsupported);
        } finally {
            ops.clear();
        }
    }

    private void writeInsertBatch(MongoCollection<Document> col,
                                  List<PipelinePayload<Object>> batch,
                                  int batchSize) throws Exception {
        final long startNs = System.nanoTime();
        final long batchTimestampMs = System.currentTimeMillis();

        final ArrayList<Document> docs = tlDocs.get();
        docs.clear();

        int attempted = 0;
        int persistable = 0;
        int persisted = 0;
        int unsupported = 0;

        try {
            for (final PipelinePayload<Object> p : batch) {
                if (p == null) continue;

                final Object v = p.data();
                if (v == null) continue;

                attempted++;

                final boolean added;
                try {
                    if (v instanceof WireEvent w) {
                        added = addWireEventInsertDoc(docs, p, w, batchTimestampMs);
                    } else if (v instanceof EnrichedTicket t) {
                        added = addEnrichedTicketInsertDoc(docs, p, t, batchTimestampMs);
                    } else {
                        unsupported++;
                        handleUnsupportedPayload(v);
                        continue;
                    }
                } catch (IllegalArgumentException e) {
                    unsupported++;
                    handleInvalidPayload(v, e);
                    continue;
                }

                if (added) {
                    persistable++;
                }

                if (docs.size() >= bulkMaxOps) {
                    persisted += flushAndClearDocuments(col, docs);
                }
            }

            if (!docs.isEmpty()) {
                persisted += flushAndClearDocuments(col, docs);
            }

            recordBatchOutcome(System.nanoTime() - startNs, batchSize, attempted, persistable, persisted, unsupported);
        } finally {
            docs.clear();
        }
    }

    // -------------------------------------------------------------------------
    // Write model builders
    // -------------------------------------------------------------------------

    /**
     * Builds an upsert WriteModel for a WireEvent payload.
     */
    private boolean addWireEventOp(List<WriteModel<Document>> ops,
                                   PipelinePayload<Object> payload,
                                   WireEvent w,
                                   long timestampMs) {
        final String key = safeString(w.key());
        if (key.isBlank()) return false;

        final float[] vector = w.vector();
        if (vector == null || vector.length == 0) return false;

        final Map<String, String> provenance = PipelineProvenance.extractProvenanceHeaders(
                payload == null ? null : payload.metadata()
        );
        final Bson filter = Filters.eq(identityField, key);

        final List<Double> mongoVector = vectorToDoubleList(vector);

        final Document headersDoc = buildHeadersDoc(mergeHeaders(w.headers(), provenance));
        final Document lineageDoc = buildHeadersDoc(provenance);

        if ("update".equals(upsertMode)) {
            ops.add(new UpdateOneModel<>(filter,
                    Updates.combine(
                            useKeyAsDocumentId ? Updates.setOnInsert("_id", key) : Updates.setOnInsert(businessKeyField, key),
                            Updates.set(businessKeyField, key),
                            Updates.set(vectorField,      mongoVector),
                            Updates.set("headers",        headersDoc),
                            Updates.set("lineage",        lineageDoc),
                            Updates.set(updatedField,     timestampMs)
                    ),
                    updateOptions));   // precomputed — no allocation
        } else {
            final Document replacement = new Document();
            if (useKeyAsDocumentId) {
                replacement.append("_id", key);
            }
            replacement.append(businessKeyField, key)
                    .append(vectorField,      mongoVector)
                    .append("headers",        headersDoc)
                    .append("lineage",        lineageDoc)
                    .append(updatedField,     timestampMs);

            ops.add(new ReplaceOneModel<>(filter, replacement, replaceOptions)); // precomputed — no allocation
        }
        return true;
    }

    /**
     * Builds a BSON Document for append-only insert mode.
     *
     * This avoids per-record WriteModel wrappers and replace/update bookkeeping,
     * which is faster when the workload is strictly append-only and keys are unique.
     */
    private boolean addWireEventInsertDoc(List<Document> docs,
                                          PipelinePayload<Object> payload,
                                          WireEvent w,
                                          long timestampMs) {
        final String key = safeString(w.key());
        if (key.isBlank()) return false;

        final float[] vector = w.vector();
        if (vector == null || vector.length == 0) return false;

        final Map<String, String> provenance = PipelineProvenance.extractProvenanceHeaders(
                payload == null ? null : payload.metadata()
        );

        final Document doc = new Document();
        if (useKeyAsDocumentId) {
            doc.append("_id", key);
        }
        doc.append(businessKeyField, key)
                .append(vectorField, vectorToDoubleList(vector))
                .append("headers", buildHeadersDoc(mergeHeaders(w.headers(), provenance)))
                .append("lineage", buildHeadersDoc(provenance))
                .append(updatedField, timestampMs);

        docs.add(doc);
        return true;
    }

    private boolean addEnrichedTicketOp(List<WriteModel<Document>> ops,
                                        PipelinePayload<Object> payload,
                                        EnrichedTicket t,
                                        long timestampMs) {
        final String ticketId = safeString(t.getTicketId());
        if (ticketId.isBlank()) return false;

        final String description = safeString(t.getDescription());
        final String sentiment   = safeString(t.getSentiment());
        final List<Double> embedding = toDoubleListFromFloatList(t.getEmbedding());
        final Map<String, String> provenance = PipelineProvenance.extractProvenanceHeaders(
                payload == null ? null : payload.metadata()
        );

        final Bson filter = Filters.eq(identityField, ticketId);

        if ("update".equals(upsertMode)) {
            ops.add(new UpdateOneModel<>(filter,
                    Updates.combine(
                            useKeyAsDocumentId ? Updates.setOnInsert("_id", ticketId) : Updates.setOnInsert(businessKeyField, ticketId),
                            Updates.set(businessKeyField, ticketId),
                            Updates.set("description", description),
                            Updates.set("sentiment", sentiment),
                            Updates.set(vectorField, embedding),
                            Updates.set("lineage", buildHeadersDoc(provenance)),
                            Updates.set(updatedField, timestampMs)
                    ),
                    updateOptions));
        } else {
            final Document replacement = new Document();
            if (useKeyAsDocumentId) {
                replacement.append("_id", ticketId);
            }
            replacement.append(businessKeyField, ticketId)
                    .append("description", description)
                    .append("sentiment", sentiment)
                    .append(vectorField, embedding)
                    .append("lineage", buildHeadersDoc(provenance))
                    .append(updatedField, timestampMs);

            ops.add(new ReplaceOneModel<>(filter, replacement, replaceOptions));
        }
        return true;
    }

    private boolean addEnrichedTicketInsertDoc(List<Document> docs,
                                               PipelinePayload<Object> payload,
                                               EnrichedTicket t,
                                               long timestampMs) {
        final String ticketId = safeString(t.getTicketId());
        if (ticketId.isBlank()) return false;
        final Map<String, String> provenance = PipelineProvenance.extractProvenanceHeaders(
                payload == null ? null : payload.metadata()
        );

        final Document doc = new Document();
        if (useKeyAsDocumentId) {
            doc.append("_id", ticketId);
        }
        doc.append(businessKeyField, ticketId)
                .append("description", safeString(t.getDescription()))
                .append("sentiment", safeString(t.getSentiment()))
                .append(vectorField, toDoubleListFromFloatList(t.getEmbedding()))
                .append("lineage", buildHeadersDoc(provenance))
                .append(updatedField, timestampMs);

        docs.add(doc);
        return true;
    }

    // -------------------------------------------------------------------------
    // Flush
    // -------------------------------------------------------------------------

    private int flush(MongoCollection<Document> col, List<WriteModel<Document>> ops) {
        if (ops.isEmpty() || disabled) {
            return 0;
        }
        final long start = System.nanoTime();
        try {
            col.bulkWrite(ops, bulkWriteOptions);
            final long ms = (System.nanoTime() - start) / 1_000_000L;

            if (log.isDebugEnabled()) {
                log.debug("MONGO_FLUSH ops={} ms={}", ops.size(), ms);
            }

            recordFlushMetrics(ops.size(), 0, ms);
            return ops.size();
        } catch (MongoBulkWriteException e) {
            final long ms = (System.nanoTime() - start) / 1_000_000L;
            final int failed = e.getWriteErrors() == null ? 0 : e.getWriteErrors().size();
            final int succeeded = Math.max(0, ops.size() - failed);
            recordFlushMetrics(succeeded, failed, ms);
            log.error("MongoVectorSink partial bulk flush: {}/{} ops succeeded — {} failed",
                    succeeded, ops.size(), failed);
            if (failFast) {
                throw new RuntimeException("MongoVectorSink flush failed", e);
            }
            return succeeded;
        } catch (Exception e) {
            final long ms = (System.nanoTime() - start) / 1_000_000L;
            recordFlushMetrics(0, ops.size(), ms);
            log.error("MongoVectorSink flush failed: {} — {} ops dropped", e.getMessage(), ops.size());
            if (failFast) {
                throw new RuntimeException("MongoVectorSink flush failed", e);
            }
            return 0;
        }
    }

    private int flushAndClearOps(MongoCollection<Document> col, ArrayList<WriteModel<Document>> ops) {
        try {
            return flush(col, ops);
        } finally {
            ops.clear();
        }
    }

    private int flushDocuments(MongoCollection<Document> col, List<Document> docs) {
        if (docs.isEmpty() || disabled) {
            return 0;
        }
        final long start = System.nanoTime();
        try {
            col.insertMany(docs, insertOptions);
            final long ms = (System.nanoTime() - start) / 1_000_000L;

            if (log.isDebugEnabled()) {
                log.debug("MONGO_INSERT_FLUSH docs={} ms={}", docs.size(), ms);
            }

            recordFlushMetrics(docs.size(), 0, ms);
            return docs.size();
        } catch (MongoBulkWriteException e) {
            final long ms = (System.nanoTime() - start) / 1_000_000L;
            final int succeeded = Math.max(0, e.getWriteResult().getInsertedCount());
            final int failed = Math.max(0, docs.size() - succeeded);
            recordFlushMetrics(succeeded, failed, ms);
            log.error("MongoVectorSink partial insert flush: {}/{} docs inserted — {} failed",
                    succeeded, docs.size(), failed);
            if (failFast) {
                throw new RuntimeException("MongoVectorSink flush failed", e);
            }
            return succeeded;
        } catch (Exception e) {
            final long ms = (System.nanoTime() - start) / 1_000_000L;
            recordFlushMetrics(0, docs.size(), ms);
            log.error("MongoVectorSink insert flush failed: {} — {} docs dropped", e.getMessage(), docs.size());
            if (failFast) {
                throw new RuntimeException("MongoVectorSink flush failed", e);
            }
            return 0;
        }
    }

    private int flushAndClearDocuments(MongoCollection<Document> col, ArrayList<Document> docs) {
        try {
            return flushDocuments(col, docs);
        } finally {
            docs.clear();
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws Exception {
        final MongoClient c;
        synchronized (this) {
            disabled = true;
            c = this.client;
            this.client = null;
            this.collection = null;
        }
        tlOps.remove();
        tlDocs.remove();
        if (c != null) {
            c.close();
        }
    }

    // -------------------------------------------------------------------------
    // Init
    // -------------------------------------------------------------------------

    private void ensureInitialized() {
        if (disabled)        return;
        if (collection != null) return; // fast volatile read — already up

        synchronized (this) {
            if (disabled)        return;
            if (collection != null) return; // re-check under lock
            MongoClient mc = null;
            try {
                final MongoClientSettings settings = MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(uri))
                        .retryWrites(retryWrites)
                        .applyToClusterSettings(b ->
                                b.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                        .applyToSocketSettings(b -> {
                            b.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS);
                            if (readTimeoutMs > 0) {
                                b.readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS);
                            }
                        })
                        .applyToConnectionPoolSettings(b -> b
                                .maxSize(poolMaxSize)
                                .maxWaitTime(poolMaxWaitMs, TimeUnit.MILLISECONDS))
                        .build();

                mc = MongoClients.create(settings);
                final MongoDatabase db = mc.getDatabase(dbName);

                if (failFast) {
                    db.runCommand(new Document("ping", 1));
                }

                final MongoCollection<Document> initializedCollection = db.getCollection(collectionName);
                if (ensureBusinessKeyIndex && !useKeyAsDocumentId) {
                    initializedCollection.createIndex(Indexes.ascending(businessKeyField));
                }

                this.client = mc;
                this.collection = initializedCollection;

                log.info("MongoVectorSink active. uri='{}' db='{}' collection='{}'",
                        safeUri(uri), dbName, collectionName);
                if (!useKeyAsDocumentId && !ensureBusinessKeyIndex) {
                    log.warn("MongoVectorSink is using businessKeyField='{}' for upserts without _id identity mode or guaranteed index creation; benchmark throughput may be limited by lookup cost.",
                            businessKeyField);
                }
            } catch (RuntimeException e) {
                closeClientQuietly(mc);
                this.client = null;
                this.collection = null;

                if (failFast) {
                    throw e;
                }
                if (disableOnConnectFailure) {
                    disabled = true;
                    log.error("MongoVectorSink disabled due to connect failure. uri='{}' db='{}' collection='{}' msg={}",
                            safeUri(uri), dbName, collectionName, e.getMessage());
                    return;
                }
                throw e;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Vector helpers
    // -------------------------------------------------------------------------

    /**
     * Converts a float[] vector to a List<Double> for BSON.
     *
     * A fresh list is required because each pending write retains its own vector
     * payload until the next flush. Reusing a mutable buffer here would corrupt
     * queued documents or write models before the driver encodes them.
     * Atlas vector search indexes ultimately quantize to float32, so the transient
     * float -> double widening here is tolerated on the write path.
     */
    private static List<Double> vectorToDoubleList(float[] values) {
        if (values == null || values.length == 0) return List.of();
        final ArrayList<Double> out = new ArrayList<>(values.length);
        for (final float v : values) {
            out.add((double) v);
        }
        return out;
    }

    /**
     * Converts a List<Float> to a List<Double> for EnrichedTicket payloads.
     * Not reusing the ThreadLocal here since EnrichedTicket is not the hot
     * demo path and the returned list is appended to a Document that may
     * be reused in future; a fresh list is safer for this less-common path.
     */
    private static List<Double> toDoubleListFromFloatList(List<Float> values) {
        if (values == null || values.isEmpty()) return List.of();
        final ArrayList<Double> out = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            final Float v = values.get(i);
            if (v == null) {
                throw new IllegalArgumentException("Embedding list contains null element at index " + i);
            }
            out.add(v.doubleValue());
        }
        return out;
    }

    /**
     * Returns a Document containing the WireEvent headers.
     * Returns a fresh empty document in the no-headers case to avoid sharing
     * mutable state across write models.
     */
    private static Document buildHeadersDoc(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return new Document();
        }
        final Document doc = new Document();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            doc.append(entry.getKey(), entry.getValue());
        }
        return doc;
    }

    private static Map<String, String> mergeHeaders(Map<String, String> headers, Map<String, String> lineage) {
        final boolean hasHeaders = headers != null && !headers.isEmpty();
        final boolean hasLineage = lineage != null && !lineage.isEmpty();
        if (!hasHeaders && !hasLineage) {
            return Map.of();
        }
        if (!hasHeaders) {
            return lineage;
        }
        if (!hasLineage) {
            return headers;
        }

        final LinkedHashMap<String, String> merged = new LinkedHashMap<>(headers.size() + lineage.size());
        merged.putAll(headers);
        merged.putAll(lineage);
        return merged;
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    private void recordBatchOutcome(long elapsedNs, int batchSize,
                                    int attempted, int persistable, int persisted, int unsupported) {
        try {
            final long ms = elapsedNs / 1_000_000L; // raw arithmetic — no Duration allocation
            metrics.timer(  "streamkernel.mongo.sink.latency.ms",              ms);
            metrics.counter("streamkernel.mongo.sink.batches.total",           1.0);
            metrics.counter("streamkernel.mongo.sink.records.seen.total",      batchSize);
            metrics.counter("streamkernel.mongo.sink.records.attempted.total", attempted);
            metrics.counter("streamkernel.mongo.sink.records.persistable.total", persistable);
            metrics.counter("streamkernel.mongo.sink.records.unsupported.total", unsupported);

            if (persisted == 0) {
                metrics.counter("streamkernel.mongo.sink.batches.empty.total", 1.0);
            } else {
                metrics.counter("streamkernel.mongo.sink.batches.ok.total",    1.0);
                metrics.counter("streamkernel.mongo.sink.records.total",       persisted);
            }
        } catch (Exception e) {
            debugMetricsFailure("batch outcome", e);
        }
    }

    private void recordFlushMetrics(int succeeded, int failed, long durationMs) {
        try {
            metrics.counter("streamkernel.mongo.sink.flushes.total", 1.0);
            if (succeeded > 0) {
                metrics.counter("streamkernel.mongo.sink.ops.total", succeeded);
            }
            if (failed > 0) {
                metrics.counter("streamkernel.mongo.sink.errors.total", failed);
                metrics.counter("streamkernel.mongo.sink.records.dropped.total", failed);
            }
            metrics.timer("streamkernel.mongo.sink.flush.ms", durationMs);
        } catch (Exception e) {
            debugMetricsFailure("flush metrics", e);
        }
    }

    private void debugMetricsFailure(String operation, Exception e) {
        if (log.isDebugEnabled()) {
            log.debug("MongoVectorSink metrics recording failed during {}", operation, e);
        }
    }

    // -------------------------------------------------------------------------
    // Error handling
    // -------------------------------------------------------------------------

    private void handleUnsupportedPayload(Object v) {
        try {
            metrics.counter("streamkernel.mongo.sink.payload.unsupported.total", 1.0);
        } catch (Exception e) {
            debugMetricsFailure("unsupported payload", e);
        }

        if (strictPayloadType) {
            throw new IllegalArgumentException(
                    "MongoVectorSink does not support payload type: " + v.getClass().getName());
        }
        log.warn("MongoVectorSink skipped unsupported payload type: {}", v.getClass().getName());
    }

    private void handleInvalidPayload(Object v, IllegalArgumentException error) {
        try {
            metrics.counter("streamkernel.mongo.sink.payload.invalid.total", 1.0);
        } catch (Exception e) {
            debugMetricsFailure("invalid payload", e);
        }
        log.warn("MongoVectorSink skipped invalid payload type={} reason={}",
                v.getClass().getName(), error.getMessage());
    }

    // -------------------------------------------------------------------------
    // Config helpers
    // -------------------------------------------------------------------------

    private static String normalizeMode(String raw) {
        final String s = (raw == null) ? DEFAULT_UPSERT_MODE : raw.trim().toLowerCase();
        if (s.equals("update") || s.equals("replace") || s.equals("insert")) {
            return s;
        }
        log.warn("MongoVectorSink: unrecognized upsert.mode='{}'; defaulting to '{}'", raw, DEFAULT_UPSERT_MODE);
        return DEFAULT_UPSERT_MODE;
    }

    private static String nonBlank(String v, String def) {
        if (v == null) return def;
        final String s = v.trim();
        return s.isEmpty() ? def : s;
    }

    /** Returns the first non-null non-blank value from the candidates, or the last one. */
    private static String firstNonBlank(String... candidates) {
        for (final String c : candidates) {
            if (c != null && !c.isBlank()) return c;
        }
        return candidates.length > 0 ? candidates[candidates.length - 1] : null;
    }

    private static int getInt(PipelineConfig cfg, String key, int def) {
        return getInt(cfg, key, shortAlias(key), def);
    }

    private static int getInt(PipelineConfig cfg, String key, String alias, int def) {
        try {
            final String v = firstNonBlank(cfg.getString(key, null),
                    alias == null ? null : cfg.getString(alias, null));
            return (v == null || v.isBlank()) ? def : Integer.parseInt(v.trim());
        } catch (Exception e) { return def; }
    }

    private static long getLong(PipelineConfig cfg, String key, long def) {
        return getLong(cfg, key, shortAlias(key), def);
    }

    private static long getLong(PipelineConfig cfg, String key, String alias, long def) {
        try {
            final String v = firstNonBlank(cfg.getString(key, null),
                    alias == null ? null : cfg.getString(alias, null));
            return (v == null || v.isBlank()) ? def : Long.parseLong(v.trim());
        } catch (Exception e) { return def; }
    }

    private static boolean getBool(PipelineConfig cfg, String key, boolean def) {
        return getBool(cfg, key, shortAlias(key), def);
    }

    private static boolean getBool(PipelineConfig cfg, String key, String alias, boolean def) {
        try {
            final String v = firstNonBlank(cfg.getString(key, null),
                    alias == null ? null : cfg.getString(alias, null));
            return (v == null || v.isBlank()) ? def : Boolean.parseBoolean(v.trim());
        } catch (Exception e) { return def; }
    }

    private static String getString(PipelineConfig cfg, String key, String def) {
        try {
            final String alias = shortAlias(key);
            return nonBlank(firstNonBlank(cfg.getString(key, null),
                    alias == null ? null : cfg.getString(alias, null)), def);
        } catch (Exception e) {
            return def;
        }
    }

    private static String shortAlias(String key) {
        final String prefix = "mongodb.";
        return key != null && key.startsWith(prefix)
                ? "mongo." + key.substring(prefix.length())
                : null;
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }

    private static String safeString(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private static String safeUri(String uri) {
        if (uri == null) return "null";
        final int schemeIdx = uri.indexOf("://");
        if (schemeIdx < 0) return uri;
        final int authorityStart = schemeIdx + 3;
        final int atIdx = uri.lastIndexOf('@');
        if (atIdx <= authorityStart) return uri;

        final int slashIdx = uri.indexOf('/', authorityStart);
        final int queryIdx = uri.indexOf('?', authorityStart);
        int authorityEnd = uri.length();
        if (slashIdx >= 0) authorityEnd = Math.min(authorityEnd, slashIdx);
        if (queryIdx >= 0) authorityEnd = Math.min(authorityEnd, queryIdx);
        if (atIdx > authorityEnd) return uri;

        return uri.substring(0, authorityStart) + "***" + uri.substring(atIdx);
    }

    private static void closeClientQuietly(MongoClient client) {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (Exception ignored) {
        }
    }
}
