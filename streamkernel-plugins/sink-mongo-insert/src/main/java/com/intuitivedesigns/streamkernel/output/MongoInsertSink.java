/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

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
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class MongoInsertSink implements BatchOutputSink<Object> {

    private static final Logger log = LoggerFactory.getLogger(MongoInsertSink.class);

    // -------------------------------------------------------------------------
    // Config keys — canonical mongodb.* prefix, mongo.* accepted as alias
    // -------------------------------------------------------------------------
    private static final String KEY_URI             = "mongodb.uri";
    private static final String KEY_DB              = "mongodb.database";
    private static final String KEY_COLLECTION      = "mongodb.collection";
    private static final String KEY_BULK_MAX        = "mongodb.bulk.max.ops";
    private static final String KEY_BULK_ORDERED    = "mongodb.bulk.ordered";
    private static final String KEY_POOL_MAX_SIZE   = "mongodb.pool.max.size";
    private static final String KEY_POOL_MAX_WAIT   = "mongodb.pool.max.wait.ms";
    private static final String KEY_SEL_TIMEOUT     = "mongodb.server.selection.timeout.ms";
    private static final String KEY_CONN_TIMEOUT    = "mongodb.socket.connect.timeout.ms";
    private static final String KEY_FAIL_FAST       = "mongodb.fail.fast";
    private static final String KEY_DISABLE_ON_FAIL = "mongodb.disable.on.connect.failure";

    // -------------------------------------------------------------------------
    // Defaults
    // -------------------------------------------------------------------------
    private static final String  DEFAULT_URI          = "mongodb://localhost:27017";
    private static final String  DEFAULT_DB           = "support_db";
    private static final String  DEFAULT_COLLECTION   = "sk_insert_baseline";
    private static final int     DEFAULT_BULK_MAX     = 500;
    private static final boolean DEFAULT_BULK_ORDERED = false;
    private static final int     DEFAULT_POOL_MAX     = 100;
    private static final long    DEFAULT_POOL_WAIT    = 2_000L;
    private static final long    DEFAULT_SEL_TIMEOUT  = 5_000L;
    private static final int     DEFAULT_CONN_TIMEOUT = 2_000;
    private static final boolean DEFAULT_FAIL_FAST    = true;
    private static final boolean DEFAULT_DISABLE_FAIL = false;

    // -------------------------------------------------------------------------
    // Instance fields
    // -------------------------------------------------------------------------
    private final MetricsRuntime   metrics;
    private final String           uri;
    private final String           dbName;
    private final String           collectionName;
    private final int              bulkMax;
    private final InsertManyOptions insertOptions;   // precomputed, reused every flush
    private final int              poolMaxSize;
    private final long             poolMaxWaitMs;
    private final long             selTimeoutMs;
    private final int              connTimeoutMs;
    private final boolean          failFast;
    private final boolean          disableOnFail;

    /**
     * Per-thread document accumulation buffer.
     * Pre-sized to bulkMax so it never needs to resize during normal operation.
     * Cleared and refilled on each writeBatch call — documents are not shared.
     */
    private final ThreadLocal<ArrayList<Document>> tlDocs;

    // -------------------------------------------------------------------------
    // Volatile state
    // -------------------------------------------------------------------------
    private volatile MongoClient               client;
    private volatile MongoCollection<Document> collection;
    private volatile boolean                   disabled    = false;

    // -------------------------------------------------------------------------
    // Constructor (private — use fromConfig)
    // -------------------------------------------------------------------------
    private MongoInsertSink(
            MetricsRuntime metrics,
            String uri, String dbName, String collectionName,
            int bulkMax, boolean bulkOrdered,
            int poolMaxSize, long poolMaxWaitMs, long selTimeoutMs, int connTimeoutMs,
            boolean failFast, boolean disableOnFail
    ) {
        this.metrics        = metrics;
        this.uri            = uri;
        this.dbName         = dbName;
        this.collectionName = collectionName;
        this.bulkMax        = bulkMax;
        this.poolMaxSize    = poolMaxSize;
        this.poolMaxWaitMs  = poolMaxWaitMs;
        this.selTimeoutMs   = selTimeoutMs;
        this.connTimeoutMs  = connTimeoutMs;
        this.failFast       = failFast;
        this.disableOnFail  = disableOnFail;

        // Precomputed once — ordered flag never changes after construction
        this.insertOptions = new InsertManyOptions().ordered(bulkOrdered);

        // Pre-size to bulkMax so append never triggers a backing-array resize
        this.tlDocs = ThreadLocal.withInitial(() -> new ArrayList<>(bulkMax));
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------
    public static MongoInsertSink fromConfig(PipelineConfig cfg, MetricsRuntime metrics) {
        Objects.requireNonNull(cfg,     "cfg");
        Objects.requireNonNull(metrics, "metrics");

        // Accept both mongodb.* (canonical) and mongo.* (short alias) — canonical wins.
        final String uri = nonBlank(
                firstNonBlank(cfg.getString(KEY_URI, null),
                        cfg.getString("mongo.uri", null),
                        DEFAULT_URI),
                DEFAULT_URI);

        final String db = nonBlank(
                firstNonBlank(cfg.getString(KEY_DB, null),
                        cfg.getString("mongo.database", null),
                        DEFAULT_DB),
                DEFAULT_DB);

        final String col = nonBlank(
                firstNonBlank(cfg.getString(KEY_COLLECTION, null),
                        cfg.getString("mongo.collection", null),
                        DEFAULT_COLLECTION),
                DEFAULT_COLLECTION);

        final int     bulkMax     = clampInt(getInt(cfg, KEY_BULK_MAX,     DEFAULT_BULK_MAX),  1,   100_000);
        final boolean bulkOrdered = getBool(cfg, KEY_BULK_ORDERED, DEFAULT_BULK_ORDERED);
        final int     poolMax     = clampInt(getInt(cfg, KEY_POOL_MAX_SIZE, DEFAULT_POOL_MAX),  1,    10_000);
        final long    poolWait    = clampLong(getLong(cfg, KEY_POOL_MAX_WAIT, DEFAULT_POOL_WAIT), 1L, 120_000L);
        final long    selTimeout  = clampLong(getLong(cfg, KEY_SEL_TIMEOUT,  DEFAULT_SEL_TIMEOUT), 250L, 120_000L);
        final int     connTimeout = clampInt(getInt(cfg, KEY_CONN_TIMEOUT, DEFAULT_CONN_TIMEOUT), 250, 120_000);
        final boolean failFast    = getBool(cfg, KEY_FAIL_FAST,       DEFAULT_FAIL_FAST);
        final boolean disableFail = getBool(cfg, KEY_DISABLE_ON_FAIL, DEFAULT_DISABLE_FAIL);

        log.info("MongoInsertSink configured. db='{}' collection='{}' " +
                        "bulkMax={} ordered={} poolMax={} selTimeoutMs={}",
                db, col, bulkMax, bulkOrdered, poolMax, selTimeout);

        return new MongoInsertSink(
                metrics, uri, db, col,
                bulkMax, bulkOrdered,
                poolMax, poolWait, selTimeout, connTimeout,
                failFast, disableFail
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

        ensureInitialized();
        if (disabled) return;

        final MongoCollection<Document> col = this.collection;
        if (col == null) {
            log.warn("MongoInsertSink: collection null after init — dropping batch of {}", batch.size());
            return;
        }

        final long startNs = System.nanoTime();
        final long batchTimestampMs = System.currentTimeMillis();

        final ArrayList<Document> docs = tlDocs.get();
        docs.clear();

        int seen     = 0;
        int inserted = 0;
        int skipped  = 0;

        try {
            for (final PipelinePayload<Object> p : batch) {
                if (p == null) continue;
                final Object v = p.data();
                if (v == null) continue;
                seen++;

                if (v instanceof WireEvent w) {
                    docs.add(buildDocument(p, w, batchTimestampMs));

                    // Flush when we hit bulkMax to keep memory bounded and
                    // allow MongoDB to start persisting before the batch ends.
                    if (docs.size() >= bulkMax) {
                        inserted += flush(col, docs);
                        docs.clear();
                    }
                } else {
                    skipped++;
                    recordSkippedType(v);
                    if (log.isDebugEnabled()) {
                        log.debug("MongoInsertSink skipped unsupported payload type: {}",
                                v.getClass().getSimpleName());
                    }
                }
            }

            // Flush remainder
            if (!docs.isEmpty()) {
                inserted += flush(col, docs);
            }

            recordBatchOutcome(System.nanoTime() - startNs, seen, inserted, skipped);
        } finally {
            docs.clear();
            tlDocs.remove();
        }
    }

    // -------------------------------------------------------------------------
    // Document builder
    // -------------------------------------------------------------------------

    /**
     * Builds a BSON Document from a WireEvent.
     *
     * _id is set to WireEvent.key() — the UUID assigned by STRING_TO_WIREEVENT.
     * Using a client-supplied _id avoids server-side ObjectId generation and
     * removes one allocation from the MongoDB driver's internal write path.
     *
     * vector is appended only when non-null and non-empty — this makes the sink
     * forward-compatible with the embedding pipeline without requiring a code change.
     * When vector=null (baseline profile), the field is simply absent from the doc.
     */
    private Document buildDocument(PipelinePayload<Object> payload, WireEvent w, long timestampMs) {
        final Map<String, String> provenance = PipelineProvenance.extractProvenanceHeaders(
                payload == null ? null : payload.metadata()
        );
        // No _id field — MongoDB driver generates a unique ObjectId per document.
        // This eliminates duplicate key errors regardless of entropy mode or key reuse.
        final Document doc = new Document()
                .append("key",     safeString(w.key()))   // store key as a regular field
                .append("bytes",   w.bytes() != null ? w.bytes().length : 0)
                .append("headers", buildHeadersDoc(mergeHeaders(w.headers(), provenance)))
                .append("lineage", buildHeadersDoc(provenance))
                .append("ts",      timestampMs);

        final float[] vector = w.vector();
        if (vector != null && vector.length > 0) {
            doc.append("vector", floatArrayToDoubleList(vector));
        }

        return doc;
    }

    // -------------------------------------------------------------------------
    // Flush
    // -------------------------------------------------------------------------

    /**
     * Calls collection.insertMany() and returns the number of documents inserted.
     * insertMany() is used rather than bulkWrite(List<InsertOneModel>) to avoid
     * the per-document WriteModel wrapper and BulkWriteOptions dispatch overhead.
     */
    private int flush(MongoCollection<Document> col, List<Document> docs) {
        if (docs.isEmpty()) return 0;
        final long start = System.nanoTime();
        try {
            col.insertMany(docs, insertOptions);
            final long ms = (System.nanoTime() - start) / 1_000_000L;
            recordFlushMetrics(docs.size(), 0, ms);

            if (log.isDebugEnabled()) {
                log.debug("MONGO_INSERT_FLUSH docs={} ms={}", docs.size(), ms);
            }
            return docs.size();

        } catch (MongoBulkWriteException e) {
            final long ms = (System.nanoTime() - start) / 1_000_000L;
            final int inserted = Math.max(0, e.getWriteResult().getInsertedCount());
            final int failed = Math.max(0, docs.size() - inserted);
            recordFlushMetrics(inserted, failed, ms);

            log.error("MongoInsertSink partial flush: {}/{} inserted — {} failed",
                    inserted, docs.size(), failed);
            if (failFast) throw new RuntimeException("MongoInsertSink flush failed", e);
            return inserted;
        } catch (Exception e) {
            recordFlushMetrics(0, docs.size(), (System.nanoTime() - start) / 1_000_000L);
            log.error("MongoInsertSink flush failed: {} — batch of {} dropped",
                    e.getMessage(), docs.size());
            if (failFast) throw new RuntimeException("MongoInsertSink flush failed", e);
            return 0;
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
        tlDocs.remove();
        if (c != null) c.close();
    }

    // -------------------------------------------------------------------------
    // Init
    // -------------------------------------------------------------------------

    private void ensureInitialized() {
        if (disabled) return;
        if (collection != null) return;

        synchronized (this) {
            if (disabled) return;
            if (collection != null) return;
            MongoClient mc = null;
            try {
                final MongoClientSettings settings = MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(uri))
                        .applyToClusterSettings(b ->
                                b.serverSelectionTimeout(selTimeoutMs, TimeUnit.MILLISECONDS))
                        .applyToSocketSettings(b ->
                                b.connectTimeout(connTimeoutMs, TimeUnit.MILLISECONDS))
                        .applyToConnectionPoolSettings(b -> b
                                .maxSize(poolMaxSize)
                                .maxWaitTime(poolMaxWaitMs, TimeUnit.MILLISECONDS))
                        .build();

                mc = MongoClients.create(settings);
                final MongoDatabase db = mc.getDatabase(dbName);

                if (failFast) {
                    db.runCommand(new Document("ping", 1));
                }

                this.client = mc;
                this.collection = db.getCollection(collectionName);

                log.info("MongoInsertSink active. uri='{}' db='{}' collection='{}'",
                        safeUri(uri), dbName, collectionName);
            } catch (RuntimeException e) {
                closeClientQuietly(mc);
                this.client = null;
                this.collection = null;
                if (failFast) throw e;
                if (disableOnFail) {
                    disabled = true;
                    log.error("MongoInsertSink disabled — connect failed. uri='{}' db='{}' collection='{}' msg={}",
                            safeUri(uri), dbName, collectionName, e.getMessage());
                    return;
                }
                throw e;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Document buildHeadersDoc(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) return new Document();
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

    /**
     * Converts a float[] to List<Double> for BSON storage.
     * Boxing float → Double cannot be eliminated (BSON List<Double> requires objects).
     * Atlas vector search indexes ultimately quantize to float32, so the transient
     * float → double widening here is tolerated on the write path.
     * A fresh ArrayList is allocated here because the document may outlive the
     * current thread's processing cycle — unlike MongoVectorSink's ThreadLocal
     * approach which is safe only because the list is consumed before the next record.
     */
    private static List<Double> floatArrayToDoubleList(float[] values) {
        final ArrayList<Double> out = new ArrayList<>(values.length);
        for (final float v : values) out.add((double) v);
        return out;
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

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    private void recordBatchOutcome(long elapsedNs, int seen, int inserted, int skipped) {
        try {
            metrics.timer(  "streamkernel.mongo.insert.sink.latency.ms",        elapsedNs / 1_000_000L);
            metrics.counter("streamkernel.mongo.insert.sink.batches.total",     1.0);
            metrics.counter("streamkernel.mongo.insert.sink.records.seen.total", seen);
            metrics.counter("streamkernel.mongo.insert.sink.records.skipped.total", skipped);
            if (inserted > 0) {
                metrics.counter("streamkernel.mongo.insert.sink.batches.ok.total", 1.0);
            } else {
                metrics.counter("streamkernel.mongo.insert.sink.batches.empty.total", 1.0);
            }
        } catch (Exception ignored) {}
    }

    private void recordFlushMetrics(int inserted, int failed, long durationMs) {
        try {
            metrics.counter("streamkernel.mongo.insert.sink.flushes.total", 1.0);
            if (inserted > 0) {
                metrics.counter("streamkernel.mongo.insert.sink.records.total", inserted);
            }
            if (failed > 0) {
                metrics.counter("streamkernel.mongo.insert.sink.errors.total", failed);
            }
            metrics.timer("streamkernel.mongo.insert.sink.flush.ms", durationMs);
        } catch (Exception ignored) {}
    }

    private void recordSkippedType(Object value) {
        final String type = sanitizeMetricSegment(value == null ? "unknown" : value.getClass().getSimpleName());
        try {
            metrics.counter("streamkernel.mongo.insert.sink.records.skipped.type." + type + ".total", 1.0);
        } catch (Exception ignored) {}
    }

    private static String sanitizeMetricSegment(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        final StringBuilder out = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final char ch = Character.toLowerCase(value.charAt(i));
            if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                out.append(ch);
            } else {
                out.append('_');
            }
        }
        return out.toString();
    }

    private static void closeClientQuietly(MongoClient client) {
        if (client == null) return;
        try {
            client.close();
        } catch (Exception ignored) {
            // best effort
        }
    }

    // -------------------------------------------------------------------------
    // Config helpers
    // -------------------------------------------------------------------------

    private static String firstNonBlank(String... candidates) {
        for (final String c : candidates) {
            if (c != null && !c.isBlank()) return c;
        }
        return candidates.length > 0 ? candidates[candidates.length - 1] : null;
    }

    private static String nonBlank(String v, String def) {
        if (v == null) return def;
        final String s = v.trim();
        return s.isEmpty() ? def : s;
    }

    private static int getInt(PipelineConfig cfg, String key, int def) {
        try {
            final String v = cfg.getString(key, null);
            return (v == null || v.isBlank()) ? def : Integer.parseInt(v.trim());
        } catch (Exception e) { return def; }
    }

    private static long getLong(PipelineConfig cfg, String key, long def) {
        try {
            final String v = cfg.getString(key, null);
            return (v == null || v.isBlank()) ? def : Long.parseLong(v.trim());
        } catch (Exception e) { return def; }
    }

    private static boolean getBool(PipelineConfig cfg, String key, boolean def) {
        try {
            final String v = cfg.getString(key, null);
            return (v == null || v.isBlank()) ? def : Boolean.parseBoolean(v.trim());
        } catch (Exception e) { return def; }
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }
}
