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
import com.intuitivedesigns.streamkernel.core.DeferredBatchOutputSink;
import com.intuitivedesigns.streamkernel.core.DeferredWriteResult;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import com.intuitivedesigns.streamkernel.spi.Readyable;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local Databricks-style demo sink:
 * StreamKernel writes enriched rows directly into a Delta table backed by MinIO
 * over S3A, while Spark reads the same table back for validation.
 */
public final class DeltaSink implements BatchOutputSink<Object>, DeferredBatchOutputSink<Object, Object>, Readyable {

    private static final Logger log = LoggerFactory.getLogger(DeltaSink.class);

    private static final String KEY_TABLE_PATH = "delta.table.path";
    private static final String KEY_CREATE_IF_MISSING = "delta.table.create.if.missing";
    private static final String KEY_CHECKPOINT_INTERVAL = "delta.checkpoint.interval";
    private static final String KEY_MAX_RETRIES = "delta.commit.max.retries";
    private static final String KEY_COALESCE_MAX_ROWS = "delta.commit.coalesce.max.rows";
    private static final String KEY_COALESCE_MAX_WAIT_MS = "delta.commit.coalesce.max.wait.ms";
    private static final String KEY_STRICT_PAYLOAD_TYPE = "delta.strict.payload.type";
    private static final String SOURCE_TEXT_METADATA_KEY = "streamkernel.source.text";
    private static final String DEFAULT_SENTIMENT = "NEUTRAL";

    private static final String KEY_S3_ENDPOINT = "delta.s3.endpoint";
    private static final String KEY_S3_ACCESS_KEY = "delta.s3.access.key";
    private static final String KEY_S3_SECRET_KEY = "delta.s3.secret.key";
    private static final String KEY_S3_REGION = "delta.s3.region";
    private static final String KEY_S3_PATH_STYLE = "delta.s3.path.style.access";
    private static final String KEY_S3_SSL_ENABLED = "delta.s3.ssl.enabled";
    private static final String KEY_S3_LOCAL_TMP_DIR = "delta.s3.local.tmp.dir";

    private static final String M_ROWS_TOTAL = "streamkernel.delta.sink.rows.total";
    private static final String M_BATCHES_TOTAL = "streamkernel.delta.sink.batches.total";
    private static final String M_BATCHES_EMPTY = "streamkernel.delta.sink.batches.empty.total";
    private static final String M_BATCHES_SKIPPED = "streamkernel.delta.sink.batches.skipped.total";
    private static final String M_UNSUPPORTED = "streamkernel.delta.sink.payload.unsupported.total";
    private static final String M_COMMIT_MS = "streamkernel.delta.sink.commit.ms";
    private static final String TX_APP_ID = "streamkernel-delta-sink";
    private static final long FLUSH_WAIT_SAFETY_MS = 1_000L;

    // Keep the Delta schema nullable-compatible with Spark's empty-table bootstrap path.
    // Spark normalizes the precreated table metadata to nullable columns, and Delta Kernel
    // requires exact schema equality on append.
    private static final StructType TABLE_SCHEMA = new StructType()
            .add("ticketId", StringType.STRING, true)
            .add("description", StringType.STRING, true)
            .add("sentiment", StringType.STRING, true)
            .add("embedding", new ArrayType(FloatType.FLOAT, false), true);

    private final MetricsRuntime metrics;
    private final String tablePath;
    private final boolean createIfMissing;
    private final int checkpointInterval;
    private final int maxRetries;
    private final int coalesceMaxRows;
    private final long coalesceMaxWaitMs;
    private final boolean strictPayloadType;
    private final Configuration hadoopConf;
    private final Object initLock = new Object();

    private final Object writeLock = new Object();
    private final ArrayDeque<PendingWrite> pendingWrites = new ArrayDeque<>();
    private final ArrayDeque<DeferredWriteResult<Object>> completedWrites = new ArrayDeque<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean tableKnownToExist = new AtomicBoolean(false);
    private final AtomicLong transactionVersion = new AtomicLong(0L);
    private int pendingRows = 0;
    private long coalesceWindowStartNs = 0L;
    private boolean flushInProgress = false;
    private boolean closing = false;
    private Thread commitThread;

    private volatile Engine engine;
    private volatile Table table;
    private volatile Exception backgroundFailure;

    private DeltaSink(MetricsRuntime metrics,
                      String tablePath,
                      boolean createIfMissing,
                      int checkpointInterval,
                      int maxRetries,
                      int coalesceMaxRows,
                      long coalesceMaxWaitMs,
                      boolean strictPayloadType,
                      Configuration hadoopConf) {
        this.metrics = metrics;
        this.tablePath = tablePath;
        this.createIfMissing = createIfMissing;
        this.checkpointInterval = checkpointInterval;
        this.maxRetries = maxRetries;
        this.coalesceMaxRows = coalesceMaxRows;
        this.coalesceMaxWaitMs = coalesceMaxWaitMs;
        this.strictPayloadType = strictPayloadType;
        this.hadoopConf = hadoopConf;
    }

    public static DeltaSink fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String tablePath = requireNonBlank(config.getString(KEY_TABLE_PATH, null), KEY_TABLE_PATH);
        final boolean createIfMissing = config.getBoolean(KEY_CREATE_IF_MISSING, true);
        final int checkpointInterval = Math.max(0, config.getInt(KEY_CHECKPOINT_INTERVAL, 10));
        final int maxRetries = Math.max(1, config.getInt(KEY_MAX_RETRIES, 10));
        final int coalesceMaxRows = Math.max(1, config.getInt(KEY_COALESCE_MAX_ROWS, 128));
        final long coalesceMaxWaitMs = Math.max(0L, config.getLong(KEY_COALESCE_MAX_WAIT_MS, 25L));
        final boolean strictPayloadType = config.getBoolean(KEY_STRICT_PAYLOAD_TYPE, false);
        final java.nio.file.Path localTmpDir = prepareLocalTempDir(config);

        final Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        hadoopConf.setBoolean("fs.s3a.path.style.access", config.getBoolean(KEY_S3_PATH_STYLE, true));
        hadoopConf.set("fs.s3a.endpoint", requireNonBlank(config.getString(KEY_S3_ENDPOINT, null), KEY_S3_ENDPOINT));
        hadoopConf.set("fs.s3a.access.key", requireNonBlank(config.getString(KEY_S3_ACCESS_KEY, null), KEY_S3_ACCESS_KEY));
        hadoopConf.set("fs.s3a.secret.key", requireNonBlank(config.getString(KEY_S3_SECRET_KEY, null), KEY_S3_SECRET_KEY));
        hadoopConf.set("fs.s3a.endpoint.region", config.getString(KEY_S3_REGION, "us-east-1"));
        hadoopConf.setBoolean("fs.s3a.connection.ssl.enabled", config.getBoolean(KEY_S3_SSL_ENABLED, false));
        hadoopConf.set("fs.s3a.committer.name", "directory");
        hadoopConf.setBoolean("fs.s3a.committer.staging.conflict-mode", false);
        hadoopConf.set("hadoop.tmp.dir", localTmpDir.toString());
        hadoopConf.set("fs.s3a.buffer.dir", localTmpDir.toString());
        hadoopConf.setBoolean("fs.s3a.fast.upload", true);
        hadoopConf.set("fs.s3a.fast.upload.buffer", "bytebuffer");

        return new DeltaSink(
                metrics,
                tablePath,
                createIfMissing,
                checkpointInterval,
                maxRetries,
                coalesceMaxRows,
                coalesceMaxWaitMs,
                strictPayloadType,
                hadoopConf
        );
    }

    @Override
    public void verifyReady() throws Exception {
        ensureInitialized();
        final Path deltaLog = new Path(new Path(tablePath), "_delta_log");
        if (deltaLog.getFileSystem(hadoopConf).exists(deltaLog)) {
            tableKnownToExist.set(true);
        }
    }

    @Override
    public void write(PipelinePayload<Object> payload) throws Exception {
        if (payload == null || payload.data() == null) {
            return;
        }
        writeBatch(List.of(payload));
    }

    @Override
    public void writeBatch(List<PipelinePayload<Object>> batch) throws Exception {
        final PendingWrite request = preparePendingWrite(batch, null, false);
        if (request == null) {
            return;
        }

        ensureInitialized();
        enqueueWrite(request);
        awaitCompletion(request);
    }

    @Override
    public void writeBatchDeferred(
            List<PipelinePayload<Object>> batch,
            List<PipelinePayload<Object>> sourceInputs) throws Exception {
        final PendingWrite request = preparePendingWrite(batch, sourceInputs, true);
        if (request == null) {
            return;
        }

        ensureInitialized();
        enqueueWrite(request);
    }

    @Override
    public List<DeferredWriteResult<Object>> drainCompletedWrites() {
        synchronized (writeLock) {
            if (completedWrites.isEmpty()) {
                return List.of();
            }
            final ArrayList<DeferredWriteResult<Object>> drained = new ArrayList<>(completedWrites);
            completedWrites.clear();
            return drained;
        }
    }

    @Override
    public void close() throws Exception {
        final Thread threadToJoin;
        synchronized (writeLock) {
            closing = true;
            writeLock.notifyAll();
            threadToJoin = commitThread;
        }

        boolean interrupted = false;
        if (threadToJoin != null) {
            while (threadToJoin.isAlive()) {
                try {
                    threadToJoin.join(250L);
                } catch (InterruptedException ie) {
                    // Preserve the caller's interrupt status, but keep waiting so
                    // shutdown can drain the background commit thread cleanly.
                    interrupted = true;
                }
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        final Exception failure = backgroundFailure;
        if (failure != null) {
            throw cloneFailure(failure);
        }
    }

    private void ensureInitialized() {
        if (initialized.get()) {
            return;
        }
        synchronized (initLock) {
            if (initialized.get()) {
                return;
            }
            final Engine newEngine = DefaultEngine.create(hadoopConf);
            final Table newTable = Table.forPath(newEngine, tablePath);
            this.engine = newEngine;
            this.table = newTable;
            synchronized (writeLock) {
                if (commitThread == null) {
                    commitThread = new Thread(this::runCommitLoop, "sk-delta-commit");
                    commitThread.setDaemon(true);
                    commitThread.start();
                }
            }
            initialized.set(true);
            log.info("DeltaSink initialized. tablePath={}", tablePath);
        }
    }

    private boolean tableExists() throws IOException {
        final Path deltaLog = new Path(new Path(tablePath), "_delta_log");
        final var fs = deltaLog.getFileSystem(hadoopConf);
        if (!fs.exists(deltaLog)) {
            return false;
        }

        final FileStatus[] statuses = fs.listStatus(deltaLog);
        if (statuses == null || statuses.length == 0) {
            return false;
        }

        for (FileStatus status : statuses) {
            if (!status.isFile()) {
                continue;
            }

            final String name = status.getPath().getName();
            if (name.endsWith(".json") || name.contains(".checkpoint.") || name.endsWith(".checkpoint.parquet")) {
                return true;
            }
        }

        return false;
    }

    private boolean ensureTableExistsState() throws IOException {
        if (tableKnownToExist.get()) {
            return true;
        }
        final boolean exists = tableExists();
        if (exists) {
            tableKnownToExist.set(true);
        }
        return exists;
    }

    private void checkpointIfNeeded(long version) {
        if (checkpointInterval <= 0 || version <= 0 || version % checkpointInterval != 0) {
            return;
        }
        try {
            table.checkpoint(engine, version);
        } catch (Exception e) {
            log.warn("DeltaSink checkpoint attempt failed at version {}: {}", version, e.getMessage());
        }
    }

    private void runPostCommitHooks(TransactionCommitResult commitResult) {
        for (PostCommitHook hook : commitResult.getPostCommitHooks()) {
            try {
                hook.threadSafeInvoke(engine);
            } catch (Exception e) {
                log.warn("DeltaSink post-commit hook {} failed: {}", hook.getType(), e.getMessage());
            }
        }
    }

    private PendingWrite preparePendingWrite(
            List<PipelinePayload<Object>> batch,
            List<PipelinePayload<Object>> sourceInputs,
            boolean deferred) throws Exception {
        if (batch == null || batch.isEmpty()) {
            metrics.counter(M_BATCHES_EMPTY);
            return null;
        }
        if (deferred && (sourceInputs == null || sourceInputs.size() != batch.size())) {
            throw new IllegalArgumentException("Deferred DeltaSink writes require source inputs aligned to the batch");
        }

        final ArrayList<DeltaRow> rows = new ArrayList<>(batch.size());
        final ArrayList<PipelinePayload<Object>> acceptedSourceInputs =
                deferred ? new ArrayList<>(batch.size()) : null;

        for (int i = 0; i < batch.size(); i++) {
            final PipelinePayload<Object> payload = batch.get(i);
            if (payload == null || payload.data() == null) {
                continue;
            }

            final Object value = payload.data();
            if (value instanceof EnrichedTicket ticket) {
                rows.add(toDeltaRow(ticket));
                if (acceptedSourceInputs != null) {
                    acceptedSourceInputs.add(sourceInputs.get(i));
                }
                continue;
            }

            if (value instanceof WireEvent wireEvent) {
                try {
                    rows.add(toDeltaRow(payload, wireEvent));
                    if (acceptedSourceInputs != null) {
                        acceptedSourceInputs.add(sourceInputs.get(i));
                    }
                } catch (IllegalArgumentException | IllegalStateException e) {
                    metrics.counter(M_UNSUPPORTED);
                    if (strictPayloadType) {
                        throw new IllegalArgumentException(
                                "DeltaSink requires EnrichedTicket or embedded WireEvent payloads but received invalid WireEvent: "
                                        + e.getMessage(),
                                e);
                    }
                    log.warn("DeltaSink skipped invalid WireEvent payload: {}", e.getMessage());
                }
                continue;
            }

            metrics.counter(M_UNSUPPORTED);
            if (strictPayloadType) {
                throw new IllegalArgumentException(
                        "DeltaSink requires EnrichedTicket or embedded WireEvent payloads but received "
                                + value.getClass().getName());
            }
            log.warn("DeltaSink skipped unsupported payload type {}", value.getClass().getName());
        }

        if (rows.isEmpty()) {
            metrics.counter(M_BATCHES_SKIPPED);
            return null;
        }

        return new PendingWrite(
                List.copyOf(rows),
                acceptedSourceInputs == null ? List.of() : List.copyOf(acceptedSourceInputs),
                deferred
        );
    }

    private void enqueueWrite(PendingWrite request) throws Exception {
        synchronized (writeLock) {
            if (closing) {
                throw new IllegalStateException("DeltaSink is closing and cannot accept new writes");
            }
            if (backgroundFailure != null) {
                throw cloneFailure(backgroundFailure);
            }

            final boolean wasEmpty = pendingWrites.isEmpty();
            pendingWrites.addLast(request);
            pendingRows += request.rowCount();
            request.enqueued = true;
            if (wasEmpty && !flushInProgress) {
                coalesceWindowStartNs = System.nanoTime();
            }
            writeLock.notifyAll();
        }
    }

    private void awaitCompletion(PendingWrite request) throws Exception {
        boolean interrupted = false;
        while (true) {
            synchronized (writeLock) {
                if (request.completed) {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    request.rethrowIfFailed();
                    return;
                }
                if (backgroundFailure != null) {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    throw cloneFailure(backgroundFailure);
                }

                try {
                    writeLock.wait(Math.max(1L, coalesceMaxWaitMs + FLUSH_WAIT_SAFETY_MS));
                } catch (InterruptedException ie) {
                    interrupted = true;
                }
            }
        }
    }

    private void runCommitLoop() {
        try {
            while (true) {
                final CommitWork work;
                synchronized (writeLock) {
                    while (true) {
                        if (closing && !flushInProgress && pendingWrites.isEmpty()) {
                            return;
                        }

                        if (!flushInProgress && shouldFlushLocked()) {
                            work = beginFlushLocked();
                            break;
                        }

                        final long waitMs = pendingWrites.isEmpty() ? 0L : computeWaitMsLocked();
                        try {
                            if (waitMs > 0L) {
                                writeLock.wait(waitMs);
                            } else {
                                writeLock.wait();
                            }
                        } catch (InterruptedException ie) {
                            if (closing) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }

                flushCommit(work, materializeRows(work.requests(), work.rowCapacity()));
            }
        } catch (Throwable t) {
            final Exception failure = asException(t);
            log.error("DeltaSink commit thread crashed: {}", failure.getMessage(), failure);
            synchronized (writeLock) {
                backgroundFailure = failure;
                flushInProgress = false;
                failPendingWritesLocked(failure);
                closing = true;
                writeLock.notifyAll();
            }
        }
    }

    private boolean shouldFlushLocked() {
        if (pendingWrites.isEmpty()) {
            return false;
        }
        if (closing || coalesceMaxWaitMs <= 0L || pendingRows >= coalesceMaxRows) {
            return true;
        }
        return coalesceWindowAgeMsLocked() >= coalesceMaxWaitMs;
    }

    private long computeWaitMsLocked() {
        if (pendingWrites.isEmpty()) {
            return 0L;
        }
        if (flushInProgress) {
            return Math.max(1L, coalesceMaxWaitMs + FLUSH_WAIT_SAFETY_MS);
        }
        if (closing || coalesceMaxWaitMs <= 0L || pendingRows >= coalesceMaxRows) {
            return 0L;
        }
        final long remainingMs = coalesceMaxWaitMs - coalesceWindowAgeMsLocked();
        return Math.max(1L, remainingMs);
    }

    private long coalesceWindowAgeMsLocked() {
        final PendingWrite oldest = pendingWrites.peekFirst();
        if (oldest == null) {
            return 0L;
        }
        final long windowStartNs = coalesceWindowStartNs > 0L ? coalesceWindowStartNs : oldest.enqueuedNs;
        return Math.max(0L, (System.nanoTime() - windowStartNs) / 1_000_000L);
    }

    private CommitWork beginFlushLocked() {
        flushInProgress = true;
        final ArrayList<PendingWrite> writes = new ArrayList<>(pendingWrites);
        pendingWrites.clear();
        coalesceWindowStartNs = 0L;
        final long txVersion = transactionVersion.incrementAndGet();
        final int rowCapacity = pendingRows;
        pendingRows = 0;
        return new CommitWork(writes, rowCapacity, txVersion);
    }

    private static ArrayList<DeltaRow> materializeRows(List<PendingWrite> writes, int rowCapacity) {
        final ArrayList<DeltaRow> rows = new ArrayList<>(Math.max(0, rowCapacity));
        for (PendingWrite write : writes) {
            rows.addAll(write.rows);
        }
        return rows;
    }

    private Exception flushCommit(CommitWork work, List<DeltaRow> rows) {
        Exception failure = null;
        try {
            commitRows(rows, work.requestCount(), work.txVersion());
        } catch (Exception e) {
            failure = e;
        } finally {
            synchronized (writeLock) {
                flushInProgress = false;
                if (!pendingWrites.isEmpty() && !closing) {
                    // Writes that arrived while a commit was in flight get a new
                    // coalescing window once the sink becomes idle again.
                    coalesceWindowStartNs = System.nanoTime();
                }
                for (PendingWrite write : work.requests) {
                    write.complete(failure);
                    if (write.deferred) {
                        completedWrites.addLast(new DeferredWriteResult<>(
                                write.sourceInputs,
                                failure == null ? null : cloneFailure(failure)
                        ));
                    }
                }
                writeLock.notifyAll();
            }
        }
        return failure;
    }

    private void failPendingWritesLocked(Exception failure) {
        if (pendingWrites.isEmpty()) {
            return;
        }

        final ArrayList<PendingWrite> stranded = new ArrayList<>(pendingWrites);
        pendingWrites.clear();
        pendingRows = 0;
        coalesceWindowStartNs = 0L;
        for (PendingWrite write : stranded) {
            write.complete(failure);
            if (write.deferred) {
                completedWrites.addLast(new DeferredWriteResult<>(
                        write.sourceInputs,
                        cloneFailure(failure)
                ));
            }
        }
    }

    private static Exception asException(Throwable t) {
        if (t instanceof Exception e) {
            return e;
        }
        return new IllegalStateException("DeltaSink commit thread failed", t);
    }

    private void commitRows(List<DeltaRow> rows, int inputBatchCount, long txVersion) throws Exception {
        final long startNs = System.nanoTime();
        final boolean tableExists = ensureTableExistsState();
        if (!tableExists && !createIfMissing) {
            throw new IllegalStateException("Delta table does not exist and auto-create is disabled: " + tablePath);
        }

        final Operation operation = tableExists ? Operation.WRITE : Operation.CREATE_TABLE;

        TransactionBuilder builder = table.createTransactionBuilder(engine, "StreamKernel DeltaSink", operation)
                .withMaxRetries(maxRetries)
                .withTransactionId(engine, TX_APP_ID, txVersion);

        if (!tableExists) {
            builder = builder.withSchema(engine, TABLE_SCHEMA);
        }

        final Transaction transaction = builder.build(engine);
        final TransactionCommitResult commit;
        final Row transactionState = transaction.getTransactionState(engine);
        final DataWriteContext writeContext =
                Transaction.getWriteContext(engine, transactionState, Map.of());
        try (CloseableIterator<FilteredColumnarBatch> logicalRows =
                     new SingletonIterator<>(new FilteredColumnarBatch(buildBatch(rows), Optional.empty()));
             CloseableIterator<FilteredColumnarBatch> physicalRows =
                     Transaction.transformLogicalData(engine, transactionState, logicalRows, Map.of());
             CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
                     .writeParquetFiles(writeContext.getTargetDirectory(), physicalRows, writeContext.getStatisticsColumns());
             CloseableIterator<Row> actions =
                     Transaction.generateAppendActions(engine, transactionState, dataFiles, writeContext);
             CloseableIterable<Row> commitActions = CloseableIterable.inMemoryIterable(actions)) {
            commit = transaction.commit(engine, commitActions);
        }

        tableKnownToExist.set(true);
        runPostCommitHooks(commit);
        checkpointIfNeeded(commit.getVersion());

        final long elapsedMs = Math.max(1L, (System.nanoTime() - startNs) / 1_000_000L);
        metrics.counter(M_BATCHES_TOTAL);
        metrics.counter(M_ROWS_TOTAL, rows.size());
        metrics.timer(M_COMMIT_MS, elapsedMs);
        log.info("DeltaSink committed {} rows ({} input batches) to {} as version {}",
                rows.size(), inputBatchCount, tablePath, commit.getVersion());
    }

    private static ColumnarBatch buildBatch(List<DeltaRow> rows) {
        final ArrayList<String> ticketIds = new ArrayList<>(rows.size());
        final ArrayList<String> descriptions = new ArrayList<>(rows.size());
        final ArrayList<String> sentiments = new ArrayList<>(rows.size());
        final ArrayList<Object> embeddings = new ArrayList<>(rows.size());

        for (DeltaRow row : rows) {
            ticketIds.add(row == null ? null : row.ticketId());
            descriptions.add(row == null ? null : row.description());
            sentiments.add(row == null ? null : row.sentiment());
            embeddings.add(row == null ? null : row.embedding());
        }

        return new SimpleColumnarBatch(
                TABLE_SCHEMA,
                List.of(
                        new StringListColumnVector(ticketIds),
                        new StringListColumnVector(descriptions),
                        new StringListColumnVector(sentiments),
                        new FloatArrayColumnVector(embeddings)
                ),
                rows.size()
        );
    }

    private static DeltaRow toDeltaRow(EnrichedTicket ticket) {
        if (ticket == null) {
            return null;
        }
        return new DeltaRow(
                ticket.getTicketId(),
                safeText(ticket.getDescription()),
                safeSentiment(ticket.getSentiment()),
                normalizeEmbedding(ticket.getEmbedding())
        );
    }

    private static DeltaRow toDeltaRow(PipelinePayload<Object> payload, WireEvent wireEvent) {
        if (wireEvent == null) {
            throw new IllegalArgumentException("WireEvent payload is null");
        }
        return new DeltaRow(
                resolveTicketId(payload, wireEvent),
                resolveDescription(payload, wireEvent),
                DEFAULT_SENTIMENT,
                requireEmbedding(wireEvent.vector(), "WireEvent")
        );
    }

    private static List<Float> normalizeEmbedding(List<Float> embedding) {
        if (embedding == null || embedding.isEmpty()) {
            return List.of();
        }
        boolean hasNull = false;
        for (Float value : embedding) {
            if (value == null) {
                hasNull = true;
                break;
            }
        }
        if (!hasNull) {
            return embedding;
        }
        final ArrayList<Float> values = new ArrayList<>(embedding.size());
        for (Float value : embedding) {
            if (value != null) {
                values.add(value);
            }
        }
        return List.copyOf(values);
    }

    private static float[] requireEmbedding(float[] embedding, String source) {
        if (embedding == null || embedding.length == 0) {
            throw new IllegalStateException(source + " is missing an embedding vector.");
        }
        return embedding;
    }

    private static String resolveDescription(PipelinePayload<Object> payload, WireEvent wireEvent) {
        if (payload != null) {
            final String metadataText = payload.metadata().get(SOURCE_TEXT_METADATA_KEY);
            if (metadataText != null) {
                return safeText(metadataText);
            }
        }
        return wireEvent == null ? "" : safeText(wireEvent.text());
    }

    private static String resolveTicketId(PipelinePayload<Object> payload, WireEvent wireEvent) {
        if (wireEvent != null && wireEvent.key() != null && !wireEvent.key().isBlank()) {
            return wireEvent.key();
        }
        if (payload != null && payload.id() != null && !payload.id().isBlank()) {
            return payload.id();
        }
        return UUID.randomUUID().toString();
    }

    private static String safeText(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value;
    }

    private static String safeSentiment(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_SENTIMENT;
        }
        return value;
    }

    private static String requireNonBlank(String value, String key) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required Delta sink config key: " + key);
        }
        return value.trim();
    }

    private static java.nio.file.Path prepareLocalTempDir(PipelineConfig config) {
        final String raw = config.getString(KEY_S3_LOCAL_TMP_DIR, "streamkernel-tmp/delta-s3a");
        final java.nio.file.Path dir = java.nio.file.Path.of(raw).toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create Delta S3 temp directory: " + dir, e);
        }
        return dir;
    }

    private static final class PendingWrite {
        private final List<DeltaRow> rows;
        private final List<PipelinePayload<Object>> sourceInputs;
        private final boolean deferred;
        private final long enqueuedNs;
        private boolean enqueued;
        private boolean completed;
        private Exception failure;

        private PendingWrite(List<DeltaRow> rows,
                             List<PipelinePayload<Object>> sourceInputs,
                             boolean deferred) {
            this.rows = rows;
            this.sourceInputs = sourceInputs;
            this.deferred = deferred;
            this.enqueuedNs = System.nanoTime();
        }

        private int rowCount() {
            return rows.size();
        }

        private void complete(Exception failure) {
            this.failure = failure;
            this.completed = true;
        }

        private void rethrowIfFailed() throws Exception {
            if (failure != null) {
                throw cloneFailure(failure);
            }
        }
    }

    private record CommitWork(List<PendingWrite> requests, int rowCapacity, long txVersion) {
        private int requestCount() {
            return requests.size();
        }
    }

    private record DeltaRow(String ticketId, String description, String sentiment, Object embedding) {
    }

    private static final class SimpleColumnarBatch implements ColumnarBatch {
        private final StructType schema;
        private final List<ColumnVector> vectors;
        private final int size;

        private SimpleColumnarBatch(StructType schema, List<ColumnVector> vectors, int size) {
            this.schema = schema;
            this.vectors = vectors;
            this.size = size;
        }

        @Override
        public StructType getSchema() {
            return schema;
        }

        @Override
        public ColumnVector getColumnVector(int ordinal) {
            return vectors.get(ordinal);
        }

        @Override
        public int getSize() {
            return size;
        }
    }

    private abstract static class BaseColumnVector implements ColumnVector {
        private final io.delta.kernel.types.DataType dataType;
        private final int size;

        private BaseColumnVector(io.delta.kernel.types.DataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        @Override
        public io.delta.kernel.types.DataType getDataType() {
            return dataType;
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static final class StringListColumnVector extends BaseColumnVector {
        private final List<String> values;

        private StringListColumnVector(List<String> values) {
            super(StringType.STRING, values.size());
            this.values = values;
        }

        @Override
        public boolean isNullAt(int rowId) {
            return values.get(rowId) == null;
        }

        @Override
        public String getString(int rowId) {
            return values.get(rowId);
        }
    }

    private static final class FloatArrayColumnVector extends BaseColumnVector {
        private final List<Object> values;

        private FloatArrayColumnVector(List<Object> values) {
            super(new ArrayType(FloatType.FLOAT, false), values.size());
            this.values = values;
        }

        @Override
        public boolean isNullAt(int rowId) {
            return values.get(rowId) == null;
        }

        @Override
        public ArrayValue getArray(int rowId) {
            final Object array = values.get(rowId);
            return array == null ? null : new FloatArrayValue(array);
        }
    }

    private static final class FloatArrayValue implements ArrayValue {
        private final Object values;

        private FloatArrayValue(Object values) {
            this.values = values;
        }

        @Override
        public int getSize() {
            if (values instanceof List<?> list) {
                return list.size();
            }
            if (values instanceof float[] array) {
                return array.length;
            }
            throw new IllegalStateException("Unsupported Delta embedding container: " + values.getClass().getName());
        }

        @Override
        public ColumnVector getElements() {
            if (values instanceof List<?> list) {
                @SuppressWarnings("unchecked")
                final List<Float> floatList = (List<Float>) list;
                return new FloatListColumnVector(floatList);
            }
            if (values instanceof float[] array) {
                return new PrimitiveFloatArrayColumnVector(array);
            }
            throw new IllegalStateException("Unsupported Delta embedding container: " + values.getClass().getName());
        }
    }

    private static final class FloatListColumnVector extends BaseColumnVector {
        private final List<Float> values;

        private FloatListColumnVector(List<Float> values) {
            super(FloatType.FLOAT, values.size());
            this.values = values;
        }

        @Override
        public boolean isNullAt(int rowId) {
            return values.get(rowId) == null;
        }

        @Override
        public float getFloat(int rowId) {
            final Float value = values.get(rowId);
            if (value == null) {
                throw new IllegalStateException("Float embedding contains null element at index " + rowId);
            }
            return value;
        }
    }

    private static final class PrimitiveFloatArrayColumnVector extends BaseColumnVector {
        private final float[] values;

        private PrimitiveFloatArrayColumnVector(float[] values) {
            super(FloatType.FLOAT, values.length);
            this.values = values;
        }

        @Override
        public boolean isNullAt(int rowId) {
            return false;
        }

        @Override
        public float getFloat(int rowId) {
            return values[rowId];
        }
    }

    private static final class SingletonIterator<T> implements CloseableIterator<T> {
        private T value;
        private boolean consumed;

        private SingletonIterator(T value) {
            this.value = value;
        }

        @Override
        public boolean hasNext() {
            return !consumed;
        }

        @Override
        public T next() {
            if (consumed) {
                throw new java.util.NoSuchElementException();
            }
            consumed = true;
            return value;
        }

        @Override
        public void close() {
            value = null;
        }
    }

    private static Exception cloneFailure(Exception failure) {
        return new Exception(failure.getMessage(), failure);
    }
}
