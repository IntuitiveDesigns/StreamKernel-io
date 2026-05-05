/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.security.SecurityContext;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.Readyable;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import com.intuitivedesigns.streamkernel.spi.SemanticCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import java.util.concurrent.TimeUnit;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Core pipeline runtime: fetches batches from source, transforms, writes to sink.
 *
 * Changes in this revision
 * ────────────────────────
 * 1. emptyBatchTotal counter
 *    Workers now increment emptyBatchTotal every time fetchBatch() returns an empty
 *    result. This counter is exported to Prometheus and visible in Grafana, making
 *    source starvation (the root cause of PROC_EPS=0) immediately diagnosable.
 *
 * 2. fetchLock is now config-driven via streamkernel.source.fetch.lock (default true).
 *    For SyntheticSource benchmarks set this to false — the source is thread-safe and
 *    the lock only causes 7/8 workers to see empty batches on every cycle.
 *
 * 3. Prometheus metrics
 *    All LongAdder counters are published to Prometheus by StreamKernel's metrics pusher
 *    scheduler via MetricsRuntime.counter(name, delta) and gauge(name, value).
 *    No new SPI methods required.
 *
 * Prometheus metric names (all prefixed streamkernel_pipeline_):
 *   processed_total   — records that produced sink output and were written to the primary sink
 *   out_total         — records written to primary sink
 *   in_total          — records admitted from source (pre-transform)
 *   dropped_total     — records discarded (null transform result or exception)
 *   denied_total      — records rejected by security policy
 *   dlq_total         — records routed to DLQ
 *   empty_batch_total — fetchBatch() calls that returned empty (source starvation signal)
 *   source_errors     — source fetch exceptions
 *   auth_errors       — authorization check exceptions
 *   dlq_errors        — DLQ write failures
 */
public final class PipelineOrchestrator<I, O> {

    private static final Logger log = LoggerFactory.getLogger(PipelineOrchestrator.class);

    // -------------------------------------------------------------------------
    // SPI Components
    // -------------------------------------------------------------------------
    private final SourceConnector<I>  source;
    private final OutputSink<O>       primarySink;
    private final OutputSink<I>       dlqSink;
    private final Transformer<I, O>   transformer;
    private final Cache<?, ?>         cache;

    @SuppressWarnings("unchecked")
    private final SemanticCache<I, O> semanticCache;

    @SuppressWarnings("unused")
    private final MetricsRuntime      metrics;

    // -------------------------------------------------------------------------
    // Security
    // -------------------------------------------------------------------------
    private final SecurityProvider securityProvider;
    private final SecurityContext  securityCtx;

    // -------------------------------------------------------------------------
    // Configuration (frozen at construction)
    // -------------------------------------------------------------------------
    private final int     parallelism;
    private final int     batchSize;
    private final boolean authorizePerRecord;
    private final long    authTtlNanos;
    private final long    drainTimeoutNanos;
    private final boolean latencyRecordingEnabled;
    private final int latencySampleMask;
    private final boolean cacheEnabled;
    private final boolean sinkBatchDefensiveCopy;
    private final String executorMode;
    private final boolean sourceFetchLockingEnabled;
    private final int inflightCeiling;
    private final int latencyBufferCapacity;
    private final boolean metricsLatencyEnabled;
    private final long metricsMaxLatencyUs;
    private final long dlqErrorThreshold;

    private final AtomicInteger workerId = new AtomicInteger(0);

    /**
     * fetchLock serialises source.fetchBatch() across all worker threads.
     *
     * Default: TRUE (safe for all source types).
     *
     * Set to FALSE for SyntheticSource benchmarks. SyntheticSource is thread-safe and
     * the lock causes 7/8 workers to get empty batches on every cycle when the source
     * refills slower than workers drain it — this is the root cause of PROC_EPS=0
     * seen in bench runs with parallelism=8 and batchSize=5000.
     *
     * Set via: streamkernel.source.fetch.lock=false in your .properties file,
     * or -Dstreamkernel.source.fetch.lock=false on the JVM command line.
     *
     * NOTE: This flag is read from a JVM system property. Setting it in a PipelineConfig
     * properties file will only work if your launcher bridges config -> System.setProperty.
     *
     */
    private final Object fetchLock = new Object();

    /**
     * Maximum number of records allowed in-flight across all workers simultaneously.
     *
     * When this ceiling is reached, workers park for 1ms before fetching the next batch.
     * This is the backpressure mechanism that prevents memory climbing to 1GB+ when
     * the transformer or downstream sink is slower than the source.
     *
     * Set via: streamkernel.sink.inflight.max in your .properties file.
     * Default: Integer.MAX_VALUE (no ceiling) for backward compatibility.
     * Suggested starting point: parallelism x batchSize x 2
     *   e.g. parallelism=8, batchSize=96 → inflight.max = 1536
     */
    // -------------------------------------------------------------------------
    // Public counters — all published to Prometheus by StreamKernel.startMetricsPusher()
    // -------------------------------------------------------------------------
    private final AtomicBoolean  running          = new AtomicBoolean(false);
    private final AtomicInteger  inFlightBatches  = new AtomicInteger(0);
    private final AtomicInteger  inFlightRecords  = new AtomicInteger(0);

    private final LongAdder inTotal          = new LongAdder();  // admitted from source
    private final LongAdder processedTotal   = new LongAdder();  // acknowledged by primary sink (immediate for sync sinks, deferred on completion for async sinks)
    private final LongAdder outTotal         = new LongAdder();  // written to primary sink
    private final LongAdder droppedTotal     = new LongAdder();  // discarded (null result / exception)
    private final LongAdder deniedCount      = new LongAdder();  // rejected by security
    private final LongAdder dlqCount         = new LongAdder();  // routed to DLQ
    private final LongAdder emptyBatchCount  = new LongAdder();  // fetchBatch() returned empty  ← NEW
    private final LongAdder sourceErrorCount = new LongAdder();  // source fetch exceptions
    private final LongAdder authErrorCount   = new LongAdder();  // auth check exceptions
    private final AtomicLong dlqErrorCount   = new AtomicLong(0L); // DLQ write failures — AtomicLong so incrementAndGet() returns the exact post-increment value for threshold checks

    // -------------------------------------------------------------------------
    // Runtime state
    // -------------------------------------------------------------------------
    private final AtomicLong                           lastDlqCriticalLogAtMs = new AtomicLong(0L);
    private final AtomicReference<CompletableFuture<Boolean>> authRefresh     = new AtomicReference<>();
    private final AtomicBoolean dlqThresholdTripped = new AtomicBoolean(false);

    // -------------------------------------------------------------------------
    // Latency recording (thread-local double-buffered, bench-only)
    // -------------------------------------------------------------------------
    private final ConcurrentLinkedQueue<LatencyBuffer> latencyBuffers;
    private final ThreadLocal<LatencyBuffer> latencyLocal;
    private final ThreadLocal<long[]> sampleCounter = ThreadLocal.withInitial(() -> new long[1]);

    // -------------------------------------------------------------------------
    // Latency recording for Prometheus (interval histogram, non-sampling)
    // -------------------------------------------------------------------------
    // Store in microseconds to keep histogram range small and precision high.
    // Store in microseconds to keep histogram range small and precision high.
    private final Recorder metricsLatencyRecorder;

    // -------------------------------------------------------------------------
    // Output batch (thread-local reuse)
    // -------------------------------------------------------------------------
    private final int outBatchInitialCapacity;
    private final ThreadLocal<ArrayList<PipelinePayload<O>>> outBatchLocal;
    private final ThreadLocal<ArrayList<PipelinePayload<I>>> sinkInputLocal;
    private final ThreadLocal<ArrayList<PipelinePayload<I>>> transformInputLocal;
    private final ThreadLocal<int[]> transformIndexLocal;

    private volatile ExecutorService executor;
    private final AtomicReference<AuthCacheEntry> authCache =
            new AtomicReference<>(AuthCacheEntry.expired());

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------
    /** @deprecated Provide a {@link PipelineConfig} to avoid silent fallback to system properties. */
    @Deprecated
    @SuppressWarnings("unchecked")
    public PipelineOrchestrator(
            SourceConnector<I>  source,
            OutputSink<O>       primarySink,
            OutputSink<I>       dlqSink,
            Transformer<I, O>   transformer,
            Cache<?, ?>         cache,
            MetricsRuntime      metrics,
            int                 parallelism,
            int                 batchSize,
            SecurityProvider    securityProvider,
            SecurityContext     securityCtx,
            boolean             authorizePerRecord,
            long                authTtlNanos,
            long                drainTimeoutNanos) {
        this(source,
                primarySink,
                dlqSink,
                transformer,
                cache,
                metrics,
                parallelism,
                batchSize,
                securityProvider,
                securityCtx,
                authorizePerRecord,
                authTtlNanos,
                drainTimeoutNanos,
                null);
    }

    @SuppressWarnings("unchecked")
    public PipelineOrchestrator(
            SourceConnector<I>  source,
            OutputSink<O>       primarySink,
            OutputSink<I>       dlqSink,
            Transformer<I, O>   transformer,
            Cache<?, ?>         cache,
            MetricsRuntime      metrics,
            int                 parallelism,
            int                 batchSize,
            SecurityProvider    securityProvider,
            SecurityContext     securityCtx,
            boolean             authorizePerRecord,
            long                authTtlNanos,
            long                drainTimeoutNanos,
            PipelineConfig      runtimeConfig) {

        this.source           = Objects.requireNonNull(source,           "source");
        this.primarySink      = Objects.requireNonNull(primarySink,      "primarySink");
        this.dlqSink          = Objects.requireNonNull(dlqSink,          "dlqSink");
        this.transformer      = Objects.requireNonNull(transformer,      "transformer");
        this.cache            = Objects.requireNonNull(cache,            "cache");
        this.metrics          = metrics;
        this.securityProvider = Objects.requireNonNull(securityProvider, "securityProvider");
        this.securityCtx      = Objects.requireNonNull(securityCtx,      "securityCtx");

        this.semanticCache = (cache instanceof SemanticCache<?, ?> sc)
                ? (SemanticCache<I, O>) sc
                : null;

        this.parallelism        = Math.max(1, parallelism);
        this.batchSize          = Math.max(1, batchSize);
        this.authorizePerRecord = authorizePerRecord;
        this.authTtlNanos       = Math.max(0L, authTtlNanos);
        this.drainTimeoutNanos  = (drainTimeoutNanos > 0) ? drainTimeoutNanos : TimeUnit.SECONDS.toNanos(15);

        final PipelineRuntimeSettings settings = (runtimeConfig == null)
                ? PipelineRuntimeSettings.fromSystemProperties(this.batchSize)
                : PipelineRuntimeSettings.fromConfig(runtimeConfig, this.batchSize);

        this.latencyRecordingEnabled = settings.latencyRecordingEnabled();
        this.latencySampleMask = settings.latencySampleMask();
        this.cacheEnabled = settings.cacheEnabled();
        this.sinkBatchDefensiveCopy = settings.sinkBatchDefensiveCopy();
        this.executorMode = settings.executorMode();
        this.sourceFetchLockingEnabled = settings.sourceFetchLockingEnabled();
        this.inflightCeiling = settings.inflightCeiling();
        this.latencyBufferCapacity = settings.latencyBufferCapacity();
        this.metricsLatencyEnabled = settings.metricsLatencyEnabled();
        this.metricsMaxLatencyUs = settings.metricsMaxLatencyUs();
        this.dlqErrorThreshold = settings.dlqErrorThreshold();
        this.latencyBuffers = new ConcurrentLinkedQueue<>();
        this.latencyLocal = ThreadLocal.withInitial(() -> {
            final LatencyBuffer b = new LatencyBuffer(this.latencyBufferCapacity);
            latencyBuffers.add(b);
            return b;
        });
        this.metricsLatencyRecorder = new Recorder(this.metricsMaxLatencyUs, 3);

        this.outBatchInitialCapacity = settings.outBatchInitialCapacity();
        this.outBatchLocal = ThreadLocal.withInitial(() -> new ArrayList<>(outBatchInitialCapacity));
        this.sinkInputLocal = ThreadLocal.withInitial(() -> new ArrayList<>(outBatchInitialCapacity));
        this.transformInputLocal = ThreadLocal.withInitial(() -> new ArrayList<>(outBatchInitialCapacity));
        this.transformIndexLocal = ThreadLocal.withInitial(
                () -> new int[Math.max(this.batchSize, outBatchInitialCapacity)]);
    }

    // -------------------------------------------------------------------------
    // Public counter accessors
    // Used by StreamKernel's metrics pusher (Prometheus) and bench reporter (log).
    // -------------------------------------------------------------------------
    public long inTotal()          { return inTotal.sum(); }
    public long processedTotal()   { return processedTotal.sum(); }
    public long outTotal()         { return outTotal.sum(); }
    public long droppedTotal()     { return droppedTotal.sum(); }
    public long deniedTotal()      { return deniedCount.sum(); }
    public long dlqTotal()         { return dlqCount.sum(); }
    public long emptyBatchTotal()  { return emptyBatchCount.sum(); }   // NEW
    public long sourceErrorTotal() { return sourceErrorCount.sum(); }
    public long authErrorTotal()   { return authErrorCount.sum(); }
    public long dlqErrorTotal()    { return dlqErrorCount.get(); }
    public int  inFlightBatches()  { return inFlightBatches.get(); }
    public int  inFlightRecords()  { return inFlightRecords.get(); }

    public int loadPercent() {
        final int inflight = Math.max(0, inFlightBatches.get());
        // In VIRTUAL mode this remains an approximation against configured submission parallelism,
        // not a carrier-thread saturation metric.
        return Math.max(0, Math.min(100, (int) Math.round((inflight * 100.0) / parallelism)));
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        try {
            log.info("Starting pipeline components (Phase 1: Serial Init)...");
            safeInit(cache, "cache");
            safeInit(securityProvider, "securityProvider");
            safeInitTransformer(transformer);
            verifyReadyState();
            safeInit(primarySink, "primarySink");
            safeInit(dlqSink, "dlqSink");

            log.info("Starting pipeline components (Phase 2: Allocation)...");
            this.executor = createExecutor();

            log.info("Starting pipeline components (Phase 3: Workers & Source)...");
            source.connect();
            for (int i = 0; i < parallelism; i++) {
                executor.submit(this::runWorkerLoop);
            }

            log.info(
                    "Pipeline Started Successfully: parallelism={} batchSize={} execMode={} "
                            + "latencyEnabled={} sampleMask={} cacheEnabled={} fetchLock={} "
                            + "sinkBatchCopy={} outBatchCap={} dlqErrorThreshold={}",
                    parallelism, batchSize, executorMode,
                    latencyRecordingEnabled, latencySampleMask,
                    cacheEnabled, sourceFetchLockingEnabled,
                    sinkBatchDefensiveCopy, outBatchInitialCapacity,
                    dlqErrorThreshold > 0 ? dlqErrorThreshold : "disabled");

        } catch (Throwable t) {
            log.error("CRITICAL: Pipeline failed to start. Fail-fast triggered.", t);
            running.set(false);
            safeStopAfterFailedStart();
            throw (t instanceof RuntimeException re) ? re : new IllegalStateException("Pipeline failed to start", t);
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        log.info("Stop requested. Draining batches...");

        final long deadline = System.nanoTime() + drainTimeoutNanos;
        final ExecutorService ex = executor;
        if (ex != null) {
            ex.shutdown();
        }

        while ((inFlightBatches.get() > 0 || inFlightRecords.get() > 0) && System.nanoTime() < deadline) {
            drainDeferredPrimarySinkCompletions();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
        }

        if (ex != null) {
            try {
                final long remainingNanos = Math.max(0L, deadline - System.nanoTime());
                if (remainingNanos > 0L) {
                    ex.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception ignored) {
            }

            if (!ex.isTerminated()) {
                log.warn("Pipeline drain timed out. forcing shutdown with inFlightBatches={} inFlightRecords={}",
                        inFlightBatches.get(),
                        inFlightRecords.get());
                ex.shutdownNow();
                try {
                    ex.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Exception ignored) {
                }
            }
        }

        // Fresh deadline so executor awaitTermination cannot consume the entire sink-drain budget.
        final long sinkDrainDeadline = System.nanoTime() + drainTimeoutNanos;
        drainDeferredPrimarySinkCompletionsUntil(sinkDrainDeadline);
        drainDeferredPrimarySinkCompletions();
        safeFlush(primarySink, "primarySink");
        safeClose(primarySink, "primarySink");
        drainDeferredPrimarySinkCompletionsUntil(sinkDrainDeadline);
        drainDeferredPrimarySinkCompletions();
        safeClose(dlqSink,     "dlqSink");
        safeClose(transformer, "transformer");
        safeClose(cache,       "cache");
        safeClose(securityProvider, "securityProvider");
        safeDisconnectSource();
        log.info("Pipeline Stopped.");
    }

    public Transformer<I, O> transformer() {
        return transformer;
    }

    // -------------------------------------------------------------------------
    // Worker loop
    // -------------------------------------------------------------------------
    private void runWorkerLoop() {
        long backoffMs = Math.max(0L, securityCtx.sourceErrorBackoffInitialMs());

        while (running.get() && !Thread.currentThread().isInterrupted()) {

            // Backpressure: if in-flight records already exceed the ceiling, park this
            // worker rather than fetching more. Prevents unbounded memory growth when
            // the transformer or downstream sink is the bottleneck.
            if (inFlightRecords.get() >= inflightCeiling) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                continue;
            }

            final List<PipelinePayload<I>> batch;
            try {
                if (sourceFetchLockingEnabled) {
                    synchronized (fetchLock) {
                        batch = source.fetchBatch(batchSize);
                    }
                } else {
                    batch = source.fetchBatch(batchSize);
                }
            } catch (Exception e) {
                sourceErrorCount.increment();
                if (securityCtx.failFastOnSourceError()) {
                    running.set(false);
                    break;
                }
                if (backoffMs > 0) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(backoffMs));
                backoffMs = Math.min(Math.max(1, backoffMs) * 2, securityCtx.sourceErrorBackoffMaxMs());
                continue;
            }

            backoffMs = Math.max(0L, securityCtx.sourceErrorBackoffInitialMs());

            if (batch == null || batch.isEmpty()) {
                // Increment the starvation counter. Visible in Grafana as
                // streamkernel_pipeline_empty_batch_total.
                // If this climbs steadily: set streamkernel.source.fetch.lock=false
                // or increase source.synthetic.buffer.size.
                emptyBatchCount.increment();                               // ← NEW
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                continue;
            }

            final int admitted = batch.size();
            inTotal.add(admitted);
            inFlightRecords.addAndGet(admitted);
            inFlightBatches.incrementAndGet();

            try {
                processBatch(batch);
            } finally {
                inFlightBatches.decrementAndGet();
                final int remaining = inFlightRecords.addAndGet(-admitted);
                if (remaining < 0) {
                    log.warn("inFlightRecords went negative after batch completion; correcting counter. remaining={} admitted={}",
                            remaining,
                            admitted);
                    inFlightRecords.updateAndGet(v -> Math.max(0, v));
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Batch processing
    // -------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private void processBatch(List<PipelinePayload<I>> batch) {
        drainDeferredPrimarySinkCompletions();

        final BatchTransformer<I, O> batchTransformer =
                (transformer instanceof BatchTransformer<?, ?> bt)
                        ? (BatchTransformer<I, O>) bt
                        : null;
        if (batchTransformer != null && batchTransformer.isBatchTransformPreferred()) {
            processBatchWithBatchTransformer(batch, batchTransformer);
            return;
        }

        if (applyBatchDenialIfNeeded(batch)) return;

        final int n = batch.size();
        long localDenied  = 0L;
        long localDropped = 0L;
        long localDlq     = 0L;

        final ArrayList<PipelinePayload<O>> outBatch = outBatchLocal.get();
        outBatch.clear();
        final ArrayList<PipelinePayload<I>> sinkInputs = sinkInputLocal.get();
        sinkInputs.clear();

        final long[] ctrHolder = sampleCounter.get();
        long ctr = ctrHolder[0];

        final boolean doBenchLatency = latencyRecordingEnabled;
        final boolean doMetricsLatency = metricsLatencyEnabled;

        final boolean doCache = cacheEnabled && semanticCache != null;
        final SemanticCache<I, O> sc = doCache ? semanticCache : null;

        for (int i = 0; i < n; i++) {
            final PipelinePayload<I> payload = batch.get(i);

            // Latency sampling (bench-only). Metrics latency is not sampled.
            final boolean sampleLatency;
            if (!doBenchLatency) {
                sampleLatency = false;
            } else if (latencySampleMask == 0) {
                sampleLatency = true;
            } else {
                ctr++;
                sampleLatency = ((ctr & latencySampleMask) == 0);
            }

            final boolean timeThisRecord = sampleLatency || doMetricsLatency;
            final long startNs = timeThisRecord ? System.nanoTime() : 0L;

            try {
                safeCachePut(payload);

                // Per-record security.
                if (authorizePerRecord && !isAllowedFailClosed()) {
                    localDenied++;
                    if (safeDlqWriteCritical(payload, "SECURITY_DENIAL_RECORD")) localDlq++;
                    else localDropped++;
                    continue;
                }

                PipelinePayload<O> out = null;

                // Semantic cache fast-path.
                if (doCache) {
                    try {
                        out = sc.lookup(payload);
                    } catch (Exception ignored) {
                        // Treat lookup failure as cache miss.
                    }
                }

                if (out == null) {
                    // Transform path.
                    out = transformer.transform(payload);

                    if (out != null && doCache) {
                        try {
                            sc.store(payload, out);
                        } catch (Exception ignored) {
                            // Store failure should not fail the pipeline.
                        }
                    }
                }

                if (out != null) {
                    outBatch.add(out);
                    sinkInputs.add(payload);
                } else {
                    localDropped++;
                }

            } catch (Exception e) {
                log.warn("Record failed id={} msg={}", safeId(payload), e.getMessage(), e);
                if (safeDlqWriteCritical(payload, "RECORD_FAILED")) localDlq++;
                else localDropped++;
            } finally {
                if (startNs != 0L) {
                    final long durNs = System.nanoTime() - startNs;

                    // Bench path (sampled, buffers)
                    if (sampleLatency) {
                        latencyLocal.get().recordNanos(durNs);
                    }

                    // Metrics path (not sampled, histogram)
                    if (doMetricsLatency) {
                        long us = TimeUnit.NANOSECONDS.toMicros(durNs);
                        if (us <= 0) us = 1;
                        if (us > metricsMaxLatencyUs) us = metricsMaxLatencyUs;
                        metricsLatencyRecorder.recordValue(us);
                    }
                }
            }
        }

        ctrHolder[0] = ctr;
        finalizeBatch(outBatch, sinkInputs, localDenied, localDropped, localDlq);
    }

    @SuppressWarnings("unchecked")
    private void processBatchWithBatchTransformer(
            List<PipelinePayload<I>> batch,
            BatchTransformer<I, O> batchTransformer) {
        drainDeferredPrimarySinkCompletions();

        if (applyBatchDenialIfNeeded(batch)) return;

        final int n = batch.size();
        long localDenied  = 0L;
        long localDropped = 0L;
        long localDlq     = 0L;

        final ArrayList<PipelinePayload<O>> outBatch = outBatchLocal.get();
        outBatch.clear();
        final ArrayList<PipelinePayload<I>> sinkInputs = sinkInputLocal.get();
        sinkInputs.clear();

        final long[] ctrHolder = sampleCounter.get();
        long ctr = ctrHolder[0];

        final boolean doBenchLatency = latencyRecordingEnabled;
        final boolean doMetricsLatency = metricsLatencyEnabled;

        final boolean doCache = cacheEnabled && semanticCache != null;
        final SemanticCache<I, O> sc = doCache ? semanticCache : null;

        final long[] startNsByIndex = (doBenchLatency || doMetricsLatency) ? new long[n] : null;
        final boolean[] sampleLatencyByIndex = doBenchLatency ? new boolean[n] : null;

        final ArrayList<PipelinePayload<I>> transformInputs = transformInputLocal.get();
        transformInputs.clear();
        int[] transformIndexes = transformIndexLocal.get();
        if (transformIndexes.length < n) {
            transformIndexes = new int[n];
            transformIndexLocal.set(transformIndexes);
        } else if (transformIndexes.length > Math.max(n, batchSize) * 4) {
            // Shrink after an anomalous large batch to prevent permanent per-thread inflation.
            transformIndexes = new int[Math.max(n, batchSize)];
            transformIndexLocal.set(transformIndexes);
        }

        for (int i = 0; i < n; i++) {
            final PipelinePayload<I> payload = batch.get(i);

            final boolean sampleLatency;
            if (!doBenchLatency) {
                sampleLatency = false;
            } else if (latencySampleMask == 0) {
                sampleLatency = true;
            } else {
                ctr++;
                sampleLatency = ((ctr & latencySampleMask) == 0);
            }

            final boolean timeThisRecord = sampleLatency || doMetricsLatency;
            if (startNsByIndex != null && timeThisRecord) {
                startNsByIndex[i] = System.nanoTime();
            }
            if (sampleLatencyByIndex != null) {
                sampleLatencyByIndex[i] = sampleLatency;
            }

            try {
                safeCachePut(payload);

                if (authorizePerRecord && !isAllowedFailClosed()) {
                    localDenied++;
                    if (safeDlqWriteCritical(payload, "SECURITY_DENIAL_RECORD")) localDlq++;
                    else localDropped++;
                    recordRecordLatency(startNsByIndex, sampleLatencyByIndex, i, doMetricsLatency);
                    continue;
                }

                PipelinePayload<O> out = null;
                if (doCache) {
                    try {
                        out = sc.lookup(payload);
                    } catch (Exception ignored) {
                        // Treat lookup failure as cache miss.
                    }
                }

                if (out != null) {
                    outBatch.add(out);
                    sinkInputs.add(payload);
                    recordRecordLatency(startNsByIndex, sampleLatencyByIndex, i, doMetricsLatency);
                    continue;
                }

                transformInputs.add(payload);
                transformIndexes[transformInputs.size() - 1] = i;
            } catch (Exception e) {
                log.warn("Record failed id={} msg={}", safeId(payload), e.getMessage(), e);
                if (safeDlqWriteCritical(payload, "RECORD_FAILED")) localDlq++;
                else localDropped++;
                recordRecordLatency(startNsByIndex, sampleLatencyByIndex, i, doMetricsLatency);
            }
        }

        if (!transformInputs.isEmpty()) {
            final List<BatchTransformer.Result<O>> transformResults;
            try {
                transformResults = batchTransformer.transformBatch(transformInputs);
                if (transformResults == null || transformResults.size() != transformInputs.size()) {
                    throw new IllegalStateException("Batch transformer returned "
                            + ((transformResults == null) ? "null" : transformResults.size())
                            + " results for " + transformInputs.size() + " inputs");
                }
            } catch (Exception e) {
                log.warn("Batch transform failed size={} msg={}",
                        transformInputs.size(), e.getMessage(), e);
                for (int i = 0; i < transformInputs.size(); i++) {
                    final PipelinePayload<I> payload = transformInputs.get(i);
                    if (safeDlqWriteCritical(payload, "RECORD_FAILED")) localDlq++;
                    else localDropped++;
                    recordRecordLatency(startNsByIndex, sampleLatencyByIndex,
                            transformIndexes[i], doMetricsLatency);
                }
                ctrHolder[0] = ctr;
                finalizeBatch(outBatch, sinkInputs, localDenied, localDropped, localDlq);
                return;
            }

            for (int i = 0; i < transformResults.size(); i++) {
                final PipelinePayload<I> payload = transformInputs.get(i);
                final int slot = transformIndexes[i];
                final BatchTransformer.Result<O> result = transformResults.get(i);

                try {
                    if (result == null) {
                        throw new IllegalStateException("Batch transformer returned null result");
                    }

                    if (result.hasError()) {
                        final Exception error = result.error();
                        log.warn("Record failed id={} msg={}", safeId(payload), error.getMessage(), error);
                        if (safeDlqWriteCritical(payload, "RECORD_FAILED")) localDlq++;
                        else localDropped++;
                        continue;
                    }

                    final PipelinePayload<O> out = result.output();
                    if (out != null && doCache) {
                        try {
                            sc.store(payload, out);
                        } catch (Exception ignored) {
                            // Store failure should not fail the pipeline.
                        }
                    }

                    if (out != null) {
                        outBatch.add(out);
                        sinkInputs.add(payload);
                    } else {
                        localDropped++;
                    }
                } catch (Exception e) {
                    log.warn("Record failed id={} msg={}", safeId(payload), e.getMessage(), e);
                    if (safeDlqWriteCritical(payload, "RECORD_FAILED")) localDlq++;
                    else localDropped++;
                } finally {
                    recordRecordLatency(startNsByIndex, sampleLatencyByIndex, slot, doMetricsLatency);
                }
            }
        }

        ctrHolder[0] = ctr;
        finalizeBatch(outBatch, sinkInputs, localDenied, localDropped, localDlq);
    }

    private void recordRecordLatency(
            long[] startNsByIndex,
            boolean[] sampleLatencyByIndex,
            int index,
            boolean doMetricsLatency) {
        if (startNsByIndex == null || index < 0 || index >= startNsByIndex.length) {
            return;
        }

        final long startNs = startNsByIndex[index];
        if (startNs == 0L) {
            return;
        }

        final long durNs = System.nanoTime() - startNs;

        if (sampleLatencyByIndex != null && sampleLatencyByIndex[index]) {
            latencyLocal.get().recordNanos(durNs);
        }

        if (doMetricsLatency) {
            long us = TimeUnit.NANOSECONDS.toMicros(durNs);
            if (us <= 0) us = 1;
            if (us > metricsMaxLatencyUs) us = metricsMaxLatencyUs;
            // Recorder is thread-safe and intentionally interval-based; a snapshot may miss
            // in-flight writes from the current epoch, which is acceptable for operational metrics.
            metricsLatencyRecorder.recordValue(us);
        }
    }

    /** Returns true if the batch was denied and fully handled; caller should return immediately. */
    private boolean applyBatchDenialIfNeeded(List<PipelinePayload<I>> batch) {
        if (authorizePerRecord || isAllowedCachedFailClosed()) return false;
        final int n = batch.size();
        long localDlq = 0L;
        long localDropped = 0L;
        for (int i = 0; i < n; i++) {
            final PipelinePayload<I> p = batch.get(i);
            safeCachePut(p);
            if (safeDlqWriteCritical(p, "SECURITY_DENIAL_BATCH")) localDlq++;
            else localDropped++;
        }
        deniedCount.add(n);
        if (localDlq     > 0) dlqCount.add(localDlq);
        if (localDropped > 0) droppedTotal.add(localDropped);
        return true;
    }

    private void finalizeBatch(
            ArrayList<PipelinePayload<O>> outBatch,
            ArrayList<PipelinePayload<I>> sinkInputs,
            long localDenied,
            long localDropped,
            long localDlq) {

        long totalOut = 0L;
        long totalDlq = localDlq;
        long totalDropped = localDropped;

        if (!outBatch.isEmpty()) {
            final SinkDispatchResult sinkResult = dispatchToPrimarySink(outBatch, sinkInputs);
            totalOut += sinkResult.acknowledgedCount();
            totalDlq += sinkResult.dlqCount();
            totalDropped += sinkResult.droppedCount();
        }

        drainDeferredPrimarySinkCompletions();

        if (totalOut     > 0) outTotal.add(totalOut);
        if (localDenied  > 0) deniedCount.add(localDenied);
        if (totalDlq     > 0) dlqCount.add(totalDlq);
        if (totalDropped > 0) droppedTotal.add(totalDropped);
    }

    @SuppressWarnings("unchecked")
    private SinkDispatchResult dispatchToPrimarySink(
            List<PipelinePayload<O>> outBatch,
            List<PipelinePayload<I>> sinkInputs) {
        try {
            if (primarySink instanceof DeferredBatchOutputSink<?, ?> deferred) {
                // Deferred sinks only acknowledge writes when completions are drained.
                // Returning NONE here avoids double-counting out/processed totals.
                ((DeferredBatchOutputSink<I, O>) deferred).writeBatchDeferred(
                        new ArrayList<>(outBatch),
                        new ArrayList<>(sinkInputs)
                );
                return SinkDispatchResult.NONE;
            }

            if (primarySink instanceof BatchOutputSink<?> bs) {
                final List<PipelinePayload<O>> toWrite = sinkBatchDefensiveCopy
                        ? new ArrayList<>(outBatch)
                        : outBatch;
                ((BatchOutputSink<O>) bs).writeBatch(toWrite);
            } else {
                for (int i = 0, m = outBatch.size(); i < m; i++) {
                    primarySink.write(outBatch.get(i));
                }
            }

            final long acknowledged = outBatch.size();
            if (acknowledged > 0) {
                processedTotal.add(acknowledged);
            }
            return new SinkDispatchResult(acknowledged, 0L, 0L);
        } catch (Exception e) {
            log.warn("Primary sink write failed; routing inputs to DLQ. msg={}", e.getMessage(), e);
            long localDlq = 0L;
            long localDropped = 0L;
            for (int i = 0, m = sinkInputs.size(); i < m; i++) {
                if (safeDlqWriteCritical(sinkInputs.get(i), "PRIMARY_SINK_FAILED")) localDlq++;
                else localDropped++;
            }
            return new SinkDispatchResult(0L, localDlq, localDropped);
        } finally {
            outBatch.clear();
            sinkInputs.clear();
        }
    }

    @SuppressWarnings("unchecked")
    private long drainDeferredPrimarySinkCompletions() {
        if (!(primarySink instanceof DeferredBatchOutputSink<?, ?> deferred)) {
            return 0L;
        }

        final List<DeferredWriteResult<I>> completions =
                ((DeferredBatchOutputSink<I, O>) deferred).drainCompletedWrites();
        if (completions == null || completions.isEmpty()) {
            return 0L;
        }

        long acknowledged = 0L;
        long localDlq = 0L;
        long localDropped = 0L;
        long drained = 0L;

        for (DeferredWriteResult<I> completion : completions) {
            if (completion == null) {
                continue;
            }

            final List<PipelinePayload<I>> sourceInputs = completion.sourceInputs();
            if (sourceInputs == null || sourceInputs.isEmpty()) {
                continue;
            }
            drained += sourceInputs.size();

            if (completion.succeeded()) {
                acknowledged += sourceInputs.size();
                continue;
            }

            final Exception failure = completion.failure();
            log.warn("Primary sink async write failed; routing inputs to DLQ. msg={}",
                    failure == null ? "unknown" : failure.getMessage(),
                    failure);
            for (int i = 0, m = sourceInputs.size(); i < m; i++) {
                if (safeDlqWriteCritical(sourceInputs.get(i), "PRIMARY_SINK_FAILED")) localDlq++;
                else localDropped++;
            }
        }

        if (acknowledged > 0) {
            processedTotal.add(acknowledged);
            outTotal.add(acknowledged);
        }
        if (localDlq > 0) {
            dlqCount.add(localDlq);
        }
        if (localDropped > 0) {
            droppedTotal.add(localDropped);
        }
        return drained;
    }

    private record SinkDispatchResult(long acknowledgedCount, long dlqCount, long droppedCount) {
        private static final SinkDispatchResult NONE = new SinkDispatchResult(0L, 0L, 0L);
    }

    private void drainDeferredPrimarySinkCompletionsUntil(long deadlineNanos) {
        if (!(primarySink instanceof DeferredBatchOutputSink<?, ?>)) {
            return;
        }
        // Park 2ms when idle rather than busy-spinning; do NOT exit early on consecutive idle
        // cycles — async sinks with multi-ms linger would otherwise have completions dropped.
        do {
            final long drained = drainDeferredPrimarySinkCompletions();
            if (drained == 0L) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            }
        } while (System.nanoTime() < deadlineNanos);
    }

    // -------------------------------------------------------------------------
    // Bench-only latency snapshot (resets buffers — do NOT use for Prometheus)
    // -------------------------------------------------------------------------

    /**
     * Returns a one-shot latency snapshot by draining all thread-local buffers.
     *
     * IMPORTANT: This resets the internal buffers. Use only for the BENCH log line.
     * Prometheus-exported latency must be tracked separately (non-resetting histogram)
     * in your MetricsRuntime implementation.
     */
    public LatencySnapshot benchLatencySnapshotAndReset() {
        if (!latencyRecordingEnabled) {
            return new LatencySnapshot(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0);
        }

        final ArrayList<long[]> chunks = new ArrayList<>();
        long total = 0;

        for (final LatencyBuffer b : latencyBuffers) {
            final long[] snap = b.snapshotAndReset();
            if (snap.length > 0) {
                chunks.add(snap);
                total += snap.length;
            }
        }

        if (total <= 0) return new LatencySnapshot(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0);

        final int    count  = (int) Math.min(Integer.MAX_VALUE, total);
        final long[] merged = new long[count];
        int pos = 0;
        for (final long[] c : chunks) {
            final int len = Math.min(c.length, merged.length - pos);
            if (len <= 0) break;
            System.arraycopy(c, 0, merged, pos, len);
            pos += len;
        }

        if (pos <= 0) return new LatencySnapshot(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0);

        Arrays.sort(merged, 0, pos);
        final int p50idx = (int) (0.50 * (pos - 1));
        final int p95idx = (int) (0.95 * (pos - 1));
        final int p99idx = (int) (0.99 * (pos - 1));
        final int p999idx = (int) (0.999 * (pos - 1));

        return new LatencySnapshot(
                merged[p50idx]   / 1_000_000.0,
                merged[p95idx]   / 1_000_000.0,
                merged[p99idx]   / 1_000_000.0,
                merged[p999idx]   / 1_000_000.0,
                merged[pos - 1]  / 1_000_000.0,
                pos);
    }

    public record LatencySnapshot(double p50Millis, double p95Millis, double p99Millis, double p999Millis, double maxMillis, long samples) {}

    // -------------------------------------------------------------------------
    // Authorization
    // -------------------------------------------------------------------------
    private boolean isAllowedCachedFailClosed() {
        if (authTtlNanos <= 0) return isAllowedFailClosed();

        final long now = System.nanoTime();
        final AuthCacheEntry snapshot = authCache.get();
        if (snapshot.isUsable(now, authTtlNanos)) return snapshot.allowed();

        final CompletableFuture<Boolean> mine     = new CompletableFuture<>();
        final CompletableFuture<Boolean> existing = authRefresh.get();

        if (existing == null && authRefresh.compareAndSet(null, mine)) {
            try {
                final AuthCacheEntry current = authCache.get();
                if (current.isUsable(System.nanoTime(), authTtlNanos)) {
                    mine.complete(current.allowed());
                    return current.allowed();
                }
                final boolean allowed = isAllowedFailClosed();
                authCache.set(new AuthCacheEntry(allowed, System.nanoTime()));
                mine.complete(allowed);
                return allowed;
            } catch (Throwable t) {
                mine.complete(false);
                return false;
            } finally {
                authRefresh.compareAndSet(mine, null);
            }
        }

        try {
            final CompletableFuture<Boolean> f = (existing != null) ? existing : authRefresh.get();
            if (f == null) {
                final AuthCacheEntry fallback = authCache.get();
                return fallback.isUsable(System.nanoTime(), authTtlNanos)
                        ? fallback.allowed()
                        : isAllowedFailClosed();
            }
            return f.join();
        } catch (Throwable t) {
            return false;
        }
    }

    private boolean isAllowedFailClosed() {
        try {
            return securityProvider.isAllowed(
                    securityCtx.principal(), securityCtx.action(), securityCtx.resource());
        } catch (Exception e) {
            authErrorCount.increment();
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------
    private ExecutorService createExecutor() {
        if ("VIRTUAL".equals(executorMode)) {
            // Use named virtual threads for easier production debugging/profiling.
            final ThreadFactory tf = Thread.ofVirtual()
                    .name("pipeline-vt-", 0)
                    .uncaughtExceptionHandler((th, ex) ->
                            log.error("Uncaught exception in {}", th.getName(), ex))
                    .factory();
            return Executors.newThreadPerTaskExecutor(tf);
        }

        return Executors.newFixedThreadPool(parallelism, r -> {
            final Thread t = new Thread(r, "pipeline-worker-" + workerId.incrementAndGet());
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((th, ex) ->
                    log.error("Uncaught exception in {}", th.getName(), ex));
            return t;
        });
    }

    private void safeStopAfterFailedStart() {
        try {
            final ExecutorService ex = executor;
            if (ex != null) ex.shutdownNow();
            drainDeferredPrimarySinkCompletions();
            safeClose(primarySink,      "primarySink");
            drainDeferredPrimarySinkCompletions();
            safeClose(dlqSink,          "dlqSink");
            safeClose(transformer,      "transformer");
            safeClose(cache,            "cache");
            safeClose(securityProvider, "securityProvider");
            safeDisconnectSource();
        } catch (Exception ignored) {}
    }

    private void safeDisconnectSource() {
        try { source.disconnect(); } catch (Exception ignored) {}
    }

    private static void safeFlush(Object o, String name) {
        try {
            if (o instanceof java.io.Flushable f) f.flush();
        } catch (Exception e) {
            log.warn("Error flushing {}", name, e);
        }
    }

    private static void safeClose(Object o, String name) {
        try {
            if (o instanceof AutoCloseable c) c.close();
        } catch (Exception e) {
            log.warn("Error closing {}", name, e);
        }
    }

    // Reflection-based until an Initializable SPI is introduced — tracked as future cleanup.
    private static void safeInit(Object target, String name) {
        if (target == null) return;
        try {
            final Method m = findLifecycleMethod(target.getClass(), "init");
            if (!m.canAccess(target)) m.setAccessible(true);
            m.invoke(target);
        } catch (NoSuchMethodException ignored) {
        } catch (java.lang.reflect.InvocationTargetException ite) {
            final Throwable cause = (ite.getCause() != null) ? ite.getCause() : ite;
            throw new IllegalStateException("Failed to init " + name + ": " + cause, cause);
        } catch (Throwable t) {
            throw new IllegalStateException("Failed to init " + name, t);
        }
    }

    private void safeInitTransformer(Transformer<?, ?> t) {
        if (t == null) return;
        if (t instanceof ChainedTransformer chained) {
            int i = 0;
            for (final Transformer<?, ?> step : chained.children()) {
                safeInit(step, "transformer[" + (i++) + "]:" + step.getClass().getSimpleName());
            }
            safeInit(chained, "transformer:ChainedTransformer");
        } else {
            safeInit(t, "transformer:" + t.getClass().getSimpleName());
        }
    }

    private void verifyReadyState() {
        try {
            if (transformer instanceof ChainedTransformer chained) {
                for (final Transformer<?, ?> step : chained.children()) {
                    verifyReadyStateInternal(step);
                }
            } else {
                verifyReadyStateInternal(transformer);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Pipeline readiness check failed", e);
        }
    }

    private void verifyReadyStateInternal(Object step) throws Exception {
        if (step == null) return;
        if (step instanceof Readyable r) {
            r.verifyReady();
            log.info("Semantic readiness check PASSED for {} (Readyable)", step.getClass().getSimpleName());
            return;
        }
        log.debug("Semantic readiness check SKIPPED for {} (not Readyable)",
                step.getClass().getSimpleName());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void safeCachePut(PipelinePayload<I> p) {
        if (!cacheEnabled) return;
        try { ((Cache) cache).put(p.id(), p); } catch (Exception ignored) {}
    }

    private boolean safeDlqWriteCritical(PipelinePayload<I> p, String reason) {
        try {
            dlqSink.write(p);
            return true;
        } catch (Exception e) {
            final long errors = dlqErrorCount.incrementAndGet();
            final long now  = System.currentTimeMillis();
            final long last = lastDlqCriticalLogAtMs.get();
            if (now - last >= 2_000L && lastDlqCriticalLogAtMs.compareAndSet(last, now)) {
                log.error("CRITICAL: DLQ WRITE FAILED. reason={} id={} dlqErrors={} msg={}",
                        reason, safeId(p), errors, e.getMessage(), e);
            }
            if (dlqErrorThreshold > 0L
                    && errors >= dlqErrorThreshold
                    && dlqThresholdTripped.compareAndSet(false, true)) {
                log.error("CRITICAL: DLQ error threshold reached. threshold={} currentErrors={}. Stopping pipeline.",
                        dlqErrorThreshold,
                        errors);
                running.set(false);
            }
            return false;
        }
    }

    private static Method findLifecycleMethod(Class<?> type, String methodName) throws NoSuchMethodException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredMethod(methodName);
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchMethodException(methodName);
    }

    private static String safeId(PipelinePayload<?> p) {
        try { return (p == null) ? "null" : String.valueOf(p.id()); }
        catch (Exception ignored) { return "unknown"; }
    }

    private static int parseIntOr(String s, int fallback) {
        if (s == null) return fallback;
        try { return Integer.parseInt(s.trim()); }
        catch (Exception e) { return fallback; }
    }

    private static long parseLongOr(String s, long fallback) {
        if (s == null) return fallback;
        try { return Long.parseLong(s.trim()); }
        catch (Exception e) { return fallback; }
    }

    private record AuthCacheEntry(boolean allowed, long refreshedAtNanos) {
        private static AuthCacheEntry expired() {
            return new AuthCacheEntry(false, 0L);
        }

        private boolean isUsable(long nowNanos, long ttlNanos) {
            return refreshedAtNanos > 0L && Math.max(0L, nowNanos - refreshedAtNanos) < ttlNanos;
        }
    }

    private record PipelineRuntimeSettings(
            boolean latencyRecordingEnabled,
            int latencySampleMask,
            boolean cacheEnabled,
            boolean sinkBatchDefensiveCopy,
            String executorMode,
            boolean sourceFetchLockingEnabled,
            int inflightCeiling,
            int latencyBufferCapacity,
            boolean metricsLatencyEnabled,
            long metricsMaxLatencyUs,
            int outBatchInitialCapacity,
            long dlqErrorThreshold
    ) {
        // Config key constants shared by fromSystemProperties and fromConfig to prevent silent divergence.
        // JVM -D values intentionally win over profile values because the benchmark runner injects
        // per-run tuning knobs this way without rewriting pipeline property files.
        private static final String K_LATENCY_ENABLED            = "streamkernel.latency.enabled";
        private static final String K_LATENCY_SAMPLE_MASK        = "streamkernel.latency.sample.mask";
        private static final String K_CACHE_FORCE_DISABLED       = "streamkernel.cache.force.disabled";
        private static final String K_SINK_BATCH_COPY            = "streamkernel.sink.batch.copy";
        private static final String K_EXECUTOR_MODE              = "streamkernel.executor.mode";
        private static final String K_SOURCE_FETCH_LOCK          = "streamkernel.source.fetch.lock";
        private static final String K_SINK_INFLIGHT_MAX          = "streamkernel.sink.inflight.max";
        private static final String K_LATENCY_BUFFER_SIZE        = "streamkernel.latency.buffer.size";
        private static final String K_METRICS_LATENCY_ENABLED    = "streamkernel.metrics.latency.enabled";
        private static final String K_METRICS_LATENCY_MAX_SECS   = "streamkernel.metrics.latency.max.seconds";
        private static final String K_OUTBATCH_CAPACITY          = "streamkernel.outbatch.capacity";
        private static final String K_DLQ_ERROR_THRESHOLD        = "streamkernel.dlq.error.threshold";

        private static PipelineRuntimeSettings fromSystemProperties(int batchSize) {
            return new PipelineRuntimeSettings(
                    Boolean.parseBoolean(System.getProperty(K_LATENCY_ENABLED, "true")),
                    parseIntOr(System.getProperty(K_LATENCY_SAMPLE_MASK, "1023"), 1023),
                    !Boolean.parseBoolean(System.getProperty(K_CACHE_FORCE_DISABLED, "false")),
                    Boolean.parseBoolean(System.getProperty(K_SINK_BATCH_COPY, "true")),
                    System.getProperty(K_EXECUTOR_MODE, "FIXED").trim().toUpperCase(),
                    Boolean.parseBoolean(System.getProperty(K_SOURCE_FETCH_LOCK, "true")),
                    parseIntOr(System.getProperty(K_SINK_INFLIGHT_MAX,
                            String.valueOf(Integer.MAX_VALUE)), Integer.MAX_VALUE),
                    Math.max(1024, parseIntOr(System.getProperty(K_LATENCY_BUFFER_SIZE, "32768"), 32768)),
                    Boolean.parseBoolean(System.getProperty(K_METRICS_LATENCY_ENABLED, "true")),
                    Math.max(1L, TimeUnit.SECONDS.toMicros(parseLongOr(
                            System.getProperty(K_METRICS_LATENCY_MAX_SECS, "60"), 60L))),
                    Math.max(16, Math.max(batchSize, parseIntOr(
                            System.getProperty(K_OUTBATCH_CAPACITY, String.valueOf(batchSize)),
                            batchSize))),
                    Math.max(0L, parseLongOr(System.getProperty(K_DLQ_ERROR_THRESHOLD, "0"), 0L))
            );
        }

        private static PipelineRuntimeSettings fromConfig(PipelineConfig config, int batchSize) {
            final int configuredOutBatchCapacity = readInt(config, K_OUTBATCH_CAPACITY, batchSize);
            return new PipelineRuntimeSettings(
                    readBoolean(config, K_LATENCY_ENABLED, true),
                    readInt(config, K_LATENCY_SAMPLE_MASK, 1023),
                    !readBoolean(config, K_CACHE_FORCE_DISABLED, false),
                    readBoolean(config, K_SINK_BATCH_COPY, true),
                    readString(config, K_EXECUTOR_MODE, "FIXED").trim().toUpperCase(),
                    readBoolean(config, K_SOURCE_FETCH_LOCK, true),
                    readInt(config, K_SINK_INFLIGHT_MAX, Integer.MAX_VALUE),
                    Math.max(1024, readInt(config, K_LATENCY_BUFFER_SIZE, 32768)),
                    readBoolean(config, K_METRICS_LATENCY_ENABLED, true),
                    Math.max(1L, TimeUnit.SECONDS.toMicros(
                            Math.max(1L, readLong(config, K_METRICS_LATENCY_MAX_SECS, 60L)))),
                    Math.max(16, Math.max(batchSize, configuredOutBatchCapacity)),
                    Math.max(0L, readLong(config, K_DLQ_ERROR_THRESHOLD, 0L))
            );
        }

        private static String readString(PipelineConfig config, String key, String fallback) {
            final String override = System.getProperty(key);
            if (override != null && !override.isBlank()) {
                return override.trim();
            }
            final String configured = config.getString(key, null);
            if (configured != null && !configured.isBlank()) {
                return configured.trim();
            }
            return fallback;
        }

        private static boolean readBoolean(PipelineConfig config, String key, boolean fallback) {
            return Boolean.parseBoolean(readString(config, key, Boolean.toString(fallback)));
        }

        private static int readInt(PipelineConfig config, String key, int fallback) {
            return parseIntOr(readString(config, key, Integer.toString(fallback)), fallback);
        }

        private static long readLong(PipelineConfig config, String key, long fallback) {
            return parseLongOr(readString(config, key, Long.toString(fallback)), fallback);
        }
    }

    // -------------------------------------------------------------------------
    // Latency buffer (thread-local double-buffer, bench-only)
    // -------------------------------------------------------------------------
    private static final class LatencyBuffer {
        private final long[]  a, b;
        private final Object  swapLock = new Object();
        private long[] active;
        private long[] inactive;
        private int    idxActive;
        private int    idxInactive;

        LatencyBuffer(int capacity) {
            this.a        = new long[capacity];
            this.b        = new long[capacity];
            this.active   = a;
            this.inactive = b;
        }

        void recordNanos(long nanos) {
            // This buffer is thread-local for writers. The snapshot thread may swap buffers
            // concurrently, and a sample on that boundary can be missed, which is acceptable for
            // bench telemetry. Avoiding a monitor enter here matters when sampleMask=0.
            final int i = idxActive;
            if (i < active.length) {
                active[i] = nanos;
                idxActive = i + 1;
            }
        }

        long[] snapshotAndReset() {
            final long[] readBuf;
            final int    count;
            synchronized (swapLock) {
                final long[] tmp = active;
                active      = inactive;
                inactive    = tmp;
                idxInactive = idxActive;
                idxActive   = 0;
                readBuf     = inactive;
                count       = Math.min(idxInactive, readBuf.length);
                idxInactive = 0;
            }
            if (count <= 0) return new long[0];
            return Arrays.copyOf(readBuf, count);
        }
    }

    public MetricsLatencySnapshot metricsLatencySnapshotAndReset() {
        if (!metricsLatencyEnabled) {
            return new MetricsLatencySnapshot(0, 0, 0, 0, 0, 0);
        }
        final Histogram h = metricsLatencyRecorder.getIntervalHistogram();
        if (h.getTotalCount() == 0) {
            return new MetricsLatencySnapshot(0, 0, 0, 0, 0, 0);
        }

        // histogram values are in microseconds
        final double p50  = h.getValueAtPercentile(50.0) / 1000.0;
        final double p95  = h.getValueAtPercentile(95.0) / 1000.0;
        final double p99  = h.getValueAtPercentile(99.0) / 1000.0;
        final double p999 = h.getValueAtPercentile(99.9) / 1000.0;
        final double max  = h.getMaxValue() / 1000.0;

        return new MetricsLatencySnapshot(p50, p95, p99, p999, max, h.getTotalCount());
    }

    public record MetricsLatencySnapshot(
            double p50Millis,
            double p95Millis,
            double p99Millis,
            double p999Millis,
            double maxMillis,
            long samples) {}
}
