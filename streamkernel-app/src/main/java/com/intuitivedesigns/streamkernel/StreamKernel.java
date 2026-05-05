/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel;

import com.intuitivedesigns.streamkernel.config.ConfigPreflight;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.config.PipelineContext;
import com.intuitivedesigns.streamkernel.config.PipelineFactory;
import com.intuitivedesigns.streamkernel.core.ConfigDumper;
import com.intuitivedesigns.streamkernel.core.PipelineOrchestrator;
import com.intuitivedesigns.streamkernel.metrics.MetricsFactory;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.metrics.MetricsSettings;
import com.intuitivedesigns.streamkernel.spi.CacheRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * StreamKernel entrypoint: wires configuration, plugins, metrics, security, and lifecycle.
 * <p>
 * Observability strategy
 * ──────────────────────
 * Prometheus/Grafana is the PRIMARY metrics plane. The existing MetricsRuntime SPI already
 * exposes counter(name, double) and gauge(name, double), and PrometheusMetricsProvider
 * already wires those into a PrometheusMeterRegistry scraped at /metrics.
 * <p>
 * Pipeline counters are published by a dedicated "metrics pusher" scheduler that reads
 * the orchestrator's public LongAdder accessors every window and calls:
 * metrics.counter(name, delta)  — monotonic totals; use rate() in Grafana for EPS
 * metrics.gauge(name, value)    — instantaneous state; use raw value in Grafana
 * <p>
 * Zero new SPI methods. Existing metrics providers continue to work through the same facade.
 * <p>
 * Source starvation fix
 * ─────────────────────
 * Set streamkernel.source.fetch.lock=false in your .properties file for SyntheticSource
 * benchmarks. PipelineOrchestrator now counts empty batches (emptyBatchTotal) published
 * to Prometheus as streamkernel_pipeline_empty_batch_total.
 */
public final class StreamKernel {

    private static final Logger log = LoggerFactory.getLogger(StreamKernel.class);

    // -------------------------------------------------------------------------
    // Config keys
    // -------------------------------------------------------------------------
    private static final String CFG_PARALLELISM = "pipeline.parallelism";
    private static final String CFG_BATCH_SIZE = "pipeline.batch.size";
    private static final String CFG_BENCH_ENABLED = "streamkernel.bench.enabled";
    private static final String CFG_SPEEDOMETER_ENABLED = "streamkernel.speedometer.enabled";
    private static final String CFG_SPEEDOMETER_WINDOW_SECONDS = "streamkernel.speedometer.window.seconds";
    private static final String CFG_PRINT_CONFIG_ENABLED = "streamkernel.print.config.enabled";
    private static final String CFG_SERVICE_ACCOUNT = "app.service.account";
    private static final String CFG_TARGET_RESOURCE = "security.resource";
    private static final String CFG_ACTION = "security.action";
    private static final String CFG_AUTH_PER_RECORD = "security.auth.per.record";
    private static final String CFG_AUTH_TTL_MS = "security.auth.ttl.ms";
    private static final String CFG_SOURCE_FAIL_FAST = "pipeline.source.fail.fast";
    private static final String CFG_SOURCE_BACKOFF_INITIAL_MS = "pipeline.source.backoff.initial.ms";
    private static final String CFG_SOURCE_BACKOFF_MAX_MS = "pipeline.source.backoff.max.ms";
    private static final String CFG_DRAIN_TIMEOUT_MS       = "pipeline.drain.timeout.ms";
    private static final String CFG_PIPELINE_ID            = "pipeline.id";

    // Controls whether PipelineOrchestrator serializes source.fetchBatch() across workers.
    // Set to false for SyntheticSource benchmarks (thread-safe source, no lock needed).
    // Set to true (default) for all Kafka and network sources.
    // NOTE: This value is bridged into the JVM system property streamkernel.source.fetch.lock
    // so PipelineOrchestrator can read it from System.getProperty at construction time.
    private static final String CFG_SOURCE_FETCH_LOCK = "streamkernel.source.fetch.lock";
    private static final String CFG_SINK_INFLIGHT_MAX = "streamkernel.sink.inflight.max";
    private static final String CFG_SINK_BATCH_COPY = "streamkernel.sink.batch.copy";
    private static final String CFG_EXECUTOR_MODE = "streamkernel.executor.mode";
    private static final String CFG_LATENCY_ENABLED = "streamkernel.latency.enabled";
    private static final String CFG_LATENCY_SAMPLE_MASK = "streamkernel.latency.sample.mask";
    private static final String CFG_LATENCY_BUFFER_SIZE = "streamkernel.latency.buffer.size";
    private static final String CFG_METRICS_LATENCY_ENABLED = "streamkernel.metrics.latency.enabled";
    private static final String CFG_METRICS_LATENCY_MAX_SECONDS = "streamkernel.metrics.latency.max.seconds";
    private static final String CFG_OUTBATCH_CAPACITY = "streamkernel.outbatch.capacity";
    private static final String CFG_CACHE_FORCE_DISABLED = "streamkernel.cache.force.disabled";
    private static final String CFG_TOKENIZER_MAX_LEN = "ai.embedding.tokenizer.max.length";
    private static final String CFG_BENCH_AUTO_STOP_AFTER_SECONDS = "streamkernel.bench.auto.stop.after.seconds";

    // -------------------------------------------------------------------------
    // Defaults
    // -------------------------------------------------------------------------
    private static final int DEFAULT_PARALLELISM = 8;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_WINDOW_SECONDS = 5;
    private static final long SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS = 10L;

    private StreamKernel() {
    }

    public static void main(String[] args) {
        log.info("=== Booting StreamKernel (v6.0 - SPI Security + Generics) ===");

        // Support both Gradle -PskConfigPath and JVM -Dsk.config.path launch styles.
        final String gradleProp = System.getProperty("skConfigPath");
        if ((System.getProperty("sk.config.path") == null
                || System.getProperty("sk.config.path").isBlank())
                && gradleProp != null && !gradleProp.isBlank()) {
            System.setProperty("sk.config.path", gradleProp.trim());
        }

        final PipelineConfig rawConfig = PipelineConfig.get();
        final PipelineConfig config = rawConfig;
        ConfigPreflight.validateOrThrow(config);
        ConfigDumper.dump(log, config);
        PipelineFactory.logAvailablePlugins();

        CacheRegistry registry = null;
        MetricsRuntime metrics = null;
        ScheduledExecutorService scheduler = null;
        PipelineOrchestrator<?, ?> pipeline = null;
        PipelineMetricsPublisher metricsPublisher = null;

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        final AtomicBoolean shutdownStarted = new AtomicBoolean(false);

        try {
            // Metrics first — failures after this point are observable in Prometheus.
            metrics = MetricsFactory.init(MetricsSettings.from(config));

            // Pre-register dashboard metrics so Grafana panels never show "No data" on healthy zero-error runs.
            preregisterDashboardMetrics(metrics);

            final int parallelism = clampInt(config.getInt(CFG_PARALLELISM, DEFAULT_PARALLELISM), 1, Integer.MAX_VALUE);
            final int batchSize = clampInt(config.getInt(CFG_BATCH_SIZE, DEFAULT_BATCH_SIZE), 1, Integer.MAX_VALUE);
            final boolean benchEnabled = config.getBoolean(CFG_BENCH_ENABLED, false);
            final boolean printConfig = config.getBoolean(CFG_PRINT_CONFIG_ENABLED, false);
            final boolean speedometerOn = benchEnabled && config.getBoolean(CFG_SPEEDOMETER_ENABLED, true);
            final int windowSeconds = clampInt(config.getInt(CFG_SPEEDOMETER_WINDOW_SECONDS, DEFAULT_WINDOW_SECONDS), 1, 3600);
            final String serviceAccount = firstNonBlank(
                    config.getString("security.opa.user", null),
                    config.getString(CFG_SERVICE_ACCOUNT, null),
                    "unknown-service");

            final String targetResource = firstNonBlank(
                    config.getString("security.opa.resource", null),
                    config.getString(CFG_TARGET_RESOURCE, null),
                    config.getString("sink.kafka.topic", null),
                    config.getString("sink.topic", null),
                    "unknown-resource");

            final String action = firstNonBlank(
                    config.getString("security.opa.action", null),
                    config.getString(CFG_ACTION, null),
                    "write");

            final boolean authPerRecord = config.getBoolean(CFG_AUTH_PER_RECORD, false);
            final long authTtlMs = parseLongSafe(config.getString(CFG_AUTH_TTL_MS, "1000"), 1000L);
            final boolean failFastSource = config.getBoolean(CFG_SOURCE_FAIL_FAST, false);
            final long sourceBackoffInitialMs = config.getLong(CFG_SOURCE_BACKOFF_INITIAL_MS, 250L);
            final long sourceBackoffMaxMs = config.getLong(CFG_SOURCE_BACKOFF_MAX_MS, 5_000L);
            final long drainTimeoutMs = config.getLong(CFG_DRAIN_TIMEOUT_MS, 15_000L);

            // Bridge config -> JVM system properties for components that snapshot
            // settings from System.getProperty(...) during construction time.
            // Explicit -D JVM arguments still win over profile values.
            bridgeConfigToSystemProperty(config, CFG_SOURCE_FETCH_LOCK);
            bridgeConfigToSystemProperty(config, CFG_SINK_INFLIGHT_MAX);
            bridgeConfigToSystemProperty(config, CFG_SINK_BATCH_COPY);
            bridgeConfigToSystemProperty(config, CFG_EXECUTOR_MODE);
            bridgeConfigToSystemProperty(config, CFG_LATENCY_ENABLED);
            bridgeConfigToSystemProperty(config, CFG_LATENCY_SAMPLE_MASK);
            bridgeConfigToSystemProperty(config, CFG_LATENCY_BUFFER_SIZE);
            bridgeConfigToSystemProperty(config, CFG_METRICS_LATENCY_ENABLED);
            bridgeConfigToSystemProperty(config, CFG_METRICS_LATENCY_MAX_SECONDS);
            bridgeConfigToSystemProperty(config, CFG_OUTBATCH_CAPACITY);
            bridgeConfigToSystemProperty(config, CFG_CACHE_FORCE_DISABLED);
            bridgeConfigToSystemProperty(config, CFG_TOKENIZER_MAX_LEN);

            // pipeline.id drives the $pipeline Grafana dropdown and the identity gauge.
            // Reads from pipeline.id in the .properties file — your configs already set this.
            final String pipelineId = firstNonBlank(
                    config.getString(CFG_PIPELINE_ID, null),
                    "streamkernel-default");
            final long autoStopAfterSeconds = Math.max(0L,
                    readLongSetting(config, CFG_BENCH_AUTO_STOP_AFTER_SECONDS, 0L));

            // Emit identity gauge immediately at boot so the Grafana $pipeline dropdown
            // populates as soon as the first scrape hits, before any records are processed.
            emitPipelineIdentityGauge(metrics, pipelineId);

            log.info(
                    "CONFIG: Parallelism={} | Batch={} | Speedometer={} | Window={}s | Bench={} | ConfigFile={} | "
                            + "Security=[User:{} Resource:{} Action:{} Mode:{}]",
                    parallelism, batchSize,
                    speedometerOn ? "ON" : "OFF", windowSeconds,
                    benchEnabled ? "ON" : "OFF",
                    config.loadedFromPath(),
                    serviceAccount, targetResource, action,
                    authPerRecord ? "Per-Record" : "Per-Batch");

            if (benchEnabled && printConfig) {
                log.info("CFG: {}={} | {}={} | {}={} | {}={} | {}={}",
                        CFG_PARALLELISM, parallelism,
                        CFG_BATCH_SIZE, batchSize,
                        CFG_SPEEDOMETER_WINDOW_SECONDS, windowSeconds,
                        CFG_AUTH_TTL_MS, authTtlMs,
                        CFG_DRAIN_TIMEOUT_MS, drainTimeoutMs);
            }

            // CacheRegistry is created inside createPipeline() before any plugin is
            // constructed, then deposited in PipelineContext for retrieval here.
            // Cache-aware plugins receive it automatically
            // via PipelineFactory — no changes needed in individual plugin configs.
            pipeline = PipelineFactory.createPipeline(config, metrics);
            registry = PipelineContext.take();

            @SuppressWarnings("unchecked")
            final PipelineOrchestrator createdPipeline = (PipelineOrchestrator) pipeline;

            // -----------------------------------------------------------------
            // Scheduler drives the BENCH log line, Prometheus pusher, and benchmark
            // auto-stop. Give each concern its own scheduler thread so a slow or stuck
            // periodic task cannot starve shutdown.
            //
            // The Prometheus pusher runs whenever metrics are enabled, even in
            // production (bench.enabled=false). This is by design: Grafana should
            // always work, bench logging is optional.
            // -----------------------------------------------------------------
            final boolean metricsActive = isMetricsEnabled(metrics);
            if (speedometerOn || metricsActive || autoStopAfterSeconds > 0) {
                int schedulerThreads = 0;
                if (speedometerOn) schedulerThreads++;
                if (metricsActive) schedulerThreads++;
                if (autoStopAfterSeconds > 0) schedulerThreads++;
                scheduler = Executors.newScheduledThreadPool(
                        Math.max(1, schedulerThreads),
                        new NamedDaemonThreadFactory("sk-runtime-"));

                if (metricsActive) {
                    metricsPublisher = startMetricsPusher(
                            scheduler, createdPipeline, metrics, windowSeconds, pipelineId);
                }
            }

            // Capture finals for shutdown hook.
            final MetricsRuntime fm = metrics;
            final ScheduledExecutorService fs = scheduler;
            final PipelineOrchestrator<?, ?> fp = pipeline;
            final PipelineMetricsPublisher fmp = metricsPublisher;
            final CacheRegistry freg = registry;
            final Runnable gracefulShutdown = () ->
                    initiateShutdown(shutdownStarted, fs, fp, fmp, freg, fm, shutdownLatch);

            Runtime.getRuntime().addShutdownHook(new Thread(gracefulShutdown, "sk-shutdown"));

            log.info("Starting Pipeline Orchestrator...");
            pipeline.start();
            if (speedometerOn && scheduler != null) {
                // Schedule after pipeline.start() so the first BENCH window reflects
                // active processing rather than startup work.
                startBenchReporter(scheduler, createdPipeline, windowSeconds, parallelism);
            }
            if (autoStopAfterSeconds > 0 && scheduler != null) {
                log.info("Benchmark auto-stop armed for {} seconds.", autoStopAfterSeconds);
                scheduler.schedule(() -> {
                    log.info("Benchmark auto-stop reached after {} seconds. Initiating graceful shutdown.",
                            autoStopAfterSeconds);
                    launchAsyncShutdown(gracefulShutdown, "sk-auto-stop-shutdown");
                }, autoStopAfterSeconds, TimeUnit.SECONDS);
            }
            shutdownLatch.await();

        } catch (Throwable t) {
            log.error("Fatal application error", t);
            initiateShutdown(shutdownStarted, scheduler, pipeline, metricsPublisher, registry, metrics, shutdownLatch);
            System.exit(1);
        }
    }

    /**
     * Checks whether the MetricsRuntime is actively recording.
     *
     * MetricsRuntime declares enabled() as a default interface method (returns true).
     * We call it directly rather than via reflection. Any implementation that wants to
     * be treated as inactive must override enabled() to return false.
     */
    private static boolean isMetricsEnabled(MetricsRuntime metrics) {
        if (metrics == null) return false;
        try {
            return metrics.enabled();
        } catch (Throwable t) {
            // Never fail boot due to a broken enabled() implementation.
            log.warn("MetricsRuntime.enabled() threw unexpectedly — assuming active", t);
            return true;
        }
    }

    // -------------------------------------------------------------------------
    // Prometheus metrics pusher
    //
    // Uses only MetricsRuntime.counter(name, delta) and gauge(name, value).
    // No new SPI methods. Works identically against Prometheus, Micrometer,
    // or any future MetricsRuntime implementation.
    //
    // Counter strategy:
    //   Publish the WINDOW DELTA each tick. Micrometer accumulates these into
    //   a monotonically increasing counter in the registry.
    //   Grafana query: rate(streamkernel_pipeline_processed_total[30s])
    //
    // Gauge strategy:
    //   Publish the current instantaneous value each tick.
    //   Grafana query: streamkernel_pipeline_inflight_batches (no rate())
    // -------------------------------------------------------------------------
    private static PipelineMetricsPublisher startMetricsPusher(
            ScheduledExecutorService scheduler,
            PipelineOrchestrator<?, ?> pipeline,
            MetricsRuntime metrics,
            int windowSeconds,
            String pipelineId) {

        final int periodSeconds = Math.max(1, windowSeconds);
        final PipelineMetricsPublisher publisher = new PipelineMetricsPublisher(pipeline, metrics, pipelineId);
        scheduler.scheduleAtFixedRate(publisher, periodSeconds, periodSeconds, TimeUnit.SECONDS);
        return publisher;
    }

    // -------------------------------------------------------------------------
    // BENCH log reporter — minimal human-readable sanity check only.
    //
    // EMPTY = empty batches this window.
    //   Non-zero = source starvation / fetchLock contention.
    //   Fix: set streamkernel.source.fetch.lock=false in .properties.
    //
    // PROC_TOTAL should always increase while the pipeline is running.
    //   Flat = pipeline stall. Open Grafana to diagnose which counter is stuck.
    // -------------------------------------------------------------------------
    private static void startBenchReporter(
            ScheduledExecutorService scheduler,
            PipelineOrchestrator<?, ?> pipeline,
            int windowSeconds,
            int parallelism) {

        final int periodSeconds = Math.max(1, windowSeconds);
        final int par = Math.max(1, parallelism);

        scheduler.scheduleAtFixedRate(new Runnable() {
            private final Runtime rt = Runtime.getRuntime();
            private long lastTimeNs = System.nanoTime();
            private long lastProcessed = pipeline.processedTotal();
            private long lastOut = pipeline.outTotal();
            private long lastEmpty = pipeline.emptyBatchTotal();
            private long lastDropped = pipeline.droppedTotal();

            @Override
            public void run() {
                try {
                    final long now = System.nanoTime();
                    final double secs = (now - lastTimeNs) / 1_000_000_000.0;
                    if (secs <= 0) return;
                    lastTimeNs = now;

                    final long procNow = pipeline.processedTotal();
                    final long outNow = pipeline.outTotal();
                    final long emptyNow = pipeline.emptyBatchTotal();
                    final long droppedNow = pipeline.droppedTotal();

                    final double procEps = Math.max(0, procNow - lastProcessed) / secs;
                    final double outEps = Math.max(0, outNow - lastOut) / secs;
                    final long dEmpty = Math.max(0, emptyNow - lastEmpty);
                    final long dDrop = Math.max(0, droppedNow - lastDropped);

                    final PipelineOrchestrator.LatencySnapshot lat =
                            pipeline.benchLatencySnapshotAndReset();

                    final long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);

                    log.info(
                            "BENCH window_s={} PROC_EPS={} OUT_EPS={} EFF={} "
                                    + "LAT_MS[P50={} P99={} MAX={}] "
                                    + "EMPTY={} DROPPED={} PROC_TOTAL={} MEM={}MB",
                            periodSeconds,
                            round1(procEps), round1(outEps), round1(procEps / par),
                            round1(lat.p50Millis()), round1(lat.p99Millis()), round1(lat.maxMillis()),
                            dEmpty, dDrop, procNow, usedMb);

                    lastProcessed = procNow;
                    lastOut = outNow;
                    lastEmpty = emptyNow;
                    lastDropped = droppedNow;

                } catch (Throwable t) {
                    log.warn("Bench reporter error", t);
                }
            }
        }, periodSeconds, periodSeconds, TimeUnit.SECONDS);
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }

    private static void launchAsyncShutdown(Runnable gracefulShutdown, String threadName) {
        final Thread shutdownThread = new Thread(
                Objects.requireNonNull(gracefulShutdown, "gracefulShutdown"),
                firstNonBlank(threadName, "sk-async-shutdown")
        );
        shutdownThread.setDaemon(true);
        shutdownThread.start();
    }

    /**
     * Parses a long from string config values without throwing on malformed input.
     * Logs a warning and returns the fallback if the value cannot be parsed.
     */
    private static long parseLongSafe(String s, long fallback) {
        if (s == null) return fallback;
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid long config value '{}' — using default {}", s, fallback);
            return fallback;
        }
    }

    private static long readLongSetting(PipelineConfig config, String key, long fallback) {
        Objects.requireNonNull(config, "config");
        if (key == null || key.isBlank()) return fallback;

        final String override = System.getProperty(key);
        if (override != null && !override.isBlank()) {
            return parseLongSafe(override, fallback);
        }

        return parseLongSafe(config.getString(key, null), fallback);
    }

    private static void initiateShutdown(
            AtomicBoolean shutdownStarted,
            ScheduledExecutorService scheduler,
            PipelineOrchestrator<?, ?> pipeline,
            PipelineMetricsPublisher metricsPublisher,
            CacheRegistry registry,
            MetricsRuntime metrics,
            CountDownLatch shutdownLatch) {

        if (shutdownStarted == null || shutdownLatch == null) return;
        if (!shutdownStarted.compareAndSet(false, true)) return;

        try {
            shutdownScheduler(scheduler);
            stopPipeline(pipeline);
            publishFinalMetrics(metricsPublisher);
        } finally {
            closeQuietly(registry);   // registry after pipeline
            closeQuietly(metrics);
            shutdownLatch.countDown();
        }
    }

    private static void shutdownScheduler(ScheduledExecutorService scheduler) {
        if (scheduler == null) return;

        try {
            scheduler.shutdown();
            if (scheduler.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                return;
            }

            log.warn("Runtime scheduler did not terminate within {} seconds; forcing shutdownNow().",
                    SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS);
            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Runtime scheduler still has active tasks after forced shutdown.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while awaiting runtime scheduler shutdown; forcing shutdownNow().", e);
            try {
                scheduler.shutdownNow();
            } catch (Throwable t) {
                log.warn("Forced scheduler shutdown failed after interruption.", t);
            }
        } catch (Throwable t) {
            log.warn("Failed to stop runtime scheduler cleanly.", t);
        }
    }

    private static void stopPipeline(PipelineOrchestrator<?, ?> pipeline) {
        if (pipeline == null) return;
        try {
            pipeline.stop();
        } catch (Throwable t) {
            log.warn("Pipeline stop failed during shutdown.", t);
        }
    }

    private static void publishFinalMetrics(PipelineMetricsPublisher metricsPublisher) {
        if (metricsPublisher == null) return;
        try {
            metricsPublisher.publishFinal();
        } catch (Throwable t) {
            log.warn("Final metrics publish failed during shutdown.", t);
        }
    }

    private static final class PipelineMetricsPublisher implements Runnable {
        private final PipelineOrchestrator<?, ?> pipeline;
        private final MetricsRuntime metrics;
        private final String pipelineId;
        private final Runtime runtime = Runtime.getRuntime();

        private long lastProcessed;
        private long lastOut;
        private long lastIn;
        private long lastDropped;
        private long lastDenied;
        private long lastDlq;
        private long lastEmpty;
        private long lastSrcErr;
        private long lastAuthErr;
        private long lastDlqErr;

        private PipelineMetricsPublisher(
                PipelineOrchestrator<?, ?> pipeline,
                MetricsRuntime metrics,
                String pipelineId) {
            this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
            this.metrics = Objects.requireNonNull(metrics, "metrics");
            this.pipelineId = pipelineId;
            this.lastProcessed = pipeline.processedTotal();
            this.lastOut = pipeline.outTotal();
            this.lastIn = pipeline.inTotal();
            this.lastDropped = pipeline.droppedTotal();
            this.lastDenied = pipeline.deniedTotal();
            this.lastDlq = pipeline.dlqTotal();
            this.lastEmpty = pipeline.emptyBatchTotal();
            this.lastSrcErr = pipeline.sourceErrorTotal();
            this.lastAuthErr = pipeline.authErrorTotal();
            this.lastDlqErr = pipeline.dlqErrorTotal();
        }

        @Override
        public synchronized void run() {
            publish(false);
        }

        public synchronized void publishFinal() {
            publish(true);
        }

        private void publish(boolean finalSnapshot) {
            try {
                final long procNow = pipeline.processedTotal();
                final long outNow = pipeline.outTotal();
                final long inNow = pipeline.inTotal();
                final long dropNow = pipeline.droppedTotal();
                final long deniedNow = pipeline.deniedTotal();
                final long dlqNow = pipeline.dlqTotal();
                final long emptyNow = pipeline.emptyBatchTotal();
                final long srcErrNow = pipeline.sourceErrorTotal();
                final long authErrNow = pipeline.authErrorTotal();
                final long dlqErrNow = pipeline.dlqErrorTotal();

                final double dProc = Math.max(0, procNow - lastProcessed);
                final double dOut = Math.max(0, outNow - lastOut);
                final double dIn = Math.max(0, inNow - lastIn);
                final double dDrop = Math.max(0, dropNow - lastDropped);
                final double dDenied = Math.max(0, deniedNow - lastDenied);
                final double dDlq = Math.max(0, dlqNow - lastDlq);
                final double dEmpty = Math.max(0, emptyNow - lastEmpty);
                final double dSrcErr = Math.max(0, srcErrNow - lastSrcErr);
                final double dAuthErr = Math.max(0, authErrNow - lastAuthErr);
                final double dDlqErr = Math.max(0, dlqErrNow - lastDlqErr);

                if (dProc > 0) metrics.counter("streamkernel_pipeline_processed_total", dProc);
                if (dOut > 0) metrics.counter("streamkernel_pipeline_out_total", dOut);
                if (dIn > 0) metrics.counter("streamkernel_pipeline_in_total", dIn);
                if (dDrop > 0) metrics.counter("streamkernel_pipeline_dropped_total", dDrop);
                if (dDenied > 0) metrics.counter("streamkernel_pipeline_denied_total", dDenied);
                if (dDlq > 0) metrics.counter("streamkernel_pipeline_dlq_total", dDlq);
                if (dEmpty > 0) metrics.counter("streamkernel_pipeline_empty_batch_total", dEmpty);
                if (dSrcErr > 0) metrics.counter("streamkernel_pipeline_source_errors_total", dSrcErr);
                if (dAuthErr > 0) metrics.counter("streamkernel_pipeline_auth_errors_total", dAuthErr);
                if (dDlqErr > 0) metrics.counter("streamkernel_pipeline_dlq_errors_total", dDlqErr);

                metrics.gauge("streamkernel_pipeline_inflight_batches", finalSnapshot ? 0d : pipeline.inFlightBatches());
                metrics.gauge("streamkernel_pipeline_inflight_records", finalSnapshot ? 0d : pipeline.inFlightRecords());
                metrics.gauge("streamkernel_pipeline_load_percent", finalSnapshot
                        ? 0d
                        : Math.min(100.0, Math.max(0.0, pipeline.loadPercent())));

                final PipelineOrchestrator.MetricsLatencySnapshot lat = finalSnapshot
                        ? new PipelineOrchestrator.MetricsLatencySnapshot(0.0, 0.0, 0.0, 0.0, 0.0, 0)
                        : pipeline.metricsLatencySnapshotAndReset();
                metrics.gauge("streamkernel_pipeline_latency_p50_ms",  lat.p50Millis());
                metrics.gauge("streamkernel_pipeline_latency_p95_ms",  lat.p95Millis());
                metrics.gauge("streamkernel_pipeline_latency_p99_ms",  lat.p99Millis());
                metrics.gauge("streamkernel_pipeline_latency_p999_ms", lat.p999Millis());
                metrics.gauge("streamkernel_pipeline_latency_max_ms",  lat.maxMillis());
                metrics.gauge("streamkernel_pipeline_latency_samples", lat.samples());

                metrics.gauge("streamkernel_jvm_heap_used_mb",
                        (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0));
                emitPipelineIdentityGauge(metrics, pipelineId);

                lastProcessed = procNow;
                lastOut = outNow;
                lastIn = inNow;
                lastDropped = dropNow;
                lastDenied = deniedNow;
                lastDlq = dlqNow;
                lastEmpty = emptyNow;
                lastSrcErr = srcErrNow;
                lastAuthErr = authErrNow;
                lastDlqErr = dlqErrNow;
            } catch (Throwable t) {
                log.warn("Metrics pusher error", t);
            }
        }
    }

    private static void closeQuietly(Object r) {
        if (r instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static void bridgeConfigToSystemProperty(PipelineConfig config, String key) {
        Objects.requireNonNull(config, "config");
        if (key == null || key.isBlank()) return;

        final String existing = System.getProperty(key);
        if (existing != null && !existing.isBlank()) {
            return;
        }

        final String configured = config.getString(key, null);
        if (configured == null || configured.isBlank()) {
            return;
        }

        System.setProperty(key, configured.trim());
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static String firstNonBlank(String... vals) {
        if (vals == null) return null;
        for (String v : vals) {
            if (v != null) {
                final String t = v.trim();
                if (!t.isEmpty()) return t;
            }
        }
        return null;
    }

    /**
     * Thread factory that produces named daemon threads with a stable prefix.
     *
     * Named explicitly so it appears clearly in thread dumps and profilers.
     * Daemon status ensures the thread cannot prevent JVM shutdown.
     */
    private static final class NamedDaemonThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger sequence = new AtomicInteger(0);

        NamedDaemonThreadFactory(String prefix) {
            this.prefix = Objects.requireNonNull(prefix, "prefix");
        }

        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(r, prefix + sequence.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }

    /**
     * Emits streamkernel_pipeline_up_&lt;sanitized_id&gt; = 1.0 as a liveness/identity gauge.
     *
     * <p>This gauge serves two purposes:
     * <ol>
     *   <li>Powers the {@code $pipeline} Grafana variable dropdown — query:
     *       {@code label_values(streamkernel_pipeline_processed_total, job)}</li>
     *   <li>Acts as a liveness signal — when scraping stops, Prometheus marks it stale
     *       and Grafana shows no data, which is correct behavior for a stopped pipeline.</li>
     * </ol>
     *
     * <p>The pipeline ID is encoded in the metric name (not as a label) because
     * {@link MetricsRuntime#gauge(String, double)} does not support custom labels.
     * Sanitization replaces non-alphanumeric characters with underscores to ensure
     * the resulting name is a valid Prometheus metric name.
     */
    private static void emitPipelineIdentityGauge(MetricsRuntime metrics, String pipelineId) {
        if (metrics == null || pipelineId == null) return;
        final String safe = pipelineId.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase(Locale.ROOT);
        metrics.gauge("streamkernel_pipeline_up_" + safe, 1.0);
    }

    /**
     * Pre-registers all Prometheus series that the shipped Grafana dashboard expects.
     *
     * <p>Why: Prometheus only exposes time-series after they are created. In a healthy run,
     * many counters (drops, DLQ, errors) legitimately remain 0 and would otherwise be absent,
     * causing Grafana to show "No data". We create the meters once at startup.
     */
    private static void preregisterDashboardMetrics(MetricsRuntime metrics) {
        if (metrics == null) return;

        // Counters (monotonic totals)
        metrics.counter("streamkernel_pipeline_in_total", 0d);
        metrics.counter("streamkernel_pipeline_processed_total", 0d);
        metrics.counter("streamkernel_pipeline_out_total", 0d);
        metrics.counter("streamkernel_pipeline_dropped_total", 0d);
        metrics.counter("streamkernel_pipeline_denied_total", 0d);
        metrics.counter("streamkernel_pipeline_dlq_total", 0d);
        metrics.counter("streamkernel_pipeline_empty_batch_total", 0d);
        metrics.counter("streamkernel_pipeline_source_errors_total", 0d);
        metrics.counter("streamkernel_pipeline_auth_errors_total", 0d);
        metrics.counter("streamkernel_pipeline_dlq_errors_total", 0d);

        // Gauges (instantaneous)
        metrics.gauge("streamkernel_pipeline_inflight_batches", 0d);
        metrics.gauge("streamkernel_pipeline_inflight_records", 0d);
        metrics.gauge("streamkernel_pipeline_load_percent", 0d);
        metrics.gauge("streamkernel_jvm_heap_used_mb", 0d);
        metrics.gauge("streamkernel_pipeline_latency_p50_ms", 0d);
        metrics.gauge("streamkernel_pipeline_latency_p95_ms", 0d);
        metrics.gauge("streamkernel_pipeline_latency_p99_ms", 0d);
        metrics.gauge("streamkernel_pipeline_latency_p999_ms", 0d);
        metrics.gauge("streamkernel_pipeline_latency_max_ms", 0d);
        metrics.gauge("streamkernel_pipeline_latency_samples", 0d);
    }
}
