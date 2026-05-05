/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.sources;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * RestSourceConnector
 * ==================
 * Production-grade REST polling {@link SourceConnector} that fetches data from an HTTP endpoint on a fixed cadence
 * and emits {@link PipelinePayload} records containing the response body as a String.
 *
 * Enterprise goals & design constraints
 * ------------------------------------
 * 1) Deterministic, auditable polling
 *    - Polls at a fixed interval (source.rest.poll.interval.ms).
 *    - Uses a monotonic schedule (nextPollAtNanos) to avoid drift and to behave predictably under load.
 *
 * 2) Decouple HTTP latency from pipeline throughput
 *    - Uses an internal non-blocking buffer (ConcurrentLinkedQueue) to stage fetched payloads.
 *    - fetch() is cheap in the steady state (drains buffer first).
 *
 * 3) Safe lifecycle
 *    - connect()/disconnect() are idempotent.
 *    - No background threads are required; the pipeline dispatcher drives polling by calling fetch().
 *
 * 4) Metrics + counters without fragility
 *    - Metrics failures must never break ingestion.
 *    - Internal counters provide observability even when metrics runtime is absent.
 *
 * 5) Minimal assumptions about response shape
 *    - This generic connector treats the response body as a single payload.
 *    - If you need JSON-array fan-out, add an optional "fanout" mode later (kept out of this connector for clarity).
 *
 * Config keys (canonical)
 * ----------------------
 * - source.rest.url                      (required)
 * - source.rest.poll.interval.ms         (default: 5000)
 * - source.rest.http.connect.timeout.ms  (default: 5000)
 * - source.rest.http.request.timeout.ms  (default: 10000)
 * - source.rest.http.accept              (default: application/json)
 * - source.rest.http.user.agent          (default: StreamKernel/1.0)
 *
 * Buffering & backpressure
 * ------------------------
 * - source.rest.buffer.max.size          (default: 1000)
 *     When the buffer is full, this connector drops newly fetched payloads (drop-new) and increments a counter.
 *     This preserves pipeline stability under slow downstream conditions.
 */
public final class RestSourceConnector implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(RestSourceConnector.class);

    // ---- Canonical config keys ----
    private static final String K_URL = "source.rest.url";
    private static final String K_POLL_MS = "source.rest.poll.interval.ms";

    private static final String K_CONNECT_TIMEOUT_MS = "source.rest.http.connect.timeout.ms";
    private static final String K_REQUEST_TIMEOUT_MS = "source.rest.http.request.timeout.ms";
    private static final String K_ACCEPT = "source.rest.http.accept";
    private static final String K_USER_AGENT = "source.rest.http.user.agent";

    private static final String K_BUFFER_MAX_SIZE = "source.rest.buffer.max.size";

    // ---- Defaults ----
    private static final long D_POLL_MS = 5000L;
    private static final long D_CONNECT_TIMEOUT_MS = 5000L;
    private static final long D_REQUEST_TIMEOUT_MS = 10_000L;
    private static final String D_ACCEPT = "application/json";
    private static final String D_USER_AGENT = "StreamKernel/1.0";
    private static final int D_BUFFER_MAX_SIZE = 1000;

    private final HttpClient client;
    private final URI targetUri;

    /** Poll interval expressed in nanos for drift-resistant scheduling. */
    private final long pollIntervalNanos;

    private final long requestTimeoutMs;
    private final String accept;
    private final String userAgent;

    private final MetricsRuntime metrics;

    /**
     * Internal buffer to decouple HTTP latency from Pipeline throughput.
     * Queue is non-blocking; fetch() drains it quickly.
     */
    private final Queue<PipelinePayload<String>> buffer = new ConcurrentLinkedQueue<>();

    /** Soft bound on buffer growth for downstream backpressure. */
    private final int bufferMaxSize;

    /** Idempotent lifecycle flags. */
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Next poll time in nanos (monotonic). */
    private volatile long nextPollAtNanos;

    // ---- Internal counters (ops visibility even without MetricsRuntime) ----
    private final LongAdder fetchOk = new LongAdder();
    private final LongAdder fetchFail = new LongAdder();
    private final LongAdder fetchError = new LongAdder();
    private final LongAdder droppedDueToBackpressure = new LongAdder();

    private RestSourceConnector(
            URI targetUri,
            long pollIntervalNanos,
            long connectTimeoutMs,
            long requestTimeoutMs,
            String accept,
            String userAgent,
            int bufferMaxSize,
            MetricsRuntime metrics
    ) {
        this.targetUri = Objects.requireNonNull(targetUri, "targetUri");
        this.pollIntervalNanos = Math.max(0L, pollIntervalNanos);
        this.requestTimeoutMs = Math.max(1L, requestTimeoutMs);
        this.accept = (accept == null || accept.isBlank()) ? D_ACCEPT : accept.trim();
        this.userAgent = (userAgent == null || userAgent.isBlank()) ? D_USER_AGENT : userAgent.trim();
        this.bufferMaxSize = Math.max(1, bufferMaxSize);
        this.metrics = metrics;

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Math.max(1L, connectTimeoutMs)))
                // Prefer HTTP/2 when available; JVM will downgrade to 1.1 if needed.
                .version(HttpClient.Version.HTTP_2)
                .build();

        // Start eligible to poll immediately; connect() will log schedule.
        this.nextPollAtNanos = System.nanoTime();
    }

    /**
     * Factory: resolves and validates configuration.
     */
    public static RestSourceConnector fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");

        final String url = config.getString(K_URL, null);
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("Missing config: " + K_URL);
        }

        final long pollMs = config.getLong(K_POLL_MS, D_POLL_MS);

        final long connectTimeoutMs = config.getLong(K_CONNECT_TIMEOUT_MS, D_CONNECT_TIMEOUT_MS);
        final long requestTimeoutMs = config.getLong(K_REQUEST_TIMEOUT_MS, D_REQUEST_TIMEOUT_MS);

        final String accept = config.getString(K_ACCEPT, D_ACCEPT);
        final String userAgent = config.getString(K_USER_AGENT, D_USER_AGENT);

        final int bufferMaxSize = (int) clampLong(config.getLong(K_BUFFER_MAX_SIZE, D_BUFFER_MAX_SIZE), 1, 1_000_000);

        return new RestSourceConnector(
                URI.create(url.trim()),
                TimeUnit.MILLISECONDS.toNanos(Math.max(0L, pollMs)),
                connectTimeoutMs,
                requestTimeoutMs,
                accept,
                userAgent,
                bufferMaxSize,
                metrics
        );
    }

    @Override
    public void connect() {
        if (!connected.compareAndSet(false, true)) return;

        final long pollMs = TimeUnit.NANOSECONDS.toMillis(pollIntervalNanos);
        log.info("REST Source Connected. target={} pollIntervalMs={} bufferMaxSize={}",
                targetUri, pollMs, bufferMaxSize);

        // Reset schedule to "now" on connect for deterministic startup behavior.
        nextPollAtNanos = System.nanoTime();
    }

    @Override
    public void disconnect() {
        if (!closed.compareAndSet(false, true)) return;

        // HttpClient resources are managed by JVM; no explicit close required.
        buffer.clear();

        log.info("REST Source Disconnected. ok={} fail={} error={} droppedBackpressure={}",
                fetchOk.sum(), fetchFail.sum(), fetchError.sum(), droppedDueToBackpressure.sum());
    }

    /**
     * Fetches one payload:
     *  1) drains buffer first
     *  2) if it's time to poll, performs one HTTP call and enqueues at most one payload
     *  3) returns the next buffered payload (or null if none)
     */
    @Override
    public PipelinePayload<String> fetch() {
        if (closed.get()) return null;

        // 1) Drain buffer first
        PipelinePayload<String> fromBuf = buffer.poll();
        if (fromBuf != null) return fromBuf;

        // 2) Not time to poll yet => idle
        final long now = System.nanoTime();
        if (now < nextPollAtNanos) return null;

        // 3) Poll once
        performHttpCall();

        // Monotonic next schedule (prevents drift when fetch() is called late under load).
        // If we're far behind, schedule the next poll one interval from now (not from the old deadline).
        nextPollAtNanos = now + pollIntervalNanos;

        // Return whatever was enqueued
        return buffer.poll();
    }

    private void performHttpCall() {
        // Backpressure guard: avoid unbounded memory if downstream stalls.
        if (approxBufferSize() >= bufferMaxSize) {
            droppedDueToBackpressure.increment();
            counterSafe("source.rest.buffer.dropped.total", 1);
            if (connected.get()) {
                log.warn("REST Source buffer full ({}). Dropping new fetch. target={}", bufferMaxSize, targetUri);
            }
            return;
        }

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(targetUri)
                    .GET()
                    .timeout(Duration.ofMillis(requestTimeoutMs))
                    .header("Accept", accept)
                    .header("User-Agent", userAgent)
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            final int sc = response.statusCode();

            if (sc == 200) {
                final String body = response.body();
                if (body != null && !body.isBlank()) {
                    buffer.add(new PipelinePayload<>(
                            UUID.randomUUID().toString(),
                            body,
                            Instant.now(),
                            Map.of(
                                    "rest.url", targetUri.toString(),
                                    "rest.status", Integer.toString(sc)
                            )
                    ));
                    fetchOk.increment();
                    counterSafe("source.rest.fetch.ok.total", 1);
                } else {
                    // 200 but empty body is treated as a non-fatal "no data" response.
                    fetchFail.increment();
                    counterSafe("source.rest.fetch.empty.total", 1);
                }
            } else {
                fetchFail.increment();
                counterSafe("source.rest.fetch.fail.total", 1);
                log.warn("REST poll failed. status={} target={}", sc, targetUri);
            }

        } catch (Exception e) {
            fetchError.increment();
            counterSafe("source.rest.fetch.error.total", 1);
            log.error("REST connection error. target={} msg={}", targetUri, e.getMessage());
        }
    }

    /**
     * ConcurrentLinkedQueue does not expose size cheaply.
     * This is a best-effort approximation used ONLY for backpressure gating.
     */
    private int approxBufferSize() {
        // NOTE: size() is O(n) for CLQ; we avoid calling it repeatedly.
        // For this connector, fetch() calls are already paced; a bounded scan is acceptable here.
        int n = 0;
        for (var ignored : buffer) {
            if (++n >= bufferMaxSize) break;
        }
        return n;
    }

    private void counterSafe(String name, long inc) {
        if (metrics == null) return;
        try {
            // try common signatures
            try {
                var m = metrics.getClass().getMethod("counter", String.class, double.class);
                m.invoke(metrics, name, (double) inc);
                return;
            } catch (NoSuchMethodException ignored) {}
            try {
                var m = metrics.getClass().getMethod("counter", String.class, long.class);
                m.invoke(metrics, name, inc);
            } catch (NoSuchMethodException ignored) {}
        } catch (Throwable ignored) {
            // metrics must never impact the hot path
        }
    }

    private static long clampLong(long v, long lo, long hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}
