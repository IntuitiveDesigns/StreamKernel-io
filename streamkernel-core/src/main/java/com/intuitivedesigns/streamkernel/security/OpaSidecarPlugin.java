/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SecurityPlugin;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * OpaSidecarPlugin provides a {@link SecurityProvider} implementation that delegates authorization
 * decisions to Open Policy Agent (OPA) using the OPA Data API.
 *
 * Typical deployment pattern:
 * - StreamKernel runs as an application container.
 * - OPA runs as a sidecar container in the same pod (Kubernetes) or as a local process (dev).
 * - StreamKernel calls OPA over HTTP to ask: "Is principal allowed to perform action on resource?"
 *
 * Enterprise / acquisition posture:
 * - Fail-closed by default (deny on errors) with explicit fail-open override.
 * - Short-lived in-memory decision cache to reduce network overhead and protect OPA under load.
 * - Bounded-cache guard to prevent unbounded memory growth (approximate entry tracking + gated eviction/flush).
 * - Metrics hooks for:
 *     - cache hit/miss,
 *     - call success/error,
 *     - allow/deny,
 *     - parse/status errors,
 *     - latency,
 *     - suppressed error logs (useful during outages).
 * - Log throttling so a failing OPA sidecar does not DOS your log pipeline.
 *
 * Security notes:
 * - This implementation sends only principal/action/resource (no payload contents), minimizing sensitive data exposure.
 * - If you need attribute-based access control (ABAC), extend "input" with relevant attributes (tenant, env, labels).
 * - For regulated environments, ensure TLS/mTLS between app and OPA and restrict OPA policy endpoints.
 */
public final class OpaSidecarPlugin implements SecurityPlugin {

    public static final String ID = "OPA_SIDECAR";
    private static final Logger log = LoggerFactory.getLogger(OpaSidecarPlugin.class);

    @Override
    public String id() { return ID; }

    @Override
    public PluginKind kind() { return PluginKind.SECURITY; }

    /**
     * Creates a {@link SecurityProvider} instance wired to an OPA Data API endpoint.
     *
     * Config keys (operator contract):
     *
     * Required:
     * - security.opa.url                           OPA Data API endpoint; must return {"result": true|false}
     *
     * Timeouts (always set both explicitly — they have different failure semantics):
     * - security.opa.http.connect.timeout.ms       TCP connect timeout in ms              (default: 500)
     * - security.opa.http.request.timeout.ms       Full end-to-end request timeout in ms  (default: 1000)
     *
     * Policy:
     * - security.opa.fail.open                     Allow on error instead of deny         (default: false)
     * - security.opa.cache.ttl.ms                  Decision TTL; 0 disables caching       (default: 30000)
     * - security.opa.cache.max.size                Approximate max cache entries           (default: 10000)
     *
     * Enterprise note:
     * - Prefer keeping fail.open=false in production. Fail-open is sometimes acceptable for low-risk actions.
     * - Make sure the OPA endpoint corresponds to a stable policy path (versioned bundle or pinned policy).
     */
    @Override
    public SecurityProvider create(PipelineConfig config, MetricsRuntime metrics) {
        // OPA Data API endpoint. By convention, the policy should expose a boolean "allow" decision.
        // Example: /v1/data/<package>/<rule> where rule evaluates to boolean.
        String opaUrl = config.getString("security.opa.url", "http://localhost:8181/v1/data/streamkernel/authz/allow");

        // Cache TTL: a short TTL reduces policy-staleness risk while still reducing call volume.
        long ttlMs = config.getLong("security.opa.cache.ttl.ms", 30_000L);

        // Fail-open is explicitly configurable. Default is fail-closed.
        boolean failOpen = config.getBoolean("security.opa.fail.open", false);

        // HTTP timeouts:
        //
        // security.opa.http.connect.timeout.ms  (default: 500ms)
        //   TCP connect timeout. For a sidecar (same host/pod), 500ms is generous.
        //   If connect does not complete within this window, the call is treated as an
        //   error and the fail-closed/fail-open policy applies.
        //
        // security.opa.http.request.timeout.ms  (default: 1000ms)
        //   End-to-end per-request timeout from the moment the request is sent until
        //   the full response body is received. This covers:
        //     - TCP connect (if not yet established)
        //     - TLS handshake
        //     - OPA policy evaluation
        //     - response transfer
        //   Set this to match your authorization SLO. For batch-level auth (one call per
        //   batch of N events), 1000ms is a safe ceiling; most OPA decisions are < 5ms.
        //
        // Enterprise guidance:
        //   Both timeouts must be explicitly tuned for your deployment topology.
        //   A sidecar in the same Kubernetes pod should use much tighter values (e.g., 200ms / 500ms).
        //   A remote OPA cluster across a WAN segment needs looser values.
        //   There is intentionally no single "security.opa.timeout.ms" shorthand —
        //   connect and request timeouts have meaningfully different failure semantics
        //   and should be controlled independently.
        long connectTimeoutMs = config.getLong("security.opa.http.connect.timeout.ms", 500L);
        long requestTimeoutMs = config.getLong("security.opa.http.request.timeout.ms", 1_000L);

        // Guardrail to prevent unbounded cache growth.
        // Note: this is a best-effort cap using approximate counting.
        int maxCacheSize = (int) config.getLong("security.opa.cache.max.size", 10_000L);

        log.info("Initializing OPA Provider: url={} ttlMs={} failOpen={} maxCacheSize={} connectTimeoutMs={} requestTimeoutMs={}",
                opaUrl, ttlMs, failOpen, maxCacheSize, connectTimeoutMs, requestTimeoutMs);

        return new OpaProvider(
                opaUrl,
                ttlMs,
                maxCacheSize,
                failOpen,
                connectTimeoutMs,
                requestTimeoutMs,
                metrics
        );
    }

    /**
     * Concrete {@link SecurityProvider} that performs:
     * - key construction (principal|action|resource),
     * - cache lookups and updates,
     * - HTTP calls to OPA,
     * - minimal parsing of OPA response,
     * - metric emission,
     * - bounded cache maintenance,
     * - graceful close.
     */
    private static final class OpaProvider implements SecurityProvider {

        private static final String CONTENT_TYPE = "Content-Type";
        private static final String JSON_CT = "application/json";

        /**
         * Log throttling window:
         * while OPA is down, emit at most one ERROR per window to avoid log storms.
         */
        private static final long ERROR_LOG_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(10);

        /** Shared HTTP client for all auth calls; built once per provider instance. */
        private final HttpClient client;

        /** Target OPA endpoint. */
        private final URI uri;

        /** JSON codec for request/response. */
        private final ObjectMapper json;

        /** Decision TTL (nanos). If 0, caching is disabled. */
        private final long ttlNanos;

        /** Approximate max cache entries before eviction/flush protection triggers. */
        private final int maxCacheSize;

        /** Fail-open toggle: if true and OPA call fails, authorization returns "allowed". */
        private final boolean failOpen;

        /** Per-request timeout in milliseconds. */
        private final long requestTimeoutMs;

        /** Metrics runtime (optional). */
        private final MetricsRuntime metrics;

        /**
         * Decision cache keyed by "principal|action|resource".
         * ConcurrentHashMap provides lock-free reads and scalable writes under contention.
         */
        private final Map<String, CacheEntry> decisionCache = new ConcurrentHashMap<>();

        /**
         * Approximate entry count. We use LongAdder because it scales under concurrent increments.
         * Note: Because removals are not tracked precisely, this is an approximation used for a guardrail.
         */
        private final LongAdder approxCacheEntries = new LongAdder();

        /**
         * Guard to ensure only one thread performs eviction/flush at a time (prevents stampeding).
         */
        private final AtomicBoolean isClearing = new AtomicBoolean(false);

        /**
         * Next time we are allowed to log an ERROR (nanos). Used for outage log throttling.
         */
        private final AtomicLong nextErrorLogAtNanos = new AtomicLong(0);

        OpaProvider(
                String url,
                long ttlMs,
                int maxCacheSize,
                boolean failOpen,
                long connectTimeoutMs,
                long requestTimeoutMs,
                MetricsRuntime metrics
        ) {
            // Validate operator inputs early to prevent silent misconfiguration.
            if (ttlMs < 0) throw new IllegalArgumentException("security.opa.cache.ttl.ms must be >= 0");
            if (maxCacheSize <= 0) throw new IllegalArgumentException("security.opa.cache.max.size must be > 0");


            this.client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                    .build();

            this.uri = URI.create(url);
            this.json = new ObjectMapper();

            this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMs);
            this.maxCacheSize = maxCacheSize;
            this.failOpen = failOpen;
            this.requestTimeoutMs = requestTimeoutMs;

            this.metrics = metrics;
        }

        /**
         * Returns whether {@code principal} is allowed to perform {@code action} on {@code resource}.
         *
         * Decision flow:
         *  1) Cache lookup (if entry exists and is not expired -> return).
         *  2) Call OPA to obtain decision.
         *  3) On error: return failOpen (default false = deny).
         *  4) Cache decision (if TTL > 0).
         *
         * Fail-closed default:
         * - Any network/HTTP/parse error returns deny (unless failOpen=true).
         */
        @Override
        public boolean isAllowed(String principal, String action, String resource) {
            // Key construction:
            // This compact key supports high-throughput lookups while remaining human-readable in diagnostics.
            final String key = principal + "|" + action + "|" + resource;
            final long nowNanos = System.nanoTime();

            // 1) Cache check (fast path).
            CacheEntry cached = decisionCache.get(key);
            if (cached != null && nowNanos < cached.expiresAtNanos) {
                counter("opa_cache_hit_total", 1);
                return cached.allowed;
            }
            counter("opa_cache_miss_total", 1);

            // 2) Remote policy decision.
            Boolean decision = callOpa(principal, action, resource);

            // If callOpa returns null => error path.
            final boolean allowed;
            if (decision == null) {
                counter("opa_call_error_total", 1);
                allowed = failOpen; // deny by default
            } else {
                allowed = decision;
                counter("opa_call_success_total", 1);
                counter(allowed ? "opa_allow_total" : "opa_deny_total", 1);
            }

            // 3) Cache update (only if caching is enabled).
            if (ttlNanos > 0) {
                maybeBoundCacheAndEvict();
                long expiresAt = nowNanos + ttlNanos;

                // Put overwrites any prior entry.
                CacheEntry prev = decisionCache.put(key, new CacheEntry(allowed, expiresAt));
                if (prev == null) {
                    approxCacheEntries.increment();
                }
            }

            return allowed;
        }

        /**
         * Best-effort bounded-cache guard.
         *
         * Behavior:
         * - If approximate size is under maxCacheSize: do nothing.
         * - If over:
         *     - attempt a gated eviction pass of expired entries (up to 1024 removals).
         *     - if still at/over max: flush entire cache as a last resort safety valve.
         *
         * Enterprise rationale:
         * - Protects the JVM from unbounded growth if principals/resources are high-cardinality.
         * - Favors availability and bounded memory over perfect cache behavior.
         *
         * Note:
         * - This is an approximate guard; it intentionally trades precision for speed and simplicity.
         * - For very large deployments, consider:
         *     - a real bounded cache (Caffeine) with size + TTL policies,
         *     - or adding a "max cardinality" policy at the orchestrator layer.
         */
        private void maybeBoundCacheAndEvict() {
            if (approxCacheEntries.sum() < maxCacheSize) return;

            // Ensure only one thread does maintenance.
            if (!isClearing.compareAndSet(false, true)) return;

            try {
                // First pass: remove expired entries (bounded to keep maintenance cost predictable).
                long now = System.nanoTime();
                int removedExpired = 0;

                for (var it = decisionCache.entrySet().iterator(); it.hasNext(); ) {
                    var e = it.next();
                    CacheEntry ce = e.getValue();
                    if (ce != null && now >= ce.expiresAtNanos) {
                        it.remove();
                        removedExpired++;
                        if (removedExpired >= 1024) break; // bound work per maintenance cycle
                    }
                }

                if (removedExpired > 0) {
                    counter("opa_cache_expired_evictions_total", removedExpired);
                }

                // Last resort: full flush if still at/over max (approx).
                if (approxCacheEntries.sum() >= maxCacheSize) {
                    decisionCache.clear();
                    approxCacheEntries.reset();
                    counter("opa_cache_flush_total", 1);
                    log.warn("OPA cache reached max size ({}). Flushed.", maxCacheSize);
                }
            } finally {
                isClearing.set(false);
            }
        }

        /**
         * Performs the HTTP call to OPA and returns:
         * - Boolean decision if successful and parseable
         * - null on any error (network, non-200, parse mismatch)
         *
         * Request format:
         * {
         *   "input": { "principal": "...", "action": "...", "resource": "..." }
         * }
         *
         * Response expectation:
         * { "result": true|false }
         *
         * Enterprise note:
         * - This is intentionally minimal to preserve performance and limit coupling to policy structure.
         * - If your policy returns nested results, adjust parsing accordingly (e.g., result.allow).
         */
        private Boolean callOpa(String principal, String action, String resource) {
            final long startNanos = System.nanoTime();
            try {
                // Build JSON request body.
                ObjectNode root = json.createObjectNode();
                ObjectNode input = root.putObject("input");
                input.put("principal", principal);
                input.put("action", action);
                input.put("resource", resource);

                byte[] requestBody = json.writeValueAsBytes(root);

                // Build HTTP request with content type and a firm timeout.
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .header(CONTENT_TYPE, JSON_CT)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
                        .timeout(Duration.ofMillis(requestTimeoutMs))
                        .build();

                // Synchronous send. Authorization is typically on the hot path; if you need higher throughput,
                // consider:
                // - stronger caching,
                // - batch-level auth in the orchestrator,
                // - or a local WASM policy evaluation mode.
                HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

                // Record latency regardless of outcome.
                long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                timer("opa_request_latency_ms", durationMs);

                // Non-200 is treated as an error (fail-closed via caller).
                if (response.statusCode() != 200) {
                    counter("opa_http_status_total", 1);
                    throttledWarn("OPA sidecar returned HTTP {}", response.statusCode());
                    return null;
                }

                // Parse response and enforce expected schema.
                JsonNode resultNode = json.readTree(response.body());
                if (!resultNode.has("result")) {
                    counter("opa_parse_error_total", 1);
                    throttledWarn("OPA response missing 'result' field", null);
                    return null;
                }

                return resultNode.get("result").asBoolean();

            } catch (Exception e) {
                // Ensure latency metric still gets recorded on exceptions.
                long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                timer("opa_request_latency_ms", durationMs);
                throttledError("Failed to contact OPA sidecar", e);
                return null;
            }
        }

        /**
         * Warning logger.
         *
         * Note:
         * - Currently unthrottled. This is acceptable for low-rate warnings (e.g., misconfiguration),
         *   but if your system generates warning storms under failure, throttle similarly to errors.
         */
        private void throttledWarn(String msg, Object arg) {
            log.warn(msg, arg);
        }

        /**
         * Error logger with throttling:
         * emits at most one ERROR per {@link #ERROR_LOG_WINDOW_NANOS} window.
         *
         * Enterprise rationale:
         * - Prevents log pipeline overload when OPA is down.
         * - Still preserves periodic evidence of the outage with stack trace for diagnostics.
         */
        private void throttledError(String msg, Exception e) {
            long now = System.nanoTime();
            long nextAt = nextErrorLogAtNanos.get();
            if (now >= nextAt && nextErrorLogAtNanos.compareAndSet(nextAt, now + ERROR_LOG_WINDOW_NANOS)) {
                log.error(msg, e);
            } else {
                counter("opa_error_log_suppressed_total", 1);
            }
        }

        /**
         * Emit counter metrics (best-effort).
         * MetricsRuntime is optional to keep plugin usable in minimal environments.
         */
        private void counter(String name, long inc) {
            if (metrics == null) return;
            metrics.counter(name, (double) inc);
        }

        /**
         * Emit timer metrics in milliseconds (best-effort).
         * For richer latency tracking, pair this with histogram support in MetricsRuntime.
         */
        private void timer(String name, long durationMs) {
            if (metrics == null) return;
            metrics.timer(name, durationMs);
        }

        /**
         * Closes this provider and releases memory.
         *
         * Note:
         * - HttpClient does not require explicit close.
         * - We clear cache to reduce memory footprint on shutdown and to avoid stale decisions on reuse.
         */
        @Override
        public void close() {
            decisionCache.clear();
            approxCacheEntries.reset();
        }

        /**
         * Cache entry holding a decision and expiration.
         * Record keeps it compact and immutable.
         */
        private record CacheEntry(boolean allowed, long expiresAtNanos) {}
    }
}