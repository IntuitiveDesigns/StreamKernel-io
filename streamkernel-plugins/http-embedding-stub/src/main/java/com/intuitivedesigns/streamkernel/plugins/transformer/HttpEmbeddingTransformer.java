/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins.transformer;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Enterprise-grade HTTP enrichment transformer designed for **benchmarkable** and **auditable** pipelines.
 *
 * What it does
 * ------------
 * Depending on configuration, this transformer:
 *  - PASS_THROUGH: returns the record unchanged (control mode for benchmarking).
 *  - APPEND_LEN:   calls the remote endpoint and appends the inferred embedding length for quick smoke checks.
 *  - EMBED:        calls the remote endpoint as an "embedding service" and optionally validates response dimension.
 *
 * Why this exists (enterprise posture)
 * -----------------------------------
 * In acquisition / enterprise review, systems are evaluated on:
 *  - determinism (configurable behavior, stable outcomes)
 *  - bounded resource usage (no unbounded concurrency)
 *  - operational transparency (counters and health checks)
 *  - failure semantics (explicit handling of non-2xx, retries, backoff)
 *
 * This transformer intentionally avoids complex reactive frameworks and instead uses:
 *  - JDK HttpClient (auditable dependency surface)
 *  - Semaphore bulkhead for bounded in-flight concurrency
 *  - simple backoff policy for explainability
 *
 * Important: Payload handling
 * ---------------------------
 * The default EMBED mode returns the **same PipelinePayload data** unchanged.
 * This is intentional: it allows you to benchmark the I/O and latency costs of the enrichment hop
 * without forcing the pipeline’s data model to carry the embedding bytes at this stage.
 *
 * If you want to propagate embeddings downstream:
 *  - modify EMBED mode to attach the embedding in metadata or replace payload data, OR
 *  - use a downstream transformer that reads the service response and converts it into WireEvent/bytes.
 *
 * Security notes
 * --------------
 * - The request body is JSON with an "inputs" field. Payload is JSON-escaped.
 * - This transformer does not log raw payloads unless debug is enabled (and even then truncates responses).
 * - For regulated environments, ensure payload does not contain secrets before sending to remote services.
 *
 * Metrics/counters
 * ----------------
 * - httpAttempts: incremented on each attempt (including retries)
 * - httpFailures: incremented on transport errors, and on non-2xx when failOnNon200=true
 * - integrityFailures: incremented when response dimension validation fails
 */
public final class HttpEmbeddingTransformer implements Transformer<String, String> {

    private static final Logger log = LoggerFactory.getLogger(HttpEmbeddingTransformer.class);

    // ---------------------------------------------------------------------
    // Configuration keys (canonical names)
    // ---------------------------------------------------------------------

    /** Target URL for the embedding/enrichment endpoint. */
    private static final String KEY_URL                    = "transform.http.url";

    /** Per-request timeout in milliseconds (connect + request). */
    private static final String KEY_TIMEOUT_MS             = "transform.http.timeout.ms";

    /**
     * Transformer behavior mode.
     * Values: PASS_THROUGH | APPEND_LEN | EMBED
     */
    private static final String KEY_MODE                   = "transform.http.mode";

    /** Maximum number of in-flight requests across all pipeline threads (bulkhead). */
    private static final String KEY_MAX_INFLIGHT           = "transform.http.max.inflight";

    /**
     * If true, non-2xx responses fail the record and count as httpFailures.
     * If false, non-2xx responses are treated as pass-through (auditable but non-fatal).
     */
    private static final String KEY_FAIL_ON_NON200         = "transform.http.fail.on.non200";

    /** Enables additional log output for troubleshooting (uses truncation). */
    private static final String KEY_DEBUG                  = "transform.http.debug";

    /** Maximum retry attempts for each record (>=1). */
    private static final String KEY_RETRY_MAX_ATTEMPTS     = "transform.http.retry.max.attempts";

    /** Base backoff in milliseconds for retries. Linear backoff = base * attempt. */
    private static final String KEY_RETRY_BASE_BACKOFF_MS  = "transform.http.retry.base.backoff.ms";

    /** If true, allow retries on non-2xx responses (requires failOnNon200=true to matter). */
    private static final String KEY_RETRY_ON_NON200        = "transform.http.retry.on.non200";

    /**
     * Optional integrity check: expected embedding dimension.
     * - If > 0, the transformer parses response JSON and verifies number of numeric elements.
     * - If <= 0, validation is disabled.
     */
    private static final String KEY_VALIDATE_DIM           = "transform.http.validate.response.dim";

    /**
     * Execution modes:
     * - PASS_THROUGH: does nothing (benchmark/control mode)
     * - APPEND_LEN: calls service, estimates embedding length, appends to payload string for debugging
     * - EMBED: calls service, optionally validates response dimension, returns payload unchanged
     */
    private enum Mode { PASS_THROUGH, APPEND_LEN, EMBED }

    // ---------------------------------------------------------------------
    // Immutable runtime components
    // ---------------------------------------------------------------------

    /** JDK HTTP client used for all requests (built once). */
    private final HttpClient http;

    /** Endpoint URI. */
    private final URI uri;

    /** Request timeout. */
    private final Duration timeout;

    /** Selected mode. */
    private final Mode mode;

    /**
     * Bulkhead / concurrency limiter:
     * - ensures we do not create unbounded concurrent HTTP calls
     * - protects the embedding service and the local process from overload
     */
    private final Semaphore bulkhead;

    /** Behavior controls for error handling and debugging. */
    private final boolean failOnNon200;
    private final boolean retryOnNon200;
    private final boolean debug;

    /** Retry behavior. */
    private final int retryMaxAttempts;
    private final long retryBaseBackoffMs;

    /** Expected embedding dimension for integrity validation; <=0 disables. */
    private final int validateDim;

    // ---------------------------------------------------------------------
    // Counters (lightweight; safe for hot-path)
    // ---------------------------------------------------------------------

    /** Request attempts (each try; includes retries). */
    private final LongAdder httpAttempts = new LongAdder();

    /** Failed attempts (transport errors or non-2xx when failOnNon200=true). */
    private final LongAdder httpFailures = new LongAdder();

    /** Integrity failures when validateDim is enabled. */
    private final LongAdder integrityFailures = new LongAdder();

    /** Exposed for downstream speedometer/telemetry without extra reflection. */
    public long httpAttemptsTotal() { return httpAttempts.sum(); }
    public long httpFailuresTotal() { return httpFailures.sum(); }
    public long integrityFailuresTotal() { return integrityFailures.sum(); }

    /**
     * Creates an HttpEmbeddingTransformer from PipelineConfig.
     *
     * Validation:
     * - transform.http.url is required.
     *
     * Defaults chosen for enterprise operability:
     * - timeout defaults to 2s (reasonable for local sidecars or nearby services)
     * - max inflight defaults to 64 (bulkhead; tune based on service capacity)
     * - failOnNon200 defaults to true (fail fast for correctness)
     * - retries default to 1 (no retry) to keep benchmark runs deterministic
     * - validateDim defaults to -1 (disabled)
     *
     * @param cfg     pipeline configuration
     * @param metrics metrics runtime (not used directly here but kept in signature for plugin consistency)
     */
    public HttpEmbeddingTransformer(PipelineConfig cfg, MetricsRuntime metrics) {
        final String url = cfg.getString(KEY_URL, null);
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("Missing required config: " + KEY_URL);
        }

        final long timeoutMs = parseLongOrDefault(cfg.getString(KEY_TIMEOUT_MS, "2000"), 2000L);
        final String modeStr = cfg.getString(KEY_MODE, "PASS_THROUGH");

        this.mode = parseMode(modeStr);
        this.uri = URI.create(url.trim());
        this.timeout = Duration.ofMillis(timeoutMs);

        // Bulkhead: limits concurrent requests across the pipeline.
        final int maxInflight = (int) clamp(parseLongOrDefault(cfg.getString(KEY_MAX_INFLIGHT, "64"), 64L), 1, 1_000_000);
        this.bulkhead = new Semaphore(maxInflight);

        this.failOnNon200 = parseBool(cfg.getString(KEY_FAIL_ON_NON200, "true"), true);
        this.retryOnNon200 = parseBool(cfg.getString(KEY_RETRY_ON_NON200, "false"), false);
        this.debug = parseBool(cfg.getString(KEY_DEBUG, "false"), false);

        this.retryMaxAttempts = (int) clamp(parseLongOrDefault(cfg.getString(KEY_RETRY_MAX_ATTEMPTS, "1"), 1L), 1, 100);
        this.retryBaseBackoffMs = clamp(parseLongOrDefault(cfg.getString(KEY_RETRY_BASE_BACKOFF_MS, "25"), 25L), 0, 60_000);

        this.validateDim = (int) clamp(parseLongOrDefault(cfg.getString(KEY_VALIDATE_DIM, "-1"), -1L), -1, 100_000);

        // HttpClient tuning:
        // - HTTP/1.1 chosen for debuggability and consistent behavior across environments.
        // - connectTimeout also uses 'timeout' to keep operational reasoning simple.
        this.http = HttpClient.newBuilder()
                .connectTimeout(this.timeout)
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        log.info("HTTP_EMBEDDING ready: url={} mode={} timeoutMs={} maxInflight={} failOnNon200={} retryMaxAttempts={} validateDim={}",
                this.uri, this.mode, timeoutMs, maxInflight, this.failOnNon200, this.retryMaxAttempts, this.validateDim);
    }

    /**
     * Transforms a pipeline payload by optionally calling the configured HTTP endpoint.
     *
     * Hot-path behavior:
     * - In PASS_THROUGH mode, returns immediately (zero overhead).
     * - Otherwise:
     *     - acquires bulkhead semaphore (bounded concurrency)
     *     - constructs request JSON body {"inputs":"..."} with proper escaping
     *     - executes request, applying retry/backoff policy
     *
     * Failure semantics:
     * - Transport failures throw RuntimeException (counts as httpFailures).
     * - Non-2xx:
     *     - if failOnNon200=true: throw (counts as httpFailures)
     *     - else: return input unchanged (with optional debug warning)
     *
     * Integrity validation:
     * - If validateDim > 0 and mode=EMBED, parse response and enforce embedding element count.
     *
     * @param in input payload
     * @return transformed payload (or same payload), or null if upstream chooses to drop elsewhere
     */
    @Override
    public PipelinePayload<String> transform(PipelinePayload<String> in) {
        // Benchmark control mode: does nothing by design.
        if (mode == Mode.PASS_THROUGH) return in;

        // Bulkhead (bounded concurrency).
        // Enterprise note: acquireUninterruptibly ensures the transformer stays deterministic under load.
        // If you prefer "fail fast", add a config that uses tryAcquire(...) and returns in or throws.
        bulkhead.acquireUninterruptibly();
        try {
            final String payload = in.data();

            // Minimal JSON body: {"inputs":"..."} with proper JSON escaping.
            final String bodyJson = "{\"inputs\":\"" + escapeJson(payload) + "\"}";

            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(bodyJson, StandardCharsets.UTF_8))
                    .build();

            RuntimeException last = null;

            // Attempt loop:
            // - deterministic attempt count
            // - linear backoff (easy to explain in enterprise audits)
            for (int attempt = 1; attempt <= retryMaxAttempts; attempt++) {
                httpAttempts.increment();
                try {
                    final HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                    final int sc = resp.statusCode();

                    // Non-2xx handling
                    if (sc < 200 || sc >= 300) {
                        if (failOnNon200) {
                            httpFailures.increment();
                            final String excerpt = excerpt(resp.body(), 160);
                            final boolean canRetry = retryOnNon200 && attempt < retryMaxAttempts;

                            last = new RuntimeException("HTTP " + sc + " | " + excerpt + (canRetry ? " | retrying" : ""));
                            if (canRetry) {
                                backoff(attempt);
                                continue;
                            }
                            throw last;
                        }

                        // failOnNon200=false: ignore and pass-through.
                        // This can be useful in certain resilience modes where enrichment is optional.
                        if (debug) {
                            log.warn("HTTP_EMBEDDING non-2xx ignored (failOnNon200=false): status={} body={}",
                                    sc, excerpt(resp.body(), 160));
                        }
                        return in;
                    }

                    // Success path
                    final String body = resp.body();

                    // APPEND_LEN is a lightweight response sanity check useful in early development.
                    if (mode == Mode.APPEND_LEN) {
                        final int embLen = estimateEmbeddingLength(body);
                        return in.withData(payload + "|emb_len=" + embLen);
                    }

                    // EMBED integrity check: ensure the returned embedding "shape" matches expectations.
                    if (mode == Mode.EMBED && validateDim > 0) {
                        final int dim = extractEmbeddingDim(body);
                        if (dim != validateDim) {
                            integrityFailures.increment();
                            httpFailures.increment(); // count as failure for benchmark truthfulness
                            final String ex = excerpt(body, 220);
                            throw new RuntimeException(
                                    "Embedding dimension mismatch: expected=" + validateDim + " actual=" + dim + " | " + ex
                            );
                        }
                    }

                    // EMBED mode intentionally returns the input unchanged.
                    // The enrichment hop is measured/validated here; downstream transforms may carry embeddings.
                    return in;

                } catch (RuntimeException re) {
                    // RuntimeExceptions represent semantic failures (non-2xx when failOnNon200=true, integrity failures, etc.)
                    last = re;
                    throw re;
                } catch (Exception e) {
                    // Transport errors / timeouts / IO:
                    // - Count as failures
                    // - Optionally retry with backoff
                    httpFailures.increment();
                    last = new RuntimeException("HTTP transport error: " + e.getMessage(), e);

                    if (attempt < retryMaxAttempts) {
                        backoff(attempt);
                        continue;
                    }
                    throw last;
                }
            }

            // Defensive: should be unreachable because loop either returns or throws.
            throw (last != null) ? last : new RuntimeException("HTTP_EMBEDDING failed with unknown error");

        } finally {
            bulkhead.release();
        }
    }

    // ---------------------------------------------------------------------
    // Backoff / parsing helpers (kept simple for auditability)
    // ---------------------------------------------------------------------

    /**
     * Linear backoff policy: baseBackoffMs * attempt.
     *
     * Enterprise rationale:
     * - Linear backoff is predictable, easy to reason about in postmortems, and deterministic across runs.
     * - Exponential backoff with jitter may be preferable for large distributed fleets, but is less deterministic
     *   for benchmarking and "acquisition-grade" reproducibility.
     */
    private void backoff(int attempt) {
        if (retryBaseBackoffMs <= 0) return;

        final long sleepMs = retryBaseBackoffMs * (long) attempt;
        try {
            TimeUnit.MILLISECONDS.sleep(sleepMs);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    /** Parses Mode from config string. Defaults to PASS_THROUGH on invalid values (safe + deterministic). */
    private static Mode parseMode(String modeStr) {
        if (modeStr == null || modeStr.isBlank()) return Mode.PASS_THROUGH;
        try {
            return Mode.valueOf(modeStr.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return Mode.PASS_THROUGH;
        }
    }

    /** Parses a long with a default value. */
    private static long parseLongOrDefault(String s, long def) {
        if (s == null || s.isBlank()) return def;
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return def; }
    }

    /** Parses a boolean with permissive string values (true/false/1/0/yes/no/on/off). */
    private static boolean parseBool(String s, boolean def) {
        if (s == null || s.isBlank()) return def;
        final String v = s.trim().toLowerCase(Locale.ROOT);
        return switch (v) {
            case "true", "1", "yes", "y", "on" -> true;
            case "false", "0", "no", "n", "off" -> false;
            default -> def;
        };
    }

    /** Clamps numeric values to avoid pathological configurations. */
    private static long clamp(long v, long lo, long hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    /** Truncates long bodies for safe logging. */
    private static String excerpt(String body, int max) {
        if (body == null || body.isBlank()) return "no-body";
        final String b = body.trim();
        return (b.length() <= max) ? b : b.substring(0, max) + "…";
    }

    /**
     * Escapes a string for safe embedding into a JSON string literal.
     *
     * This avoids pulling a JSON library dependency into the hot path and keeps the transformer self-contained.
     * It handles:
     * - quotes, backslashes
     * - common whitespace escapes
     * - control characters < 0x20
     */
    private static String escapeJson(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"'  -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    /** Convenience: returns the parsed dimension as a "length estimate" (>=0). */
    private static int estimateEmbeddingLength(String body) {
        final int dim = extractEmbeddingDim(body);
        return Math.max(dim, 0);
    }

    /**
     * Extracts embedding dimensionality from a JSON array response without adding JSON dependencies.
     *
     * Supported shapes:
     *  - [  ...numbers...  ]       (flat array)
     *  - [[ ...numbers... ]]       (nested array; counts inner array elements)
     *
     * Returns 0 if parsing fails.
     *
     * Enterprise note:
     * - This is intentionally conservative and "dumb but deterministic".
     * - For production-grade schema-aware parsing, consider a JSON parser and a strict response contract.
     */
    private static int extractEmbeddingDim(String body) {
        if (body == null) return 0;
        final int n = body.length();
        if (n < 2) return 0;

        int i = 0;

        // Find first '['
        while (i < n && body.charAt(i) != '[') i++;
        if (i >= n) return 0;

        // If nested array, find second '['
        int j = i + 1;
        while (j < n && Character.isWhitespace(body.charAt(j))) j++;
        if (j < n && body.charAt(j) == '[') {
            i = j; // start counting within the inner array
        }

        // Now i points to '[' of the array we count
        i++; // move past '['

        boolean inNumber = false;
        int count = 0;

        for (; i < n; i++) {
            final char c = body.charAt(i);

            if (c == ']') {
                // End of array
                if (inNumber) {
                    count++;
                    inNumber = false;
                }
                return count;
            }

            // number characters: digits, sign, decimal point, exponent notation
            final boolean numberChar =
                    (c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E';

            if (numberChar) {
                inNumber = true;
                continue;
            }

            if (c == ',') {
                if (inNumber) {
                    count++;
                    inNumber = false;
                }
            }
        }

        return 0;
    }

    // ---------------------------------------------------------------------
    // Operational helper
    // ---------------------------------------------------------------------

    /**
     * Performs an active connectivity check to the configured endpoint.
     *
     * Notes:
     * - Uses HEAD to minimize payload overhead.
     * - Intended for startup readiness checks or operator-triggered diagnostics.
     *
     * Failure semantics:
     * - Throws IllegalStateException when unreachable.
     */
    public void verifyHealthy() {
        try {
            HttpRequest headReq = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(timeout)
                    .method("HEAD", HttpRequest.BodyPublishers.noBody())
                    .build();

            http.send(headReq, HttpResponse.BodyHandlers.discarding());
            log.info("HTTP Embedding target is reachable: {}", uri);

        } catch (Exception e) {
            log.error("HTTP Embedding target unreachable: {} - {}", uri, e.getMessage());
            throw new IllegalStateException("Embedding service is offline", e);
        }
    }
}
