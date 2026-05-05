/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.metrics;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * Prometheus Metrics Provider (Micrometer-backed).
 *
 * <p>Enterprise intent:
 * <ul>
 *   <li>Provider-agnostic surface via {@link MetricsRuntime} while using Micrometer internally.</li>
 *   <li>Dedicated lightweight HTTP endpoint for scraping: {@code /metrics}.</li>
 *   <li>Operational endpoints for probes: {@code /-/healthy} and {@code /-/ready}.</li>
 *   <li>Scrape responses are deterministic and parser-safe (Windows newline normalization, final newline).</li>
 *   <li>Defensive I/O: explicit Content-Length and connection-close semantics for easy troubleshooting.</li>
 * </ul>
 *
 * <p>Prometheus/Grafana credibility notes:
 * <ul>
 *   <li>Use counters for totals (e.g., {@code *_total}), timers/histograms for latency, gauges for current state.</li>
 *   <li>Prefer stable metric names and consistent units (e.g., {@code *_seconds}, {@code *_bytes}).</li>
 * </ul>
 *
 * <p>Threading model:
 * <ul>
 *   <li>Dedicated HTTP server with a small daemon executor.</li>
 *   <li>Handlers are synchronous and bounded; they must remain lightweight and non-blocking.</li>
 * </ul>
 *
 * <p>Runtime dependency note: the embedded endpoint uses the JDK {@code jdk.httpserver}
 * module ({@code com.sun.net.httpserver}). Native-image or custom-runtime deployments must
 * include that module, or replace this provider with an alternate embedded server adapter.
 */
public final class PrometheusMetricsProvider implements MetricsProvider {

    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsProvider.class);

    // Scrape + probe endpoints (Prometheus conventions use /-/healthy and /-/ready).
    private static final String PATH_METRICS = "/metrics";
    private static final String PATH_HEALTH  = "/-/healthy";
    private static final String PATH_READY   = "/-/ready";
    private static final String METRIC_GAUGE_INVALID_TOTAL =
            "streamkernel_metrics_gauge_invalid_total";
    private static final String METRIC_SCRAPE_TOTAL =
            "streamkernel_metrics_scrape_total";
    private static final String METRIC_SCRAPE_FAILURES_TOTAL =
            "streamkernel_metrics_scrape_failures_total";
    private static final String METRIC_SCRAPE_DURATION_MS =
            "streamkernel_metrics_scrape_duration_ms";
    private static final String METRIC_SCRAPE_LAST_BODY_BYTES =
            "streamkernel_metrics_scrape_last_body_bytes";
    private static final String METRIC_AUTH_FAILURES_TOTAL =
            "streamkernel_metrics_auth_failures_total";
    private static final String METRIC_IP_DENIED_TOTAL =
            "streamkernel_metrics_ip_denied_total";

    // Scrape hardening:
    // - remove standalone '#' lines if they ever show up (can break some parsers)
    private static final Pattern BARE_HASH_LINE = Pattern.compile("(?m)^#\\s*$\\R?");
    // - fix rare legacy artifacts like ",}" (tolerate whitespace)
    private static final Pattern TRAILING_COMMA_LABEL = Pattern.compile(",\\s*}");

    // Optional: strictly limit allowed HTTP methods for scrape/probe.
    private static final boolean ALLOW_HEAD = true;

    // Default port if config is missing/invalid.
    private static final int DEFAULT_PORT = 9090;
    private static final int HTTP_EXECUTOR_THREADS = 2;
    @Override
    public String id() {
        return "PROMETHEUS";
    }

    @Override
    public boolean matches(String requestedProviderId) {
        final String req = normalizeUpper(requestedProviderId);
        return "PROMETHEUS".equals(req);
    }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        if (s == null || !matches(s.providerId)) return null;

        final int port = clampPort(s.prometheusPort);

        // Micrometer Prometheus registry (in-process, no sidecars required).
        final PrometheusMeterRegistry reg = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        // Apply standard enterprise tags (env, region, service, version, etc).
        MetricsUtil.applyCommonTags(reg, s);

        // Bind standard JVM binders — these populate the JVM & Resources section of Grafana.
        // Each binder auto-registers metrics on the registry at bind time.
        final JvmGcMetrics gcMetrics = new JvmGcMetrics();
        final JvmHeapPressureMetrics heapPressureMetrics = new JvmHeapPressureMetrics();
        new JvmMemoryMetrics().bindTo(reg);      // jvm_memory_used_bytes, jvm_memory_max_bytes, etc.
        new JvmThreadMetrics().bindTo(reg);      // jvm_threads_live_threads, jvm_threads_daemon_threads, etc.
        gcMetrics.bindTo(reg);                   // jvm_gc_pause_seconds_count/sum, jvm_gc_live_data_size_bytes, etc.
        heapPressureMetrics.bindTo(reg);         // jvm_gc_overhead_percent, jvm_memory_usage_after_gc_percent
        new ClassLoaderMetrics().bindTo(reg);    // jvm_classes_loaded_classes, jvm_classes_unloaded_classes

        log.info("JVM binders registered (memory, threads, GC, heap pressure, classloader).");

        // Common operational metric: process start time (seconds since epoch).
        // This is frequently useful in Grafana dashboards for "uptime-ish" visualizations.
        final AtomicLong processStartEpochSeconds = new AtomicLong(Instant.now().getEpochSecond());
        Gauge.builder("streamkernel_process_start_time_seconds", processStartEpochSeconds, AtomicLong::doubleValue)
                .description("Process start time in seconds since epoch.")
                .register(reg);

        final PrometheusRuntime runtime = new PrometheusRuntime(
                reg,
                gcMetrics,
                heapPressureMetrics,
                processStartEpochSeconds,
                s.prometheusSnapshotPath
        );
        final EndpointAccess endpointAccess = EndpointAccess.from(s, runtime);
        logEndpointSecurityPosture(s, endpointAccess);

        // Start HTTP server for metrics + probes. Readiness is a first-class operational signal.
        final ServerHandle handle = startServer(reg, port, s, runtime, endpointAccess);
        runtime.attachServer(handle);
        runtime.installShutdownHook();

        log.info("Prometheus Metrics Active (scheme={}, bindAddress={}, port={}, metricsPath={}, " +
                        "healthPath={}, readyPath={}, auth={}, probesProtected={}, remoteAllowlist={})",
                s.prometheusTlsEnabled ? "https" : "http",
                s.prometheusBindAddress,
                port,
                PATH_METRICS,
                PATH_HEALTH,
                PATH_READY,
                endpointAccess.enabled(),
                endpointAccess.protectProbes(),
                endpointAccess.hasRemoteAllowlist());

        return runtime;
    }

    private static ServerHandle startServer(PrometheusMeterRegistry registry,
                                            int port,
                                            MetricsSettings settings,
                                            PrometheusRuntime runtime,
                                            EndpointAccess endpointAccess) {
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(endpointAccess, "endpointAccess");

        try {
            final String host = normalizeBindAddress(settings.prometheusBindAddress);
            final HttpServer server = createServer(new InetSocketAddress(host, port), settings);

            // Keep the server lightweight and deterministic.
            // Bound the endpoint to a small fixed pool; one scrape and one probe can overlap
            // without opening the door to unbounded thread creation under scrape storms.
            final ExecutorService executor = Executors.newFixedThreadPool(
                    HTTP_EXECUTOR_THREADS,
                    new NamedDaemonThreadFactory("sk-metrics-http-")
            );
            server.setExecutor(executor);

            final ServerHandle handle = new ServerHandle(server, executor);

            // Probes
            server.createContext(PATH_HEALTH, ex -> handleProbe(ex, 200, "ok\n", endpointAccess));
            server.createContext(PATH_READY,  ex -> handleReady(ex, handle, endpointAccess));

            // Scrape
            server.createContext(PATH_METRICS, ex -> handleScrape(ex, registry, runtime, endpointAccess));

            server.start();
            return handle;
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Prometheus metrics server on port " + port, e);
        }
    }

    private static HttpServer createServer(InetSocketAddress address, MetricsSettings settings) throws Exception {
        if (!settings.prometheusTlsEnabled) {
            return HttpServer.create(address, 0);
        }

        final HttpsServer server = HttpsServer.create(address, 0);
        final SSLContext sslContext = buildSslContext(settings);
        final String[] protocols = parseCsvList(settings.prometheusTlsProtocols).toArray(String[]::new);
        final String clientAuth = normalizeUpper(settings.prometheusTlsClientAuth);

        server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
            @Override
            public void configure(HttpsParameters params) {
                final SSLParameters sslParameters = getSSLContext().getDefaultSSLParameters();
                if (protocols.length > 0) {
                    sslParameters.setProtocols(protocols);
                }
                if ("NEED".equals(clientAuth) || "REQUIRE".equals(clientAuth) || "REQUIRED".equals(clientAuth)) {
                    sslParameters.setNeedClientAuth(true);
                } else if ("WANT".equals(clientAuth) || "OPTIONAL".equals(clientAuth)) {
                    sslParameters.setWantClientAuth(true);
                }
                params.setSSLParameters(sslParameters);
            }
        });
        return server;
    }

    private static SSLContext buildSslContext(MetricsSettings settings) throws Exception {
        final String keyStorePath = normalizeMetricName(settings.prometheusTlsKeyStorePath);
        if (keyStorePath == null) {
            throw new IllegalStateException(
                    "metrics.prometheus.tls.enabled=true requires metrics.prometheus.tls.keystore.path");
        }

        final KeyStore keyStore = loadKeyStore(
                keyStorePath,
                settings.prometheusTlsKeyStoreType,
                settings.prometheusTlsKeyStorePassword
        );
        final KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        // Best-effort heap hygiene only: MetricsSettings stores passwords as Strings because
        // they arrive from config/env APIs, and KeyManagerFactory may copy the char[] internally.
        final char[] keyPassword = toPassword(settings.prometheusTlsKeyPassword);
        try {
            keyManagerFactory.init(keyStore, keyPassword);
        } finally {
            wipe(keyPassword);
        }

        TrustManager[] trustManagers = null;
        final String trustStorePath = normalizeMetricName(settings.prometheusTlsTrustStorePath);
        if (trustStorePath != null) {
            final KeyStore trustStore = loadKeyStore(
                    trustStorePath,
                    settings.prometheusTlsTrustStoreType,
                    settings.prometheusTlsTrustStorePassword
            );
            final TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            trustManagers = trustManagerFactory.getTrustManagers();
        }

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        final KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
        sslContext.init(keyManagers, trustManagers, new SecureRandom());
        return sslContext;
    }

    private static KeyStore loadKeyStore(String path, String type, String password) throws Exception {
        final String keyStoreType = firstNonBlank(type, KeyStore.getDefaultType());
        final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        final char[] pass = toPassword(password);
        try (var input = Files.newInputStream(Path.of(path).toAbsolutePath().normalize())) {
            keyStore.load(input, pass);
            return keyStore;
        } finally {
            wipe(pass);
        }
    }

    private static void handleReady(HttpExchange exchange, ServerHandle handle, EndpointAccess endpointAccess) {
        if (!isAllowedMethod(exchange)) {
            respond(exchange, 405, "method not allowed\n", "text/plain; charset=utf-8");
            return;
        }
        if (!endpointAccess.authorize(exchange, true)) {
            endpointAccess.reject(exchange);
            return;
        }

        if (handle.isReady()) {
            respond(exchange, 200, "ready\n", "text/plain; charset=utf-8");
        } else {
            // 503 is the conventional not-ready signal.
            respond(exchange, 503, "not ready\n", "text/plain; charset=utf-8");
        }
    }

    private static void handleProbe(HttpExchange exchange, int status, String body, EndpointAccess endpointAccess) {
        if (!isAllowedMethod(exchange)) {
            respond(exchange, 405, "method not allowed\n", "text/plain; charset=utf-8");
            return;
        }
        if (!endpointAccess.authorize(exchange, true)) {
            endpointAccess.reject(exchange);
            return;
        }
        respond(exchange, status, body, "text/plain; charset=utf-8");
    }

    private static void handleScrape(HttpExchange exchange,
                                     PrometheusMeterRegistry registry,
                                     PrometheusRuntime runtime,
                                     EndpointAccess endpointAccess) {
        if (!isAllowedMethod(exchange)) {
            respond(exchange, 405, "method not allowed\n", "text/plain; charset=utf-8");
            return;
        }
        if (!endpointAccess.authorize(exchange, false)) {
            endpointAccess.reject(exchange);
            return;
        }

        final long startedAt = System.nanoTime();
        boolean headersSent = false;
        long responseBytes = 0L;
        try {
            final String body = normalizedScrape(registry);

            final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            final long contentLength = bytes.length;
            responseBytes = contentLength;

            final Headers h = exchange.getResponseHeaders();
            h.set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");

            // Force non-chunked: explicit Content-Length and connection close.
            // This reduces variance and simplifies troubleshooting with curl.
            h.set("Content-Length", Long.toString(contentLength));
            h.set("Connection", "close");

            // IMPORTANT: sendResponseHeaders length must match Content-Length above.
            exchange.sendResponseHeaders(200, contentLength);
            headersSent = true;

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
                os.flush();
            }
            runtime.recordScrapeSuccess(responseBytes, System.nanoTime() - startedAt);
        } catch (Throwable t) {
            runtime.recordScrapeFailure();
            log.warn("Prometheus scrape failed", t);
            if (!headersSent) {
                try { exchange.sendResponseHeaders(500, -1); } catch (Throwable ignored) {}
            }
        } finally {
            try { exchange.close(); } catch (Exception ignored) {}
        }
    }

    private static boolean isAllowedMethod(HttpExchange exchange) {
        final String m = exchange.getRequestMethod();
        if (m == null) return false;

        final String mm = m.trim().toUpperCase(Locale.ROOT);
        if ("GET".equals(mm)) return true;
        return ALLOW_HEAD && "HEAD".equals(mm);
    }

    private static void respond(HttpExchange exchange, int status, String body, String contentType) {
        boolean headersSent = false;
        try {
            final String b = (body == null) ? "" : body;
            final byte[] bytes = b.getBytes(StandardCharsets.UTF_8);
            final long contentLength = bytes.length;

            final Headers h = exchange.getResponseHeaders();
            h.set("Content-Type", contentType);
            h.set("Content-Length", Long.toString(contentLength));
            h.set("Connection", "close");

            exchange.sendResponseHeaders(status, contentLength);
            headersSent = true;
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
                os.flush();
            }
        } catch (Throwable t) {
            log.warn("Prometheus response write failed: {}", t.toString());
            if (!headersSent) {
                try { exchange.sendResponseHeaders(500, -1); } catch (Throwable ignored) {}
            }
        } finally {
            try { exchange.close(); } catch (Exception ignored) {}
        }
    }

    private static int clampPort(int port) {
        if (port <= 0) return DEFAULT_PORT;
        if (port > 65_535) return 65_535;
        return port;
    }

    /**
     * Metric-name normalization:
     * - trims
     * - returns null for empty
     *
     * <p>We intentionally do NOT "fix up" names (e.g., spaces -> underscores) because:
     * <ul>
     *   <li>it hides caller mistakes</li>
     *   <li>it can create silent name collisions</li>
     * </ul>
     *
     * If you later want enforcement, add a strict validation mode that logs/rejects nonconforming names.
     */
    private static String normalizeMetricName(String name) {
        if (name == null) return null;
        int start = 0;
        int end = name.length();
        while (start < end && Character.isWhitespace(name.charAt(start))) {
            start++;
        }
        while (end > start && Character.isWhitespace(name.charAt(end - 1))) {
            end--;
        }
        if (start == end) return null;
        return (start == 0 && end == name.length()) ? name : name.substring(start, end);
    }

    /**
     * Best-effort snapshot only.
     *
     * Hard JVM termination (for example SIGKILL / process kill) can skip both close() and the
     * shutdown hook, so operators should treat this as an opportunistic capture rather than a
     * guaranteed final scrape artifact.
     */
    private static void writeSnapshotIfConfigured(PrometheusMeterRegistry registry,
                                                  AtomicBoolean snapshotWritten,
                                                  String snapshotPath) {
        if (registry == null || snapshotWritten == null) return;
        if (!snapshotWritten.compareAndSet(false, true)) return;
        if (snapshotPath == null || snapshotPath.isBlank()) return;

        try {
            final Path path = Path.of(snapshotPath.trim()).toAbsolutePath().normalize();
            final Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, normalizedScrape(registry), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("Failed to write Prometheus snapshot to {}: {}", snapshotPath, e.toString());
        }
    }

    private static String normalizedScrape(PrometheusMeterRegistry registry) {
        String body = registry.scrape();

        if (body.indexOf('\r') >= 0) {
            body = body.replace("\r\n", "\n");
        }
        if (body.contains("#\n")) {
            body = BARE_HASH_LINE.matcher(body).replaceAll("");
        }
        if (body.indexOf(',') >= 0 && body.indexOf('}') >= 0) {
            body = TRAILING_COMMA_LABEL.matcher(body).replaceAll("}");
        }

        if (!body.endsWith("\n")) body += "\n";
        return body;
    }

    private static String normalizeBindAddress(String bindAddress) {
        final String normalized = normalizeMetricName(bindAddress);
        return normalized == null ? "0.0.0.0" : normalized;
    }

    private static void logEndpointSecurityPosture(MetricsSettings settings, EndpointAccess endpointAccess) {
        final String bindAddress = normalizeBindAddress(settings.prometheusBindAddress);
        final boolean wildcardBind = "0.0.0.0".equals(bindAddress) || "::".equals(bindAddress);
        if (wildcardBind && !endpointAccess.enabled() && !settings.prometheusTlsEnabled) {
            log.warn("Prometheus metrics endpoint is bound to {} without auth or TLS. " +
                    "Use metrics.prometheus.auth.* and/or metrics.prometheus.tls.* for enterprise deployments.",
                    bindAddress);
        }
        if (endpointAccess.enabled()) {
            log.info("Prometheus scrape auth enabled. bearerConfigured={} headerConfigured={} protectProbes={}",
                    settings.prometheusAuthBearerToken != null && !settings.prometheusAuthBearerToken.isBlank(),
                    settings.prometheusAuthHeaderValue != null && !settings.prometheusAuthHeaderValue.isBlank(),
                    endpointAccess.protectProbes());
        }
        if (settings.prometheusTlsEnabled) {
            log.info("Prometheus TLS enabled. clientAuth={} protocols={}",
                    settings.prometheusTlsClientAuth, settings.prometheusTlsProtocols);
        }
    }

    private static String normalizeUpper(String s) {
        if (s == null) return null;
        final String t = s.trim();
        if (t.isEmpty()) return null;
        return t.toUpperCase(Locale.ROOT);
    }

    private static String firstNonBlank(String... candidates) {
        if (candidates == null) return null;
        for (String candidate : candidates) {
            final String normalized = normalizeMetricName(candidate);
            if (normalized != null) {
                return normalized;
            }
        }
        return null;
    }

    private static Set<String> parseCsv(String csv) {
        return new HashSet<>(splitCsv(csv));
    }

    private static List<String> parseCsvList(String csv) {
        return splitCsv(csv);
    }

    private static List<String> splitCsv(String csv) {
        final List<String> values = new ArrayList<>();
        if (csv == null || csv.isBlank()) {
            return values;
        }
        for (String part : csv.split(",")) {
            final String value = normalizeMetricName(part);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }

    private static char[] toPassword(String password) {
        return password == null ? null : password.toCharArray();
    }

    private static void wipe(char[] chars) {
        if (chars != null) {
            Arrays.fill(chars, '\0');
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    private static final class EndpointAccess {
        private final PrometheusRuntime runtime;
        private final Set<String> allowedRemoteAddresses;
        private final boolean authEnabled;
        private final boolean protectProbes;
        private final String bearerToken;
        private final String headerName;
        private final String headerValue;

        private EndpointAccess(PrometheusRuntime runtime,
                               Set<String> allowedRemoteAddresses,
                               boolean authEnabled,
                               boolean protectProbes,
                               String bearerToken,
                               String headerName,
                               String headerValue) {
            this.runtime = Objects.requireNonNull(runtime, "runtime");
            this.allowedRemoteAddresses = Set.copyOf(allowedRemoteAddresses);
            this.authEnabled = authEnabled;
            this.protectProbes = protectProbes;
            this.bearerToken = bearerToken;
            this.headerName = headerName;
            this.headerValue = headerValue;
        }

        static EndpointAccess from(MetricsSettings settings, PrometheusRuntime runtime) {
            final String bearer = normalizeMetricName(settings.prometheusAuthBearerToken);
            final String headerName = normalizeMetricName(settings.prometheusAuthHeaderName);
            final String headerValue = normalizeMetricName(settings.prometheusAuthHeaderValue);
            final boolean hasSecret = bearer != null || (headerName != null && headerValue != null);
            if (settings.prometheusAuthEnabled && !hasSecret) {
                throw new IllegalStateException(
                        "metrics.prometheus.auth.enabled=true requires a bearer token or header secret");
            }
            return new EndpointAccess(
                    runtime,
                    parseCsv(settings.prometheusAllowedRemoteAddresses),
                    settings.prometheusAuthEnabled,
                    settings.prometheusAuthProtectProbes,
                    bearer,
                    headerName,
                    headerValue
            );
        }

        boolean enabled() {
            return authEnabled;
        }

        boolean protectProbes() {
            return protectProbes;
        }

        boolean hasRemoteAllowlist() {
            return !allowedRemoteAddresses.isEmpty();
        }

        boolean authorize(HttpExchange exchange, boolean probe) {
            if (!remoteAddressAllowed(exchange)) {
                runtime.recordIpDenied();
                return false;
            }
            if (!authEnabled || (probe && !protectProbes)) {
                return true;
            }
            if (bearerToken != null && bearerMatches(exchange)) {
                return true;
            }
            if (headerName != null && headerValue != null && headerMatches(exchange)) {
                return true;
            }
            runtime.recordAuthFailure();
            return false;
        }

        void reject(HttpExchange exchange) {
            if (bearerToken != null) {
                exchange.getResponseHeaders().set(
                        "WWW-Authenticate",
                        "Bearer realm=\"StreamKernel Prometheus\"");
            }
            respond(exchange, 401, "unauthorized\n", "text/plain; charset=utf-8");
        }

        private boolean remoteAddressAllowed(HttpExchange exchange) {
            if (allowedRemoteAddresses.isEmpty()) {
                return true;
            }
            final InetSocketAddress remote = exchange.getRemoteAddress();
            if (remote == null || remote.getAddress() == null) {
                return false;
            }
            final String hostAddress = remote.getAddress().getHostAddress();
            final String hostString = remote.getHostString();
            return allowedRemoteAddresses.contains(hostAddress) || allowedRemoteAddresses.contains(hostString);
        }

        private boolean bearerMatches(HttpExchange exchange) {
            final String raw = exchange.getRequestHeaders().getFirst("Authorization");
            // Strip only HTTP header framing whitespace; the token value itself is compared as supplied.
            final String normalized = raw == null ? null : raw.strip();
            if (normalized == null) {
                return false;
            }
            final String prefix = "Bearer ";
            if (!normalized.regionMatches(true, 0, prefix, 0, prefix.length())) {
                return false;
            }
            return constantTimeEquals(normalized.substring(prefix.length()), bearerToken);
        }

        private boolean headerMatches(HttpExchange exchange) {
            for (String candidate : exchange.getRequestHeaders().getOrDefault(headerName, List.of())) {
                if (constantTimeEquals(normalizeMetricName(candidate), headerValue)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean constantTimeEquals(String left, String right) {
            if (left == null || right == null) {
                return false;
            }
            final byte[] leftHash = sha256(left);
            final byte[] rightHash = sha256(right);
            return MessageDigest.isEqual(leftHash, rightHash);
        }

        private static byte[] sha256(String value) {
            try {
                return MessageDigest.getInstance("SHA-256")
                        .digest(value.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new IllegalStateException("SHA-256 unavailable", e);
            }
        }
    }

    private static final class PrometheusRuntime implements MetricsRuntime {
        private final PrometheusMeterRegistry registry;
        private final JvmGcMetrics gcMetrics;
        private final JvmHeapPressureMetrics heapPressureMetrics;
        @SuppressWarnings("unused")
        private final AtomicLong processStartGaugeRef;
        private final String snapshotPath;
        private final AtomicBoolean snapshotWritten = new AtomicBoolean(false);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, GaugeSlot> gaugeState = new ConcurrentHashMap<>();

        private volatile ServerHandle handle;
        private volatile Thread shutdownHook;

        private PrometheusRuntime(PrometheusMeterRegistry registry,
                                  JvmGcMetrics gcMetrics,
                                  JvmHeapPressureMetrics heapPressureMetrics,
                                  AtomicLong processStartGaugeRef,
                                  String snapshotPath) {
            this.registry = Objects.requireNonNull(registry, "registry");
            this.gcMetrics = gcMetrics;
            this.heapPressureMetrics = heapPressureMetrics;
            this.processStartGaugeRef = Objects.requireNonNull(processStartGaugeRef, "processStartGaugeRef");
            this.snapshotPath = snapshotPath;
        }

        private void attachServer(ServerHandle handle) {
            this.handle = Objects.requireNonNull(handle, "handle");
            this.handle.setReady(true);
        }

        private void installShutdownHook() {
            final Thread hook = new Thread(this::shutdown, "sk-metrics-shutdown");
            Runtime.getRuntime().addShutdownHook(hook);
            this.shutdownHook = hook;
        }

        private void shutdown() {
            if (!closed.compareAndSet(false, true)) return;

            try { writeSnapshotIfConfigured(registry, snapshotWritten, snapshotPath); } catch (Exception ignored) {}
            try {
                final ServerHandle current = handle;
                if (current != null) {
                    current.setReady(false);
                    current.close();
                }
            } catch (Exception ignored) {}
            try { closeQuietly(gcMetrics); } catch (Exception ignored) {}
            try { closeQuietly(heapPressureMetrics); } catch (Exception ignored) {}
            counters.clear();
            timers.clear();
            gaugeState.clear();
            try { registry.close(); } catch (Exception ignored) {}
        }

        private Counter counterRef(String name) {
            // First registration performs Micrometer synchronization; steady-state calls hit this cache.
            return counters.computeIfAbsent(name, n ->
                    Counter.builder(n).register(registry));
        }

        private Timer timerRef(String name) {
            return timers.computeIfAbsent(name, n ->
                    Timer.builder(n).register(registry));
        }

        private GaugeSlot registerGaugeSlot(String name) {
            final AtomicDouble state = new AtomicDouble(0.0);
            try {
                Gauge.builder(name, state, AtomicDouble::get)
                        .strongReference(true)
                        .description("StreamKernel gauge (set-style).")
                        .register(registry);
                return GaugeSlot.writable(state);
            } catch (RuntimeException e) {
                final Meter existing = registry.find(name).meter();
                if (existing != null) {
                    log.warn("Gauge '{}' conflicts with existing meter type '{}'; existing meter remains scrapeable, " +
                            "set-style gauge updates will be ignored.", name, existing.getId().getType());
                } else {
                    log.warn("Unable to register gauge '{}'; updates will be ignored: {}", name, e.toString());
                }
                return GaugeSlot.unwritable();
            }
        }

        private void recordScrapeSuccess(long responseBytes, long durationNanos) {
            if (closed.get()) return;
            counterRef(METRIC_SCRAPE_TOTAL).increment();
            timer(METRIC_SCRAPE_DURATION_MS, TimeUnit.NANOSECONDS.toMillis(Math.max(0L, durationNanos)));
            gauge(METRIC_SCRAPE_LAST_BODY_BYTES, responseBytes);
        }

        private void recordScrapeFailure() {
            if (closed.get()) return;
            counterRef(METRIC_SCRAPE_FAILURES_TOTAL).increment();
            gauge(METRIC_SCRAPE_LAST_BODY_BYTES, 0L);
        }

        private void recordAuthFailure() {
            if (closed.get()) return;
            counterRef(METRIC_AUTH_FAILURES_TOTAL).increment();
        }

        private void recordIpDenied() {
            if (closed.get()) return;
            counterRef(METRIC_IP_DENIED_TOTAL).increment();
        }

        @Override
        public Object registry() {
            return registry;
        }

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public String type() {
            return "PROMETHEUS";
        }

        @Override
        public void counter(String name, double increment) {
            if (closed.get()) return;
            final String n = normalizeMetricName(name);
            if (n == null) return;

            final Counter counter = counterRef(n);
            if (increment > 0d) {
                counter.increment(increment);
            } else if (increment < 0d) {
                log.warn("Ignoring negative counter increment for {}: {}", n, increment);
            }
        }

        @Override
        public void timer(String name, long durationMillis) {
            if (closed.get()) return;
            final String n = normalizeMetricName(name);
            if (n == null) return;

            final long recorded = Math.max(0L, durationMillis);
            timerRef(n).record(recorded, TimeUnit.MILLISECONDS);
        }

        @Override
        public void gauge(String name, double value) {
            if (closed.get()) return;
            final String n = normalizeMetricName(name);
            if (n == null) return;

            if (!Double.isFinite(value)) {
                counterRef(METRIC_GAUGE_INVALID_TOTAL).increment();
                log.debug("Ignoring non-finite gauge value for {}: {}", n, value);
                return;
            }

            final GaugeSlot slot = gaugeState.computeIfAbsent(n, this::registerGaugeSlot);
            if (slot.writable()) {
                slot.state().set(value);
            }
        }

        @Override
        public void gauge(String name, long value) {
            // Prometheus gauge samples are double-valued; realistic scrape-size/count gauges
            // remain exactly represented after this conversion.
            gauge(name, (double) value);
        }

        @Override
        public double counterValue(String name) {
            final String n = normalizeMetricName(name);
            if (n == null) return Double.NaN;
            try {
                final Counter counter = registry.find(n).counter();
                return counter == null ? Double.NaN : counter.count();
            } catch (Exception ignored) {
                return Double.NaN;
            }
        }

        @Override
        public double gaugeValue(String name) {
            final String n = normalizeMetricName(name);
            if (n == null) return Double.NaN;
            final GaugeSlot slot = gaugeState.get(n);
            if (slot != null && slot.writable()) {
                return slot.state().get();
            }
            try {
                final Meter meter = registry.find(n).meter();
                if (meter == null) return Double.NaN;
                for (Measurement measurement : meter.measure()) {
                    if (measurement != null) {
                        return measurement.getValue();
                    }
                }
            } catch (Exception ignored) {
            }
            return Double.NaN;
        }

        @Override
        public TimerSnapshot timerSnapshot(String name) {
            final String n = normalizeMetricName(name);
            if (n == null) return TimerSnapshot.NaN;
            try {
                final Timer timer = registry.find(n).timer();
                if (timer == null) return TimerSnapshot.NaN;
                final io.micrometer.core.instrument.distribution.HistogramSnapshot snapshot =
                        timer.takeSnapshot();
                double p50 = Double.NaN;
                double p99 = Double.NaN;
                for (io.micrometer.core.instrument.distribution.ValueAtPercentile percentile
                        : snapshot.percentileValues()) {
                    if (percentile == null) {
                        continue;
                    }
                    if (Math.abs(percentile.percentile() - 0.50d) < 0.001d) {
                        p50 = percentile.value(TimeUnit.MILLISECONDS);
                    } else if (Math.abs(percentile.percentile() - 0.99d) < 0.001d) {
                        p99 = percentile.value(TimeUnit.MILLISECONDS);
                    }
                }
                return new TimerSnapshot(
                        p50,
                        p99,
                        snapshot.max(TimeUnit.MILLISECONDS)
                );
            } catch (Exception ignored) {
                return TimerSnapshot.NaN;
            }
        }

        @Override
        public void close() {
            final Thread hook = shutdownHook;
            if (hook != null) {
                try { Runtime.getRuntime().removeShutdownHook(hook); } catch (Exception ignored) {}
            }
            shutdown();
        }
    }

    /**
     * Handle for server lifecycle + readiness signaling.
     */
    private static final class ServerHandle implements AutoCloseable {
        private final HttpServer server;
        private final ExecutorService executor;
        private final AtomicBoolean ready = new AtomicBoolean(false);

        private ServerHandle(HttpServer server, ExecutorService executor) {
            this.server = Objects.requireNonNull(server, "server");
            this.executor = Objects.requireNonNull(executor, "executor");
        }

        boolean isReady() {
            return ready.get();
        }

        void setReady(boolean ready) {
            this.ready.set(ready);
        }

        @Override
        public void close() {
            // Give in-flight scrapes a brief chance to finish during rolling restarts.
            try { server.stop(1); } catch (Exception ignored) {}

            try {
                executor.shutdown();
                boolean terminated = executor.awaitTermination(100, TimeUnit.MILLISECONDS);
                if (!terminated) {
                    executor.shutdownNow();
                    terminated = executor.awaitTermination(100, TimeUnit.MILLISECONDS);
                }
                if (!terminated) {
                    log.warn("Metrics HTTP executor did not terminate within timeout");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while shutting down metrics HTTP executor");
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Strongly-referenced mutable double, safe for Micrometer Gauge polling.
     *
     * <p>Implementation detail: stores IEEE-754 bits in an AtomicLong so updates are atomic and lock-free.</p>
     */
    private static final class AtomicDouble extends Number {
        private final AtomicLong bits;

        AtomicDouble(double initialValue) {
            this.bits = new AtomicLong(Double.doubleToRawLongBits(initialValue));
        }

        void set(double newValue) {
            bits.set(Double.doubleToRawLongBits(newValue));
        }

        double get() {
            return Double.longBitsToDouble(bits.get());
        }

        @Override public int intValue() { return (int) get(); }
        @Override public long longValue() { return (long) get(); }
        @Override public float floatValue() { return (float) get(); }
        @Override public double doubleValue() { return get(); }
    }

    private record GaugeSlot(AtomicDouble state, boolean writable) {
        private static final AtomicDouble UNWRITABLE_STATE = new AtomicDouble(Double.NaN);

        private static GaugeSlot writable(AtomicDouble state) {
            return new GaugeSlot(Objects.requireNonNull(state, "state"), true);
        }

        private static GaugeSlot unwritable() {
            return new GaugeSlot(UNWRITABLE_STATE, false);
        }
    }

    /**
     * Named daemon threads for the metrics HTTP server.
     *
     * <p>Daemon threads ensure metrics cannot block JVM shutdown.</p>
     */
    private static final class NamedDaemonThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicLong seq = new AtomicLong(0);

        NamedDaemonThreadFactory(String prefix) {
            this.prefix = (prefix == null || prefix.isBlank()) ? "sk-" : prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(r, prefix + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
