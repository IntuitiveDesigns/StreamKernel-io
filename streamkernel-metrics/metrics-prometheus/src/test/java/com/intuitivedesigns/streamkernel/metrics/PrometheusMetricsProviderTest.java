/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.metrics;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.ServerSocket;
import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrometheusMetricsProviderTest {
    private static final Pattern METRIC_LINE =
            Pattern.compile("(?m)^(%s)(?:\\{[^}]*})?\\s+([^\\s]+)$");

    @Test
    void zeroIncrementCounterStillRegistersMeter() throws Exception {
        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(prometheusSettings());

        try {
            runtime.counter("streamkernel_test_zero_register", 0d);

            final String scrape = ((io.micrometer.prometheus.PrometheusMeterRegistry) runtime.registry()).scrape();
            assertTrue(scrape.contains("streamkernel_test_zero_register"));
        } finally {
            runtime.close();
        }
    }

    @Test
    void noArgCounterDefaultIncrementsByOne() throws Exception {
        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(prometheusSettings());

        try {
            runtime.counter("streamkernel_test_default_counter");

            assertEquals(1.0d, runtime.counterValue("streamkernel_test_default_counter"));
        } finally {
            runtime.close();
        }
    }

    @Test
    void longGaugeOverloadRecordsByteStyleValues() throws Exception {
        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(prometheusSettings());

        try {
            runtime.gauge("streamkernel_test_long_gauge", 123L);

            assertEquals(123.0d, runtime.gaugeValue("streamkernel_test_long_gauge"));
        } finally {
            runtime.close();
        }
    }

    @Test
    void gaugeStateIsIsolatedPerRuntimeInstance() throws Exception {
        final MetricsRuntime first = new PrometheusMetricsProvider().create(prometheusSettings());
        final MetricsRuntime second = new PrometheusMetricsProvider().create(prometheusSettings());

        try {
            first.gauge("streamkernel_test_isolated_gauge", 1.0d);
            second.gauge("streamkernel_test_isolated_gauge", 2.0d);

            final String firstScrape =
                    ((io.micrometer.prometheus.PrometheusMeterRegistry) first.registry()).scrape();
            final String secondScrape =
                    ((io.micrometer.prometheus.PrometheusMeterRegistry) second.registry()).scrape();

            assertEquals(1.0d, metricValue(firstScrape, "streamkernel_test_isolated_gauge"));
            assertEquals(2.0d, metricValue(secondScrape, "streamkernel_test_isolated_gauge"));
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void timerSnapshotReportsFiniteMaxWithoutHotPathPercentiles() throws Exception {
        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(prometheusSettings());

        try {
            runtime.timer("streamkernel_test_latency_ms", 10);
            runtime.timer("streamkernel_test_latency_ms", 20);
            runtime.timer("streamkernel_test_latency_ms", 30);
            runtime.timer("streamkernel_test_latency_ms", 40);
            runtime.timer("streamkernel_test_latency_ms", 50);

            final MetricsRuntime.TimerSnapshot snapshot =
                    runtime.timerSnapshot("streamkernel_test_latency_ms");

            assertTrue(Double.isNaN(snapshot.p50Millis()));
            assertTrue(Double.isNaN(snapshot.p99Millis()));
            assertFalse(Double.isNaN(snapshot.maxMillis()));
            assertTrue(snapshot.maxMillis() >= 50.0d);
        } finally {
            runtime.close();
        }
    }

    @Test
    void nonFiniteGaugeValuesAreCounted() throws Exception {
        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(prometheusSettings());

        try {
            runtime.gauge("streamkernel_test_non_finite", Double.NaN);
            runtime.gauge("streamkernel_test_non_finite", Double.POSITIVE_INFINITY);

            assertEquals(2d, runtime.counterValue("streamkernel_metrics_gauge_invalid_total"));
        } finally {
            runtime.close();
        }
    }

    @Test
    void closeWritesSnapshotFromMetricsSettings() throws Exception {
        final Path snapshot = Files.createTempFile("streamkernel-prometheus-", ".prom");
        Files.deleteIfExists(snapshot);

        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(
                prometheusSettings(props -> props.setProperty(
                        "metrics.prometheus.snapshot.path",
                        snapshot.toAbsolutePath().toString()
                ))
        );

        runtime.counter("streamkernel_test_snapshot_counter", 3d);
        runtime.close();

        final String body = Files.readString(snapshot);
        assertTrue(body.contains("streamkernel_test_snapshot_counter"));
        assertTrue(body.contains(" 3.0"));
    }

    @Test
    void metricsEndpointRequiresConfiguredBearerToken() throws Exception {
        final MetricsSettings settings = prometheusSettings(props -> {
            props.setProperty("metrics.prometheus.auth.enabled", "true");
            props.setProperty("metrics.prometheus.auth.bearer.token", "top-secret-token");
        });
        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(settings);

        try {
            final HttpResponse<String> anonymous = get(settings, "/metrics", null, null);
            assertEquals(401, anonymous.statusCode());
            assertEquals(1.0d, runtime.counterValue("streamkernel_metrics_auth_failures_total"));

            final HttpResponse<String> authenticated = get(
                    settings,
                    "/metrics",
                    "Authorization",
                    "Bearer top-secret-token");
            assertEquals(200, authenticated.statusCode());
            assertTrue(authenticated.body().contains("streamkernel_process_start_time_seconds"));

            final HttpResponse<String> health = get(settings, "/-/healthy", null, null);
            assertEquals(200, health.statusCode());
        } finally {
            runtime.close();
        }
    }

    @Test
    void remoteAllowlistRejectionsHaveSeparateCounter() throws Exception {
        final MetricsSettings settings = prometheusSettings(props ->
                props.setProperty("metrics.prometheus.allowed.remote.addresses", "10.10.10.10"));
        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(settings);

        try {
            final HttpResponse<String> denied = get(settings, "/metrics", null, null);

            assertEquals(401, denied.statusCode());
            assertEquals(1.0d, runtime.counterValue("streamkernel_metrics_ip_denied_total"));
            assertTrue(Double.isNaN(runtime.counterValue("streamkernel_metrics_auth_failures_total")));
        } finally {
            runtime.close();
        }
    }

    @Test
    void authCanProtectProbeEndpoints() throws Exception {
        final MetricsSettings settings = prometheusSettings(props -> {
            props.setProperty("metrics.prometheus.auth.enabled", "true");
            props.setProperty("metrics.prometheus.auth.protect.probes", "true");
            props.setProperty("metrics.prometheus.auth.header.value", "scrape-secret");
        });
        final MetricsRuntime runtime = new PrometheusMetricsProvider().create(settings);

        try {
            final HttpResponse<String> anonymousHealth = get(settings, "/-/healthy", null, null);
            assertEquals(401, anonymousHealth.statusCode());

            final HttpResponse<String> authenticatedHealth = get(
                    settings,
                    "/-/healthy",
                    "X-Prometheus-Scrape-Secret",
                    "scrape-secret");
            assertEquals(200, authenticatedHealth.statusCode());
        } finally {
            runtime.close();
        }
    }

    @Test
    void settingsToStringMasksPrometheusSecrets() throws Exception {
        final MetricsSettings settings = prometheusSettings(props -> {
            props.setProperty("metrics.prometheus.auth.bearer.token", "changeit");
            props.setProperty("metrics.prometheus.auth.header.value", "scrape-secret");
            props.setProperty("metrics.prometheus.tls.keystore.password", "keystore-secret");
            props.setProperty("metrics.datadog.apiKey", "1234567890abcdef");
        });

        final String rendered = settings.toString();
        assertFalse(rendered.contains("changeit"));
        assertFalse(rendered.contains("geit"));
        assertFalse(rendered.contains("scrape-secret"));
        assertFalse(rendered.contains("keystore-secret"));
        assertTrue(rendered.contains("****cdef"));
        assertTrue(rendered.contains("****"));
    }

    @Test
    void unknownProviderFallsBackToNone() throws Exception {
        final Properties props = new Properties();
        props.setProperty("metrics.provider", "PROMETEUS");
        props.setProperty("metrics.prometheus.enabled", "true");

        final MetricsSettings settings = MetricsSettings.from(PipelineConfig.from(props, "inline"));

        assertEquals("NONE", settings.providerId);
        assertFalse(settings.prometheusEnabled);
        assertFalse(settings.datadogEnabled);
    }

    @Test
    void explicitProviderKeepsSingleEffectiveBackend() throws Exception {
        final Properties props = new Properties();
        props.setProperty("metrics.provider", "PROMETHEUS");
        props.setProperty("metrics.datadog.enabled", "true");

        final MetricsSettings settings = MetricsSettings.from(PipelineConfig.from(props, "inline"));

        assertTrue(settings.prometheusEnabled);
        assertFalse(settings.datadogEnabled);
    }

    @Test
    void autoProviderStillUsesEnabledFlags() throws Exception {
        final Properties props = new Properties();
        props.setProperty("metrics.provider", "AUTO");
        props.setProperty("metrics.prometheus.enabled", "true");

        final MetricsSettings settings = MetricsSettings.from(PipelineConfig.from(props, "inline"));

        assertEquals("PROMETHEUS", settings.providerId);
        assertTrue(settings.prometheusEnabled);
        assertFalse(settings.datadogEnabled);
    }

    @Test
    void authEnabledRequiresAConfiguredSecret() {
        final Properties props = new Properties();
        props.setProperty("metrics.provider", "PROMETHEUS");
        props.setProperty("metrics.prometheus.auth.enabled", "true");

        assertThrows(IllegalArgumentException.class,
                () -> MetricsSettings.from(PipelineConfig.from(props, "inline")));
    }

    @Test
    void tlsSettingsFailFastOnMissingRequiredFields() throws Exception {
        final Path keyStore = Files.createTempFile("streamkernel-metrics-test-", ".p12");
        final Properties props = new Properties();
        props.setProperty("metrics.provider", "PROMETHEUS");
        props.setProperty("metrics.prometheus.tls.enabled", "true");
        props.setProperty("metrics.prometheus.tls.keystore.path", keyStore.toString());
        props.setProperty("metrics.prometheus.tls.keystore.password", "store-secret");

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> MetricsSettings.from(PipelineConfig.from(props, "inline")));
        assertTrue(failure.getMessage().contains("metrics.prometheus.tls.key.password"));
    }

    @Test
    void tlsClientAuthAndProtocolsAreNormalized() throws Exception {
        final Properties props = new Properties();
        props.setProperty("metrics.provider", "PROMETHEUS");
        props.setProperty("metrics.prometheus.tls.client.auth", "required");
        props.setProperty("metrics.prometheus.tls.protocols", "TLSv1.3, TLSv1.2");

        final MetricsSettings settings = MetricsSettings.from(PipelineConfig.from(props, "inline"));

        assertEquals("NEED", settings.prometheusTlsClientAuth);
        assertEquals("TLSv1.3,TLSv1.2", settings.prometheusTlsProtocols);
    }

    @Test
    void invalidTagKeysAndNewlineValuesAreSkipped() throws Exception {
        final Properties props = new Properties();
        props.setProperty("metrics.provider", "PROMETHEUS");
        props.setProperty("metrics.tag.good_tag", "ok");
        props.setProperty("metrics.tag.bad-tag", "bad");
        props.setProperty("metrics.tag.bad_value", "line1\nline2");
        props.setProperty("pipeline.id", "pipeline-a");

        final MetricsSettings settings = MetricsSettings.from(PipelineConfig.from(props, "inline"));

        assertEquals("ok", settings.commonTags.get("good_tag"));
        assertFalse(settings.commonTags.containsKey("bad-tag"));
        assertFalse(settings.commonTags.containsKey("bad_value"));
        assertEquals("pipeline-a", settings.commonTags.get("pipeline"));
    }

    private static MetricsSettings prometheusSettings() throws IOException {
        return prometheusSettings(props -> { });
    }

    private static MetricsSettings prometheusSettings(java.util.function.Consumer<Properties> customizer)
            throws IOException {
        final Properties props = new Properties();
        props.setProperty("metrics.provider", "PROMETHEUS");
        props.setProperty("metrics.prometheus.enabled", "true");
        props.setProperty("metrics.prometheus.port", Integer.toString(freePort()));
        props.setProperty("metrics.prometheus.bind.address", "127.0.0.1");
        customizer.accept(props);
        return MetricsSettings.from(PipelineConfig.from(props, "inline"));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static HttpResponse<String> get(MetricsSettings settings,
                                            String path,
                                            String headerName,
                                            String headerValue) throws Exception {
        final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + settings.prometheusPort + path))
                .timeout(Duration.ofSeconds(3))
                .GET();
        if (headerName != null && headerValue != null) {
            builder.header(headerName, headerValue);
        }
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static double metricValue(String scrape, String metricName) {
        final Pattern pattern = Pattern.compile(String.format(METRIC_LINE.pattern(), Pattern.quote(metricName)));
        final Matcher matcher = pattern.matcher(scrape);
        assertTrue(matcher.find(), "Expected scrape to contain metric " + metricName);
        final String value = matcher.group(2);
        assertNotNull(value);
        return Double.parseDouble(value);
    }
}
