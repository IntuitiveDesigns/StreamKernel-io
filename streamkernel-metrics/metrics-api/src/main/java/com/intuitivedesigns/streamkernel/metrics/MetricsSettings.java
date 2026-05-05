/*
 * Copyright 2026 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intuitivedesigns.streamkernel.metrics;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Immutable configuration container for Metrics Runtime.
 *
 * <p><b>Scope</b>
 * <ul>
 *   <li>Normalizes and validates metrics-related configuration from {@link PipelineConfig}.</li>
 *   <li>Provides a stable, typed snapshot that downstream runtime components can rely on.</li>
 *   <li>Supports backward compatibility while enabling forward extensibility.</li>
 * </ul>
 *
 * <p><b>Enterprise characteristics</b>
 * <ul>
 *   <li><b>Immutability:</b> all fields are final and maps are wrapped as unmodifiable.</li>
 *   <li><b>Backwards compatibility:</b> defaults preserve legacy behavior when keys are missing.</li>
 *   <li><b>Defensive parsing:</b> guards against missing config APIs, malformed values, and nulls.</li>
 *   <li><b>Least surprise:</b> explicit provider choice wins over AUTO/derived behavior.</li>
 *   <li><b>Secret hygiene:</b> secrets are masked in {@link #toString()}.</li>
 * </ul>
 *
 * <p><b>Provider resolution rules</b>
 * <ol>
 *   <li>If {@code metrics.provider} is set and not {@code AUTO} → honor it.</li>
 *   <li>Else if {@code metrics.prometheus.enabled=true} → PROMETHEUS.</li>
 *   <li>Else if {@code metrics.datadog.enabled=true} → DATADOG.</li>
 *   <li>Else → NONE (NOOP metrics).</li>
 * </ol>
 *
 * <p><b>Tags</b>
 * <ul>
 *   <li>All config keys starting with {@code metrics.tag.} are imported into {@link #commonTags}.</li>
 *   <li>Example: {@code metrics.tag.env=prod} → tag {@code env=prod}.</li>
 *   <li>If {@code metrics.tag.pipeline} is absent, {@code pipeline.id} is used as the pipeline tag.</li>
 * </ul>
 */
public final class MetricsSettings {
    private static final Logger log = LoggerFactory.getLogger(MetricsSettings.class);

    // ---- Config keys ----
    private static final String KEY_PROVIDER = "metrics.provider";
    private static final String KEY_STEP_SECONDS = "metrics.step.seconds";
    private static final String KEY_TAG_PREFIX = "metrics.tag.";
    private static final String KEY_PIPELINE_ID = "pipeline.id";
    private static final String KEY_PIPELINE_TAG = KEY_TAG_PREFIX + "pipeline";

    // Prometheus
    private static final String KEY_PROM_ENABLED = "metrics.prometheus.enabled";
    private static final String KEY_PROM_PORT = "metrics.prometheus.port";
    private static final String KEY_PROM_BIND_ADDRESS = "metrics.prometheus.bind.address";
    private static final String KEY_PROM_SNAPSHOT_PATH = "metrics.prometheus.snapshot.path";
    private static final String KEY_PROM_ALLOWED_REMOTE_ADDRESSES =
            "metrics.prometheus.allowed.remote.addresses";
    private static final String KEY_PROM_AUTH_ENABLED = "metrics.prometheus.auth.enabled";
    private static final String KEY_PROM_AUTH_PROTECT_PROBES =
            "metrics.prometheus.auth.protect.probes";
    private static final String KEY_PROM_AUTH_BEARER_TOKEN =
            "metrics.prometheus.auth.bearer.token";
    private static final String KEY_PROM_AUTH_HEADER_NAME =
            "metrics.prometheus.auth.header.name";
    private static final String KEY_PROM_AUTH_HEADER_VALUE =
            "metrics.prometheus.auth.header.value";
    private static final String KEY_PROM_TLS_ENABLED = "metrics.prometheus.tls.enabled";
    private static final String KEY_PROM_TLS_KEYSTORE_PATH =
            "metrics.prometheus.tls.keystore.path";
    private static final String KEY_PROM_TLS_KEYSTORE_PASSWORD =
            "metrics.prometheus.tls.keystore.password";
    private static final String KEY_PROM_TLS_KEYSTORE_TYPE =
            "metrics.prometheus.tls.keystore.type";
    private static final String KEY_PROM_TLS_KEY_PASSWORD =
            "metrics.prometheus.tls.key.password";
    private static final String KEY_PROM_TLS_TRUSTSTORE_PATH =
            "metrics.prometheus.tls.truststore.path";
    private static final String KEY_PROM_TLS_TRUSTSTORE_PASSWORD =
            "metrics.prometheus.tls.truststore.password";
    private static final String KEY_PROM_TLS_TRUSTSTORE_TYPE =
            "metrics.prometheus.tls.truststore.type";
    private static final String KEY_PROM_TLS_CLIENT_AUTH =
            "metrics.prometheus.tls.client.auth";
    private static final String KEY_PROM_TLS_PROTOCOLS =
            "metrics.prometheus.tls.protocols";
    private static final String SYS_PROM_SNAPSHOT_PATH = "streamkernel.prometheus.snapshot.path";

    // Datadog
    private static final String KEY_DD_ENABLED = "metrics.datadog.enabled";
    private static final String KEY_DD_API_KEY = "metrics.datadog.apiKey";
    private static final String KEY_DD_URI = "metrics.datadog.uri";
    private static final String KEY_DD_HOST_TAG = "metrics.datadog.hostTag";

    // Env vars
    private static final String ENV_DD_API_KEY = "DD_API_KEY";
    private static final String ENV_PROM_AUTH_BEARER_TOKEN =
            "STREAMKERNEL_PROMETHEUS_BEARER_TOKEN";
    private static final String ENV_PROM_AUTH_HEADER_VALUE =
            "STREAMKERNEL_PROMETHEUS_SCRAPE_SECRET";
    private static final String ENV_PROM_TLS_KEYSTORE_PASSWORD =
            "STREAMKERNEL_PROMETHEUS_TLS_KEYSTORE_PASSWORD";
    private static final String ENV_PROM_TLS_KEY_PASSWORD =
            "STREAMKERNEL_PROMETHEUS_TLS_KEY_PASSWORD";
    private static final String ENV_PROM_TLS_TRUSTSTORE_PASSWORD =
            "STREAMKERNEL_PROMETHEUS_TLS_TRUSTSTORE_PASSWORD";

    // ---- Defaults ----
    /**
     * Backwards compatible default: metrics are disabled unless explicitly enabled/configured.
     */
    private static final String DEFAULT_PROVIDER = "NONE";

    /**
     * Optional AUTO selection: derive provider from enabled flags.
     */
    private static final String AUTO_PROVIDER = "AUTO";

    private static final int DEFAULT_STEP_SECONDS = 10;
    private static final int DEFAULT_PROM_PORT = 9090;
    private static final String DEFAULT_PROM_BIND_ADDRESS = "0.0.0.0";
    private static final String DEFAULT_PROM_AUTH_HEADER_NAME = "X-Prometheus-Scrape-Secret";
    private static final String DEFAULT_PROM_TLS_KEYSTORE_TYPE = "PKCS12";
    private static final String DEFAULT_PROM_TLS_TRUSTSTORE_TYPE = "PKCS12";
    private static final String DEFAULT_PROM_TLS_CLIENT_AUTH = "NONE";
    private static final String DEFAULT_PROM_TLS_PROTOCOLS = "TLSv1.3,TLSv1.2";
    private static final String DEFAULT_DD_URI = "https://api.datadoghq.com";
    private static final int MIN_SECRET_LENGTH_FOR_SUFFIX = 12;
    private static final Pattern VALID_TAG_KEY =
            Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    private static final Set<String> KNOWN_PROVIDERS =
            Set.of(DEFAULT_PROVIDER, "PROMETHEUS", "DATADOG");
    private static final Set<String> VALID_TLS_CLIENT_AUTH =
            Set.of("NONE", "WANT", "NEED");
    private static final Set<String> TLS_CLIENT_AUTH_NEED_ALIASES =
            Set.of("NEED", "REQUIRE", "REQUIRED");
    private static final Set<String> TLS_CLIENT_AUTH_WANT_ALIASES =
            Set.of("WANT", "OPTIONAL");
    private static final Set<String> KNOWN_TLS_PROTOCOLS =
            Set.of("TLSv1.3", "TLSv1.2", "TLSv1.1", "TLSv1", "SSLv3");

    // ---- Public Immutable Fields ----
    /**
     * Selected provider ID (normalized to upper-case), e.g. NONE, PROMETHEUS, DATADOG.
     */
    public final String providerId;

    /**
     * Provider-agnostic tags applied to all emitted metrics (immutable).
     */
    public final Map<String, String> commonTags;

    /**
     * Metrics reporting/aggregation step interval (used by some backends).
     */
    public final Duration step;

    // Provider-specific flags/settings (kept explicit for clarity/auditability)
    public final boolean prometheusEnabled;
    public final int prometheusPort;
    public final String prometheusBindAddress;
    public final String prometheusSnapshotPath;
    public final String prometheusAllowedRemoteAddresses;
    public final boolean prometheusAuthEnabled;
    public final boolean prometheusAuthProtectProbes;
    public final String prometheusAuthBearerToken;
    public final String prometheusAuthHeaderName;
    public final String prometheusAuthHeaderValue;
    public final boolean prometheusTlsEnabled;
    public final String prometheusTlsKeyStorePath;
    public final String prometheusTlsKeyStorePassword;
    public final String prometheusTlsKeyStoreType;
    public final String prometheusTlsKeyPassword;
    public final String prometheusTlsTrustStorePath;
    public final String prometheusTlsTrustStorePassword;
    public final String prometheusTlsTrustStoreType;
    public final String prometheusTlsClientAuth;
    public final String prometheusTlsProtocols;

    public final boolean datadogEnabled;
    public final String datadogApiKey;
    public final String datadogUri;
    public final String datadogHostTag;

    private MetricsSettings(String providerId,
                            Map<String, String> commonTags,
                            Duration step,
                            boolean prometheusEnabled,
                            int prometheusPort,
                            String prometheusBindAddress,
                            String prometheusSnapshotPath,
                            String prometheusAllowedRemoteAddresses,
                            boolean prometheusAuthEnabled,
                            boolean prometheusAuthProtectProbes,
                            String prometheusAuthBearerToken,
                            String prometheusAuthHeaderName,
                            String prometheusAuthHeaderValue,
                            boolean prometheusTlsEnabled,
                            String prometheusTlsKeyStorePath,
                            String prometheusTlsKeyStorePassword,
                            String prometheusTlsKeyStoreType,
                            String prometheusTlsKeyPassword,
                            String prometheusTlsTrustStorePath,
                            String prometheusTlsTrustStorePassword,
                            String prometheusTlsTrustStoreType,
                            String prometheusTlsClientAuth,
                            String prometheusTlsProtocols,
                            boolean datadogEnabled,
                            String datadogApiKey,
                            String datadogUri,
                            String datadogHostTag) {
        this.providerId = providerId;
        this.commonTags = commonTags;
        this.step = step;
        this.prometheusEnabled = prometheusEnabled;
        this.prometheusPort = prometheusPort;
        this.prometheusBindAddress = prometheusBindAddress;
        this.prometheusSnapshotPath = prometheusSnapshotPath;
        this.prometheusAllowedRemoteAddresses = prometheusAllowedRemoteAddresses;
        this.prometheusAuthEnabled = prometheusAuthEnabled;
        this.prometheusAuthProtectProbes = prometheusAuthProtectProbes;
        this.prometheusAuthBearerToken = prometheusAuthBearerToken;
        this.prometheusAuthHeaderName = prometheusAuthHeaderName;
        this.prometheusAuthHeaderValue = prometheusAuthHeaderValue;
        this.prometheusTlsEnabled = prometheusTlsEnabled;
        this.prometheusTlsKeyStorePath = prometheusTlsKeyStorePath;
        this.prometheusTlsKeyStorePassword = prometheusTlsKeyStorePassword;
        this.prometheusTlsKeyStoreType = prometheusTlsKeyStoreType;
        this.prometheusTlsKeyPassword = prometheusTlsKeyPassword;
        this.prometheusTlsTrustStorePath = prometheusTlsTrustStorePath;
        this.prometheusTlsTrustStorePassword = prometheusTlsTrustStorePassword;
        this.prometheusTlsTrustStoreType = prometheusTlsTrustStoreType;
        this.prometheusTlsClientAuth = prometheusTlsClientAuth;
        this.prometheusTlsProtocols = prometheusTlsProtocols;
        this.datadogEnabled = datadogEnabled;
        this.datadogApiKey = datadogApiKey;
        this.datadogUri = datadogUri;
        this.datadogHostTag = datadogHostTag;
    }

    /**
     * Builds an immutable {@link MetricsSettings} snapshot from pipeline configuration.
     *
     * <p>Parsing is intentionally defensive and does not assume a particular {@link PipelineConfig}
     * implementation beyond common getters. Any missing/invalid fields are replaced with safe defaults.</p>
     *
     * @param config pipeline configuration
     * @return a fully-initialized settings snapshot (never null)
     */
    public static MetricsSettings from(PipelineConfig config) {
        Objects.requireNonNull(config, "config");

        // ------------------------------------------------------------------
        // 1) Common Tags (metrics.tag.*)
        // ------------------------------------------------------------------
        final Map<String, String> tags = new HashMap<>();
        for (Map.Entry<String, Object> entry : safeMap(config).entrySet()) {
            final String k = entry.getKey();
            if (k == null || !k.startsWith(KEY_TAG_PREFIX)) continue;

            final String tagKey = k.substring(KEY_TAG_PREFIX.length()).trim();
            if (tagKey.isEmpty()) continue;
            if (!VALID_TAG_KEY.matcher(tagKey).matches()) {
                log.warn("Skipping invalid metrics tag key '{}'. Prometheus label keys must match {}.",
                        tagKey, VALID_TAG_KEY.pattern());
                continue;
            }

            final Object rawValue = entry.getValue();
            final String valStr = rawValue == null ? null : normalize(rawValue.toString());
            if (valStr == null || valStr.isEmpty()) continue;
            if (valStr.indexOf('\n') >= 0 || valStr.indexOf('\r') >= 0) {
                log.warn("Skipping metrics tag '{}' because its value contains a newline.", tagKey);
                continue;
            }

            tags.put(tagKey, valStr);
        }

        // After collecting metrics.tag.* into `tags`:
        final String pipelineId = config.getString(KEY_PIPELINE_ID, null);
        final String configuredPipelineTag = config.getString(KEY_PIPELINE_TAG, null);

        if ((configuredPipelineTag == null || configuredPipelineTag.isBlank())
                && pipelineId != null && !pipelineId.isBlank()) {
            final String normalizedPipelineId = normalize(pipelineId);
            if (normalizedPipelineId != null
                    && normalizedPipelineId.indexOf('\n') < 0
                    && normalizedPipelineId.indexOf('\r') < 0) {
                tags.put("pipeline", normalizedPipelineId);
                log.info("MetricsSettings auto-populated metrics.tag.pipeline from {}.", KEY_PIPELINE_ID);
            } else {
                log.warn("Skipping auto-populated pipeline tag because {} contains an invalid value.",
                        KEY_PIPELINE_ID);
            }
        }

        // ------------------------------------------------------------------
        // 2) Step interval
        // ------------------------------------------------------------------
        final int stepSec = clampInt(config.getInt(KEY_STEP_SECONDS, DEFAULT_STEP_SECONDS), 1, 3_600);
        final Duration step = Duration.ofSeconds(stepSec);

        // ------------------------------------------------------------------
        // 3) Enabled flags (optional; supports AUTO provider selection)
        // ------------------------------------------------------------------
        final boolean promEnabled = getBoolean(config, KEY_PROM_ENABLED, false);
        final boolean ddEnabled = getBoolean(config, KEY_DD_ENABLED, false);

        // ------------------------------------------------------------------
        // 4) Provider selection (backwards compatible)
        // ------------------------------------------------------------------
        final String rawProvider = normalizeUpper(config.getString(KEY_PROVIDER, null));
        final String provider = resolveProvider(rawProvider, promEnabled, ddEnabled);

        // ------------------------------------------------------------------
        // 5) Provider-specific settings
        // ------------------------------------------------------------------
        final int promPort = clampInt(config.getInt(KEY_PROM_PORT, DEFAULT_PROM_PORT), 1, 65_535);
        final String promBindAddress = firstNonBlank(
                config.getString(KEY_PROM_BIND_ADDRESS, null),
                DEFAULT_PROM_BIND_ADDRESS
        );
        final String promSnapshotPath = firstNonBlank(
                config.getString(KEY_PROM_SNAPSHOT_PATH, null),
                sysProp(SYS_PROM_SNAPSHOT_PATH)
        );
        final String promAllowedRemoteAddresses =
                normalize(config.getString(KEY_PROM_ALLOWED_REMOTE_ADDRESSES, null));
        final boolean promAuthEnabled = getBoolean(config, KEY_PROM_AUTH_ENABLED, false);
        final boolean promAuthProtectProbes = getBoolean(config, KEY_PROM_AUTH_PROTECT_PROBES, false);
        final String promAuthBearerToken = firstNonBlank(
                env(ENV_PROM_AUTH_BEARER_TOKEN),
                config.getString(KEY_PROM_AUTH_BEARER_TOKEN, null)
        );
        final String promAuthHeaderName = firstNonBlank(
                config.getString(KEY_PROM_AUTH_HEADER_NAME, null),
                DEFAULT_PROM_AUTH_HEADER_NAME
        );
        final String promAuthHeaderValue = firstNonBlank(
                env(ENV_PROM_AUTH_HEADER_VALUE),
                config.getString(KEY_PROM_AUTH_HEADER_VALUE, null)
        );
        final boolean promTlsEnabled = getBoolean(config, KEY_PROM_TLS_ENABLED, false);
        final String promTlsKeyStorePath = normalize(config.getString(KEY_PROM_TLS_KEYSTORE_PATH, null));
        final String promTlsKeyStorePassword = firstNonBlank(
                env(ENV_PROM_TLS_KEYSTORE_PASSWORD),
                config.getString(KEY_PROM_TLS_KEYSTORE_PASSWORD, null)
        );
        final String promTlsKeyStoreType = firstNonBlank(
                config.getString(KEY_PROM_TLS_KEYSTORE_TYPE, null),
                DEFAULT_PROM_TLS_KEYSTORE_TYPE
        );
        final String promTlsKeyPassword = firstNonBlank(
                env(ENV_PROM_TLS_KEY_PASSWORD),
                config.getString(KEY_PROM_TLS_KEY_PASSWORD, null)
        );
        final String promTlsTrustStorePath = normalize(config.getString(KEY_PROM_TLS_TRUSTSTORE_PATH, null));
        final String promTlsTrustStorePassword = firstNonBlank(
                env(ENV_PROM_TLS_TRUSTSTORE_PASSWORD),
                config.getString(KEY_PROM_TLS_TRUSTSTORE_PASSWORD, null)
        );
        final String promTlsTrustStoreType = firstNonBlank(
                config.getString(KEY_PROM_TLS_TRUSTSTORE_TYPE, null),
                DEFAULT_PROM_TLS_TRUSTSTORE_TYPE
        );
        final String promTlsClientAuth = normalizeTlsClientAuth(firstNonBlank(
                normalizeUpper(config.getString(KEY_PROM_TLS_CLIENT_AUTH, null)),
                DEFAULT_PROM_TLS_CLIENT_AUTH
        ));
        final String promTlsProtocols = normalizeTlsProtocols(firstNonBlank(
                config.getString(KEY_PROM_TLS_PROTOCOLS, null),
                DEFAULT_PROM_TLS_PROTOCOLS
        ));

        // Datadog API Key resolution order: Environment > Config
        final String ddKey = firstNonBlank(
                env(ENV_DD_API_KEY),
                config.getString(KEY_DD_API_KEY, null)
        );

        final String ddUri = firstNonBlank(
                config.getString(KEY_DD_URI, null),
                DEFAULT_DD_URI
        );

        final String ddHostTag = normalize(config.getString(KEY_DD_HOST_TAG, null));

        validateProviderSettings(
                promAuthEnabled,
                promAuthBearerToken,
                promAuthHeaderValue,
                promTlsEnabled,
                promTlsKeyStorePath,
                promTlsKeyStorePassword,
                promTlsKeyPassword,
                promTlsTrustStorePath,
                promTlsTrustStorePassword,
                promTlsClientAuth
        );

        // ------------------------------------------------------------------
        // 6) Effective enablement flags
        // ------------------------------------------------------------------
        // Keep booleans aligned with provider so runtime components can be simple and safe.
        final boolean effectivePromEnabled = "PROMETHEUS".equals(provider);
        final boolean effectiveDdEnabled = "DATADOG".equals(provider);

        final MetricsSettings settings = new MetricsSettings(
                provider,
                Collections.unmodifiableMap(tags),
                step,
                effectivePromEnabled,
                promPort,
                promBindAddress,
                promSnapshotPath,
                promAllowedRemoteAddresses,
                promAuthEnabled,
                promAuthProtectProbes,
                promAuthBearerToken,
                promAuthHeaderName,
                promAuthHeaderValue,
                promTlsEnabled,
                promTlsKeyStorePath,
                promTlsKeyStorePassword,
                promTlsKeyStoreType,
                promTlsKeyPassword,
                promTlsTrustStorePath,
                promTlsTrustStorePassword,
                promTlsTrustStoreType,
                promTlsClientAuth,
                promTlsProtocols,
                effectiveDdEnabled,
                ddKey,
                ddUri,
                ddHostTag
        );
        log.info("MetricsSettings resolved. provider={} prometheusEnabled={} datadogEnabled={} " +
                        "prometheusPort={} auth={} tls={} tags={}",
                settings.providerId,
                settings.prometheusEnabled,
                settings.datadogEnabled,
                settings.prometheusPort,
                settings.prometheusAuthEnabled,
                settings.prometheusTlsEnabled,
                settings.commonTags.keySet());
        return settings;
    }

    /**
     * Resolves the final provider ID to use.
     *
     * <p>Rules:
     * <ul>
     *   <li>If metrics.provider is explicitly set (and not AUTO), it wins.</li>
     *   <li>If AUTO or missing, derive from enabled flags (PROMETHEUS preferred over DATADOG).</li>
     *   <li>If nothing is enabled, return NONE.</li>
     * </ul>
     */
    private static String resolveProvider(String rawProviderUpper, boolean promEnabled, boolean ddEnabled) {
        // Explicit provider overrides AUTO/missing.
        if (rawProviderUpper != null && !rawProviderUpper.isBlank() && !AUTO_PROVIDER.equals(rawProviderUpper)) {
            if (KNOWN_PROVIDERS.contains(rawProviderUpper)) {
                return rawProviderUpper;
            }
            log.warn("Unknown {}='{}'; treating metrics provider as {}.",
                    KEY_PROVIDER, rawProviderUpper, DEFAULT_PROVIDER);
            return DEFAULT_PROVIDER;
        }

        // AUTO/missing -> derive from flags.
        if (promEnabled) return "PROMETHEUS";
        if (ddEnabled) return "DATADOG";
        return DEFAULT_PROVIDER;
    }

    @Override
    public String toString() {
        return "MetricsSettings{" +
                "providerId='" + providerId + '\'' +
                ", commonTags=" + commonTags +
                ", step=" + step +
                ", prometheusEnabled=" + prometheusEnabled +
                ", prometheusPort=" + prometheusPort +
                ", prometheusBindAddress='" + prometheusBindAddress + '\'' +
                ", prometheusSnapshotPath='" + prometheusSnapshotPath + '\'' +
                ", prometheusAllowedRemoteAddresses='" + prometheusAllowedRemoteAddresses + '\'' +
                ", prometheusAuthEnabled=" + prometheusAuthEnabled +
                ", prometheusAuthProtectProbes=" + prometheusAuthProtectProbes +
                ", prometheusAuthBearerToken=" + mask(prometheusAuthBearerToken) +
                ", prometheusAuthHeaderName='" + prometheusAuthHeaderName + '\'' +
                ", prometheusAuthHeaderValue=" + mask(prometheusAuthHeaderValue) +
                ", prometheusTlsEnabled=" + prometheusTlsEnabled +
                ", prometheusTlsKeyStorePath='" + prometheusTlsKeyStorePath + '\'' +
                ", prometheusTlsKeyStorePassword=" + mask(prometheusTlsKeyStorePassword) +
                ", prometheusTlsKeyStoreType='" + prometheusTlsKeyStoreType + '\'' +
                ", prometheusTlsKeyPassword=" + mask(prometheusTlsKeyPassword) +
                ", prometheusTlsTrustStorePath='" + prometheusTlsTrustStorePath + '\'' +
                ", prometheusTlsTrustStorePassword=" + mask(prometheusTlsTrustStorePassword) +
                ", prometheusTlsTrustStoreType='" + prometheusTlsTrustStoreType + '\'' +
                ", prometheusTlsClientAuth='" + prometheusTlsClientAuth + '\'' +
                ", prometheusTlsProtocols='" + prometheusTlsProtocols + '\'' +
                ", datadogEnabled=" + datadogEnabled +
                ", datadogUri='" + datadogUri + '\'' +
                ", datadogHostTag='" + datadogHostTag + '\'' +
                ", datadogApiKey=" + mask(datadogApiKey) +
                '}';
    }

    // ------------------------------------------------------------------
    // Helpers (defensive; must never throw in normal operation)
    // ------------------------------------------------------------------

    /**
     * Returns {@code config.asMap()} if available; otherwise returns an empty map.
     *
     * <p>This enables tag discovery without requiring new APIs on {@link PipelineConfig}.</p>
     */
    private static Map<String, Object> safeMap(PipelineConfig config) {
        try {
            Map<String, Object> m = config.asMap();
            return (m != null) ? m : Map.of();
        } catch (Exception ignored) {
            return Map.of();
        }
    }

    /**
     * Reads an environment variable without throwing if security managers or policies block access.
     */
    private static String env(String k) {
        try {
            return System.getenv(k);
        } catch (SecurityException se) {
            log.warn("Security policy blocked access to environment variable '{}': {}", k, se.getMessage());
            return null;
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * Reads a system property without turning settings parsing into a JVM-global footgun.
     *
     * <p>This remains for backward compatibility with the benchmark runner's historical
     * {@code -Dstreamkernel.prometheus.snapshot.path} override.</p>
     */
    private static String sysProp(String key) {
        try {
            return System.getProperty(key);
        } catch (SecurityException se) {
            log.warn("Security policy blocked access to system property '{}': {}", key, se.getMessage());
            return null;
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * Returns the first non-blank string in the provided list, or null.
     */
    private static String firstNonBlank(String... candidates) {
        if (candidates == null) return null;
        for (String c : candidates) {
            String s = normalize(c);
            if (s != null) return s;
        }
        return null;
    }

    /**
     * Trims and converts empty to null.
     */
    private static String normalize(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    /**
     * Normalizes to upper-case using Locale.ROOT (deterministic).
     */
    private static String normalizeUpper(String s) {
        String n = normalize(s);
        return (n != null) ? n.toUpperCase(Locale.ROOT) : null;
    }

    private static String normalizeTlsClientAuth(String value) {
        final String normalized = normalizeUpper(value);
        if (normalized == null) {
            return DEFAULT_PROM_TLS_CLIENT_AUTH;
        }
        if (TLS_CLIENT_AUTH_NEED_ALIASES.contains(normalized)) {
            return "NEED";
        }
        if (TLS_CLIENT_AUTH_WANT_ALIASES.contains(normalized)) {
            return "WANT";
        }
        if (VALID_TLS_CLIENT_AUTH.contains(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException("Invalid " + KEY_PROM_TLS_CLIENT_AUTH + "='" + value
                + "'. Must be one of: NONE, WANT, NEED.");
    }

    private static String normalizeTlsProtocols(String csv) {
        final String raw = firstNonBlank(csv, DEFAULT_PROM_TLS_PROTOCOLS);
        final List<String> protocols = new ArrayList<>();
        for (String part : raw.split(",")) {
            final String protocol = normalize(part);
            if (protocol == null) {
                continue;
            }
            if (!KNOWN_TLS_PROTOCOLS.contains(protocol)) {
                log.warn("Unrecognized TLS protocol '{}' in {}. It will be passed to the JVM TLS provider.",
                        protocol, KEY_PROM_TLS_PROTOCOLS);
            }
            protocols.add(protocol);
        }
        if (protocols.isEmpty()) {
            return DEFAULT_PROM_TLS_PROTOCOLS;
        }
        return String.join(",", protocols);
    }

    private static void validateProviderSettings(boolean promAuthEnabled,
                                                 String promAuthBearerToken,
                                                 String promAuthHeaderValue,
                                                 boolean promTlsEnabled,
                                                 String promTlsKeyStorePath,
                                                 String promTlsKeyStorePassword,
                                                 String promTlsKeyPassword,
                                                 String promTlsTrustStorePath,
                                                 String promTlsTrustStorePassword,
                                                 String promTlsClientAuth) {
        if (promAuthEnabled
                && normalize(promAuthBearerToken) == null
                && normalize(promAuthHeaderValue) == null) {
            throw new IllegalArgumentException(KEY_PROM_AUTH_ENABLED + "=true requires at least one of "
                    + KEY_PROM_AUTH_BEARER_TOKEN + " or " + KEY_PROM_AUTH_HEADER_VALUE + ".");
        }

        if (!promTlsEnabled) {
            return;
        }
        if (normalize(promTlsKeyStorePath) == null) {
            throw new IllegalArgumentException(KEY_PROM_TLS_KEYSTORE_PATH
                    + " is required when " + KEY_PROM_TLS_ENABLED + "=true.");
        }
        requireExistingPath(KEY_PROM_TLS_KEYSTORE_PATH, promTlsKeyStorePath);
        if (normalize(promTlsKeyStorePassword) == null) {
            throw new IllegalArgumentException(KEY_PROM_TLS_KEYSTORE_PASSWORD
                    + " is required when " + KEY_PROM_TLS_ENABLED + "=true.");
        }
        if (normalize(promTlsKeyPassword) == null) {
            throw new IllegalArgumentException(KEY_PROM_TLS_KEY_PASSWORD
                    + " is required when " + KEY_PROM_TLS_ENABLED + "=true.");
        }

        if (normalize(promTlsTrustStorePath) != null) {
            requireExistingPath(KEY_PROM_TLS_TRUSTSTORE_PATH, promTlsTrustStorePath);
            if (normalize(promTlsTrustStorePassword) == null) {
                log.warn("{} is set without {}. This only works for trust stores that do not require a password.",
                        KEY_PROM_TLS_TRUSTSTORE_PATH, KEY_PROM_TLS_TRUSTSTORE_PASSWORD);
            }
        } else if (!"NONE".equals(promTlsClientAuth)) {
            log.warn("{}={} without {}. The JVM default trust store will be used for client certificate validation.",
                    KEY_PROM_TLS_CLIENT_AUTH, promTlsClientAuth, KEY_PROM_TLS_TRUSTSTORE_PATH);
        }
    }

    private static void requireExistingPath(String key, String rawPath) {
        try {
            final Path path = Path.of(rawPath).toAbsolutePath().normalize();
            if (!Files.exists(path)) {
                throw new IllegalArgumentException(key + " does not exist: " + path);
            }
            if (!Files.isRegularFile(path)) {
                throw new IllegalArgumentException(key + " is not a regular file: " + path);
            }
        } catch (InvalidPathException ipe) {
            throw new IllegalArgumentException(key + " is not a valid path: " + rawPath, ipe);
        }
    }

    /**
     * Clamps an integer to a closed interval.
     */
    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    /**
     * Masks secrets for logs and toString output.
     *
     * <p>Returns "****" for null/short secrets, otherwise preserves last 4 chars.</p>
     */
    private static String mask(String secret) {
        if (secret == null || secret.isEmpty()) return "****";
        if (secret.length() < MIN_SECRET_LENGTH_FOR_SUFFIX) return "****";
        return "****" + secret.substring(secret.length() - 4);
    }

    /**
     * Reads a boolean configuration value with strong compatibility:
     * <ul>
     *   <li>Prefer {@code config.getBoolean(key, def)} if supported.</li>
     *   <li>Otherwise parse string values in common forms (true/false, 1/0, yes/no, on/off).</li>
     * </ul>
     */
    private static boolean getBoolean(PipelineConfig config, String key, boolean def) {
        // Try boolean getter if it exists.
        try {
            return config.getBoolean(key, def);
        } catch (Throwable ignored) {
            // fall through to string parsing
        }

        String s;
        try {
            s = config.getString(key, null);
        } catch (Throwable ignored) {
            return def;
        }

        if (s == null) return def;

        final String v = s.trim().toLowerCase(Locale.ROOT);
        return switch (v) {
            case "true", "1", "yes", "y", "on" -> true;
            case "false", "0", "no", "n", "off" -> false;
            default -> def;
        };
    }
}
