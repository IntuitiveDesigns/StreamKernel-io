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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Metrics runtime bootstrap and provider resolution.
 *
 * <p><b>Purpose</b>
 * <ul>
 *   <li>Discovers {@link MetricsProvider} implementations via Java {@link ServiceLoader}.</li>
 *   <li>Selects the provider requested by configuration.</li>
 *   <li>Initializes a {@link MetricsRuntime} instance used by the entire StreamKernel runtime.</li>
 *   <li>Guarantees a safe fallback (NOOP metrics) when metrics are disabled or misconfigured.</li>
 * </ul>
 *
 * <p><b>Design goals (enterprise readiness)</b>
 * <ul>
 *   <li><b>Zero hard dependencies:</b> Metrics backends are optional and loaded dynamically.</li>
 *   <li><b>Fail-safe behavior:</b> Metrics must never prevent pipeline startup.</li>
 *   <li><b>Deterministic startup logs:</b> Provider discovery and selection are always logged.</li>
 *   <li><b>ServiceLoader isolation:</b> Uses thread context classloader for container compatibility.</li>
 *   <li><b>Security & reliability:</b> All provider failures are contained and downgraded to NOOP.</li>
 * </ul>
 *
 * <p><b>Configuration</b>
 * <pre>
 * metrics.provider = NONE | PROMETHEUS | OTEL | CUSTOM_ID
 * </pre>
 *
 * If the provider cannot be found or fails to initialize, a NOOP runtime is used.
 */
public final class MetricsFactory {

    private static final Logger log = LoggerFactory.getLogger(MetricsFactory.class);

    /**
     * Singleton NOOP runtime used when metrics are disabled or unavailable.
     */
    private static final MetricsRuntime NOOP = new NoopMetricsRuntime();

    private MetricsFactory() {}

    /**
     * Initializes the metrics runtime using ServiceLoader discovery.
     *
     * <p>This method is intentionally resilient:
     * any failure during provider discovery or initialization will be logged
     * and downgraded to a NOOP runtime.</p>
     *
     * @param settings metrics configuration
     * @return initialized {@link MetricsRuntime} (never null)
     */
    public static MetricsRuntime init(MetricsSettings settings) {
        Objects.requireNonNull(settings, "settings");

        final String requested = normalizeUpper(settings.providerId);

        // Explicit opt-out path
        if (requested == null || "NONE".equals(requested)) {
            log.info("Metrics disabled (metrics.provider=NONE). NOOP active.");
            return NOOP;
        }

        final ClassLoader cl = resolveClassLoader();
        final ServiceLoader<MetricsProvider> loader = ServiceLoader.load(MetricsProvider.class, cl);

        final List<String> discovered = new ArrayList<>();

        // Iterate providers discovered via SPI
        for (MetricsProvider p : loader) {
            final String id = safeId(p);
            if (id != null) discovered.add(id);

            if (!p.matches(requested)) {
                continue;
            }

            try {
                final MetricsRuntime rt = p.create(settings);
                if (rt != null) {
                    log.info("✅ Metrics Runtime initialized: providerId={} impl={}", id, p.getClass().getName());
                    return rt;
                }
            } catch (Throwable t) {
                // Metrics must never crash the runtime
                log.warn("Failed to initialize metrics provider [{}]: {}", p.getClass().getName(), t.getMessage());
                log.debug("Provider init stack trace:", t);
            }
        }

        // No matching provider found
        log.warn(
                "Metrics requested but no provider matched/initialized (metrics.provider={}). NOOP active. Discovered providers: {}",
                requested, discovered
        );

        return NOOP;
    }

    /**
     * Safely extracts provider ID for logging.
     */
    private static String safeId(MetricsProvider p) {
        try {
            String s = (p == null) ? null : p.id();
            return normalizeUpper(s);
        } catch (Throwable ignored) {
            return null;
        }
    }

    /**
     * Normalizes IDs to upper case for case-insensitive matching.
     */
    private static String normalizeUpper(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (t.isEmpty()) return null;
        return t.toUpperCase(Locale.ROOT);
    }

    /**
     * Resolves the classloader used for ServiceLoader discovery.
     *
     * <p>Using the thread context classloader improves compatibility with:
     * <ul>
     *   <li>Containers</li>
     *   <li>Plugin systems</li>
     *   <li>Shaded/fat JAR deployments</li>
     * </ul>
     */
    private static ClassLoader resolveClassLoader() {
        final ClassLoader threadCl = Thread.currentThread().getContextClassLoader();
        return (threadCl != null) ? threadCl : MetricsFactory.class.getClassLoader();
    }

    // ---------------------------------------------------------------------
    // NOOP METRICS RUNTIME
    // ---------------------------------------------------------------------

    /**
     * Safe fallback metrics runtime.
     *
     * <p>This implementation intentionally performs no operations but still
     * satisfies the {@link MetricsRuntime} contract so that the rest of the
     * system can remain metrics-agnostic.</p>
     */
    private static final class NoopMetricsRuntime implements MetricsRuntime {

        /**
         * Sentinel registry object returned to callers that expect a registry.
         * This prevents null handling logic in callers.
         */
        private final Object sentinelRegistry = new Object();

        @Override
        public Object registry() {
            return sentinelRegistry;
        }

        @Override public void counter(String name) { /* no-op */ }
        @Override public void counter(String name, double increment) { /* no-op */ }
        @Override public void timer(String name, long durationMillis) { /* no-op */ }
        @Override public void gauge(String name, double value) { /* no-op */ }

        @Override
        public void close() { /* no-op */ }
    }
}
