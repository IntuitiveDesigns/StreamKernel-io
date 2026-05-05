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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Objects;

/**
 * SPI {@link MetricsProvider} for Micrometer-based metrics runtime.
 *
 * <p><b>Enterprise notes</b>
 * <ul>
 *   <li><b>Strict matching:</b> provider ID is compared case-insensitively after trimming.</li>
 *   <li><b>Fail-soft:</b> creation returns {@code null} on mismatch so the factory can continue scanning providers.</li>
 *   <li><b>Observability:</b> logs key lifecycle events at INFO/DEBUG without leaking secrets.</li>
 * </ul>
 *
 * <p>Expected usage: discovered via {@link java.util.ServiceLoader} and selected by {@code metrics.provider=MICROMETER}.
 */
public final class MicrometerMetricsProvider implements MetricsProvider {
    private final MeterRegistry registry;
    private static final Logger log = LoggerFactory.getLogger(MicrometerMetricsProvider.class);

    /** Canonical provider ID (what users configure in {@code metrics.provider}). */
    public static final String ID = "MICROMETER";

    public MicrometerMetricsProvider() {
        this(new SimpleMeterRegistry());
    }

    public MicrometerMetricsProvider(MeterRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public MetricsRuntime createRuntime(MetricsSettings settings) {
        return new MicrometerMetricsRuntime(registry);
    }

    @Override
    public String id() {
        return ID;
    }

    /**
     * Returns true if the requested provider ID resolves to {@link #ID}.
     *
     * <p>This method must be resilient: providers are discovered dynamically and may be probed with nulls.</p>
     */
    @Override
    public boolean matches(String requestedProviderId) {
        final String req = normalizeUpper(requestedProviderId);
        return ID.equals(req);
    }

    /**
     * Creates a {@link MetricsRuntime} when Micrometer is requested.
     *
     * <p><b>Contract</b>
     * <ul>
     *   <li>Return {@code null} when this provider is not selected (factory will continue scanning).</li>
     *   <li>Throw only for truly unrecoverable provider-internal failures.</li>
     * </ul>
     *
     * <p>Settings are accepted primarily as a selection guard here; concrete registry wiring is owned
     * by {@link MicrometerMetricsRuntime} (or future enhanced runtime variants).</p>
     */
    @Override
    public MetricsRuntime create(MetricsSettings settings) {
        if (settings == null) {
            log.debug("MicrometerMetricsProvider.create called with null settings; skipping.");
            return null;
        }

        // Guard: only create when selected.
        if (!matches(settings.providerId)) {
            return null;
        }

        // Defensive: ensure providerId is present for auditability (should already be true here).
        Objects.requireNonNull(settings.providerId, "settings.providerId");

        log.info("Initializing Micrometer metrics runtime (providerId={}).", ID);

        // NOTE: runtime may internally choose a MeterRegistry implementation or act as an adapter.
        return new MicrometerMetricsRuntime(registry);
    }

    private static String normalizeUpper(String s) {
        if (s == null) return null;
        final String t = s.trim();
        if (t.isEmpty()) return null;
        return t.toUpperCase(Locale.ROOT);
    }
}
