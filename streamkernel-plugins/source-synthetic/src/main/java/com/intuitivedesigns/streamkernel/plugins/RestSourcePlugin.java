/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.sources.RestSourceConnector;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SourcePlugin;

import java.util.Objects;

/**
 * RestSourcePlugin
 * ================
 * Registers the REST polling source under {@code source.type=REST}.
 *
 * Role in the plugin architecture
 * -------------------------------
 * This class is intentionally thin. In the StreamKernel SPI model, plugins act as:
 *
 *   configuration contract + validation layer
 *                 ↓
 *         connector factory
 *
 * The actual HTTP polling logic, buffering, and runtime behavior lives in
 * {@link RestSourceConnector}. Keeping the plugin lightweight provides:
 *
 *  • Stable public configuration contract
 *  • Early validation with clear failure messages
 *  • Separation of concerns (SPI vs runtime behavior)
 *  • Easier long-term evolution without breaking pipeline configs
 *
 * Configuration contract
 * ----------------------
 * Canonical keys validated here:
 *
 *   source.rest.base.url
 *   source.rest.poll.interval.ms
 *   source.rest.timeout.ms
 *   source.rest.max.inflight
 *
 * Note:
 * The current RestSourceConnector only requires {@code source.rest.url}.
 * The additional keys above are forward-looking and ensure pipelines fail fast
 * when misconfigured, even before the connector is instantiated.
 *
 * This is an enterprise design pattern: validate aggressively at the plugin
 * boundary so runtime failures are minimized and logs are deterministic.
 *
 * Design principles
 * -----------------
 * 1) Fail fast on required configuration.
 * 2) Clamp untrusted values to safe bounds.
 * 3) Never mutate PipelineConfig.
 * 4) Delegate all runtime behavior to the connector.
 */
public final class RestSourcePlugin implements SourcePlugin {

    /** Stable plugin identifier used by the SPI catalog. */
    public static final String ID = "REST";

    // ---------------------------------------------------------------------
    // Canonical config keys (public contract)
    // ---------------------------------------------------------------------

    /** Base URL of the REST endpoint. Required. */
    private static final String CFG_BASE_URL          = "source.rest.base.url";

    /** Poll interval in milliseconds. */
    private static final String CFG_POLL_INTERVAL_MS  = "source.rest.poll.interval.ms";

    /** Request timeout in milliseconds. */
    private static final String CFG_TIMEOUT_MS        = "source.rest.timeout.ms";

    /** Maximum concurrent in-flight requests (future use / validation). */
    private static final String CFG_MAX_INFLIGHT      = "source.rest.max.inflight";

    // ---------------------------------------------------------------------
    // Defaults and safety bounds
    // ---------------------------------------------------------------------

    private static final long DEFAULT_POLL_INTERVAL_MS = 1000L;
    private static final long DEFAULT_TIMEOUT_MS       = 2500L;
    private static final int  DEFAULT_MAX_INFLIGHT     = 256;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SOURCE;
    }

    /**
     * Creates the REST source connector after validating configuration.
     *
     * Validation strategy:
     *  - Required fields must exist and be non-blank.
     *  - Numeric values are clamped to safe operational ranges.
     *  - Connector instantiation is delegated to RestSourceConnector.
     */
    @Override
    public SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        // Fail fast on required URL.
        requireNonBlank(config, CFG_BASE_URL);

        // Validate optional settings (bounds enforced even if connector ignores them today).
        long pollMs = clampLong(
                config.getLong(CFG_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS),
                1L,
                60_000L
        );

        long timeoutMs = clampLong(
                config.getLong(CFG_TIMEOUT_MS, DEFAULT_TIMEOUT_MS),
                50L,
                120_000L
        );

        int maxInflight = clampInt(
                config.getInt(CFG_MAX_INFLIGHT, DEFAULT_MAX_INFLIGHT),
                1,
                1_000_000
        );

        // Intentionally unused locally — validation ensures deterministic configs
        // and preserves forward compatibility when connector begins consuming them.
        if (pollMs <= 0 || timeoutMs <= 0 || maxInflight <= 0) {
            throw new IllegalStateException("REST source configuration validation failed.");
        }

        // Delegate creation to the connector factory.
        return RestSourceConnector.fromConfig(config, metrics);
    }

    // ---------------------------------------------------------------------
    // Validation helpers
    // ---------------------------------------------------------------------

    private static void requireNonBlank(PipelineConfig config, String key) {
        String v = config.getString(key, null);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required configuration key: " + key);
        }
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }
}
