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
import com.intuitivedesigns.streamkernel.sources.SalesforceConnector;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SourcePlugin;

import java.util.Objects;

public final class SalesforceSourcePlugin implements SourcePlugin {

    public static final String ID = "SALESFORCE";

    // Canonical config keys (adjust to match your connector)
    private static final String CFG_AUTH_TYPE         = "source.salesforce.auth.type";
    private static final String CFG_CLIENT_ID         = "source.salesforce.client.id";
    private static final String CFG_CLIENT_SECRET     = "source.salesforce.client.secret";
    private static final String CFG_LOGIN_URL         = "source.salesforce.login.url";
    private static final String CFG_POLL_INTERVAL_MS  = "source.salesforce.poll.interval.ms";

    private static final String DEFAULT_LOGIN_URL = "https://login.salesforce.com";
    private static final long DEFAULT_POLL_INTERVAL_MS = 5000L;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SOURCE;
    }

    @Override
    public SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        // Require minimum auth inputs (finalize based on your supported auth flows)
        requireNonBlank(config, CFG_CLIENT_ID);
        requireNonBlank(config, CFG_CLIENT_SECRET);

        // Optional w/ default
        String loginUrl = firstNonBlank(config.getString(CFG_LOGIN_URL, null), DEFAULT_LOGIN_URL);

        long pollMs = clampLong(config.getLong(CFG_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS), 250L, 300_000L);

        return SalesforceConnector.fromConfig(config, metrics);
    }

    private static void requireNonBlank(PipelineConfig config, String key) {
        String v = config.getString(key, null);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required configuration key: " + key);
        }
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.trim().isEmpty()) return a.trim();
        return b;
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }
}
