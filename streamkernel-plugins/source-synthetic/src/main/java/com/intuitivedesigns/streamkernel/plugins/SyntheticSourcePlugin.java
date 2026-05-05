/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.bench.SyntheticSource;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SourcePlugin;

import java.util.Objects;

public final class SyntheticSourcePlugin implements SourcePlugin {

    public static final String ID = "SYNTHETIC";

    private static final String CFG_PAYLOAD_SIZE = "source.synthetic.payload.size";
    private static final String CFG_BUFFER_SIZE  = "source.synthetic.buffer.size";
    private static final String CFG_HIGH_ENTROPY = "source.synthetic.high.entropy";
    private static final String CFG_ENTROPY_ALIAS = "source.synthetic.entropy";
    private static final String CFG_TEXT_PROFILE = "source.synthetic.text.profile";
    private static final String CFG_UNSAFE_REUSE = "source.unsafe.reuse.batch";

    private static final int DEFAULT_PAYLOAD_SIZE = 1024;
    private static final int DEFAULT_BUFFER_SIZE  = 262_144;
    private static final boolean DEFAULT_HIGH_ENTROPY = false;
    private static final boolean DEFAULT_UNSAFE_REUSE = false;

    @Override
    public String id() { return ID; }

    @Override
    public PluginKind kind() { return PluginKind.SOURCE; }

    @Override
    public SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final int payloadSize = clamp(config.getInt(CFG_PAYLOAD_SIZE, DEFAULT_PAYLOAD_SIZE), 1, Integer.MAX_VALUE);
        final int bufferSize = clamp(config.getInt(CFG_BUFFER_SIZE, DEFAULT_BUFFER_SIZE), 1024, 1 << 30);
        final boolean highEntropy = resolveHighEntropy(config);
        final SyntheticSource.TextProfile textProfile = resolveTextProfile(config);
        final boolean unsafeReuse = config.getBoolean(CFG_UNSAFE_REUSE, DEFAULT_UNSAFE_REUSE);

        return new SyntheticSource(payloadSize, bufferSize, highEntropy, unsafeReuse, textProfile);
    }

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static boolean resolveHighEntropy(PipelineConfig config) {
        final String explicit = config.getString(CFG_HIGH_ENTROPY, null);
        if (explicit != null && !explicit.isBlank()) {
            return Boolean.parseBoolean(explicit.trim());
        }
        final String alias = config.getString(CFG_ENTROPY_ALIAS, null);
        if (alias == null || alias.isBlank()) {
            return DEFAULT_HIGH_ENTROPY;
        }
        final String normalized = alias.trim().toUpperCase();
        return switch (normalized) {
            case "HIGH", "TRUE", "ON", "YES", "1" -> true;
            case "LOW", "FALSE", "OFF", "NO", "0" -> false;
            default -> DEFAULT_HIGH_ENTROPY;
        };
    }

    private static SyntheticSource.TextProfile resolveTextProfile(PipelineConfig config) {
        final String raw = config.getString(CFG_TEXT_PROFILE, null);
        if (raw == null || raw.isBlank()) {
            return SyntheticSource.TextProfile.ENTROPY;
        }
        final String normalized = raw.trim().toUpperCase();
        return switch (normalized) {
            case "ENTROPY", "DEFAULT", "AUTO" -> SyntheticSource.TextProfile.ENTROPY;
            case "SUPPORT", "SUPPORT_TICKET", "TICKET" -> SyntheticSource.TextProfile.SUPPORT;
            case "DATABRICKS", "DELTA", "LAKEHOUSE" -> SyntheticSource.TextProfile.DATABRICKS;
            default -> throw new IllegalArgumentException(
                    "Unsupported " + CFG_TEXT_PROFILE + " value: " + raw + ". Expected ENTROPY, SUPPORT, or DATABRICKS.");
        };
    }
}
