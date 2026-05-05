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
import com.intuitivedesigns.streamkernel.sources.KafkaSourceConnector;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SourcePlugin;

import java.util.Objects;

/**
 * KafkaSourcePlugin
 * =================
 * Registers the Kafka source connector under {@code source.type=KAFKA}.
 *
 * Architectural role
 * ------------------
 * This plugin acts as the configuration validation and compatibility layer between:
 *
 *      PipelineConfig  →  StreamKernel SPI  →  KafkaSourceConnector runtime
 *
 * Responsibilities:
 *  • Validate required configuration early (fail-fast).
 *  • Support canonical AND legacy configuration keys.
 *  • Enforce safe bounds and sanity checks.
 *  • Preserve backward compatibility without mutating configuration.
 *  • Delegate runtime behavior to KafkaSourceConnector.
 *
 * This separation keeps the runtime connector deterministic and focused on
 * Kafka I/O while the plugin owns the public configuration contract.
 *
 * Stability note:
 * The plugin ID and canonical keys are part of the public pipeline contract.
 * Treat changes as breaking.
 */
public final class KafkaSourcePlugin implements SourcePlugin {

    /** Stable SPI identifier */
    public static final String ID = "KAFKA";

    // ---------------------------------------------------------------------
    // Canonical configuration keys (preferred)
    // ---------------------------------------------------------------------
    private static final String CFG_BOOTSTRAP = "source.kafka.bootstrap.servers";
    private static final String CFG_TOPIC     = "source.kafka.topic";
    private static final String CFG_GROUP_ID  = "source.kafka.group.id";
    private static final String CFG_MAX_POLL  = "source.kafka.max.poll.records";

    private static final String CFG_FETCH_MIN_BYTES   = "source.kafka.fetch.min.bytes";
    private static final String CFG_FETCH_MAX_WAIT_MS = "source.kafka.fetch.max.wait.ms";
    private static final String CFG_AUTO_OFFSET_RESET = "source.kafka.auto.offset.reset";
    private static final String CFG_POLL_MS           = "source.kafka.poll.ms";
    private static final String CFG_CLIENT_ID         = "source.kafka.client.id";
    private static final String CFG_ENABLE_AUTO_COMMIT = "source.kafka.enable.auto.commit";
    private static final String CFG_COMMIT_ON_DISCONNECT = "source.kafka.commit.on.disconnect";

    // ---------------------------------------------------------------------
    // Backward-compatible aliases (legacy configs)
    // ---------------------------------------------------------------------
    private static final String LEGACY_BOOTSTRAP   = "kafka.bootstrap.servers";
    private static final String LEGACY_BROKER      = "kafka.broker";
    private static final String LEGACY_INPUT_TOPIC = "kafka.input.topic";
    private static final String LEGACY_GROUPID_1   = "kafka.consumer.group.id";
    private static final String LEGACY_GROUPID_2   = "kafka.consumer.group";
    private static final String LEGACY_OFFSET_RESET = "kafka.consumer.auto.offset.reset";
    private static final String LEGACY_MAX_POLL     = "kafka.consumer.max.poll.records";
    private static final String LEGACY_CLIENT_ID    = "kafka.client.id";

    // ---------------------------------------------------------------------
    // Defaults and safe bounds
    // ---------------------------------------------------------------------
    private static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    private static final String DEFAULT_GROUP_ID  = "streamkernel-consumer";
    private static final String DEFAULT_CLIENT_ID = "streamkernel-consumer";
    private static final int DEFAULT_MAX_POLL     = 5000;

    private static final int DEFAULT_FETCH_MIN_BYTES   = 1;
    private static final int DEFAULT_FETCH_MAX_WAIT_MS = 500;

    private static final String DEFAULT_AUTO_OFFSET_RESET = "latest";
    private static final int DEFAULT_POLL_MS = 25;
    private static final boolean DEFAULT_ENABLE_AUTO_COMMIT = false;
    private static final boolean DEFAULT_COMMIT_ON_DISCONNECT = false;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SOURCE;
    }

    /**
     * Validates Kafka configuration and delegates connector creation.
     *
     * Validation philosophy:
     *  - Required values must exist.
     *  - Optional values are clamped to safe ranges.
     *  - Configuration is never mutated.
     *  - Connector remains the single source of runtime behavior.
     */
    @Override
    public SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        // --- Required: topic ---
        final String topic = firstNonBlank(
                config.getString(CFG_TOPIC, null),
                config.getString(LEGACY_INPUT_TOPIC, null)
        );
        if (topic == null) {
            throw new IllegalArgumentException("Missing required configuration key: " + CFG_TOPIC);
        }

        // --- Optional but validated ---
        final String bootstrap = firstNonBlank(
                config.getString(CFG_BOOTSTRAP, null),
                config.getString(LEGACY_BOOTSTRAP, null),
                config.getString(LEGACY_BROKER, null),
                DEFAULT_BOOTSTRAP
        );

        final String groupId = firstNonBlank(
                config.getString(CFG_GROUP_ID, null),
                config.getString(LEGACY_GROUPID_1, null),
                config.getString(LEGACY_GROUPID_2, null),
                DEFAULT_GROUP_ID
        );

        final String clientId = firstNonBlank(
                config.getString(CFG_CLIENT_ID, null),
                config.getString(LEGACY_CLIENT_ID, null),
                DEFAULT_CLIENT_ID
        );

        final int maxPoll = clamp(
                firstInt(config, CFG_MAX_POLL, LEGACY_MAX_POLL, DEFAULT_MAX_POLL),
                1, 1_000_000
        );

        final int fetchMinBytes = clamp(config.getInt(CFG_FETCH_MIN_BYTES, DEFAULT_FETCH_MIN_BYTES), 1, 1 << 28);
        final int fetchMaxWait  = clamp(config.getInt(CFG_FETCH_MAX_WAIT_MS, DEFAULT_FETCH_MAX_WAIT_MS), 1, 60_000);

        final String offsetReset = firstNonBlank(
                config.getString(CFG_AUTO_OFFSET_RESET, null),
                config.getString(LEGACY_OFFSET_RESET, null),
                DEFAULT_AUTO_OFFSET_RESET
        );

        final int pollMs = clamp(config.getInt(CFG_POLL_MS, DEFAULT_POLL_MS), 1, 60_000);

        final boolean enableAutoCommit = config.getBoolean(CFG_ENABLE_AUTO_COMMIT, DEFAULT_ENABLE_AUTO_COMMIT);
        final boolean commitOnDisconnect = config.getBoolean(CFG_COMMIT_ON_DISCONNECT, DEFAULT_COMMIT_ON_DISCONNECT);

        // --- Sanity checks ---
        if (bootstrap.isBlank()) throw new IllegalArgumentException("Invalid bootstrap servers");
        if (groupId.isBlank()) throw new IllegalArgumentException("Invalid group id");
        if (clientId.isBlank()) throw new IllegalArgumentException("Invalid client id");
        if (offsetReset.isBlank()) throw new IllegalArgumentException("Invalid offset reset");
        if (maxPoll <= 0) throw new IllegalArgumentException("Invalid max poll records");
        if (fetchMinBytes <= 0) throw new IllegalArgumentException("Invalid fetch.min.bytes");
        if (fetchMaxWait <= 0) throw new IllegalArgumentException("Invalid fetch.max.wait.ms");
        if (pollMs <= 0) throw new IllegalArgumentException("Invalid poll interval");

        // Delegate to connector factory (reads canonical + legacy keys)
        return KafkaSourceConnector.fromConfig(config, metrics);
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.trim().isEmpty()) return v.trim();
        }
        return null;
    }

    private static int firstInt(PipelineConfig config, String k1, String k2, int def) {
        try {
            final String v1 = config.getString(k1, null);
            if (v1 != null && !v1.isBlank()) return Integer.parseInt(v1.trim());
        } catch (Throwable ignored) {}
        try {
            final String v2 = config.getString(k2, null);
            if (v2 != null && !v2.isBlank()) return Integer.parseInt(v2.trim());
        } catch (Throwable ignored) {}
        return def;
    }

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }
}
