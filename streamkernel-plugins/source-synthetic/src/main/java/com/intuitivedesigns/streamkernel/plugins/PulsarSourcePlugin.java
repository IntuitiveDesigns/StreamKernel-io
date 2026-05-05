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
import com.intuitivedesigns.streamkernel.sources.PulsarSourceConnector;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SourcePlugin;

import java.util.Objects;

/**
 * Registers the Pulsar source connector under {@code source.type=PULSAR}.
 *
 * The canonical contract follows the repo's namespaced source style:
 *
 *   source.pulsar.service.url
 *   source.pulsar.topic
 *   source.pulsar.subscription
 *   source.pulsar.batch.size
 *
 * For migration and article-friendly demos, the shorter aliases below are also
 * accepted:
 *
 *   pulsar.service.url
 *   pulsar.topic
 *   pulsar.subscription
 *   pulsar.batch.size
 */
public final class PulsarSourcePlugin implements SourcePlugin {

    public static final String ID = "PULSAR";

    private static final String CFG_SERVICE_URL = "source.pulsar.service.url";
    private static final String CFG_TOPIC = "source.pulsar.topic";
    private static final String CFG_SUBSCRIPTION = "source.pulsar.subscription";
    private static final String CFG_BATCH_SIZE = "source.pulsar.batch.size";
    private static final String CFG_POLL_MS = "source.pulsar.poll.ms";
    private static final String CFG_RECEIVER_QUEUE_SIZE = "source.pulsar.receiver.queue.size";

    private static final String LEGACY_SERVICE_URL = "pulsar.service.url";
    private static final String LEGACY_TOPIC = "pulsar.topic";
    private static final String LEGACY_SUBSCRIPTION = "pulsar.subscription";
    private static final String LEGACY_BATCH_SIZE = "pulsar.batch.size";

    private static final String DEFAULT_SERVICE_URL = "pulsar://localhost:6650";
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_POLL_MS = 25;
    private static final int DEFAULT_RECEIVER_QUEUE_SIZE = 1000;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SOURCE;
    }

    @Override
    public SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String topic = firstNonBlank(
                config.getString(CFG_TOPIC, null),
                config.getString(LEGACY_TOPIC, null)
        );
        if (topic == null) {
            throw new IllegalArgumentException("Missing required configuration key: " + CFG_TOPIC);
        }

        final String serviceUrl = firstNonBlank(
                config.getString(CFG_SERVICE_URL, null),
                config.getString(LEGACY_SERVICE_URL, null),
                DEFAULT_SERVICE_URL
        );
        final String subscription = firstNonBlank(
                config.getString(CFG_SUBSCRIPTION, null),
                config.getString(LEGACY_SUBSCRIPTION, null)
        );

        final int batchSize = clamp(
                firstInt(config, CFG_BATCH_SIZE, LEGACY_BATCH_SIZE, DEFAULT_BATCH_SIZE),
                1,
                1_000_000
        );
        final int pollMs = clamp(config.getInt(CFG_POLL_MS, DEFAULT_POLL_MS), 1, 60_000);
        final int receiverQueueSize = clamp(
                config.getInt(CFG_RECEIVER_QUEUE_SIZE, DEFAULT_RECEIVER_QUEUE_SIZE),
                1,
                1_000_000
        );

        if (serviceUrl == null || serviceUrl.isBlank()) {
            throw new IllegalArgumentException("Invalid Pulsar service URL");
        }
        if (subscription == null || subscription.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration key: " + CFG_SUBSCRIPTION);
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid Pulsar batch size");
        }
        if (pollMs <= 0) {
            throw new IllegalArgumentException("Invalid Pulsar poll interval");
        }
        if (receiverQueueSize <= 0) {
            throw new IllegalArgumentException("Invalid Pulsar receiver queue size");
        }

        return PulsarSourceConnector.fromConfig(config, metrics);
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    private static int firstInt(PipelineConfig config, String key1, String key2, int def) {
        try {
            final String value = config.getString(key1, null);
            if (value != null && !value.isBlank()) {
                return Integer.parseInt(value.trim());
            }
        } catch (Throwable ignored) {
        }
        try {
            final String value = config.getString(key2, null);
            if (value != null && !value.isBlank()) {
                return Integer.parseInt(value.trim());
            }
        } catch (Throwable ignored) {
        }
        return def;
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
