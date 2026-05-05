/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.output.KafkaSink;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;

import java.util.Objects;

/**
 * Kafka sink plugin:
 * - Accepts recommended keys (sink.kafka.*)
 * - Also supports legacy/global keys (kafka.*) to avoid breaking older profiles
 * - Delegates Kafka client config to KafkaSink#fromConfig (single source of truth)
 */
public final class KafkaSinkPlugin implements SinkPlugin {

    public static final String ID = "KAFKA";

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SINK;
    }

    @Override
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        // KafkaSink#fromConfig performs full validation for topic + bootstrap (primary + legacy).
        // Keep the plugin thin to avoid duplicating rules and drifting over time.
        return KafkaSink.fromConfig(config, metrics);
    }
}
