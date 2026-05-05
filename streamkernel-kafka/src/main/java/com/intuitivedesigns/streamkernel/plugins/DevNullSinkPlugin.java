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
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * DEVNULL Sink Plugin
 * ===================
 * Lightweight sink used for:
 *  - Benchmarking pipelines (no IO cost)
 *  - Safe default DLQ fallback
 *  - Testing pipeline correctness without external systems
 *
 * Behavior:
 *  - All records are intentionally discarded.
 *  - Flush/close are no-ops.
 */
public final class DevNullSinkPlugin implements SinkPlugin {

    public static final String ID = "DEVNULL";
    private static final Logger log = LoggerFactory.getLogger(DevNullSinkPlugin.class);

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

        log.warn("DEVNULL sink created. All output will be discarded.");
        return new DevNullSink<>();
    }

    private static final class DevNullSink<T> implements OutputSink<T> {

        @Override
        public void write(PipelinePayload<T> payload) {
            // Intentionally discard all data.
        }

        @Override
        public void flush() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
