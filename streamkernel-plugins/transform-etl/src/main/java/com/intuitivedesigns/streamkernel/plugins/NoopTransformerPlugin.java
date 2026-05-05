/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.TransformerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * A Pass-Through Transformer.
 * Returns the input payload exactly as is.
 * Used for benchmarking or simple ingest pipelines.
 * <p>
 * ID: NOOP
 */
public final class NoopTransformerPlugin implements TransformerPlugin {

    public static final String ID = "NOOP";
    private static final Logger log = LoggerFactory.getLogger(NoopTransformerPlugin.class);

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.TRANSFORMER;
    }

    @Override
    public Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        return new NoopTransformer<>();
    }

    private static final class NoopTransformer<T> implements Transformer<T, T> {
        @Override
        public PipelinePayload<T> transform(PipelinePayload<T> payload) {
            // Pass-through: return the payload container itself
            return payload;
        }
    }
}
