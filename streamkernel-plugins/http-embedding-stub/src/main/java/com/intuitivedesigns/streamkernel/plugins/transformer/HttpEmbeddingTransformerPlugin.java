/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins.transformer;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.TransformerPlugin;

import java.util.Objects;

/**
 * StreamKernel Plugin: HTTP_EMBEDDING Transformer
 *
 * Registers {@link HttpEmbeddingTransformer} under the stable plugin identifier "HTTP_EMBEDDING".
 *
 * Config usage:
 *   transform.type=HTTP_EMBEDDING
 * or (if you support chains):
 *   transform.chain=...,HTTP_EMBEDDING,...
 */
public final class HttpEmbeddingTransformerPlugin implements TransformerPlugin {

    @Override
    public String id() {
        return "HTTP_EMBEDDING";
    }

    @Override
    public Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");
        return new HttpEmbeddingTransformer(config, metrics);
    }
}
