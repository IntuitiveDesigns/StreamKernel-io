/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins.transform;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.TransformerPlugin;

import java.util.Objects;

public final class DeterministicEnrichmentPlugin implements TransformerPlugin {

    public static final String ID = "DETERMINISTIC_ENRICHMENT";

    private static final String KEY_DIMENSION = "transform.deterministic_enrichment.dimension";
    private static final int DEFAULT_DIMENSION = 16;
    private static final int MAX_DIMENSION = 4096;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        final int dimension = clamp(config.getInt(KEY_DIMENSION, DEFAULT_DIMENSION), 1, MAX_DIMENSION);
        return new DeterministicEnrichmentTransformer(dimension);
    }

    private static int clamp(int value, int min, int max) {
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }
}
