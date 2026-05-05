/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins.wireevent;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.plugins.wireevent.StringToWireEventTransformer;
import com.intuitivedesigns.streamkernel.spi.TransformerPlugin;

import java.util.Objects;

public final class StringToWireEventTransformPlugin implements TransformerPlugin {

    public static final String ID = "STRING_TO_WIREEVENT";

    private static String getString(PipelineConfig cfg, String key, String def) {
        try {
            Object v = cfg.getClass().getMethod("getString", String.class, String.class).invoke(cfg, key, def);
            if (v instanceof String s) return s;
        } catch (Exception ignored) {
        }
        try {
            Object v = cfg.getClass().getMethod("get", String.class).invoke(cfg, key);
            return (v == null) ? def : String.valueOf(v);
        } catch (Exception ignored) {
        }
        return def;
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        // Preferred: config-driven transformer (keeps all functionality)
        return new StringToWireEventTransformer(config, metrics);

        /*
         * If you ever wanted the older behavior explicitly, you could switch to:
         *
         * final String defaultKey = getString(config, "transform.string_to_wireevent.default.key", "sk");
         * return new StringToWireEventTransformer(defaultKey);
         *
         * But the config constructor already reads that property, plus more.
         */
    }
}

