/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

// streamkernel-plugins/sink-mongo-vector/src/main/java/com/intuitivedesigns/streamkernel/plugins/MongoVectorSinkPlugin.java
/*
 * Copyright 2026 Steven Lopez
 * SPDX-License-Identifier: Elastic-2.0
 *
 * Licensed under the Elastic License 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://www.elastic.co/licensing/elastic-license
 *
 * Licensor: Steven Lopez / StreamKernel
 */
package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.output.MongoVectorSink;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;

import java.util.Objects;

public final class MongoVectorSinkPlugin implements SinkPlugin {

    public static final String ID = "MONGO_VECTOR";

    @Override
    public String id() {
        return ID;
    }

    @Override
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");
        return MongoVectorSink.fromConfig(config, metrics);
    }
}
