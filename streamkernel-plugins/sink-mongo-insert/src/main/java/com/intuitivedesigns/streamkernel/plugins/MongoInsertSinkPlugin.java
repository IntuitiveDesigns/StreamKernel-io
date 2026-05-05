/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.BatchOutputSink;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.output.MongoInsertSink;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;
import com.intuitivedesigns.streamkernel.core.OutputSink;

/**
 * SPI registration for the MONGO_INSERT sink plugin.
 *
 * Discovered via:
 *   META-INF/services/com.intuitivedesigns.streamkernel.spi.SinkPlugin
 *
 * Usage in pipeline config:
 *   sink.type=MONGO_INSERT
 *   sink.plugin.id=MONGO_INSERT
 *   mongodb.uri=mongodb://localhost:27017
 *   mongodb.database=support_db
 *   mongodb.collection=sk_insert_baseline
 *   mongodb.bulk.max.ops=500
 */
public final class MongoInsertSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "MONGO_INSERT";
    }

    @Override
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception {
        final MongoInsertSink sink = MongoInsertSink.fromConfig(config, metrics);
        sink.init();
        return sink;
    }
}