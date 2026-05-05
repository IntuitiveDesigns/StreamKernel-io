/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.config.PipelineFactory;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.output.KafkaDlqBytesSink;
import com.intuitivedesigns.streamkernel.spi.DlqSerializer;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;

public final class KafkaDlqSinkPlugin implements SinkPlugin {

    @Override
    public String id() {
        return "KAFKA_DLQ";
    }

    @Override
    @SuppressWarnings("unchecked")
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception {
        String topic = config.getString("dlq.topic", "streamkernel-dlq");

        DlqSerializer<?> serializer = PipelineFactory.createDlqSerializer(config, metrics);

        // Raw cast is acceptable here because serializer selection is config-driven and DLQ is an edge sink.
        return new KafkaDlqBytesSink<>(topic, KafkaDlqBytesSink.buildProducerProps(config), (DlqSerializer<Object>) serializer);
    }
}



