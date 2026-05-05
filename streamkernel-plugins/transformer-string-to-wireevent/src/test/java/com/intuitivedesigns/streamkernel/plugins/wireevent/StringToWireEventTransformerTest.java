/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins.wireevent;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StringToWireEventTransformerTest {

    private static final MetricsRuntime NOOP_METRICS = new MetricsRuntime() {
        @Override
        public Object registry() {
            return this;
        }

        @Override
        public void counter(String name, double increment) {
        }

        @Override
        public void timer(String name, long durationMillis) {
        }

        @Override
        public void gauge(String name, double value) {
        }

        @Override
        public void close() {
        }
    };

    @Test
    void transformPreservesTrimmedSourceTextInMetadata() {
        final StringToWireEventTransformer transformer = new StringToWireEventTransformer(
                config(Map.of(StringToWireEventTransformer.KEY_TRIM, "true")),
                NOOP_METRICS
        );
        final PipelinePayload<Object> input = new PipelinePayload<>(
                "payload-1",
                "  customer said hello  ",
                Map.of("existing", "value")
        );

        final PipelinePayload<WireEvent> output = transformer.transform(input);

        assertEquals("customer said hello",
                output.metadata().get(StringToWireEventTransformer.SOURCE_TEXT_METADATA_KEY));
        assertEquals("value", output.metadata().get("existing"));
        assertArrayEquals("customer said hello".getBytes(StandardCharsets.UTF_8), output.data().bytes());
    }

    private static PipelineConfig config(Map<String, String> entries) {
        final Properties props = new Properties();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
        return PipelineConfig.from(props, "test");
    }
}
