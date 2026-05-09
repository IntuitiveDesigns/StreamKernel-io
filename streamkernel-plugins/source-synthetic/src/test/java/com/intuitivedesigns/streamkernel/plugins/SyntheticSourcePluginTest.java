/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.bench.SyntheticSource;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class SyntheticSourcePluginTest {

    private final SyntheticSourcePlugin plugin = new SyntheticSourcePlugin();
    private final MetricsRuntime metrics = new TestMetricsRuntime();

    @Test
    void createDefaultsToUncappedFirehose() throws Exception {
        final SourceConnector<?> connector = plugin.create(PipelineConfig.from(new Properties(), "test"), metrics);

        assertInstanceOf(SyntheticSource.class, connector);
        assertEquals(0L, getLongField(connector, "maxRecordsPerSecond"));
    }

    @Test
    void createAcceptsOptionalRateCap() throws Exception {
        final Properties props = new Properties();
        props.setProperty("source.synthetic.max.records.per.second", "2500");

        final SourceConnector<?> connector = plugin.create(PipelineConfig.from(props, "test"), metrics);

        assertInstanceOf(SyntheticSource.class, connector);
        assertEquals(2500L, getLongField(connector, "maxRecordsPerSecond"));
    }

    private static long getLongField(Object target, String fieldName) throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getLong(target);
    }

    private static final class TestMetricsRuntime implements MetricsRuntime {

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
    }
}
