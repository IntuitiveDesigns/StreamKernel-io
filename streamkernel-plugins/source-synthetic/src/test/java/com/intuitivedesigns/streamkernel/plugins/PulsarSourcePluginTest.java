/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.sources.PulsarSourceConnector;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PulsarSourcePluginTest {

    private final PulsarSourcePlugin plugin = new PulsarSourcePlugin();
    private final MetricsRuntime metrics = new TestMetricsRuntime();

    @Test
    void createAcceptsCanonicalKeys() {
        final Properties props = new Properties();
        props.setProperty("source.pulsar.service.url", "pulsar://localhost:6650");
        props.setProperty("source.pulsar.topic", "persistent://public/default/streamkernel-bench-in");
        props.setProperty("source.pulsar.subscription", "streamkernel-pulsar-delta");
        props.setProperty("source.pulsar.batch.size", "64");

        final SourceConnector<?> connector = plugin.create(PipelineConfig.from(props, "test"), metrics);

        assertInstanceOf(PulsarSourceConnector.class, connector);
    }

    @Test
    void createAcceptsArticleFriendlyAliases() {
        final Properties props = new Properties();
        props.setProperty("pulsar.service.url", "pulsar://localhost:6650");
        props.setProperty("pulsar.topic", "persistent://public/default/streamkernel-bench-in");
        props.setProperty("pulsar.subscription", "streamkernel-pulsar-delta");
        props.setProperty("pulsar.batch.size", "64");

        final SourceConnector<?> connector = plugin.create(PipelineConfig.from(props, "test"), metrics);

        assertInstanceOf(PulsarSourceConnector.class, connector);
    }

    @Test
    void createRejectsMissingTopic() {
        final Properties props = new Properties();
        props.setProperty("source.pulsar.service.url", "pulsar://localhost:6650");
        props.setProperty("source.pulsar.subscription", "streamkernel-pulsar-delta");

        assertThrows(IllegalArgumentException.class,
                () -> plugin.create(PipelineConfig.from(props, "test"), metrics));
    }

    @Test
    void createDefaultsAckTimeoutWhenAckOnFetchDisabled() throws Exception {
        final Properties props = new Properties();
        props.setProperty("source.pulsar.service.url", "pulsar://localhost:6650");
        props.setProperty("source.pulsar.topic", "persistent://public/default/streamkernel-bench-in");
        props.setProperty("source.pulsar.subscription", "streamkernel-pulsar-delta");
        props.setProperty("source.pulsar.acknowledge.on.fetch", "false");

        final PulsarSourceConnector connector = (PulsarSourceConnector) plugin.create(PipelineConfig.from(props, "test"), metrics);

        assertEquals(30_000, getIntField(connector, "ackTimeoutMs"));
    }

    @Test
    void createAcceptsExplicitAckTimeoutOverride() throws Exception {
        final Properties props = new Properties();
        props.setProperty("source.pulsar.service.url", "pulsar://localhost:6650");
        props.setProperty("source.pulsar.topic", "persistent://public/default/streamkernel-bench-in");
        props.setProperty("source.pulsar.subscription", "streamkernel-pulsar-delta");
        props.setProperty("source.pulsar.acknowledge.on.fetch", "false");
        props.setProperty("source.pulsar.ack.timeout.ms", "45000");

        final PulsarSourceConnector connector = (PulsarSourceConnector) plugin.create(PipelineConfig.from(props, "test"), metrics);

        assertEquals(45_000, getIntField(connector, "ackTimeoutMs"));
    }

    private static int getIntField(Object target, String fieldName) throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getInt(target);
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
