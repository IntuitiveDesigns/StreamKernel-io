/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.config;

import com.intuitivedesigns.streamkernel.core.ChainedTransformer;
import com.intuitivedesigns.streamkernel.core.PipelineOrchestrator;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PipelineFactoryTest {

    private final MetricsRuntime metrics = new TestMetricsRuntime();

    @AfterEach
    void tearDown() throws Exception {
        final Object registry = PipelineContext.takeIfPresent();
        if (registry instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    @Test
    void createSourceRequiresASourceType() {
        final PipelineConfig config = PipelineConfig.from(new Properties(), "inline");

        final IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> PipelineFactory.createSource(config, metrics));

        assertTrue(thrown.getMessage().contains("source.type"));
    }

    @Test
    void createTransformerFallsBackToNoopWhenNoTransformIsConfigured() throws Exception {
        final PipelineConfig config = PipelineConfig.from(baseProperties(), "inline");

        @SuppressWarnings("unchecked")
        final Transformer<String, String> transformer =
                (Transformer<String, String>) PipelineFactory.createTransformer(config, metrics);

        final PipelinePayload<String> payload = new PipelinePayload<>("id-1", "hello");
        assertSame(payload, transformer.transform(payload));
    }

    @Test
    void createTransformerUsesTransformChainAuthoritatively() {
        final Properties props = baseProperties();
        props.setProperty("transform.type", "HTTP_EMBEDDING");
        props.setProperty("transform.chain", "NOOP, NOOP");
        props.setProperty("transform.http.url", "http://localhost:8080/embed");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final Transformer<?, ?> transformer = PipelineFactory.createTransformer(config, metrics);

        assertInstanceOf(ChainedTransformer.class, transformer);
        final ChainedTransformer chained = (ChainedTransformer) transformer;
        assertEquals(2, chained.children().size());
        assertTrue(chained.children().stream()
                .allMatch(step -> step.getClass().getSimpleName().contains("NoopTransformer")));
    }

    @Test
    void createTransformerReturnsSingleStepDirectlyWhenChainHasOneId() {
        final Properties props = baseProperties();
        props.setProperty("transform.type", "HTTP_EMBEDDING");
        props.setProperty("transform.chain", "NOOP");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final Transformer<?, ?> transformer = PipelineFactory.createTransformer(config, metrics);

        assertFalse(transformer instanceof ChainedTransformer);
        assertTrue(transformer.getClass().getSimpleName().contains("NoopTransformer"));
    }

    @Test
    void createPipelineClampsParallelismAndBatchSizeAndDepositsRegistry() throws Exception {
        final Properties props = pipelineProperties();
        props.setProperty("pipeline.parallelism", "0");
        props.setProperty("pipeline.batch.size", "-10");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final PipelineOrchestrator<?, ?> pipeline = PipelineFactory.createPipeline(config, metrics);
        final Object registry = PipelineContext.take();

        assertNotNull(pipeline);
        assertNotNull(registry);
        assertEquals(1, readIntField(pipeline, "parallelism"));
        assertEquals(1, readIntField(pipeline, "batchSize"));
        assertNull(PipelineContext.takeIfPresent());

        if (registry instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    @Test
    void createPipelineAppliesRuntimeSettingsFromPipelineConfig() throws Exception {
        final Properties props = pipelineProperties();
        props.setProperty("streamkernel.source.fetch.lock", "false");
        props.setProperty("streamkernel.executor.mode", "virtual");
        props.setProperty("streamkernel.sink.inflight.max", "123");
        props.setProperty("streamkernel.dlq.error.threshold", "7");
        props.setProperty("streamkernel.cache.force.disabled", "true");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final PipelineOrchestrator<?, ?> pipeline = PipelineFactory.createPipeline(config, metrics);
        final Object registry = PipelineContext.take();

        assertFalse(readBooleanField(pipeline, "sourceFetchLockingEnabled"));
        assertEquals("VIRTUAL", readStringField(pipeline, "executorMode"));
        assertEquals(123, readIntField(pipeline, "inflightCeiling"));
        assertEquals(7L, readLongField(pipeline, "dlqErrorThreshold"));
        assertFalse(readBooleanField(pipeline, "cacheEnabled"));

        if (registry instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    @Test
    void createPipelineLetsSystemPropertiesOverridePipelineConfigRuntimeSettings() throws Exception {
        final String[] keys = {
                "streamkernel.source.fetch.lock",
                "streamkernel.executor.mode",
                "streamkernel.sink.inflight.max",
                "streamkernel.dlq.error.threshold",
                "streamkernel.cache.force.disabled",
                "streamkernel.sink.batch.copy",
                "streamkernel.outbatch.capacity",
                "streamkernel.latency.sample.mask"
        };
        final Properties previous = snapshotSystemProperties(keys);

        try {
            final Properties props = pipelineProperties();
            props.setProperty("pipeline.batch.size", "1000");
            props.setProperty("streamkernel.source.fetch.lock", "true");
            props.setProperty("streamkernel.executor.mode", "fixed");
            props.setProperty("streamkernel.sink.inflight.max", "123");
            props.setProperty("streamkernel.dlq.error.threshold", "7");
            props.setProperty("streamkernel.cache.force.disabled", "false");
            props.setProperty("streamkernel.sink.batch.copy", "true");
            props.setProperty("streamkernel.outbatch.capacity", "1000");
            props.setProperty("streamkernel.latency.sample.mask", "1023");

            System.setProperty("streamkernel.source.fetch.lock", "false");
            System.setProperty("streamkernel.executor.mode", "virtual");
            System.setProperty("streamkernel.sink.inflight.max", "456");
            System.setProperty("streamkernel.dlq.error.threshold", "9");
            System.setProperty("streamkernel.cache.force.disabled", "true");
            System.setProperty("streamkernel.sink.batch.copy", "false");
            System.setProperty("streamkernel.outbatch.capacity", "2000");
            System.setProperty("streamkernel.latency.sample.mask", "0");

            final PipelineConfig config = PipelineConfig.from(props, "inline");
            final PipelineOrchestrator<?, ?> pipeline = PipelineFactory.createPipeline(config, metrics);
            final Object registry = PipelineContext.take();

            assertFalse(readBooleanField(pipeline, "sourceFetchLockingEnabled"));
            assertEquals("VIRTUAL", readStringField(pipeline, "executorMode"));
            assertEquals(456, readIntField(pipeline, "inflightCeiling"));
            assertEquals(9L, readLongField(pipeline, "dlqErrorThreshold"));
            assertFalse(readBooleanField(pipeline, "cacheEnabled"));
            assertFalse(readBooleanField(pipeline, "sinkBatchDefensiveCopy"));
            assertEquals(2000, readIntField(pipeline, "outBatchInitialCapacity"));
            assertEquals(0, readIntField(pipeline, "latencySampleMask"));

            if (registry instanceof AutoCloseable closeable) {
                closeable.close();
            }
        } finally {
            restoreSystemProperties(previous, keys);
        }
    }

    @Test
    void createPipelineRejectsSingleTransformHttpEmbeddingAndClearsContext() {
        final Properties props = pipelineProperties();
        props.setProperty("transform.type", "HTTP_EMBEDDING");
        props.setProperty("transform.http.url", "http://localhost:8080/embed");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> PipelineFactory.createPipeline(config, metrics));

        assertTrue(thrown.getMessage().contains("HTTP_EMBEDDING"));
        assertNull(PipelineContext.takeIfPresent());
    }

    @Test
    void createSecurityDefaultsToPermitAllWhenUnset() {
        final Properties props = baseProperties();
        props.remove("security.type");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final var security = PipelineFactory.createSecurity(config, metrics);
        assertTrue(security.isAllowed("svc", "write", "resource"));
    }

    @Test
    void createSourceWrapsConfiguredSourceWithProvenanceConnector() {
        final Properties props = baseProperties();
        props.setProperty("pipeline.id", "test-pipeline");
        props.setProperty("source.type", "REST");
        props.setProperty("source.rest.base.url", "http://localhost:8080/poll");
        props.setProperty("source.rest.url", "http://localhost:8080/poll");
        props.setProperty("streamkernel.provenance.enabled", "true");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final var source = PipelineFactory.createSource(config, metrics);
        assertEquals("ProvenanceSourceConnector", source.getClass().getSimpleName());
    }

    private static Properties baseProperties() {
        final Properties props = new Properties();
        props.setProperty("sink.type", "DEVNULL");
        props.setProperty("dlq.type", "DEVNULL");
        props.setProperty("cache.type", "NOOP");
        props.setProperty("security.type", "PERMIT_ALL");
        return props;
    }

    private static Properties pipelineProperties() {
        final Properties props = baseProperties();
        props.setProperty("source.type", "REST");
        props.setProperty("source.rest.base.url", "http://localhost:8080/poll");
        props.setProperty("source.rest.url", "http://localhost:8080/poll");
        return props;
    }

    private static int readIntField(Object target, String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private static long readLongField(Object target, String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(target);
    }

    private static boolean readBooleanField(Object target, String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getBoolean(target);
    }

    private static String readStringField(Object target, String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (String) field.get(target);
    }

    private static Properties snapshotSystemProperties(String... keys) {
        final Properties snapshot = new Properties();
        for (String key : keys) {
            final String value = System.getProperty(key);
            if (value != null) {
                snapshot.setProperty(key, value);
            }
        }
        return snapshot;
    }

    private static void restoreSystemProperties(Properties snapshot, String... keys) {
        for (String key : keys) {
            if (snapshot.containsKey(key)) {
                System.setProperty(key, snapshot.getProperty(key));
            } else {
                System.clearProperty(key);
            }
        }
    }

    private static final class TestMetricsRuntime implements MetricsRuntime {
        @Override
        public Object registry() {
            return this;
        }

        @Override
        public void counter(String name, double increment) {
            // no-op
        }

        @Override
        public void timer(String name, long durationMillis) {
            // no-op
        }

        @Override
        public void gauge(String name, double value) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
