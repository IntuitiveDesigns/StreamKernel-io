/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelineOrchestrator;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.security.SecurityContext;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class StreamKernelTest {

    @AfterEach
    void tearDown() {
        System.clearProperty("streamkernel.test.bridge");
        System.clearProperty("streamkernel.test.long");
    }

    @Test
    void helperMethodsHandleClampingParsingAndFirstNonBlank() throws Exception {
        assertEquals(5, invokePrivateStatic("clampInt",
                new Class<?>[]{int.class, int.class, int.class}, 9, 1, 5));
        assertEquals(42L, invokePrivateStatic("parseLongSafe",
                new Class<?>[]{String.class, long.class}, " 42 ", 7L));
        assertEquals(7L, invokePrivateStatic("parseLongSafe",
                new Class<?>[]{String.class, long.class}, "bad", 7L));
        assertEquals("first",
                invokePrivateStatic("firstNonBlank", new Class<?>[]{String[].class},
                        (Object) new String[]{"  ", null, " first ", "second"}));
    }

    @Test
    void bridgeConfigToSystemPropertyOnlySetsMissingValues() throws Exception {
        final PipelineConfig config = configWith("streamkernel.test.bridge", "1234");

        invokePrivateStatic("bridgeConfigToSystemProperty",
                new Class<?>[]{PipelineConfig.class, String.class}, config, "streamkernel.test.bridge");
        assertEquals("1234", System.getProperty("streamkernel.test.bridge"));

        System.setProperty("streamkernel.test.bridge", "9999");
        invokePrivateStatic("bridgeConfigToSystemProperty",
                new Class<?>[]{PipelineConfig.class, String.class}, config, "streamkernel.test.bridge");
        assertEquals("9999", System.getProperty("streamkernel.test.bridge"));
    }

    @Test
    void readLongSettingPrefersJvmOverrideWithoutBridgingBackIntoConfig() throws Exception {
        final PipelineConfig config = configWith("streamkernel.test.long", "15");

        assertEquals(15L, invokePrivateStatic("readLongSetting",
                new Class<?>[]{PipelineConfig.class, String.class, long.class},
                config, "streamkernel.test.long", 7L));

        System.setProperty("streamkernel.test.long", "42");
        assertEquals(42L, invokePrivateStatic("readLongSetting",
                new Class<?>[]{PipelineConfig.class, String.class, long.class},
                config, "streamkernel.test.long", 7L));

        System.setProperty("streamkernel.test.long", "bad");
        assertEquals(7L, invokePrivateStatic("readLongSetting",
                new Class<?>[]{PipelineConfig.class, String.class, long.class},
                config, "streamkernel.test.long", 7L));
    }

    @Test
    void metricsHelpersSanitizePipelineIdentityAndPreregisterDashboardSeries() throws Exception {
        final RecordingMetricsRuntime metrics = new RecordingMetricsRuntime();

        invokePrivateStatic("emitPipelineIdentityGauge",
                new Class<?>[]{MetricsRuntime.class, String.class}, metrics, "MongoDB Vector/Bench 01");
        invokePrivateStatic("preregisterDashboardMetrics",
                new Class<?>[]{MetricsRuntime.class}, metrics);

        assertEquals(1.0, metrics.gauges.get("streamkernel_pipeline_up_mongodb_vector_bench_01"));
        assertTrue(metrics.counters.containsKey("streamkernel_pipeline_in_total"));
        assertTrue(metrics.counters.containsKey("streamkernel_pipeline_dlq_errors_total"));
        assertTrue(metrics.gauges.containsKey("streamkernel_pipeline_latency_p99_ms"));
        assertTrue(metrics.gauges.containsKey("streamkernel_jvm_heap_used_mb"));
    }

    @Test
    void emitPipelineIdentityGaugeUsesRootLocale() throws Exception {
        final RecordingMetricsRuntime metrics = new RecordingMetricsRuntime();
        final Locale previous = Locale.getDefault();

        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            invokePrivateStatic("emitPipelineIdentityGauge",
                    new Class<?>[]{MetricsRuntime.class, String.class}, metrics, "PIPELINE-ID");
        } finally {
            Locale.setDefault(previous);
        }

        assertEquals(1.0, metrics.gauges.get("streamkernel_pipeline_up_pipeline_id"));
    }

    @Test
    void isMetricsEnabledTreatsThrowingImplementationsAsActive() throws Exception {
        final MetricsRuntime throwingMetrics = new RecordingMetricsRuntime() {
            @Override
            public boolean enabled() {
                throw new IllegalStateException("boom");
            }
        };

        assertEquals(false, invokePrivateStatic("isMetricsEnabled",
                new Class<?>[]{MetricsRuntime.class}, (Object) null));
        assertEquals(true, invokePrivateStatic("isMetricsEnabled",
                new Class<?>[]{MetricsRuntime.class}, throwingMetrics));
    }

    @Test
    void pipelineMetricsPublisherPublishesDeltasAndFinalZeroGauges() throws Exception {
        final RecordingMetricsRuntime metrics = new RecordingMetricsRuntime();
        final PipelineOrchestrator<String, String> pipeline = new PipelineOrchestrator<>(
                new IdleSource<>(),
                new CollectingSink<>(),
                new CollectingSink<>(),
                new PassThroughTransformer(),
                new NoopCache(),
                metrics,
                1,
                4,
                new FixedSecurityProvider(),
                SecurityContext.of("svc", "write", "resource"),
                false,
                0L,
                TimeUnit.SECONDS.toNanos(1));

        final Object publisher = newPipelineMetricsPublisher(pipeline, metrics, "pipeline-1");

        invokeProcessBatch(pipeline, List.of(
                new PipelinePayload<>("id-1", "one"),
                new PipelinePayload<>("id-2", "two")
        ));

        ((Runnable) publisher).run();

        assertEquals(2.0, metrics.counters.get("streamkernel_pipeline_processed_total"));
        assertEquals(2.0, metrics.counters.get("streamkernel_pipeline_out_total"));
        assertEquals(1.0, metrics.gauges.get("streamkernel_pipeline_up_pipeline_1"));

        final Method publishFinal = publisher.getClass().getDeclaredMethod("publishFinal");
        publishFinal.setAccessible(true);
        publishFinal.invoke(publisher);

        assertEquals(0.0, metrics.gauges.get("streamkernel_pipeline_inflight_batches"));
        assertEquals(0.0, metrics.gauges.get("streamkernel_pipeline_inflight_records"));
        assertEquals(0.0, metrics.gauges.get("streamkernel_pipeline_load_percent"));
        assertEquals(0.0, metrics.gauges.get("streamkernel_pipeline_latency_samples"));
    }

    @Test
    void launchAsyncShutdownRunsOnDedicatedThread() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<String> threadName = new AtomicReference<>();

        invokePrivateStatic("launchAsyncShutdown",
                new Class<?>[]{Runnable.class, String.class},
                (Runnable) () -> {
                    threadName.set(Thread.currentThread().getName());
                    latch.countDown();
                },
                "sk-test-shutdown");

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals("sk-test-shutdown", threadName.get());
        assertNotEquals(Thread.currentThread().getName(), threadName.get());
    }

    private static Object invokePrivateStatic(String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        final Method method = StreamKernel.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }

    private static Object newPipelineMetricsPublisher(PipelineOrchestrator<?, ?> pipeline,
                                                      MetricsRuntime metrics,
                                                      String pipelineId) throws Exception {
        final Class<?> publisherClass = Class.forName(
                "com.intuitivedesigns.streamkernel.StreamKernel$PipelineMetricsPublisher");
        final Constructor<?> ctor = publisherClass.getDeclaredConstructor(
                PipelineOrchestrator.class, MetricsRuntime.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(pipeline, metrics, pipelineId);
    }

    private static void invokeProcessBatch(PipelineOrchestrator<String, String> pipeline,
                                           List<PipelinePayload<String>> batch) throws Exception {
        final Method processBatch = PipelineOrchestrator.class.getDeclaredMethod("processBatch", List.class);
        processBatch.setAccessible(true);
        processBatch.invoke(pipeline, batch);
    }

    private static PipelineConfig configWith(String key, String value) {
        final java.util.Properties props = new java.util.Properties();
        props.setProperty(key, value);
        return PipelineConfig.from(props, "inline");
    }

    private static class RecordingMetricsRuntime implements MetricsRuntime {
        private final Map<String, Double> counters = new HashMap<>();
        private final Map<String, Double> gauges = new HashMap<>();

        @Override
        public Object registry() {
            return this;
        }

        @Override
        public void counter(String name, double increment) {
            counters.merge(name, increment, Double::sum);
        }

        @Override
        public void timer(String name, long durationMillis) {
            // no-op
        }

        @Override
        public void gauge(String name, double value) {
            gauges.put(name, value);
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static final class IdleSource<T> implements SourceConnector<T> {
        @Override
        public void connect() {
            // no-op
        }

        @Override
        public void disconnect() {
            // no-op
        }

        @Override
        public PipelinePayload<T> fetch() {
            return null;
        }
    }

    private static final class CollectingSink<T> implements OutputSink<T> {
        private final List<PipelinePayload<T>> writes = new ArrayList<>();

        @Override
        public void write(PipelinePayload<T> payload) {
            writes.add(payload);
        }
    }

    private static final class PassThroughTransformer implements Transformer<String, String> {
        @Override
        public PipelinePayload<String> transform(PipelinePayload<String> input) {
            return input;
        }
    }

    private static final class NoopCache implements Cache<Object, Object> {
        @Override
        public Optional<Object> get(Object key) {
            return Optional.empty();
        }

        @Override
        public void put(Object key, Object value) {
            // no-op
        }

        @Override
        public void invalidate(Object key) {
            // no-op
        }
    }

    private static final class FixedSecurityProvider implements SecurityProvider {
        @Override
        public boolean isAllowed(String identity, String action, String resource) {
            return true;
        }
    }
}
