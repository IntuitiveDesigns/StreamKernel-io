/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.security.SecurityContext;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PipelineOrchestratorTest {

    @Test
    void processBatchUsesBatchTransformerResultsInOrder() throws Exception {
        final CollectingSink<String> primarySink = new CollectingSink<>();
        final CollectingSink<String> dlqSink = new CollectingSink<>();
        final ScriptedBatchTransformer transformer = new ScriptedBatchTransformer(List.of(
                BatchTransformer.Result.success(new PipelinePayload<>("id-1", "OUT-1")),
                BatchTransformer.Result.success(null),
                BatchTransformer.Result.failure(new IllegalStateException("boom")),
                BatchTransformer.Result.success(new PipelinePayload<>("id-4", "OUT-4"))
        ));

        final PipelineOrchestrator<String, String> orchestrator = new PipelineOrchestrator<>(
                new IdleSource<>(),
                primarySink,
                dlqSink,
                transformer,
                new NoopCache(),
                new TestMetricsRuntime(),
                1,
                4,
                new FixedSecurityProvider(true),
                SecurityContext.of("svc", "write", "resource"),
                false,
                0L,
                TimeUnit.SECONDS.toNanos(1));

        invokeProcessBatch(orchestrator, List.of(
                new PipelinePayload<>("id-1", "one"),
                new PipelinePayload<>("id-2", "two"),
                new PipelinePayload<>("id-3", "three"),
                new PipelinePayload<>("id-4", "four")
        ));

        assertEquals(List.of("OUT-1", "OUT-4"), primarySink.payloads());
        assertEquals(List.of("id-3"), dlqSink.ids());
        assertEquals(2L, orchestrator.processedTotal());
        assertEquals(2L, orchestrator.outTotal());
        assertEquals(1L, orchestrator.droppedTotal());
        assertEquals(1L, orchestrator.dlqTotal());
        assertEquals(0L, orchestrator.deniedTotal());
        assertEquals(1, transformer.batchCalls.get());
    }

    @Test
    void processBatchFailsClosedWhenBatchAuthorizationIsDenied() throws Exception {
        final CollectingSink<String> primarySink = new CollectingSink<>();
        final CollectingSink<String> dlqSink = new CollectingSink<>();
        final ScriptedBatchTransformer transformer = new ScriptedBatchTransformer(List.of(
                BatchTransformer.Result.success(new PipelinePayload<>("id-1", "OUT-1")),
                BatchTransformer.Result.success(new PipelinePayload<>("id-2", "OUT-2"))
        ));

        final PipelineOrchestrator<String, String> orchestrator = new PipelineOrchestrator<>(
                new IdleSource<>(),
                primarySink,
                dlqSink,
                transformer,
                new NoopCache(),
                new TestMetricsRuntime(),
                1,
                2,
                new FixedSecurityProvider(false),
                SecurityContext.of("svc", "write", "resource"),
                false,
                0L,
                TimeUnit.SECONDS.toNanos(1));

        invokeProcessBatch(orchestrator, List.of(
                new PipelinePayload<>("id-1", "one"),
                new PipelinePayload<>("id-2", "two")
        ));

        assertTrue(primarySink.writes.isEmpty());
        assertEquals(List.of("id-1", "id-2"), dlqSink.ids());
        assertEquals(2L, orchestrator.deniedTotal());
        assertEquals(2L, orchestrator.dlqTotal());
        assertEquals(0L, orchestrator.outTotal());
        assertEquals(0L, orchestrator.droppedTotal());
        assertEquals(0, transformer.batchCalls.get());
    }

    @Test
    void startInitializesChainComponentsAndStopsCleanly() throws Exception {
        final TrackingSource<Object> source = new TrackingSource<>();
        final TrackingSink<Object> primarySink = new TrackingSink<>();
        final TrackingSink<Object> dlqSink = new TrackingSink<>();
        final TrackingTransformer first = new TrackingTransformer();
        final TrackingTransformer second = new TrackingTransformer();
        final ChainedTransformer chain = new ChainedTransformer(new Transformer[]{first, second});
        final TrackingCache cache = new TrackingCache();
        final TrackingSecurityProvider security = new TrackingSecurityProvider();

        final PipelineOrchestrator<Object, Object> orchestrator = new PipelineOrchestrator<>(
                source,
                primarySink,
                dlqSink,
                chain,
                cache,
                new TestMetricsRuntime(),
                1,
                1,
                security,
                SecurityContext.of("svc", "write", "resource"),
                false,
                0L,
                TimeUnit.MILLISECONDS.toNanos(50));

        orchestrator.start();
        Thread.sleep(40L);
        orchestrator.stop();

        assertEquals(1, cache.initCalls.get());
        assertEquals(1, security.initCalls.get());
        assertEquals(1, first.initCalls.get());
        assertEquals(1, second.initCalls.get());
        assertEquals(1, primarySink.initCalls.get());
        assertEquals(1, dlqSink.initCalls.get());
        assertEquals(1, source.connectCalls.get());
        assertEquals(1, source.disconnectCalls.get());
        assertEquals(1, cache.closeCalls.get());
        assertEquals(1, security.closeCalls.get());
    }

    @Test
    void deferredPrimarySinkCountsSuccessOnlyAfterCompletionDrain() throws Exception {
        final ManualDeferredSink<String> primarySink = new ManualDeferredSink<>();
        final CollectingSink<String> dlqSink = new CollectingSink<>();

        final PipelineOrchestrator<String, String> orchestrator = new PipelineOrchestrator<>(
                new IdleSource<>(),
                primarySink,
                dlqSink,
                new IdentityTransformer<>(),
                new NoopCache(),
                new TestMetricsRuntime(),
                1,
                4,
                new FixedSecurityProvider(true),
                SecurityContext.of("svc", "write", "resource"),
                false,
                0L,
                TimeUnit.SECONDS.toNanos(1));

        invokeProcessBatch(orchestrator, List.of(
                new PipelinePayload<>("id-1", "one"),
                new PipelinePayload<>("id-2", "two")
        ));

        assertEquals(0L, orchestrator.processedTotal());
        assertEquals(0L, orchestrator.outTotal());
        assertTrue(dlqSink.writes.isEmpty());

        primarySink.releaseNextCompletion();
        invokeProcessBatch(orchestrator, List.of());

        assertEquals(2L, orchestrator.processedTotal());
        assertEquals(2L, orchestrator.outTotal());
        assertEquals(0L, orchestrator.dlqTotal());
        assertEquals(List.of("id-1", "id-2"), primarySink.lastSourceIds());
    }

    @Test
    void deferredPrimarySinkFailureRoutesOriginalInputsToDlq() throws Exception {
        final ManualDeferredSink<String> primarySink = new ManualDeferredSink<>();
        final CollectingSink<String> dlqSink = new CollectingSink<>();

        final PipelineOrchestrator<String, String> orchestrator = new PipelineOrchestrator<>(
                new IdleSource<>(),
                primarySink,
                dlqSink,
                new IdentityTransformer<>(),
                new NoopCache(),
                new TestMetricsRuntime(),
                1,
                4,
                new FixedSecurityProvider(true),
                SecurityContext.of("svc", "write", "resource"),
                false,
                0L,
                TimeUnit.SECONDS.toNanos(1));

        primarySink.failNext(new IllegalStateException("delta down"));
        invokeProcessBatch(orchestrator, List.of(
                new PipelinePayload<>("id-1", "one"),
                new PipelinePayload<>("id-2", "two")
        ));

        assertEquals(0L, orchestrator.processedTotal());
        assertTrue(dlqSink.writes.isEmpty());

        primarySink.releaseNextCompletion();
        invokeProcessBatch(orchestrator, List.of());

        assertEquals(List.of("id-1", "id-2"), dlqSink.ids());
        assertEquals(2L, orchestrator.dlqTotal());
        assertEquals(0L, orchestrator.processedTotal());
        assertEquals(0L, orchestrator.outTotal());
    }

    @Test
    void stopDrainsDeferredPrimarySinkCompletionsAfterClose() throws Exception {
        final TrackingSource<String> source = new TrackingSource<>();
        final ManualDeferredSink<String> primarySink = new ManualDeferredSink<>();
        final CollectingSink<String> dlqSink = new CollectingSink<>();

        final PipelineOrchestrator<String, String> orchestrator = new PipelineOrchestrator<>(
                source,
                primarySink,
                dlqSink,
                new IdentityTransformer<>(),
                new NoopCache(),
                new TestMetricsRuntime(),
                1,
                4,
                new FixedSecurityProvider(true),
                SecurityContext.of("svc", "write", "resource"),
                false,
                0L,
                TimeUnit.SECONDS.toNanos(1));

        orchestrator.start();
        invokeProcessBatch(orchestrator, List.of(new PipelinePayload<>("id-1", "one")));

        assertEquals(0L, orchestrator.processedTotal());

        orchestrator.stop();

        assertEquals(1L, orchestrator.processedTotal());
        assertEquals(1L, orchestrator.outTotal());
        assertEquals(1, source.disconnectCalls.get());
    }

    private static void invokeProcessBatch(PipelineOrchestrator<String, String> orchestrator,
                                           List<PipelinePayload<String>> batch) throws Exception {
        final Method method = PipelineOrchestrator.class.getDeclaredMethod("processBatch", List.class);
        method.setAccessible(true);
        method.invoke(orchestrator, batch);
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

    private static class IdleSource<T> implements SourceConnector<T> {
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

    private static final class TrackingSource<T> extends IdleSource<T> {
        private final AtomicInteger connectCalls = new AtomicInteger();
        private final AtomicInteger disconnectCalls = new AtomicInteger();

        @Override
        public void connect() {
            connectCalls.incrementAndGet();
        }

        @Override
        public void disconnect() {
            disconnectCalls.incrementAndGet();
        }
    }

    private static class CollectingSink<T> implements OutputSink<T> {
        protected final List<PipelinePayload<T>> writes = new ArrayList<>();

        @Override
        public void write(PipelinePayload<T> payload) {
            writes.add(payload);
        }

        List<String> ids() {
            return writes.stream().map(PipelinePayload::id).toList();
        }

        List<T> payloads() {
            return writes.stream().map(PipelinePayload::data).toList();
        }
    }

    private static final class TrackingSink<T> extends CollectingSink<T> {
        private final AtomicInteger initCalls = new AtomicInteger();

        void init() {
            initCalls.incrementAndGet();
        }
    }

    private static final class ScriptedBatchTransformer
            implements Transformer<String, String>, BatchTransformer<String, String> {

        private final List<BatchTransformer.Result<String>> scriptedResults;
        private final AtomicInteger batchCalls = new AtomicInteger();

        private ScriptedBatchTransformer(List<BatchTransformer.Result<String>> scriptedResults) {
            this.scriptedResults = scriptedResults;
        }

        @Override
        public PipelinePayload<String> transform(PipelinePayload<String> input) {
            throw new AssertionError("Single-record transform path should not be used");
        }

        @Override
        public List<BatchTransformer.Result<String>> transformBatch(List<PipelinePayload<String>> inputs) {
            batchCalls.incrementAndGet();
            return scriptedResults;
        }
    }

    private static final class IdentityTransformer<T> implements Transformer<T, T> {
        @Override
        public PipelinePayload<T> transform(PipelinePayload<T> input) {
            return input;
        }
    }

    private static final class ManualDeferredSink<T>
            implements DeferredBatchOutputSink<T, T> {

        private final ArrayDeque<DeferredWriteResult<T>> staged = new ArrayDeque<>();
        private final ArrayDeque<DeferredWriteResult<T>> completed = new ArrayDeque<>();
        private final ArrayList<List<PipelinePayload<T>>> submittedSources = new ArrayList<>();
        private Exception nextFailure;

        @Override
        public void write(PipelinePayload<T> payload) {
            throw new UnsupportedOperationException("Deferred sink write() path should not be used");
        }

        @Override
        public void writeBatchDeferred(
                List<PipelinePayload<T>> batch,
                List<PipelinePayload<T>> sourceInputs) {
            submittedSources.add(List.copyOf(sourceInputs));
            staged.addLast(new DeferredWriteResult<>(sourceInputs, nextFailure));
            nextFailure = null;
        }

        @Override
        public List<DeferredWriteResult<T>> drainCompletedWrites() {
            if (completed.isEmpty()) {
                return List.of();
            }
            final ArrayList<DeferredWriteResult<T>> drained = new ArrayList<>(completed);
            completed.clear();
            return drained;
        }

        @Override
        public void close() {
            while (!staged.isEmpty()) {
                completed.addLast(staged.removeFirst());
            }
        }

        void failNext(Exception failure) {
            this.nextFailure = failure;
        }

        void releaseNextCompletion() {
            completed.addLast(staged.removeFirst());
        }

        List<String> lastSourceIds() {
            if (submittedSources.isEmpty()) {
                return List.of();
            }
            return submittedSources.get(submittedSources.size() - 1)
                    .stream()
                    .map(PipelinePayload::id)
                    .toList();
        }
    }

    private static final class TrackingTransformer implements Transformer<Object, Object> {
        private final AtomicInteger initCalls = new AtomicInteger();

        public void init() {
            initCalls.incrementAndGet();
        }

        @Override
        public PipelinePayload<Object> transform(PipelinePayload<Object> input) {
            return input;
        }
    }

    private static class NoopCache implements Cache<Object, Object> {
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

    private static final class TrackingCache extends NoopCache {
        private final AtomicInteger initCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();

        void init() {
            initCalls.incrementAndGet();
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }
    }

    private static class FixedSecurityProvider implements SecurityProvider {
        private final boolean allowed;

        private FixedSecurityProvider(boolean allowed) {
            this.allowed = allowed;
        }

        @Override
        public boolean isAllowed(String identity, String action, String resource) {
            return allowed;
        }
    }

    private static final class TrackingSecurityProvider extends FixedSecurityProvider {
        private final AtomicInteger initCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();

        private TrackingSecurityProvider() {
            super(true);
        }

        void init() {
            initCalls.incrementAndGet();
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }
    }
}
