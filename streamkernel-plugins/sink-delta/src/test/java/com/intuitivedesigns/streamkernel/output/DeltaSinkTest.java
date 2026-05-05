/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeltaSinkTest {

    @Test
    void normalizeEmbeddingReturnsOriginalListWhenAlreadyClean() throws Exception {
        final List<Float> embedding = new java.util.ArrayList<>(List.of(1.0f, 2.0f, 3.0f));

        final Object normalized = invokePrivateStatic(
                "normalizeEmbedding",
                new Class<?>[]{List.class},
                embedding
        );

        assertSame(embedding, normalized);
    }

    @Test
    void normalizeEmbeddingDropsNullElements() throws Exception {
        final List<Float> embedding = java.util.Arrays.asList(1.0f, null, 3.0f);

        assertEquals(
                List.of(1.0f, 3.0f),
                invokePrivateStatic("normalizeEmbedding", new Class<?>[]{List.class}, embedding)
        );
    }

    @Test
    void rethrowIfFailedReturnsFreshExceptionPerWaiter() throws Exception {
        final Object pendingWrite = newPendingWrite();
        final Exception failure = new IllegalStateException("boom");

        invokePrivate(pendingWrite, "complete", new Class<?>[]{Exception.class}, failure);

        final Exception first = assertThrows(Exception.class,
                () -> invokePrivate(pendingWrite, "rethrowIfFailed", new Class<?>[0]));
        final Exception second = assertThrows(Exception.class,
                () -> invokePrivate(pendingWrite, "rethrowIfFailed", new Class<?>[0]));

        assertNotSame(first, second);
        assertSame(failure, first.getCause());
        assertSame(failure, second.getCause());
    }

    @Test
    void floatListColumnVectorRejectsNullElement() throws Exception {
        final Object vector = newFloatListColumnVector(java.util.Arrays.asList(1.0f, null));

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> invokePrivate(vector, "getFloat", new Class<?>[]{int.class}, 1)
        );

        assertEquals("Float embedding contains null element at index 1", thrown.getMessage());
    }

    @Test
    void primitiveFloatArrayColumnVectorReadsPrimitiveValues() throws Exception {
        final Object vector = newPrimitiveFloatArrayColumnVector(new float[]{1.0f, 2.5f});

        assertEquals(2.5f, invokePrivate(vector, "getFloat", new Class<?>[]{int.class}, 1));
    }

    @Test
    void singletonIteratorYieldsSingleElementThenEnds() throws Exception {
        final Object iterator = newSingletonIterator("value");

        assertEquals(true, invokePrivate(iterator, "hasNext", new Class<?>[0]));
        assertEquals("value", invokePrivate(iterator, "next", new Class<?>[0]));
        assertEquals(false, invokePrivate(iterator, "hasNext", new Class<?>[0]));
    }

    @Test
    void ensureTableExistsStateReturnsCachedTrueWithoutFilesystemLookup() throws Exception {
        final DeltaSink sink = newDeltaSink(null);
        atomicBooleanField(sink, "tableKnownToExist").set(true);

        assertEquals(true, invokePrivate(sink, "ensureTableExistsState", new Class<?>[0]));
    }

    @Test
    void closeWaitsForCommitThreadEvenWhenInterrupted() throws Exception {
        final DeltaSink sink = newDeltaSink(new Configuration());
        final Thread commitThread = new Thread(() -> {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }, "delta-test-commit");
        commitThread.start();
        setField(sink, "commitThread", commitThread);

        Thread.currentThread().interrupt();
        try {
            sink.close();
            commitThread.join(1_000L);
            assertEquals(false, commitThread.isAlive());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void preparePendingWriteAcceptsEmbeddedWireEventPayloads() throws Exception {
        final DeltaSink sink = newDeltaSink(null);
        final WireEvent wireEvent = WireEvent.vector(
                "trimmed".getBytes(StandardCharsets.UTF_8),
                new float[]{1.0f, 2.0f},
                Map.of(),
                "ticket-1"
        );
        final PipelinePayload<Object> payload = new PipelinePayload<>(
                "payload-1",
                wireEvent,
                Map.of("streamkernel.source.text", "customer said hello")
        );

        final Object pendingWrite = invokePrivate(
                sink,
                "preparePendingWrite",
                new Class<?>[]{List.class, List.class, boolean.class},
                List.of(payload),
                null,
                false
        );

        assertEquals(1, invokePrivate(pendingWrite, "rowCount", new Class<?>[0]));
        final Object row = firstPendingRow(pendingWrite);
        assertEquals("ticket-1", recordValue(row, "ticketId"));
        assertEquals("customer said hello", recordValue(row, "description"));
        assertEquals("NEUTRAL", recordValue(row, "sentiment"));
        assertArrayEquals(new float[]{1.0f, 2.0f}, (float[]) recordValue(row, "embedding"));
    }

    private static Object invokePrivateStatic(String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        final Method method = DeltaSink.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }

    private static <T extends Throwable> RuntimeException sneaky(Throwable throwable) throws T {
        throw (T) throwable;
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) {
        try {
            final Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.invoke(target, args);
        } catch (ReflectiveOperationException e) {
            final Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            throw sneaky(cause);
        }
    }

    private static Object newPendingWrite() throws Exception {
        final Class<?> type = Class.forName(
                "com.intuitivedesigns.streamkernel.output.DeltaSink$PendingWrite");
        final Constructor<?> ctor = type.getDeclaredConstructor(List.class, List.class, boolean.class);
        ctor.setAccessible(true);
        return ctor.newInstance(List.of(), List.of(), false);
    }

    private static Object newFloatListColumnVector(List<Float> values) throws Exception {
        final Class<?> type = Class.forName(
                "com.intuitivedesigns.streamkernel.output.DeltaSink$FloatListColumnVector");
        final Constructor<?> ctor = type.getDeclaredConstructor(List.class);
        ctor.setAccessible(true);
        return ctor.newInstance(values);
    }

    private static Object newPrimitiveFloatArrayColumnVector(float[] values) throws Exception {
        final Class<?> type = Class.forName(
                "com.intuitivedesigns.streamkernel.output.DeltaSink$PrimitiveFloatArrayColumnVector");
        final Constructor<?> ctor = type.getDeclaredConstructor(float[].class);
        ctor.setAccessible(true);
        return ctor.newInstance((Object) values);
    }

    private static Object newSingletonIterator(Object value) throws Exception {
        final Class<?> type = Class.forName(
                "com.intuitivedesigns.streamkernel.output.DeltaSink$SingletonIterator");
        final Constructor<?> ctor = type.getDeclaredConstructor(Object.class);
        ctor.setAccessible(true);
        return ctor.newInstance(value);
    }

    private static DeltaSink newDeltaSink(Configuration hadoopConf) throws Exception {
        final Constructor<DeltaSink> ctor = DeltaSink.class.getDeclaredConstructor(
                com.intuitivedesigns.streamkernel.metrics.MetricsRuntime.class,
                String.class,
                boolean.class,
                int.class,
                int.class,
                int.class,
                long.class,
                boolean.class,
                Configuration.class
        );
        ctor.setAccessible(true);
        return ctor.newInstance(null, "s3a://streamkernel-delta/enriched-tickets",
                true, 10, 3, 256, 250L, false, hadoopConf);
    }

    private static AtomicBoolean atomicBooleanField(Object target, String fieldName) throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (AtomicBoolean) field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @SuppressWarnings("unchecked")
    private static Object firstPendingRow(Object pendingWrite) throws Exception {
        final Field field = pendingWrite.getClass().getDeclaredField("rows");
        field.setAccessible(true);
        return ((List<Object>) field.get(pendingWrite)).get(0);
    }

    private static Object recordValue(Object record, String accessorName) throws Exception {
        final Method accessor = record.getClass().getDeclaredMethod(accessorName);
        accessor.setAccessible(true);
        return accessor.invoke(record);
    }
}
