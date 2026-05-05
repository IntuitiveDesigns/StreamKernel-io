/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.config;

import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.CacheRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineContextTest {

    @AfterEach
    void tearDown() throws Exception {
        final CacheRegistry registry = PipelineContext.takeIfPresent();
        if (registry != null) {
            registry.close();
        }
    }

    @Test
    void takeThrowsWhenNothingWasDeposited() {
        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                PipelineContext::take);

        assertTrue(thrown.getMessage().contains("same thread"));
    }

    @Test
    void setRejectsNullRegistry() {
        assertThrows(NullPointerException.class, () -> PipelineContext.set(null));
        assertNull(PipelineContext.takeIfPresent());
    }

    @Test
    void setRejectsOverwriteUntilRegistryIsTaken() {
        final CacheRegistry first = new TestCacheRegistry();
        final CacheRegistry second = new TestCacheRegistry();

        PipelineContext.set(first);

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> PipelineContext.set(second));

        assertTrue(thrown.getMessage().contains("already holds"));
        assertSame(first, PipelineContext.take());
        assertNull(PipelineContext.takeIfPresent());
    }

    @Test
    void takeReturnsDepositedRegistryAndClearsContext() {
        final CacheRegistry registry = new TestCacheRegistry();

        PipelineContext.set(registry);

        assertSame(registry, PipelineContext.take());
        assertNull(PipelineContext.takeIfPresent());
    }

    @Test
    void takeFromDifferentThreadFailsWithoutConsumingOwnerThreadRegistry() throws Exception {
        final CacheRegistry registry = new TestCacheRegistry();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        PipelineContext.set(registry);

        final Thread other = new Thread(() -> failure.set(assertThrows(
                IllegalStateException.class,
                PipelineContext::take
        )), "pipeline-context-other");
        other.start();
        other.join();

        assertTrue(failure.get() instanceof IllegalStateException);
        assertSame(registry, PipelineContext.take());
        assertNull(PipelineContext.takeIfPresent());
    }

    private static final class TestCacheRegistry implements CacheRegistry {
        @Override
        public <K, V> Cache<K, V> getOrCreate(String name, PipelineConfig config) {
            throw new UnsupportedOperationException("not needed for test");
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
