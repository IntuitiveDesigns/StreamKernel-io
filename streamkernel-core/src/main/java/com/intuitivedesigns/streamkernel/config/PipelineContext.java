/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.config;

import com.intuitivedesigns.streamkernel.spi.CacheRegistry;

import java.util.Objects;

/**
 * PipelineContext
 * ===============
 * Lightweight holder that bridges the CacheRegistry created inside
 * PipelineFactory.createPipeline() back to StreamKernel.main() for
 * proper lifecycle management.
 *
 * Why this exists
 * ---------------
 * PipelineFactory.createPipeline() returns a PipelineOrchestrator. Adding
 * CacheRegistry as a second return value would require changing the public
 * method signature and every call site. Instead, PipelineFactory deposits
 * the registry here immediately after creating it, and StreamKernel.main()
 * retrieves it after createPipeline() returns.
 *
 * This pattern is also used by frameworks like Quarkus and Spring Boot for
 * exactly this situation (injecting lifecycle-managed objects without
 * changing factory return types).
 *
 * Usage in StreamKernel.main()
 * ----------------------------
 * <pre>
 *     final PipelineOrchestrator<?,?> pipeline = PipelineFactory.createPipeline(config, metrics);
 *     final CacheRegistry registry = PipelineContext.take();  // retrieve and clear; throws if missing
 *     // ... register registry for shutdown alongside other AutoCloseable resources
 * </pre>
 *
 * Thread safety
 * -------------
 * Uses ThreadLocal so concurrent pipeline construction on different threads
 * stays isolated. This is intentionally a same-thread handoff: the thread that
 * calls {@code PipelineFactory.createPipeline()} must also call {@link #take()}
 * to consume and clear its own deposited registry handle.
 *
 * Silent nulls are intentionally avoided for the main success path. If a caller
 * needs defensive cleanup after a failed construction attempt, use
 * {@link #takeIfPresent()} instead.
 */
public final class PipelineContext {

    private static final ThreadLocal<CacheRegistry> REGISTRY = new ThreadLocal<>();

    private PipelineContext() {}

    /** Called by PipelineFactory after creating the registry. */
    public static void set(CacheRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        if (REGISTRY.get() != null) {
            throw new IllegalStateException(
                    "PipelineContext already holds a CacheRegistry for thread '"
                            + Thread.currentThread().getName()
                            + "'. Call take() before depositing another registry.");
        }
        REGISTRY.set(registry);
    }

    /**
     * Called by StreamKernel.main() after createPipeline() returns.
     * Retrieves and clears the stored registry in one operation.
     *
     * @return the CacheRegistry for the current thread
     * @throws IllegalStateException if no registry is available for the current thread.
     *         This usually means createPipeline() failed before depositing the registry,
     *         take() was already called, or take() is being called from the wrong thread.
     */
    public static CacheRegistry take() {
        final CacheRegistry r = REGISTRY.get();
        REGISTRY.remove();
        if (r == null) {
            throw new IllegalStateException(
                    "PipelineContext has no CacheRegistry for thread '"
                            + Thread.currentThread().getName()
                            + "'. createPipeline() and take() must run on the same thread, "
                            + "and take() may only be called once.");
        }
        return r;
    }

    /**
     * Retrieves and clears the stored registry if one exists.
     *
     * <p>Use this only for defensive cleanup paths where "nothing deposited" is
     * already an expected outcome.</p>
     *
     * @return the CacheRegistry for the current thread, or null if none is present
     */
    public static CacheRegistry takeIfPresent() {
        final CacheRegistry r = REGISTRY.get();
        REGISTRY.remove();
        return r;
    }
}
