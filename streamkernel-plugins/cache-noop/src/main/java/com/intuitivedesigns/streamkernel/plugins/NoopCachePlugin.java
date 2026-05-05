/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.CachePlugin;
import com.intuitivedesigns.streamkernel.spi.CacheProviderPlugin;
import com.intuitivedesigns.streamkernel.spi.PluginKind;

import java.util.Optional;

/**
 * NoopCachePlugin
 * ===============
 * Zero-overhead pass-through cache. All reads return empty, all writes
 * are discarded. Used when:
 *
 *  - cache.type=NOOP (disable caching entirely, benchmark scenarios)
 *  - cache.<n>.type=NOOP (disable caching for a specific named slot)
 *  - As the safe fallback when no matching provider is found
 *  - As the required fallback provider that must always be on the classpath
 *
 * This is a REQUIRED runtime dependency. DefaultCacheRegistry will throw
 * at startup if the NOOP provider cannot be found via ServiceLoader.
 * It must be present even when all pipelines use LOCAL_CAFFEINE or REDIS,
 * because it is the fallback provider for missing configurations.
 */
public final class NoopCachePlugin implements CachePlugin, CacheProviderPlugin {

    public static final String ID = "NOOP";

    private static final Cache<Object, Object> INSTANCE = new NoopCache();

    // -------------------------------------------------------------------------
    // CachePlugin (orchestrator-level)
    // -------------------------------------------------------------------------

    @Override
    public String id() { return ID; }

    @Override
    public PluginKind kind() { return PluginKind.CACHE; }

    @Override
    @SuppressWarnings("unchecked")
    public Cache<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        return INSTANCE;
    }

    // -------------------------------------------------------------------------
    // CacheProviderPlugin (per-name registry)
    // -------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Cache<K, V> create(String name, PipelineConfig config, MetricsRuntime metrics) {
        // Single shared singleton — NoopCache has no state.
        return (Cache<K, V>) INSTANCE;
    }

    // -------------------------------------------------------------------------
    // NoopCache
    // -------------------------------------------------------------------------

    private static final class NoopCache implements Cache<Object, Object> {
        @Override public Optional<Object> get(Object key)              { return Optional.empty(); }
        @Override public void put(Object key, Object value)            { /* discard */ }
        @Override public void invalidate(Object key)                   { /* no-op   */ }
        @Override public void close()                                  { /* no-op   */ }
    }
}