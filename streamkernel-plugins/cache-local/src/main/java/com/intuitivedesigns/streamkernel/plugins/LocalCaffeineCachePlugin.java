/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.CachePlugin;
import com.intuitivedesigns.streamkernel.spi.CacheProviderPlugin;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SemanticCache;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * LocalCaffeineCachePlugin
 * ========================
 * Registers the LOCAL_CAFFEINE backend for both cache roles:
 *
 *   CachePlugin         — orchestrator-level single cache (cache.type=LOCAL_CAFFEINE)
 *   CacheProviderPlugin — per-name registry cache (cache.<n>.type=LOCAL_CAFFEINE)
 *
 * Why CaffeineCache is non-generic
 * ---------------------------------
 * SemanticCache<I,O> is defined as "extends Cache<Object,Object>" in the SPI.
 * A generic CaffeineCache<K,V> implementing SemanticCache<I,O> would inherit
 * Cache<Object,Object> through that path, making it impossible to also satisfy
 * Cache<K,V> — Java forbids inheriting the same interface with different type
 * arguments. CaffeineCache therefore implements SemanticCache<I,O> directly
 * (with its own I,O type parameters for semantic methods) and uses an underlying
 * Caffeine<Object,Object> instance. The Cache<Object,Object> contract inherited
 * through SemanticCache covers get/put/invalidate.
 *
 * Config keys (orchestrator-level)
 * ---------------------------------
 *   cache.local.max.size        max entries     (default 10,000)
 *   cache.local.ttl.seconds     TTL after write (default 300s)
 *
 * Config keys (per-name, via CacheRegistry)
 * ------------------------------------------
 *   cache.<n>.local.max.size
 *   cache.<n>.local.ttl.seconds
 *   Falls back to cache.local.* if per-name keys are absent.
 */
public final class LocalCaffeineCachePlugin implements CachePlugin, CacheProviderPlugin {

    public static final String ID = "LOCAL_CAFFEINE";

    private static final Logger log = LoggerFactory.getLogger(LocalCaffeineCachePlugin.class);

    // -------------------------------------------------------------------------
    // CachePlugin (orchestrator-level, existing contract)
    // -------------------------------------------------------------------------

    @Override
    public String id() { return ID; }

    @Override
    public PluginKind kind() { return PluginKind.CACHE; }

    @Override
    public Cache<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config,  "config");
        Objects.requireNonNull(metrics, "metrics");

        final long maxSize = config.getLong("cache.local.max.size",    10_000L);
        final long ttlSec  = config.getLong("cache.local.ttl.seconds", 300L);

        log.info("[{}] Creating orchestrator cache maxSize={} ttl={}s", ID, maxSize, ttlSec);
        return new CaffeineCache<>(ID, maxSize, ttlSec);
    }

    // -------------------------------------------------------------------------
    // CacheProviderPlugin (per-name registry contract)
    // -------------------------------------------------------------------------

    @Override
    public <K, V> Cache<K, V> create(String name, PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(name,    "name");
        Objects.requireNonNull(config,  "config");
        Objects.requireNonNull(metrics, "metrics");

        final String prefix = "cache." + name + ".local.";

        final long maxSize = firstLong(
                config.getString(prefix + "max.size",       null),
                config.getString("cache.local.max.size",    null),
                10_000L);

        final long ttlSec = firstLong(
                config.getString(prefix + "ttl.seconds",    null),
                config.getString("cache.local.ttl.seconds", null),
                300L);

        log.info("[{}] Creating registry cache name='{}' maxSize={} ttl={}s",
                ID, name, maxSize, ttlSec);

        // CaffeineCache implements Cache<Object,Object> via SemanticCache.
        // Safe cast: all storage is Object internally; callers interact
        // through the Cache<K,V> interface without violating heap pollution.
        @SuppressWarnings("unchecked")
        final Cache<K, V> cache = (Cache<K, V>) new CaffeineCache<>(name, maxSize, ttlSec);
        return cache;
    }

    // -------------------------------------------------------------------------
    // CaffeineCache
    //
    // Implements SemanticCache<I,O>, which extends Cache<Object,Object>.
    // All storage uses Object keys/values to match the SPI's fixed erasure.
    // I and O are used only for the typed semantic lookup/store methods.
    // -------------------------------------------------------------------------

    private static final class CaffeineCache<I, O>
            implements SemanticCache<I, O> {

        private final com.github.benmanes.caffeine.cache.Cache<Object, Object> underlying;
        private final String name;

        CaffeineCache(String name, long maxSize, long ttlSeconds) {
            this.name = name;
            this.underlying = Caffeine.newBuilder()
                    .maximumSize(maxSize)
                    .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                    .build();
        }

        // ------------------------------------------------------------------
        // Cache<Object,Object>  (inherited via SemanticCache<I,O>)
        // ------------------------------------------------------------------

        @Override
        public Optional<Object> get(Object key) {
            if (key == null) return Optional.empty();
            return Optional.ofNullable(underlying.getIfPresent(key));
        }

        @Override
        public void put(Object key, Object value) {
            if (key != null && value != null) {
                underlying.put(key, value);
            }
        }

        @Override
        public void invalidate(Object key) {
            if (key != null) underlying.invalidate(key);
        }

        @Override
        public void close() {
            underlying.invalidateAll();
            underlying.cleanUp();
            log.debug("[{}] CaffeineCache closed", name);
        }

        // ------------------------------------------------------------------
        // SemanticCache<I,O>
        // Cache key is payload.id() — stable string identity per record.
        // ------------------------------------------------------------------

        @Override
        @SuppressWarnings("unchecked")
        public PipelinePayload<O> lookup(PipelinePayload<I> input) {
            if (input == null || input.id() == null) return null;
            return (PipelinePayload<O>) underlying.getIfPresent(input.id());
        }

        @Override
        public void store(PipelinePayload<I> input, PipelinePayload<O> output) {
            if (input == null || input.id() == null || output == null) return;
            underlying.put(input.id(), output);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static long firstLong(String primary, String secondary, long fallback) {
        if (primary != null && !primary.isBlank()) {
            try { return Long.parseLong(primary.trim()); } catch (NumberFormatException ignored) {}
        }
        if (secondary != null && !secondary.isBlank()) {
            try { return Long.parseLong(secondary.trim()); } catch (NumberFormatException ignored) {}
        }
        return fallback;
    }
}