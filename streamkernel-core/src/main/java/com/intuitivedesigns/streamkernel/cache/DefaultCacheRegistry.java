/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.cache;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.CacheProviderPlugin;
import com.intuitivedesigns.streamkernel.spi.CacheRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DefaultCacheRegistry
 * ====================
 * Production implementation of CacheRegistry.
 *
 * Resolution order for a given name "n":
 *   1. cache.<n>.type          (per-name override)
 *   2. cache.type              (global pipeline default)
 *   3. "NOOP"                  (hardcoded safe fallback)
 *
 * Each logical name maps to exactly one Cache instance — subsequent calls
 * with the same name return the existing instance without re-creating it.
 * Names are normalized to lowercase on entry.
 *
 * Thread safety: getOrCreate() uses ConcurrentHashMap.computeIfAbsent —
 * safe for concurrent calls, though in practice plugins are constructed
 * single-threaded before the pipeline starts.
 *
 * Shutdown: close() drains all instances in creation order. Idempotent.
 */
public final class DefaultCacheRegistry implements CacheRegistry {

    private static final Logger log = LoggerFactory.getLogger(DefaultCacheRegistry.class);

    private static final String KEY_GLOBAL_TYPE = "cache.type";
    private static final String FALLBACK_TYPE   = "NOOP";

    // name → Cache instance (created lazily)
    private final Map<String, Cache<?, ?>>         instances   = new ConcurrentHashMap<>();
    // ordered list for deterministic close sequence
    private final List<Cache<?, ?>>                createOrder = new ArrayList<>();
    // id → provider (populated once at construction via ServiceLoader)
    private final Map<String, CacheProviderPlugin> providers   = new ConcurrentHashMap<>();

    private final MetricsRuntime metrics;
    private final AtomicBoolean  closed = new AtomicBoolean(false);

    /**
     * @param classLoader class loader for ServiceLoader provider discovery
     * @param metrics     metrics runtime passed through to each created Cache
     */
    public DefaultCacheRegistry(ClassLoader classLoader, MetricsRuntime metrics) {
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        loadProviders(Objects.requireNonNull(classLoader, "classLoader"));
    }

    // -------------------------------------------------------------------------
    // CacheRegistry
    // -------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Cache<K, V> getOrCreate(String name, PipelineConfig config) {
        Objects.requireNonNull(name,   "name");
        Objects.requireNonNull(config, "config");

        if (closed.get()) {
            throw new IllegalStateException("CacheRegistry has already been closed");
        }

        final String key = name.trim().toLowerCase();
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Cache name must not be blank");
        }

        return (Cache<K, V>) instances.computeIfAbsent(key, n -> {
            final Cache<?, ?> c = instantiate(n, config);
            synchronized (createOrder) { createOrder.add(c); }
            return c;
        });
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        synchronized (createOrder) {
            for (final Cache<?, ?> c : createOrder) {
                try {
                    c.close();
                } catch (Exception e) {
                    log.warn("Error closing cache instance", e);
                }
            }
            createOrder.clear();
        }
        instances.clear();
        log.info("CacheRegistry closed ({} instance(s) released)", instances.size());
    }

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    /** Returns the number of cache instances currently active. */
    public int activeCount() { return instances.size(); }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void loadProviders(ClassLoader cl) {
        // Use the classloader that loaded CacheProviderPlugin itself, not the context
        // classloader. In a fat-JAR deployment the context classloader may resolve
        // CacheProviderPlugin from a different classloader instance, causing ServiceLoader
        // to reject providers with "not a subtype" even though the class names match.
        final ClassLoader spiCl = CacheProviderPlugin.class.getClassLoader();
        final ClassLoader loaderToUse = (spiCl != null) ? spiCl : cl;
        ServiceLoader.load(CacheProviderPlugin.class, loaderToUse).forEach(p -> {
            final String raw = p.id();
            if (raw == null || raw.isBlank()) {
                log.warn("CacheProviderPlugin {} returned null/blank id — skipped",
                        p.getClass().getName());
                return;
            }
            final String id = raw.trim().toUpperCase();
            providers.put(id, p);
            log.debug("Registered CacheProviderPlugin id={} class={}", id, p.getClass().getName());
        });
        log.info("CacheRegistry discovered {} provider(s): {}", providers.size(), providers.keySet());
    }

    private Cache<?, ?> instantiate(String name, PipelineConfig config) {
        // Resolution: cache.<n>.type → cache.type → NOOP
        final String perNameKey = "cache." + name + ".type";
        String raw = config.getString(perNameKey, null);
        if (raw == null || raw.isBlank()) {
            raw = config.getString(KEY_GLOBAL_TYPE, FALLBACK_TYPE);
        }
        final String type = raw.trim().toUpperCase();

        CacheProviderPlugin provider = providers.get(type);
        if (provider == null) {
            log.warn("No CacheProviderPlugin for type='{}' (name='{}') — falling back to NOOP",
                    type, name);
            provider = providers.get(FALLBACK_TYPE);
            if (provider == null) {
                // This only happens if cache-noop is missing from the classpath.
                throw new IllegalStateException(
                        "NOOP CacheProviderPlugin not found. " +
                                "Ensure streamkernel-plugins/cache-noop is a runtime dependency.");
            }
        }

        log.info("Creating cache name='{}' type={}", name, type);
        return provider.create(name, config, metrics);
    }
}