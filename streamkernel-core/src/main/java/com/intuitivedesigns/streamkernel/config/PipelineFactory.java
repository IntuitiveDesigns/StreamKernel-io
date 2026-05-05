/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.config;

import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelineProvenance;
import com.intuitivedesigns.streamkernel.core.PipelineOrchestrator;
import com.intuitivedesigns.streamkernel.core.ProvenanceSourceConnector;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.security.SecurityContext;
import com.intuitivedesigns.streamkernel.cache.DefaultCacheRegistry;
import com.intuitivedesigns.streamkernel.spi.Cache;
import com.intuitivedesigns.streamkernel.spi.CacheAwarePlugin;
import com.intuitivedesigns.streamkernel.spi.CachePlugin;
import com.intuitivedesigns.streamkernel.spi.CacheProviderPlugin;
import com.intuitivedesigns.streamkernel.spi.CacheRegistry;
import com.intuitivedesigns.streamkernel.spi.DlqSerializer;
import com.intuitivedesigns.streamkernel.spi.DlqSerializerPlugin;
import com.intuitivedesigns.streamkernel.spi.PipelinePlugin;
import com.intuitivedesigns.streamkernel.spi.PluginCatalog;
import com.intuitivedesigns.streamkernel.spi.SecurityPlugin;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;
import com.intuitivedesigns.streamkernel.spi.SourcePlugin;
import com.intuitivedesigns.streamkernel.spi.TransformerPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * PipelineFactory is the single composition point for StreamKernel plugin wiring.
 *
 * New-design contract (no backward compatibility):
 * - Canonical operator keys only: "*.type" and "transform.chain"
 * - No silent fallback to legacy keys; missing required keys fail early.
 * - Deterministic defaults are explicit and centralized.
 */
public final class PipelineFactory {

    private static final Logger log = LoggerFactory.getLogger(PipelineFactory.class);

    // ---------------------------------------------------------------------
    // Configuration keys (public operator contract)
    // ---------------------------------------------------------------------

    // Source (required)
    private static final String KEY_SOURCE_TYPE = "source.type";

    // Sink / DLQ
    private static final String KEY_SINK_TYPE = "sink.type";
    private static final String KEY_DLQ_TYPE  = "dlq.type";

    // Transformers
    private static final String KEY_TRANSFORM_TYPE  = "transform.type";
    private static final String KEY_TRANSFORM_CHAIN = "transform.chain"; // CSV of transformer ids; authoritative if non-blank

    // Cache
    private static final String KEY_CACHE_TYPE = "cache.type";

    // Security
    private static final String KEY_SECURITY_TYPE = "security.type";

    // DLQ serializer
    private static final String KEY_DLQ_SER_TYPE = "dlq.serializer.type";

    // Pipeline knobs
    private static final String KEY_PIPELINE_PARALLELISM = "pipeline.parallelism";
    private static final String KEY_PIPELINE_BATCH_SIZE  = "pipeline.batch.size";

    // ---------------------------------------------------------------------
    // Defaults (explicit)
    // ---------------------------------------------------------------------

    private static final String DEFAULT_SINK           = "DEVNULL";
    private static final String DEFAULT_DLQ            = "DEVNULL";
    private static final String DEFAULT_TRANSFORM      = "NOOP";
    private static final String DEFAULT_CACHE          = "NOOP";
    private static final String DEFAULT_DLQ_SERIALIZER = "STRING";
    private static final String DEFAULT_SECURITY       = "PERMIT_ALL";

    private static final int DEFAULT_PIPELINE_PARALLELISM = 1;
    private static final int DEFAULT_PIPELINE_BATCH_SIZE  = 1000;

    private PipelineFactory() {}

    /**
     * Lazily captures the application ClassLoader on first real factory use instead of at
     * PipelineFactory class-load time, which is safer in embedded/containerized runtimes.
     */
    private static final class CatalogHolder {
        private static final ClassLoader LOADER = resolveClassLoader();
        private static final PluginCatalog CATALOG = new PluginCatalog(LOADER);
    }

    // ---------------------------------------------------------------------
    // Factory Methods (SPI instance creation)
    // ---------------------------------------------------------------------

    public static SourceConnector<?> createSource(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = requireNonBlank(config, KEY_SOURCE_TYPE);
        final SourcePlugin plugin = catalog().sources().require(id, KEY_SOURCE_TYPE);
        final SourceConnector<?> source = createSafe(plugin, config, metrics, "Source");
        return ProvenanceSourceConnector.wrap(source, PipelineProvenance.fromConfig(config).headers());
    }

    public static OutputSink<?> createSink(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_SINK_TYPE, null), DEFAULT_SINK);
        final SinkPlugin plugin = catalog().sinks().require(id, KEY_SINK_TYPE);
        return createSafe(plugin, config, metrics, "Sink");
    }

    public static OutputSink<?> createDlq(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_DLQ_TYPE, null), DEFAULT_DLQ);
        final SinkPlugin plugin = catalog().sinks().require(id, KEY_DLQ_TYPE);
        return createSafe(plugin, config, metrics, "DLQ");
    }

    /**
     * Create the transformer chain (no registry — delegates to registry-aware overload with null).
     * Kept for call sites that do not yet have a registry (e.g. standalone tests).
     */
    public static Transformer<?, ?> createTransformer(PipelineConfig config, MetricsRuntime metrics) {
        return createTransformer(config, metrics, null);
    }

    /**
     * Create the transformer chain.
     *
     * Contract:
     *  - If transform.chain is present (non-blank), it is authoritative and transform.type is ignored.
     *  - If transform.chain is absent/blank, fall back to transform.type, default NOOP.
     *  - If registry is non-null and a plugin implements {@link CacheAwarePlugin}, it receives
     *    the registry so it can obtain a named, isolated cache slot.
     *
     * Implementation detail:
     *  - Each transformer step gets a scoped config overlay so plugins can read transform.type
     *    and treat themselves as active without needing per-step prefixes.
     */
    public static Transformer<?, ?> createTransformer(PipelineConfig config, MetricsRuntime metrics,
                                                      CacheRegistry registry) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String chain = config.getString(KEY_TRANSFORM_CHAIN, null);
        if (chain != null && !chain.trim().isEmpty()) {
            final String[] ids = parseCsvIds(chain);

            if (ids.length == 0) {
                // transform.chain was set but contained only blank/comma entries — this is a
                // misconfiguration, not a silent fallback, because the operator explicitly opted
                // into chain mode and their intent cannot be inferred.
                throw new IllegalArgumentException(
                        "transform.chain is set to '" + chain + "' but contains no valid IDs after trimming");
            }

            // Guardrail validation happens here — after IDs are parsed but before any plugin is
            // instantiated — so the chain CSV is parsed exactly once.
            applyChainGuardrails(ids);

            if (ids.length == 1) {
                final String single = normalizeId(ids[0], DEFAULT_TRANSFORM);
                final TransformerPlugin plugin = catalog().transformers().require(single, KEY_TRANSFORM_CHAIN);
                final PipelineConfig scoped = PipelineConfig.overlay(config, Map.of(KEY_TRANSFORM_TYPE, single));
                log.info("Creating transformer step={} id={} (scoped {}={})",
                        0, single, KEY_TRANSFORM_TYPE, scoped.getString(KEY_TRANSFORM_TYPE, "null"));
                return createSafe(plugin, scoped, metrics, registry, "Transformer");
            }

            @SuppressWarnings({"rawtypes", "unchecked"})
            final Transformer[] chainTx = new Transformer[ids.length];

            for (int i = 0; i < ids.length; i++) {
                final String id = normalizeId(ids[i], DEFAULT_TRANSFORM);
                try {
                    final TransformerPlugin plugin = catalog().transformers().require(id, KEY_TRANSFORM_CHAIN);
                    final PipelineConfig scoped = PipelineConfig.overlay(config, Map.of(KEY_TRANSFORM_TYPE, id));
                    log.info("Creating transformer step={} id={} (scoped {}={})",
                            i, id, KEY_TRANSFORM_TYPE, scoped.getString(KEY_TRANSFORM_TYPE, "null"));
                    chainTx[i] = createSafe(plugin, scoped, metrics, registry, "Transformer");
                } catch (Throwable t) {
                    // Close already-constructed steps — the outer createPipeline catch cannot do
                    // this because transformer is still null (createTransformer never returned).
                    for (int j = 0; j < i; j++) {
                        if (chainTx[j] instanceof AutoCloseable ac) {
                            try { ac.close(); }
                            catch (Exception ex) { log.warn("Error closing transformer step {} during chain build failure", j, ex); }
                        }
                    }
                    throw (t instanceof RuntimeException re) ? re
                            : new RuntimeException("Failed building transformer chain at step " + i + " [" + id + "]", t);
                }
            }

            return new com.intuitivedesigns.streamkernel.core.ChainedTransformer(chainTx);
        }

        final String id = normalizeId(config.getString(KEY_TRANSFORM_TYPE, null), DEFAULT_TRANSFORM);
        if ("HTTP_EMBEDDING".equals(id)) {
            throw new IllegalStateException(
                    "HTTP_EMBEDDING requires STRING_TO_WIREEVENT in transform.chain; single transform.type=HTTP_EMBEDDING is unsupported");
        }
        final TransformerPlugin plugin = catalog().transformers().require(id, KEY_TRANSFORM_TYPE);
        return createSafe(plugin, config, metrics, registry, "Transformer");
    }

    public static Cache<?, ?> createCache(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_CACHE_TYPE, null), DEFAULT_CACHE);
        final CachePlugin plugin = catalog().caches().require(id, KEY_CACHE_TYPE);
        return createSafe(plugin, config, metrics, "Cache");
    }

    public static DlqSerializer<?> createDlqSerializer(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_DLQ_SER_TYPE, null), DEFAULT_DLQ_SERIALIZER);
        final DlqSerializerPlugin plugin = catalog().dlqSerializers().require(id, KEY_DLQ_SER_TYPE);
        return createSafe(plugin, config, metrics, "DLQ Serializer");
    }

    public static SecurityProvider createSecurity(PipelineConfig config, MetricsRuntime metrics) {
        return createSecurity(config, metrics, null);
    }

    public static SecurityProvider createSecurity(PipelineConfig config, MetricsRuntime metrics,
                                                  CacheRegistry registry) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String id = normalizeId(config.getString(KEY_SECURITY_TYPE, null), DEFAULT_SECURITY);
        final SecurityPlugin plugin = catalog().security().require(id, KEY_SECURITY_TYPE);
        return createSafe(plugin, config, metrics, registry, "Security Provider");
    }

    // ---------------------------------------------------------------------
    // Opinionated enterprise pipeline builder
    // ---------------------------------------------------------------------

    public static PipelineOrchestrator<?, ?> createPipeline(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final int parallelism = Math.max(1, config.getInt(KEY_PIPELINE_PARALLELISM, DEFAULT_PIPELINE_PARALLELISM));
        final int batchSize   = Math.max(1, config.getInt(KEY_PIPELINE_BATCH_SIZE,  DEFAULT_PIPELINE_BATCH_SIZE));

        final String principal = config.getString("app.service.account", "unknown-service");
        final String action    = config.getString("security.action", "write");

        final String resource = config.getString(
                "security.resource",
                config.getString("sink.kafka.topic", config.getString("sink.topic", "unknown-resource"))
        );

        final boolean authPerRecord = config.getBoolean("security.auth.per.record", false);
        final long authTtlMs = config.getLong("security.auth.ttl.ms", 1000L);

        final boolean failFastSource = config.getBoolean("pipeline.source.fail.fast", false);

        final long sourceBackoffInitialMs = config.getLong("pipeline.source.error.backoff.initial.ms", 250L);
        final long sourceBackoffMaxMs     = config.getLong("pipeline.source.error.backoff.max.ms", 5000L);

        final long drainTimeoutMs = config.getLong("pipeline.drain.timeout.ms", 15_000L);

        final SecurityContext securityCtx = new SecurityContext(
                principal,
                action,
                resource,
                failFastSource,
                sourceBackoffInitialMs,
                sourceBackoffMaxMs
        );

        SourceConnector<?> source = null;
        OutputSink<?> sink = null;
        OutputSink<?> dlq = null;
        CacheRegistry registry = null;
        Transformer<?, ?> transformer = null;
        Cache<?, ?> cache = null;
        SecurityProvider security = null;
        boolean registryDeposited = false;

        try {
            source = createSource(config, metrics);
            sink = createSink(config, metrics);
            dlq = createDlq(config, metrics);

            // Registry must be created before any plugin that may implement CacheAwarePlugin.
            // It is deposited in PipelineContext so StreamKernel.main() can retrieve it for
            // lifecycle management (close after pipeline.stop(), before metrics.close()).
            registry = createCacheRegistry(metrics);
            PipelineContext.set(registry);
            registryDeposited = true;

            transformer = createTransformer(config, metrics, registry);
            cache = createCache(config, metrics);
            security = createSecurity(config, metrics, registry);

            @SuppressWarnings({"rawtypes", "unchecked"})
            final PipelineOrchestrator<?, ?> pipeline =
                    new PipelineOrchestrator(
                            (SourceConnector) source,
                            (OutputSink) sink,
                            (OutputSink) dlq,
                            (Transformer) transformer,
                            (Cache) cache,
                            metrics,
                            parallelism,
                            batchSize,
                            security,
                            securityCtx,
                            authPerRecord,
                            TimeUnit.MILLISECONDS.toNanos(authTtlMs),
                            TimeUnit.MILLISECONDS.toNanos(drainTimeoutMs),
                            config
                    );

            final String transformerDesc;
            final String chainVal = config.getString(KEY_TRANSFORM_CHAIN, null);
            if (chainVal != null && !chainVal.trim().isEmpty()) {
                transformerDesc = "chain[" + chainVal.trim() + "]";
            } else {
                transformerDesc = normalizeId(config.getString(KEY_TRANSFORM_TYPE, null), DEFAULT_TRANSFORM);
            }
            log.info("Pipeline created: source={} sink={} dlq={} transformer={} security={} parallelism={} batchSize={}",
                    config.getString(KEY_SOURCE_TYPE, "?"),
                    normalizeId(config.getString(KEY_SINK_TYPE, null), DEFAULT_SINK),
                    normalizeId(config.getString(KEY_DLQ_TYPE, null), DEFAULT_DLQ),
                    transformerDesc,
                    normalizeId(config.getString(KEY_SECURITY_TYPE, null), DEFAULT_SECURITY),
                    parallelism, batchSize);
            return pipeline;
        } catch (Throwable t) {
            if (registryDeposited) {
                PipelineContext.takeIfPresent();
            }
            closeOnFailure(security, "security provider");
            closeOnFailure(cache, "cache");
            closeOnFailure(transformer, "transformer");
            closeOnFailure(registry, "cache registry");
            closeOnFailure(dlq, "DLQ sink");
            closeOnFailure(sink, "sink");
            disconnectOnFailure(source);
            if (t instanceof RuntimeException re) {
                throw re;
            }
            if (t instanceof Error err) {
                throw err;
            }
            throw new RuntimeException("Unexpected checked exception during pipeline construction", t);
        }
    }

    // ---------------------------------------------------------------------
    // Supportability / Utilities
    // ---------------------------------------------------------------------

    public static void logAvailablePlugins() {
        final PluginCatalog c = catalog();
        log.info("Plugin Catalog Loaded:");
        log.info("  Sources:         {}", c.sources().availableIds());
        log.info("  Sinks:           {}", c.sinks().availableIds());
        log.info("  Transformers:    {}", c.transformers().availableIds());
        log.info("  Caches:          {}", c.caches().availableIds());
        log.info("  Security:        {}", c.security().availableIds());
        log.info("  DLQ Serializers: {}", c.dlqSerializers().availableIds());
    }

    // ---------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------

    /**
     * Builds a {@link DefaultCacheRegistry} loaded from the same ClassLoader as the plugin
     * catalog.  The registry is passed to any plugin that implements {@link CacheAwarePlugin}
     * so each plugin obtains its own isolated, named cache slot (e.g. "embedding", "auth").
     *
     * <p>The registry is also deposited in {@link PipelineContext} immediately after creation
     * so {@code StreamKernel.main()} can retrieve it for proper lifecycle shutdown.
     */
    private static CacheRegistry createCacheRegistry(MetricsRuntime metrics) {
        return new DefaultCacheRegistry(catalogLoader(), metrics);
    }

    private static String requireNonBlank(PipelineConfig config, String key) {
        final String v = config.getString(key, null);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required configuration key: " + key);
        }
        return v.trim();
    }

    private static String normalizeId(String raw, String fallback) {
        if (raw == null) return fallback;
        final String s = raw.trim();
        return s.isEmpty() ? fallback : s;
    }

    private static ClassLoader resolveClassLoader() {
        final ClassLoader ctx = Thread.currentThread().getContextClassLoader();
        return (ctx != null) ? ctx : PipelineFactory.class.getClassLoader();
    }

    private static String[] parseCsvIds(String csv) {
        if (csv == null) return new String[0];
        final String s = csv.trim();
        if (s.isEmpty()) return new String[0];

        final String[] raw = s.split(",");
        final ArrayList<String> out = new ArrayList<>(raw.length);
        for (String r : raw) {
            final String t = r.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out.toArray(String[]::new);
    }

    /**
     * Creates a plugin instance, passing {@code registry} to plugins that implement
     * {@link CacheAwarePlugin}.  Falls back to the standard {@code create(config, metrics)}
     * contract for all other plugins so existing plugins need no changes.
     */
    @SuppressWarnings("unchecked")
    private static <T> T createSafe(PipelinePlugin<T> plugin,
                                    PipelineConfig config,
                                    MetricsRuntime metrics,
                                    CacheRegistry registry,
                                    String typeName) {
        Objects.requireNonNull(plugin, "plugin");
        try {
            if (registry != null && plugin instanceof CacheAwarePlugin cap) {
                return (T) cap.create(config, metrics, registry);
            }
            return plugin.create(config, metrics);
        } catch (Throwable t) {
            final String pluginId;
            try {
                pluginId = String.valueOf(plugin.id());
            } catch (Throwable ignored) {
                throw new RuntimeException("Failed creating " + typeName + " (plugin id unavailable)", t);
            }
            throw new RuntimeException("Failed creating " + typeName + " [" + pluginId + "]", t);
        }
    }

    private static <T> T createSafe(PipelinePlugin<T> plugin,
                                    PipelineConfig config,
                                    MetricsRuntime metrics,
                                    String typeName) {
        return createSafe(plugin, config, metrics, null, typeName);
    }

    private static PluginCatalog catalog() {
        return CatalogHolder.CATALOG;
    }

    private static ClassLoader catalogLoader() {
        return CatalogHolder.LOADER;
    }

    private static void applyChainGuardrails(String[] ids) {
        boolean hasHttpEmbedding = false;
        boolean hasStringToWireEvent = false;
        for (String id : ids) {
            if ("HTTP_EMBEDDING".equals(id))      hasHttpEmbedding = true;
            if ("STRING_TO_WIREEVENT".equals(id)) hasStringToWireEvent = true;
        }
        if (hasHttpEmbedding && !hasStringToWireEvent) {
            throw new IllegalStateException("HTTP_EMBEDDING requires STRING_TO_WIREEVENT in transform.chain");
        }
    }

    private static void closeOnFailure(AutoCloseable closeable, String label) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("Failed closing {} after pipeline construction error", label, e);
        }
    }

    private static void disconnectOnFailure(SourceConnector<?> source) {
        if (source == null) return;
        try {
            source.disconnect();
        } catch (Exception e) {
            log.warn("Failed disconnecting source after pipeline construction error", e);
        }
    }
}
