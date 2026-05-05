/*
 * Copyright 2026 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intuitivedesigns.streamkernel.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * PluginCatalog is the single authoritative registry for StreamKernel SPI plugins.
 *
 * Enterprise posture / adoption goals:
 * - Deterministic discovery: stable ordering, stable "first wins" resolution.
 * - Clear diagnostics: failures show which SPI + which plugin id collided/missing.
 * - ClassLoader strategy: explicit loader, not implicit global state.
 * - Strong typing: separate registries per SPI kind.
 *
 * Contract:
 * - Plugin ids are case-sensitive (do NOT normalize here; ids are part of API surface).
 * - Duplicate ids are rejected (fail-fast) to prevent non-deterministic behavior.
 */
public final class PluginCatalog {

    private static final Logger log = LoggerFactory.getLogger(PluginCatalog.class);

    private final ClassLoader loader;

    private final Registry<SourcePlugin> sources;
    private final Registry<SinkPlugin> sinks;
    private final Registry<TransformerPlugin> transformers;
    private final Registry<CachePlugin> caches;
    private final Registry<SecurityPlugin> security;
    private final Registry<DlqSerializerPlugin> dlqSerializers;

    public PluginCatalog(ClassLoader loader) {
        this.loader = Objects.requireNonNull(loader, "loader");

        this.sources        = loadRegistry(SourcePlugin.class, "Sources");
        this.sinks          = loadRegistry(SinkPlugin.class, "Sinks");
        this.transformers   = loadRegistry(TransformerPlugin.class, "Transformers");
        this.caches         = loadRegistry(CachePlugin.class, "Caches");
        this.security       = loadRegistry(SecurityPlugin.class, "Security");
        this.dlqSerializers = loadRegistry(DlqSerializerPlugin.class, "DLQ Serializers");

        log.info("PluginCatalog initialized: sources={} sinks={} transformers={} caches={} security={} dlqSerializers={}",
                sources.size(), sinks.size(), transformers.size(), caches.size(), security.size(), dlqSerializers.size());
    }

    public Registry<SourcePlugin> sources() { return sources; }
    public Registry<SinkPlugin> sinks() { return sinks; }
    public Registry<TransformerPlugin> transformers() { return transformers; }
    public Registry<CachePlugin> caches() { return caches; }
    public Registry<SecurityPlugin> security() { return security; }
    public Registry<DlqSerializerPlugin> dlqSerializers() { return dlqSerializers; }

    // ---------------------------------------------------------------------
    // Internal: Discovery + deterministic registration
    // ---------------------------------------------------------------------

    private <P extends PipelinePlugin<?>> Registry<P> loadRegistry(Class<P> spi, String label) {
        final List<P> discovered = discover(spi);

        // Deterministic order: sort by plugin id, then by impl class name.
        discovered.sort(
                Comparator.comparing((P p) -> safeId(p))
                        .thenComparing(p -> p.getClass().getName())
        );

        final Map<String, P> byId = new LinkedHashMap<>();
        for (P p : discovered) {
            final String id = safeId(p);
            final P prior = byId.putIfAbsent(id, p);
            if (prior != null) {
                // Fail-fast: duplicates create non-deterministic behavior in enterprises.
                throw new IllegalStateException(
                        "Duplicate plugin id detected for SPI " + spi.getName() + ": id='" + id + "' " +
                                "prior=" + prior.getClass().getName() + " new=" + p.getClass().getName()
                );
            }
        }

        log.info("Discovered {} plugins: {}", label, byId.keySet());
        return new Registry<>(spi, label, Collections.unmodifiableMap(byId));
    }

    private <P> List<P> discover(Class<P> spi) {
        final List<P> out = new ArrayList<>();
        try {
            final ServiceLoader<P> sl = ServiceLoader.load(spi, loader);
            for (P p : sl) out.add(p);
            return out;
        } catch (ServiceConfigurationError sce) {
            throw new IllegalStateException("Failed ServiceLoader discovery for SPI " + spi.getName(), sce);
        }
    }

    private static String safeId(Object plugin) {
        if (plugin instanceof PipelinePlugin<?> p) {
            try {
                final String id = String.valueOf(p.id());
                if (id == null || id.trim().isEmpty()) {
                    throw new IllegalStateException("Plugin returned blank id: " + plugin.getClass().getName());
                }
                return id.trim();
            } catch (Throwable t) {
                throw new IllegalStateException("Plugin id() threw for " + plugin.getClass().getName(), t);
            }
        }
        throw new IllegalStateException("Not a PipelinePlugin: " + plugin.getClass().getName());
    }

    // ---------------------------------------------------------------------
    // Registry: typed lookup with enterprise diagnostics
    // ---------------------------------------------------------------------

    public static final class Registry<P extends PipelinePlugin<?>> {

        private final Class<P> spi;
        private final String label;
        private final Map<String, P> byId;

        private Registry(Class<P> spi, String label, Map<String, P> byId) {
            this.spi = Objects.requireNonNull(spi, "spi");
            this.label = Objects.requireNonNull(label, "label");
            this.byId = Objects.requireNonNull(byId, "byId");
        }

        public int size() { return byId.size(); }

        public List<String> availableIds() {
            return List.copyOf(byId.keySet());
        }

        public P get(String id) {
            if (id == null) return null;
            return byId.get(id.trim());
        }

        /**
         * Require a plugin by id, with strong diagnostics.
         *
         * @param id requested plugin id
         * @param configKey key that supplied the id (for operator-facing error messages)
         */
        public P require(String id, String configKey) {
            final String want = (id == null) ? "" : id.trim();
            if (want.isEmpty()) {
                throw new IllegalArgumentException("Missing plugin id for " + label + " (config key: " + configKey + ")");
            }

            final P p = byId.get(want);
            if (p != null) return p;

            throw new IllegalStateException(
                    "Unknown " + label + " plugin id '" + want + "' from config key '" + configKey + "'. " +
                            "Available: " + byId.keySet()
            );
        }

        @Override
        public String toString() {
            return "Registry{" +
                    "spi=" + spi.getName() +
                    ", label='" + label + '\'' +
                    ", ids=" + byId.keySet() +
                    '}';
        }
    }
}
