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

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

/**
 * CacheAwarePlugin
 * ================
 * Optional marker interface for any PipelinePlugin that wants to receive a
 * CacheRegistry at construction time.
 *
 * How it works
 * ------------
 * PipelineFactory checks {@code plugin instanceof CacheAwarePlugin} before
 * calling create(). If the plugin implements this interface, it receives the
 * registry and can call registry.getOrCreate(name, config) to obtain its own
 * private cache slot. Otherwise the standard create(config, metrics) contract
 * is used and the plugin gets no cache.
 *
 * Existing plugins do NOT need to change. Adding CacheAwarePlugin is strictly
 * opt-in — only plugins that need caching implement it.
 *
 * Usage example (EnrichmentTransformerPlugin)
 * -------------------------------------------
 * <pre>
 * public final class EnrichmentTransformerPlugin
 *         implements TransformerPlugin, CacheAwarePlugin {
 *
 *     {@literal @}Override
 *     public Object create(PipelineConfig cfg, MetricsRuntime m, CacheRegistry registry) {
 *         Cache<String, Object> cache =
 *                 registry.getOrCreate("enrichment", cfg);
 *         return new EnrichmentTransformer(cfg, m, cache);
 *     }
 *
 *     // create(config, metrics) is never called when CacheAwarePlugin is present,
 *     // but implement it as a safe fallback anyway.
 *     {@literal @}Override
 *     public Object create(PipelineConfig cfg, MetricsRuntime m) {
 *         return new EnrichmentTransformer(cfg, m, null);
 *     }
 * }
 * </pre>
 *
 * Usage example (OpaSecurityPlugin)
 * ------------------------------------
 * <pre>
 * public final class OpaSecurityPlugin
 *         implements SecurityPlugin, CacheAwarePlugin {
 *
 *     {@literal @}Override
 *     public Object create(PipelineConfig cfg, MetricsRuntime m, CacheRegistry registry) {
 *         Cache<String, Boolean> decisionCache =
 *                 registry.getOrCreate("auth.decisions", cfg);
 *         return new OpaSecurityProvider(cfg, m, decisionCache);
 *     }
 * }
 * </pre>
 */
public interface CacheAwarePlugin {

    /**
     * Creates the plugin instance with access to the cache registry.
     *
     * @param config   pipeline configuration
     * @param metrics  metrics runtime
     * @param registry named cache factory — call getOrCreate(name, config) to
     *                 obtain an isolated cache slot for this plugin
     * @return the created plugin instance (same type as the base plugin's create())
     */
    Object create(PipelineConfig config, MetricsRuntime metrics, CacheRegistry registry);
}
