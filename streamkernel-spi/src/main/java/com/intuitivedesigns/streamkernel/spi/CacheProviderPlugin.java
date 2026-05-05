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
 * CacheProviderPlugin
 * ===================
 * SPI contract implemented by every cache backend module to register with
 * DefaultCacheRegistry via ServiceLoader.
 *
 * Relationship to the existing CachePlugin
 * -----------------------------------------
 * CachePlugin (existing) is the orchestrator-level factory — one instance,
 * owned by PipelineOrchestrator, used for ID dedup and semantic lookup.
 *
 * CacheProviderPlugin is the per-name factory used by CacheRegistry to create
 * independent instances for each logical namespace (embedding, auth, schema…).
 *
 * A backend module implements BOTH interfaces so it works in both roles:
 *
 *   public final class LocalCaffeineCachePlugin
 *       implements CachePlugin, CacheProviderPlugin { ... }
 *
 * Existing CachePlugin implementations do NOT need to change if they also add
 * CacheProviderPlugin. The two methods have different signatures deliberately:
 * CachePlugin.create(config, metrics) is the orchestrator contract.
 * CacheProviderPlugin.create(name, config, metrics) is the registry contract.
 *
 * SPI registration
 * ----------------
 * Each backend module must declare its implementation in:
 *   META-INF/services/com.intuitivedesigns.streamkernel.spi.CacheProviderPlugin
 */
public interface CacheProviderPlugin {

    /**
     * Stable plugin identifier matching the cache.<n>.type config value.
     * Convention: uppercase ASCII with underscores — LOCAL_CAFFEINE, REDIS, NOOP.
     */
    String id();

    /**
     * Creates a named cache instance. The config passed in is the full pipeline
     * config. The plugin reads its settings from {@code cache.<name>.*} keys,
     * falling back to global {@code cache.*} keys as needed.
     *
     * @param name    logical name (e.g. "embedding", "auth.decisions")
     * @param config  full pipeline config
     * @param metrics metrics runtime for hit/miss/size instrumentation
     * @param <K>     key type
     * @param <V>     value type
     * @return a non-null, ready-to-use Cache instance
     */
    <K, V> Cache<K, V> create(String name, PipelineConfig config, MetricsRuntime metrics);
}