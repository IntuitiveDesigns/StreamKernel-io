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

/**
 * CacheRegistry
 * =============
 * Named cache factory. Any plugin — transformer, security, source, sink —
 * receives a CacheRegistry at construction time and calls getOrCreate() to
 * obtain its own isolated cache instance backed by whichever backend
 * (LOCAL_CAFFEINE, REDIS, NOOP) is configured for that name.
 *
 * Config resolution for a name "n"
 * ----------------------------------
 *   cache.<n>.type              (per-name override, highest priority)
 *   cache.type                  (global default)
 *   "NOOP"                      (safe fallback if nothing is configured)
 *
 * Per-name backend config examples:
 *   cache.embedding.type=LOCAL_CAFFEINE
 *   cache.embedding.local.max.size=50000
 *   cache.embedding.local.ttl.seconds=600
 *
 *   cache.auth.decisions.type=NOOP
 *
 *   cache.schema.lookup.type=REDIS
 *   cache.schema.lookup.redis.host=redis-primary
 *   cache.schema.lookup.redis.port=6379
 *   cache.schema.lookup.redis.ttl.seconds=3600
 *
 * Lifecycle
 * ---------
 * Created by PipelineFactory before any plugin is instantiated.
 * Closed by StreamKernel during shutdown — which closes every cache it owns.
 * Plugins must NOT call close() on instances returned by getOrCreate().
 */
public interface CacheRegistry extends AutoCloseable {

    /**
     * Returns an existing cache for this name, or creates one from config.
     *
     * The returned instance is owned by the registry; do not close it directly.
     *
     * @param name   logical name — used as the config key prefix and in metrics
     * @param config full pipeline config
     * @param <K>    cache key type
     * @param <V>    cache value type
     * @return non-null, ready-to-use Cache
     */
    <K, V> Cache<K, V> getOrCreate(String name, PipelineConfig config);

    /** Closes all cache instances owned by this registry. Idempotent. */
    @Override
    void close();
}