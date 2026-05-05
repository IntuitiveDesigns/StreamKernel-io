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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/**
 * RedisCachePlugin
 * ================
 * Registers the REDIS backend for both cache roles.
 *
 * Values are serialized as UTF-8 JSON strings. For non-String types, the
 * caller is responsible for providing a JSON-serializable value — or swap
 * the serializer by subclassing / wrapping this plugin.
 *
 * Distributed use case
 * --------------------
 * Use REDIS when you need shared cache state across multiple StreamKernel
 * nodes — e.g., embedding dedup in a multi-instance deployment, or shared
 * auth decision caching across replicas.
 *
 * Single-node pipelines should prefer LOCAL_CAFFEINE — it is 2-3 orders of
 * magnitude faster and has no network dependency.
 *
 * Config keys
 * -----------
 * Orchestrator-level (cache.type=REDIS):
 *   cache.redis.host            Redis host        (default: localhost)
 *   cache.redis.port            Redis port        (default: 6379)
 *   cache.redis.ttl.seconds     Key TTL           (default: 300)
 *   cache.redis.pool.max.total  Max pool size     (default: 16)
 *   cache.redis.key.prefix      Namespace prefix  (default: "sk:")
 *
 * Per-name (cache.<n>.type=REDIS):
 *   cache.<n>.redis.host
 *   cache.<n>.redis.port
 *   cache.<n>.redis.ttl.seconds
 *   cache.<n>.redis.pool.max.total
 *   cache.<n>.redis.key.prefix
 *   Falls back to global cache.redis.* keys if per-name keys are absent.
 */
public final class RedisCachePlugin implements CachePlugin, CacheProviderPlugin {

    public static final String ID = "REDIS";

    private static final Logger log = LoggerFactory.getLogger(RedisCachePlugin.class);

    // -------------------------------------------------------------------------
    // CachePlugin (orchestrator-level)
    // -------------------------------------------------------------------------

    @Override
    public String id() { return ID; }

    @Override
    public PluginKind kind() { return PluginKind.CACHE; }

    @Override
    public Cache<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config,  "config");
        Objects.requireNonNull(metrics, "metrics");
        return buildCache(ID, "", config, metrics);
    }

    // -------------------------------------------------------------------------
    // CacheProviderPlugin (per-name registry)
    // -------------------------------------------------------------------------

    @Override
    public <K, V> Cache<K, V> create(String name, PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(name,    "name");
        Objects.requireNonNull(config,  "config");
        Objects.requireNonNull(metrics, "metrics");
        final String prefix = "cache." + name + ".redis.";
        return buildCache(name, prefix, config, metrics);
    }

    // -------------------------------------------------------------------------
    // Shared builder
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private <K, V> Cache<K, V> buildCache(
            String name, String perNamePrefix, PipelineConfig config, MetricsRuntime metrics) {

        final String host = firstString(
                config.getString(perNamePrefix + "host", null),
                config.getString("cache.redis.host",     null),
                "localhost");

        final int port = (int) firstLong(
                config.getString(perNamePrefix + "port",  null),
                config.getString("cache.redis.port",      null),
                6379L);

        final int ttlSec = (int) firstLong(
                config.getString(perNamePrefix + "ttl.seconds",    null),
                config.getString("cache.redis.ttl.seconds",        null),
                300L);

        final int poolMax = (int) firstLong(
                config.getString(perNamePrefix + "pool.max.total",  null),
                config.getString("cache.redis.pool.max.total",      null),
                16L);

        // Key prefix namespaces entries — prevents collisions across named caches
        // when pointing at the same Redis instance.
        final String keyPrefix = firstString(
                config.getString(perNamePrefix + "key.prefix", null),
                config.getString("cache.redis.key.prefix",     null),
                "sk:" + name + ":");

        log.info("[{}] Creating Redis cache host={}:{} ttl={}s pool={} prefix={}",
                name, host, port, ttlSec, poolMax, keyPrefix);

        return (Cache<K, V>) new RedisCache<>(name, host, port, ttlSec, poolMax, keyPrefix);
    }

    // -------------------------------------------------------------------------
    // RedisCache
    // -------------------------------------------------------------------------

    private static final class RedisCache<K, V> implements Cache<K, V> {

        private final JedisPool pool;
        private final int       ttlSeconds;
        private final String    keyPrefix;
        private final String    name;

        RedisCache(String name, String host, int port,
                   int ttlSeconds, int poolMaxTotal, String keyPrefix) {
            this.name       = name;
            this.ttlSeconds = ttlSeconds;
            this.keyPrefix  = keyPrefix;

            final JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(poolMaxTotal);
            cfg.setMaxIdle(Math.min(poolMaxTotal, 4));
            cfg.setMinIdle(1);
            cfg.setTestOnBorrow(false);   // avoid extra RTT on every acquire
            cfg.setTestOnReturn(false);
            this.pool = new JedisPool(cfg, host, port);
        }

        /** Key stored in Redis: prefix + key.toString() (UTF-8) */
        private String redisKey(K key) {
            return keyPrefix + key.toString();
        }

        /**
         * Value contract: V must be String, or the caller must ensure
         * V.toString() produces a deserializable representation.
         * For embedding vectors (float[]), use a JSON serializer wrapper.
         */
        @Override
        @SuppressWarnings("unchecked")
        public Optional<V> get(K key) {
            if (key == null) return Optional.empty();
            try (Jedis jedis = pool.getResource()) {
                final String val = jedis.get(redisKey(key));
                return (Optional<V>) Optional.ofNullable(val);
            } catch (Exception e) {
                log.warn("[{}] Redis GET failed for key={}: {}", name, key, e.getMessage());
                return Optional.empty();
            }
        }

        @Override
        public void put(K key, V value) {
            if (key == null || value == null) return;
            try (Jedis jedis = pool.getResource()) {
                jedis.setex(
                        redisKey(key).getBytes(StandardCharsets.UTF_8),
                        ttlSeconds,
                        value.toString().getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                log.warn("[{}] Redis SET failed for key={}: {}", name, key, e.getMessage());
            }
        }

        @Override
        public void invalidate(K key) {
            if (key == null) return;
            try (Jedis jedis = pool.getResource()) {
                jedis.del(redisKey(key));
            } catch (Exception e) {
                log.warn("[{}] Redis DEL failed for key={}: {}", name, key, e.getMessage());
            }
        }

        @Override
        public void close() {
            try {
                pool.close();
                log.debug("[{}] RedisCache closed", name);
            } catch (Exception e) {
                log.warn("[{}] Error closing Redis pool", name, e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String firstString(String primary, String secondary, String fallback) {
        if (primary  != null && !primary.isBlank())   return primary.trim();
        if (secondary != null && !secondary.isBlank()) return secondary.trim();
        return fallback;
    }

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