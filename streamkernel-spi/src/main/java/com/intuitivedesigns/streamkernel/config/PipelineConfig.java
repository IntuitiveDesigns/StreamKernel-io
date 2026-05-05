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

package com.intuitivedesigns.streamkernel.config;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Enterprise-grade configuration manager (thread-safe, lazy-init).
 *
 * Resolution order:
 *  1) System property: sk.config.path
 *  2) Environment variable: SK_CONFIG_PATH
 *
 * This class intentionally does NOT throw during static initialization.
 * Call {@link #get()} to load; failures are explicit and actionable.
 */
public final class PipelineConfig {

    private static final String SYS_PROP = "sk.config.path";
    private static final String ENV_VAR  = "SK_CONFIG_PATH";
    private static final String PLACEHOLDER_PREFIX = "${";
    private static final String PLACEHOLDER_SUFFIX = "}";

    private static volatile PipelineConfig INSTANCE;

    // Immutable snapshot: never mutate after construction
    private final Properties props;
    private final String loadedFromPath;

    private PipelineConfig(Properties props, String loadedFromPath) {
        this.props = freeze(Objects.requireNonNull(props, "props"));
        this.loadedFromPath = loadedFromPath;
    }

    /**
     * Loads configuration once. Throws if no config is specified or cannot be read.
     */
    public static PipelineConfig get() {
        PipelineConfig local = INSTANCE;
        if (local != null) return local;

        synchronized (PipelineConfig.class) {
            local = INSTANCE;
            if (local != null) return local;

            final String path = resolveConfigPath();
            if (path == null || path.isBlank()) {
                throw new IllegalStateException("No configuration file specified (sk.config.path / SK_CONFIG_PATH)");
            }

            final Path p = Path.of(path.trim());
            if (!Files.exists(p)) {
                throw new IllegalStateException("Config file does not exist: " + p.toAbsolutePath());
            }
            if (!Files.isRegularFile(p)) {
                throw new IllegalStateException("Config path is not a regular file: " + p.toAbsolutePath());
            }

            final Properties loaded = new Properties();
            try (InputStream is = new BufferedInputStream(new FileInputStream(p.toFile()))) {
                loaded.load(is);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load config file: " + p.toAbsolutePath(), e);
            }

            local = new PipelineConfig(loaded, p.toAbsolutePath().toString());
            INSTANCE = local;
            return local;
        }
    }

    /**
     * Allows tests or embedded bootstraps to inject config directly.
     */
    public static PipelineConfig from(Properties props, String loadedFromPath) {
        if (props == null) throw new IllegalArgumentException("props cannot be null");
        return new PipelineConfig(props, loadedFromPath);
    }

    /**
     * Creates a new PipelineConfig that overlays the given overrides on top of the base config.
     *
     * Important:
     * - This does NOT mutate the base config.
     * - Overrides are applied as string properties (consistent with Properties semantics).
     * - loadedFromPath is preserved with a suffix for traceability.
     */
    public static PipelineConfig overlay(PipelineConfig base, Map<String, String> overrides) {
        if (base == null) throw new IllegalArgumentException("base cannot be null");
        if (overrides == null || overrides.isEmpty()) return base;

        final Properties merged = new Properties();

        // Copy base snapshot
        for (String k : base.props.stringPropertyNames()) {
            merged.setProperty(k, base.props.getProperty(k));
        }

        // Apply overrides (string-only)
        for (Map.Entry<String, String> e : overrides.entrySet()) {
            if (e == null) continue;
            final String key = e.getKey();
            if (key == null) continue;

            final String k = key.trim();
            if (k.isEmpty()) continue;

            final String v = e.getValue();
            if (v == null) {
                merged.remove(k);
            } else {
                merged.setProperty(k, v);
            }
        }

        final String from = (base.loadedFromPath == null || base.loadedFromPath.isBlank())
                ? "overlay"
                : (base.loadedFromPath + " (overlay)");
        return new PipelineConfig(merged, from);
    }

    public String loadedFromPath() {
        return loadedFromPath;
    }

    private static String resolveConfigPath() {
        String path = System.getProperty(SYS_PROP);
        if (path == null || path.isBlank()) {
            path = System.getenv(ENV_VAR);
        }
        return path;
    }

    public String getString(String key, String defaultValue) {
        if (key == null) return defaultValue;
        final String v = props.getProperty(key);
        return (v == null) ? defaultValue : resolvePlaceholder(v);
    }

    public int getInt(String key, int defaultValue) {
        if (key == null) return defaultValue;
        final String val = getString(key, null);
        if (val == null) return defaultValue;
        try {
            return Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public long getLong(String key, long defaultValue) {
        if (key == null) return defaultValue;
        final String val = getString(key, null);
        if (val == null) return defaultValue;
        try {
            return Long.parseLong(val.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        if (key == null) return defaultValue;
        final String val = getString(key, null);
        return (val == null) ? defaultValue : Boolean.parseBoolean(val.trim());
    }

    public boolean hasPath(String key) {
        if (key == null) return false;
        return props.containsKey(key);
    }

    public Map<String, Object> asMap() {
        final Map<String, Object> map = new HashMap<>();
        for (String name : props.stringPropertyNames()) {
            map.put(name, resolvePlaceholder(props.getProperty(name)));
        }
        return Collections.unmodifiableMap(map);
    }

    public Object get(String key) {
        if (key == null) return null;
        return resolvePlaceholder(props.getProperty(key));
    }

    public Set<String> keys() {
        return props.stringPropertyNames();
    }

    static void resetForTests() {
        INSTANCE = null;
    }

    private static Properties freeze(Properties in) {
        final Properties out = new Properties();
        for (String k : in.stringPropertyNames()) {
            out.setProperty(k, in.getProperty(k));
        }
        return out;
    }

    private static String resolvePlaceholder(String raw) {
        if (raw == null) return null;

        final String s = raw.trim();
        if (!s.startsWith(PLACEHOLDER_PREFIX) || !s.endsWith(PLACEHOLDER_SUFFIX)) {
            return raw;
        }

        final String body = s.substring(PLACEHOLDER_PREFIX.length(), s.length() - PLACEHOLDER_SUFFIX.length());
        final int sourceSep = body.indexOf(':');
        if (sourceSep <= 0) return raw;

        final String source = body.substring(0, sourceSep).trim();
        final String remainder = body.substring(sourceSep + 1);
        if (remainder.isBlank()) return raw;

        if ("file".equals(source)) {
            return resolveFilePlaceholder(remainder, raw);
        }

        final String key;
        final String fallback;
        final int fallbackSep = remainder.indexOf(':');
        if (fallbackSep >= 0) {
            key = remainder.substring(0, fallbackSep).trim();
            fallback = remainder.substring(fallbackSep + 1);
        } else {
            key = remainder.trim();
            fallback = "";
        }

        if (key.isEmpty()) return raw;

        final String resolved = switch (source) {
            case "env" -> System.getenv(key);
            case "sys" -> System.getProperty(key);
            default -> null;
        };

        if (resolved != null && !resolved.isBlank()) {
            return resolved;
        }
        if (!fallback.isEmpty()) {
            return fallback;
        }
        return raw;
    }

    private static String resolveFilePlaceholder(String rawPath, String unresolved) {
        final String pathValue = rawPath.trim();
        if (pathValue.isEmpty()) {
            return unresolved;
        }

        try {
            final String fileValue = Files.readString(Path.of(pathValue), StandardCharsets.UTF_8);
            return stripSingleTrailingLineEnding(fileValue);
        } catch (IOException | RuntimeException ignored) {
            return unresolved;
        }
    }

    private static String stripSingleTrailingLineEnding(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        if (value.endsWith("\r\n")) {
            return value.substring(0, value.length() - 2);
        }
        if (value.endsWith("\n")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }
}
