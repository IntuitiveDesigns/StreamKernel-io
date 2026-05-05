/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import org.slf4j.Logger;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Safe configuration logger used during application startup.
 *
 * Purpose
 * -------
 * Enterprise systems must provide **diagnosability** while preserving **secrets hygiene**.
 * This utility prints the effective configuration in a deterministic, sanitized format so that:
 *
 *  • Operators can verify which configuration actually reached the runtime.
 *  • Support and incident responders can quickly reproduce environments.
 *  • Security-sensitive values are never exposed in logs.
 *
 * Why this matters for enterprise adoption:
 * -----------------------------------------
 * Configuration drift is one of the most common production failure causes.
 * A sanitized config dump:
 *   - reduces MTTR (mean time to resolution)
 *   - improves reproducibility
 *   - supports auditability and compliance reviews
 *
 * Security posture:
 * -----------------
 * This class implements **defensive secret masking**:
 *   - Masks keys commonly associated with credentials and tokens.
 *   - Masks credentials embedded in MongoDB URIs.
 *   - Avoids throwing errors if config internals change.
 *
 * Design goals:
 * -------------
 *  • Deterministic ordering → easier diffing across deployments.
 *  • Large pre-sized buffer → fewer reallocations during startup.
 *  • Reflection fallback → avoids forcing changes to PipelineConfig API.
 *  • Fail-safe → logging should never break application startup.
 */
public final class ConfigDumper {

    /**
     * Regex used to detect configuration keys that likely contain secrets.
     *
     * Matches common credential naming patterns:
     * - passwords / tokens / keys
     * - OAuth / SASL / SSL
     * - Keycloak and similar identity providers
     *
     * Case-insensitive for robustness across configuration styles.
     */
    private static final Pattern SECRET_KEY = Pattern.compile(
            ".*(password|passwd|secret|token|apikey|api_key|private|credential|keycloak|sasl|ssl|oauth).*",
            Pattern.CASE_INSENSITIVE
    );

    private ConfigDumper() {}

    /**
     * Dumps the effective configuration to logs in a sanitized form.
     *
     * Behavior:
     * - Converts PipelineConfig into a Map<String,String>.
     * - Sorts keys for deterministic output.
     * - Masks secrets.
     * - Emits a single structured log block.
     *
     * The output format is intentionally stable so that:
     *   - Log diff tools work reliably.
     *   - Support teams can request "CONFIG_DUMP" from customers.
     */
    public static void dump(Logger log, PipelineConfig cfg) {
        if (cfg == null) return;

        Map<String, String> props = toMap(cfg);
        if (props.isEmpty()) {
            log.info("CONFIG_DUMP: <empty>");
            return;
        }

        // Deterministic ordering improves debugging and reproducibility.
        List<String> keys = new ArrayList<>(props.keySet());
        Collections.sort(keys);

        // Pre-size buffer to reduce reallocation during logging.
        StringBuilder sb = new StringBuilder(4096);
        sb.append("CONFIG_DUMP_BEGIN (").append(keys.size()).append(" props)\n");

        for (String k : keys) {
            String v = props.get(k);
            sb.append(k)
                    .append('=')
                    .append(sanitize(k, v))
                    .append('\n');
        }

        sb.append("CONFIG_DUMP_END");
        log.info(sb.toString());
    }

    /**
     * Masks sensitive values before logging.
     *
     * Rules:
     *  1) Keys matching SECRET_KEY pattern → value replaced with "***".
     *  2) Special handling for MongoDB URI credentials embedded in URI.
     *  3) Otherwise return value unchanged.
     *
     * Example:
     *   mongodb://user:password@host → mongodb://***@host
     */
    private static String sanitize(String key, String value) {
        if (value == null) return "";

        final String normalizedKey = key == null ? "" : key.toLowerCase(Locale.ROOT);

        // Tokenizer settings are model-shape configuration, not secrets.
        if (normalizedKey.contains("tokenizer")) return value;

        // Mask secrets by key name.
        if (SECRET_KEY.matcher(normalizedKey).matches()) return "***";

        // Mask credentials embedded in MongoDB connection URIs.
        if (key.equalsIgnoreCase("mongodb.uri")) {
            return value.replaceAll("(mongodb(?:\\+srv)?://)([^@/]+)@", "$1***@");
        }

        return value;
    }

    /**
     * Extracts configuration as a Map<String,String>.
     *
     * Strategy:
     *  1) Prefer a public PipelineConfig.asMap() method if available.
     *  2) Fallback to reflection access of internal "props" field.
     *  3) If both fail, return empty map (fail-safe).
     *
     * Why reflection fallback?
     * ------------------------
     * This keeps ConfigDumper decoupled from the PipelineConfig API.
     * The dumper remains compatible even if the config implementation evolves.
     */
    private static Map<String, String> toMap(PipelineConfig cfg) {
        // Attempt to call PipelineConfig.asMap() if it exists.
        try {
            var m = cfg.getClass().getMethod("asMap");
            Object out = m.invoke(cfg);

            if (out instanceof Map<?, ?> mm) {
                Map<String, String> copy = new HashMap<>();
                for (var e : mm.entrySet()) {
                    if (e.getKey() != null) {
                        copy.put(
                                String.valueOf(e.getKey()),
                                e.getValue() == null ? "" : String.valueOf(e.getValue())
                        );
                    }
                }
                return copy;
            }
        } catch (Exception ignored) { }

        // Fallback: reflectively access "props" field if present.
        try {
            var f = cfg.getClass().getDeclaredField("props");
            f.setAccessible(true);
            Object out = f.get(cfg);

            if (out instanceof Map<?, ?> mm) {
                Map<String, String> copy = new HashMap<>();
                for (var e : mm.entrySet()) {
                    if (e.getKey() != null) {
                        copy.put(
                                String.valueOf(e.getKey()),
                                e.getValue() == null ? "" : String.valueOf(e.getValue())
                        );
                    }
                }
                return copy;
            }
        } catch (Exception ignored) { }

        // Fail-safe: never break startup due to config dumping.
        return Map.of();
    }
}
