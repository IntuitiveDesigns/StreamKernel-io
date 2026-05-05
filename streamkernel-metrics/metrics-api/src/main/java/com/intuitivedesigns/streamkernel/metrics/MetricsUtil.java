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

package com.intuitivedesigns.streamkernel.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Micrometer utility helpers.
 *
 * <p><b>Design goals (enterprise)</b>
 * <ul>
 *   <li><b>Safety:</b> never throws for null/empty inputs; skips invalid tag entries.</li>
 *   <li><b>Determinism:</b> trims whitespace and avoids emitting empty tag keys/values.</li>
 *   <li><b>Micrometer correctness:</b> avoids null keys/values (Micrometer may throw).</li>
 *   <li><b>Interop:</b> converts StreamKernel tag maps into Micrometer {@link Tags}.</li>
 * </ul>
 *
 * <p><b>Operational note</b>
 * <ul>
 *   <li>Common tags increase metric cardinality multiplicatively. Keep them stable (env, region, service, version),
 *       and avoid high-cardinality values (requestId, userId, hostname if ephemeral, etc.).</li>
 * </ul>
 */
public final class MetricsUtil {

    private MetricsUtil() {}

    /**
     * Applies common tags from {@link MetricsSettings} to a Micrometer {@link MeterRegistry}.
     *
     * <p>This is typically called once during metrics initialization. Micrometer stores these tags and applies them
     * to all meters created after configuration. Passing {@link Tags#empty()} is a safe no-op.</p>
     *
     * @param registry Micrometer registry (may be null)
     * @param settings metrics settings (may be null)
     */
    public static void applyCommonTags(MeterRegistry registry, MetricsSettings settings) {
        if (registry == null || settings == null) return;

        final Tags tags = toTags(settings.commonTags);

        // Tags implements Iterable, so passing an empty set is a safe no-op.
        registry.config().commonTags(tags);
    }

    /**
     * Converts a raw tag map into Micrometer {@link Tags}.
     *
     * <p><b>Rules</b>
     * <ul>
     *   <li>Null/blank keys or values are skipped.</li>
     *   <li>Keys/values are trimmed.</li>
     *   <li>Optionally normalizes obvious risky keys/values (see {@link #isValidKey(String)}).</li>
     * </ul>
     *
     * <p><b>Why so defensive?</b>
     * Micrometer may throw if tag key/value is null. Also, some registries reject invalid characters.
     *
     * @param input map of tag key → tag value (may be null)
     * @return Micrometer Tags (never null)
     */
    public static Tags toTags(Map<String, String> input) {
        if (input == null || input.isEmpty()) return Tags.empty();

        final List<Tag> out = new ArrayList<>(input.size());

        for (Map.Entry<String, String> e : input.entrySet()) {
            final String k = safeTrim(e.getKey());
            final String v = safeTrim(e.getValue());

            // Skip invalid entries: Micrometer and/or backends can reject them.
            if (k == null || v == null) continue;
            if (!isValidKey(k)) continue;
            if (!isValidValue(v)) continue;

            out.add(Tag.of(k, v));
        }

        return out.isEmpty() ? Tags.empty() : Tags.of(out);
    }

    /**
     * Returns {@code null} for null/blank strings; otherwise returns a trimmed value.
     */
    private static String safeTrim(String s) {
        if (s == null) return null;
        final String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    /**
     * Validates tag keys for basic interoperability across metrics backends.
     *
     * <p>Micrometer itself is fairly permissive, but downstream registries can be strict.
     * This keeps things predictable without being overly restrictive.</p>
     *
     * <p><b>Policy</b>
     * <ul>
     *   <li>Rejects control characters.</li>
     *   <li>Rejects extremely long keys (helps prevent accidental cardinality blowups / payload bloat).</li>
     * </ul>
     */
    private static boolean isValidKey(String k) {
        // Conservative bound; many backends allow more, but this is a safe default.
        if (k.length() > 128) return false;

        for (int i = 0; i < k.length(); i++) {
            char c = k.charAt(i);
            if (c <= 0x1F || c == 0x7F) return false; // control chars
        }

        // Prevent extremely common mistakes (optional): keys that are effectively "null"
        final String lower = k.toLowerCase(Locale.ROOT);
        return !("null".equals(lower) || "none".equals(lower));
    }

    /**
     * Validates tag values for basic interoperability across metrics backends.
     *
     * <p><b>Policy</b>
     * <ul>
     *   <li>Rejects control characters.</li>
     *   <li>Rejects extremely long values.</li>
     * </ul>
     */
    private static boolean isValidValue(String v) {
        if (v.length() > 256) return false;

        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c <= 0x1F || c == 0x7F) return false;
        }

        // Values like "null"/"none" aren't inherently wrong, but often indicate misconfiguration.
        // Keep them allowed (unlike keys) to avoid surprising drops.
        return true;
    }
}
