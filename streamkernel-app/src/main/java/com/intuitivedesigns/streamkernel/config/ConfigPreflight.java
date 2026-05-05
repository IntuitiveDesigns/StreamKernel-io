/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.config;

import java.util.*;
import java.util.stream.Collectors;

public final class ConfigPreflight {

    private ConfigPreflight() {}

    public enum Component {
        PIPELINE, SOURCE, TRANSFORM, SINK, SECURITY, DLQ, METRICS
    }

    public record Requirement(Component component, String id, List<String> requiredKeys) {}

    /**
     * Register required keys per component/plugin id.
     * Canonical-only: no legacy/global kafka.* keys.
     * Keep this small and explicit. Only include "hard required" keys.
     */
    public static List<Requirement> requirementsFor(PipelineConfig cfg) {
        final String sourceType = getString(cfg, "source.type", "").trim().toUpperCase(Locale.ROOT);
        final String sinkType = getString(cfg, "sink.type", "").trim().toUpperCase(Locale.ROOT);

        // Enterprise contract: transform.chain (if set) is authoritative; otherwise transform.type.
        final String transformChainRaw = getString(cfg, "transform.chain", null);
        final String transformType = getString(cfg, "transform.type", "").trim().toUpperCase(Locale.ROOT);

        final String securityType = firstNonBlank(
                getString(cfg, "security.type", null),
                getString(cfg, "security.plugin.id", null),
                "PERMIT_ALL"
        ).trim().toUpperCase(Locale.ROOT);

        final List<Requirement> reqs = new ArrayList<>();

        // Pipeline core
        reqs.add(new Requirement(Component.PIPELINE, "CORE", List.of(
                "pipeline.id",
                "pipeline.parallelism",
                "pipeline.batch.size"
        )));

        // Source
        if ("KAFKA".equals(sourceType)) {
            reqs.add(new Requirement(Component.SOURCE, "KAFKA", List.of(
                    "source.kafka.topic",
                    "source.kafka.bootstrap.servers"
            )));
        } else if ("PULSAR".equals(sourceType)) {
            reqs.add(new Requirement(Component.SOURCE, "PULSAR", List.of(
                    "source.pulsar.service.url",
                    "source.pulsar.topic",
                    "source.pulsar.subscription"
            )));
        } else if ("SYNTHETIC".equals(sourceType)) {
            // Only require the truly hard key(s). Keep others optional to avoid breaking profiles.
            reqs.add(new Requirement(Component.SOURCE, "SYNTHETIC", List.of(
                    "source.synthetic.payload.size"
            )));
        }

        // Sink
        if ("KAFKA".equals(sinkType)) {
            reqs.add(new Requirement(Component.SINK, "KAFKA", List.of(
                    "sink.kafka.topic",
                    "sink.kafka.bootstrap.servers"
            )));
        } else if ("SNOWFLAKE_SNOWPIPE_STREAMING".equals(sinkType)) {
            reqs.add(new Requirement(Component.SINK, "SNOWFLAKE_SNOWPIPE_STREAMING", List.of(
                    "snowflake.account",
                    "snowflake.user",
                    "snowflake.database",
                    "snowflake.schema",
                    "snowflake.pipe"
            )));
        } else if ("DEVNULL".equals(sinkType)) {
            reqs.add(new Requirement(Component.SINK, "DEVNULL", List.of()));
        }

        // Transformer(s)
        final String[] chainIds = parseCsvIds(transformChainRaw);
        if (chainIds.length > 0) {
            for (String rawId : chainIds) {
                final String id = rawId.trim().toUpperCase(Locale.ROOT);
                if (id.isEmpty()) continue;

                // Keep this explicit: only include "hard required" keys that prove the component is real.
                if ("HTTP_EMBEDDING".equals(id)) {
                    reqs.add(new Requirement(Component.TRANSFORM, "HTTP_EMBEDDING", List.of(
                            "transform.http.url"
                    )));
                } else if ("STRING_TO_WIREEVENT".equals(id)) {
                    // No hard-required keys — transformer is fully optional-config.
                    // Explicit case prevents typos from silently passing as "unknown".
                    reqs.add(new Requirement(Component.TRANSFORM, "STRING_TO_WIREEVENT", List.of()));
                } else {
                    // Unknown transformer id: still register a requirement record so errors are readable.
                    // We do not declare required keys here (plugin may have optional config).
                    reqs.add(new Requirement(Component.TRANSFORM, id, List.of()));
                }
            }
        } else {
            // Legacy/single-transform mode
            if ("NOOP".equals(transformType) || transformType.isEmpty()) {
                reqs.add(new Requirement(Component.TRANSFORM, "NOOP", List.of()));
            } else if ("HTTP_EMBEDDING".equals(transformType)) {
                reqs.add(new Requirement(Component.TRANSFORM, "HTTP_EMBEDDING", List.of(
                        "transform.http.url"
                )));
            } else if ("STRING_TO_WIREEVENT".equals(transformType)) {
                // No hard-required keys. Explicit case prevents typos from silently
                // passing as "unknown" and documents this as a known transformer type.
                reqs.add(new Requirement(Component.TRANSFORM, "STRING_TO_WIREEVENT", List.of()));
            } else {
                reqs.add(new Requirement(Component.TRANSFORM, transformType, List.of()));
            }
        }

        // Security (only require OPA URL if OPA sidecar enabled)
        if ("OPA_SIDECAR".equals(securityType)) {
            reqs.add(new Requirement(Component.SECURITY, "OPA_SIDECAR", List.of(
                    "security.opa.url"
            )));
        } else {
            reqs.add(new Requirement(Component.SECURITY, securityType, List.of()));
        }

        return reqs;
    }

    public static void validateOrThrow(PipelineConfig cfg) {
        Objects.requireNonNull(cfg, "cfg");

        final List<Requirement> reqs = requirementsFor(cfg);

        final Map<Requirement, List<String>> missingByReq = new LinkedHashMap<>();
        for (Requirement r : reqs) {
            final List<String> missing = r.requiredKeys().stream()
                    .filter(k -> isBlank(getString(cfg, k, null)))
                    .collect(Collectors.toList());
            if (!missing.isEmpty()) {
                missingByReq.put(r, missing);
            }
        }

        if (requiresExplicitRunId(cfg) && !hasEffectiveRunId(cfg)) {
            missingByReq.put(
                    new Requirement(Component.PIPELINE, "RUN_ID", List.of()),
                    List.of("Either -Dsk.run.id=<id> or metrics.tag.run_id=<id> is required when benchmarking or provenance is enabled.")
            );
        }

        if (!missingByReq.isEmpty()) {
            final String msg = formatMissing(cfg, missingByReq);
            throw new IllegalArgumentException(msg);
        }
    }

    private static String formatMissing(PipelineConfig cfg, Map<Requirement, List<String>> missingByReq) {
        final String sourceType = getString(cfg, "source.type", "");
        final String sinkType = getString(cfg, "sink.type", "");
        final String transformType = getString(cfg, "transform.type", "");
        final String transformChain = getString(cfg, "transform.chain", "");
        final String pipelineId = getString(cfg, "pipeline.id", "");

        final StringBuilder sb = new StringBuilder(512);
        sb.append("CONFIG PREFLIGHT FAILED\n");
        sb.append("pipeline.id=").append(pipelineId).append("\n");
        sb.append("source.type=").append(sourceType)
                .append(" | sink.type=").append(sinkType)
                .append(" | transform.type=").append(transformType)
                .append(" | transform.chain=").append(transformChain)
                .append("\n\n");

        for (Map.Entry<Requirement, List<String>> e : missingByReq.entrySet()) {
            Requirement r = e.getKey();
            sb.append("- ").append(r.component()).append("[").append(r.id()).append("] missing:\n");
            for (String k : e.getValue()) {
                sb.append("    ").append(k).append("\n");
            }
            sb.append("\n");
        }

        sb.append("Fix: add the missing keys to the profile properties file.\n");
        return sb.toString();
    }

    private static String[] parseCsvIds(String csv) {
        if (csv == null) return new String[0];
        final String s = csv.trim();
        if (s.isEmpty()) return new String[0];

        final String[] raw = s.split(",");
        int n = 0;
        for (String r : raw) {
            if (r != null && !r.trim().isEmpty()) n++;
        }
        if (n == 0) return new String[0];

        final String[] out = new String[n];
        int j = 0;
        for (String r : raw) {
            if (r == null) continue;
            final String t = r.trim();
            if (t.isEmpty()) continue;
            out[j++] = t;
        }
        return out;
    }

    // ---- helpers ----

    /**
     * Direct delegation to PipelineConfig.getString(key, default).
     *
     * Previous implementation used a 3-path reflection chain
     * (getMethod("getString", ...).invoke(...)) which silently swallowed all
     * exceptions and could mask a PipelineConfig that lacked the method.
     * PipelineConfig defines getString(String, String) as part of its contract —
     * all other plugins call it directly. Preflight should do the same.
     */
    private static String getString(PipelineConfig cfg, String key, String def) {
        return cfg.getString(key, def);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static boolean requiresExplicitRunId(PipelineConfig cfg) {
        return cfg.getBoolean("streamkernel.bench.enabled", false)
                || cfg.getBoolean("streamkernel.provenance.enabled", false);
    }

    private static boolean hasEffectiveRunId(PipelineConfig cfg) {
        return !isBlank(firstNonBlank(
                System.getProperty("sk.run.id"),
                getString(cfg, "metrics.tag.run_id", null),
                null
        ));
    }

    private static String firstNonBlank(String a, String b, String def) {
        if (a != null && !a.isBlank()) return a;
        if (b != null && !b.isBlank()) return b;
        return def;
    }
}
