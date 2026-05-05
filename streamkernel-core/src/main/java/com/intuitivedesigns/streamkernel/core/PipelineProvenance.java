/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.security.KafkaClientSecurity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Builds a stable, namespaced provenance header set for every payload emitted by a source.
 */
public final class PipelineProvenance {

    private static final String KEY_ENABLED = "streamkernel.provenance.enabled";
    public static final String HEADER_PREFIX = "streamkernel.provenance.";
    public static final String HEADER_PIPELINE_ID = HEADER_PREFIX + "pipeline.id";
    public static final String HEADER_RUN_ID = HEADER_PREFIX + "run.id";
    public static final String HEADER_SOURCE_TYPE = HEADER_PREFIX + "source.type";
    public static final String HEADER_SINK_TYPE = HEADER_PREFIX + "sink.type";
    public static final String HEADER_TRANSFORM_CHAIN = HEADER_PREFIX + "transform.chain";
    public static final String HEADER_SECURITY_TYPE = HEADER_PREFIX + "security.type";
    public static final String HEADER_CONFIG_SHA256 = HEADER_PREFIX + "config.sha256";
    public static final String HEADER_SOURCE_AUTH = HEADER_PREFIX + "source.auth";
    public static final String HEADER_SINK_AUTH = HEADER_PREFIX + "sink.auth";
    public static final String HEADER_MODEL_NAME = HEADER_PREFIX + "model.name";
    public static final String HEADER_MODEL_ALIAS = HEADER_PREFIX + "model.alias";
    public static final String HEADER_MODEL_VERSION = HEADER_PREFIX + "model.version";
    public static final String HEADER_MODEL_RUN_ID = HEADER_PREFIX + "model.run.id";
    public static final String HEADER_MODEL_EXPERIMENT_ID = HEADER_PREFIX + "model.experiment.id";
    public static final String HEADER_MODEL_STAGE = HEADER_PREFIX + "model.stage";
    public static final String HEADER_INFERENCE_TIMESTAMP = HEADER_PREFIX + "inference.timestamp";
    public static final String HEADER_FEATURE_VERSION = HEADER_PREFIX + "feature.version";
    public static final String HEADER_PROMPT_VERSION = HEADER_PREFIX + "prompt.version";
    public static final String HEADER_TRANSFORM_VERSION = HEADER_PREFIX + "transform.version";
    public static final String HEADER_MODEL_REF_SHA256 = HEADER_PREFIX + "model.ref.sha256";
    public static final String HEADER_POLICY_SHA256 = HEADER_PREFIX + "policy.sha256";
    public static final String HEADER_POLICY_REF_SHA256 = HEADER_PREFIX + "policy.ref.sha256";

    private final Map<String, String> headers;

    private PipelineProvenance(Map<String, String> headers) {
        this.headers = Map.copyOf(headers);
    }

    public static PipelineProvenance fromConfig(PipelineConfig config) {
        Objects.requireNonNull(config, "config");

        if (!config.getBoolean(KEY_ENABLED, false)) {
            return new PipelineProvenance(Map.of());
        }

        final LinkedHashMap<String, String> headers = new LinkedHashMap<>();
        putIfPresent(headers, HEADER_PIPELINE_ID, config.getString("pipeline.id", null));
        putIfPresent(headers, HEADER_RUN_ID, firstNonBlank(
                System.getProperty("sk.run.id"),
                config.getString("metrics.tag.run_id", null)
        ));
        putIfPresent(headers, HEADER_SOURCE_TYPE, config.getString("source.type", null));
        putIfPresent(headers, HEADER_SINK_TYPE, config.getString("sink.type", null));
        putIfPresent(headers, HEADER_TRANSFORM_CHAIN, firstNonBlank(
                config.getString("transform.chain", null),
                config.getString("transform.type", null)
        ));
        putIfPresent(headers, HEADER_SECURITY_TYPE, firstNonBlank(
                config.getString("security.type", null),
                "PERMIT_ALL"
        ));
        putIfPresent(headers, HEADER_CONFIG_SHA256, sha256(canonicalizeConfig(config)));

        final String sourceType = firstNonBlank(config.getString("source.type", null), "");
        final String sinkType = firstNonBlank(config.getString("sink.type", null), "");
        if ("KAFKA".equalsIgnoreCase(sourceType)) {
            putIfPresent(headers, HEADER_SOURCE_AUTH, KafkaClientSecurity.describeAuthMode(config, "source.kafka."));
        }
        if ("KAFKA".equalsIgnoreCase(sinkType)) {
            putIfPresent(headers, HEADER_SINK_AUTH, KafkaClientSecurity.describeAuthMode(config, "sink.kafka."));
        } else if ("SNOWFLAKE_SNOWPIPE_STREAMING".equalsIgnoreCase(sinkType)) {
            putIfPresent(headers, HEADER_SINK_AUTH, describeSnowflakeAuth(config));
        }

        putIfPresent(headers, HEADER_MODEL_NAME, firstNonBlank(
                config.getString("model.name", null),
                config.getString("ai.model.name", null)
        ));
        putIfPresent(headers, HEADER_MODEL_ALIAS, firstNonBlank(
                config.getString("model.alias", null),
                config.getString("ai.model.alias", null)
        ));
        putIfPresent(headers, HEADER_MODEL_VERSION, firstNonBlank(
                config.getString("model.resolved.version", null),
                config.getString("model.version", null),
                config.getString("ai.model.version", null)
        ));
        putIfPresent(headers, HEADER_MODEL_RUN_ID, firstNonBlank(
                config.getString("model.run.id", null),
                config.getString("ai.model.run.id", null)
        ));
        putIfPresent(headers, HEADER_MODEL_EXPERIMENT_ID, firstNonBlank(
                config.getString("model.experiment.id", null),
                config.getString("ai.model.experiment.id", null),
                config.getString("ai.experiment.id", null)
        ));
        putIfPresent(headers, HEADER_MODEL_STAGE, firstNonBlank(
                config.getString("model.stage", null),
                config.getString("model.current_stage", null),
                config.getString("ai.model.stage", null),
                config.getString("model.alias", null)
        ));
        putIfPresent(headers, HEADER_FEATURE_VERSION, firstNonBlank(
                config.getString("ai.feature.version", null),
                config.getString("feature.version", null)
        ));
        putIfPresent(headers, HEADER_PROMPT_VERSION, firstNonBlank(
                config.getString("ai.prompt.version", null),
                config.getString("prompt.version", null)
        ));
        putIfPresent(headers, HEADER_TRANSFORM_VERSION, firstNonBlank(
                config.getString("ai.transform.version", null),
                config.getString("transform.version", null)
        ));
        putIfPresent(headers, HEADER_MODEL_REF_SHA256, hashReference(firstNonBlank(
                config.getString("model.resolved.artifact.uri", null),
                config.getString("model.uri", null),
                config.getString("ai.model.uri", null)
        )));

        final String policyHash = hashPolicyArtifact(config);
        if (policyHash != null) {
            headers.put(HEADER_POLICY_SHA256, policyHash);
        } else {
            putIfPresent(headers, HEADER_POLICY_REF_SHA256, hashReference(firstNonBlank(
                    config.getString("security.opa.url", null),
                    config.getString("security.policy.url", null)
            )));
        }

        return new PipelineProvenance(headers);
    }

    public Map<String, String> headers() {
        return headers;
    }

    public static boolean isProvenanceHeader(String key) {
        return key != null && key.startsWith(HEADER_PREFIX);
    }

    public static Map<String, String> extractProvenanceHeaders(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return Map.of();
        }

        final LinkedHashMap<String, String> headers = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (isProvenanceHeader(entry.getKey()) && !isBlank(entry.getValue())) {
                headers.put(entry.getKey(), entry.getValue().trim());
            }
        }
        return headers.isEmpty() ? Map.of() : Map.copyOf(headers);
    }

    private static String canonicalizeConfig(PipelineConfig config) {
        final TreeMap<String, Object> sorted = new TreeMap<>(config.asMap());
        final StringBuilder builder = new StringBuilder(sorted.size() * 32);
        for (Map.Entry<String, Object> entry : sorted.entrySet()) {
            builder.append(entry.getKey())
                    .append('=')
                    .append(Objects.toString(entry.getValue(), ""))
                    .append('\n');
        }
        return builder.toString();
    }

    private static String describeSnowflakeAuth(PipelineConfig config) {
        if (!isBlank(config.getString("snowflake.private.key", null))
                || !isBlank(config.getString("snowflake.private.key.file", null))) {
            return "JWT_KEYPAIR";
        }
        final String authType = config.getString("snowflake.authorization.type", null);
        return firstNonBlank(authType, "UNKNOWN");
    }

    private static String hashPolicyArtifact(PipelineConfig config) {
        final String pathValue = firstNonBlank(
                config.getString("security.opa.policy.path", null),
                config.getString("security.policy.path", null)
        );
        if (isBlank(pathValue)) {
            return null;
        }

        try {
            final Path path = Path.of(pathValue.trim());
            if (!Files.isRegularFile(path)) {
                return null;
            }
            return sha256(Files.readString(path, StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException ignored) {
            return null;
        }
    }

    private static String hashReference(String value) {
        if (isBlank(value)) {
            return null;
        }
        return sha256(value.trim());
    }

    private static void putIfPresent(Map<String, String> headers, String key, String value) {
        if (!isBlank(value)) {
            headers.put(key, value.trim());
        }
    }

    private static String sha256(String value) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return toHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String toHex(byte[] bytes) {
        final StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            final int value = b & 0xff;
            if (value < 0x10) {
                hex.append('0');
            }
            hex.append(Integer.toHexString(value));
        }
        return hex.toString();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value.trim();
            }
        }
        return null;
    }
}
