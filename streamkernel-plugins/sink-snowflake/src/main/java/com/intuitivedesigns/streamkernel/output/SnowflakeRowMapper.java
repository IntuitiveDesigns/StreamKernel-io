/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.avro.EnrichedTicket;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.PipelineProvenance;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class SnowflakeRowMapper {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeRowMapper.class);

    static final String SOURCE_TEXT_METADATA_KEY = "streamkernel.source.text";

    List<Map<String, Object>> toRows(List<PipelinePayload<Object>> batch, boolean strictPayloadType) {
        if (batch == null || batch.isEmpty()) {
            return List.of();
        }

        final ArrayList<Map<String, Object>> rows = new ArrayList<>(batch.size());
        for (PipelinePayload<Object> payload : batch) {
            final Map<String, Object> row = toRow(payload, strictPayloadType);
            if (row != null) {
                rows.add(row);
            }
        }
        return rows;
    }

    Map<String, Object> toRow(PipelinePayload<Object> payload, boolean strictPayloadType) {
        if (payload == null) {
            return null;
        }

        final Object value = unwrapPayloadValue(payload.data());
        if (value == null) {
            return null;
        }

        if (value instanceof EnrichedTicket ticket) {
            final LinkedHashMap<String, Object> row = baseRow(
                    payload,
                    value,
                    requiredEventKey(ticket.getTicketId(), payload.id())
            );
            row.put("payload_text", safeText(ticket.getDescription()));
            row.put("source_text", resolveSourceText(payload, ticket.getDescription()));
            row.put("sentiment", safeText(ticket.getSentiment()));
            row.put("embedding", normalizeEmbedding(ticket.getEmbedding()));
            return row;
        }

        if (value instanceof WireEvent wireEvent) {
            final LinkedHashMap<String, Object> row = baseRow(
                    payload,
                    value,
                    requiredEventKey(wireEvent.key(), payload.id())
            );
            row.put("payload_text", safeText(wireEvent.text()));
            row.put("source_text", resolveSourceText(payload, wireEvent.text()));
            row.put("embedding", normalizeEmbedding(wireEvent.vector()));
            return row;
        }

        if (value instanceof float[] embedding) {
            final LinkedHashMap<String, Object> row = baseRow(payload, value, requiredEventKey(payload.id()));
            final String text = resolveSourceText(payload, null);
            row.put("payload_text", text);
            row.put("source_text", text);
            row.put("embedding", normalizeEmbedding(embedding));
            return row;
        }

        if (value instanceof byte[] bytes) {
            final LinkedHashMap<String, Object> row = baseRow(payload, value, requiredEventKey(payload.id()));
            // byte[] payloads are treated as UTF-8 text here; vector-bearing bytes should be decoded upstream.
            final String text = new String(bytes, StandardCharsets.UTF_8);
            row.put("payload_text", text);
            row.put("source_text", resolveSourceText(payload, text));
            return row;
        }

        if (value instanceof CharSequence chars) {
            final LinkedHashMap<String, Object> row = baseRow(payload, value, requiredEventKey(payload.id()));
            final String text = chars.toString();
            row.put("payload_text", text);
            row.put("source_text", resolveSourceText(payload, text));
            return row;
        }

        if (strictPayloadType) {
            throw new IllegalArgumentException(
                    "SnowflakeSnowpipeStreamingSink requires EnrichedTicket, WireEvent, byte[], String, or float[] payloads but received "
                            + value.getClass().getName());
        }

        final LinkedHashMap<String, Object> row = baseRow(payload, value, requiredEventKey(payload.id()));
        log.warn("SnowflakeRowMapper falling back to payload toString() for unsupported type={}",
                value.getClass().getName());
        final String text = Objects.toString(value, "");
        row.put("payload_text", text);
        row.put("source_text", resolveSourceText(payload, text));
        return row;
    }

    static LinkedHashMap<String, Object> baseRow(PipelinePayload<Object> payload, Object value, String eventKey) {
        final Map<String, String> metadata = payload.metadata();
        final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("event_id", payload.id());
        row.put("event_key", eventKey);
        row.put("event_ts", payload.timestamp() != null ? payload.timestamp().toEpochMilli() : null);
        row.put("payload_type", value.getClass().getSimpleName());
        row.put("payload_text", null);
        row.put("source_text", null);
        row.put("sentiment", null);
        row.put("embedding", null);
        row.put("metadata", metadata);
        row.put("pipeline_id", metadata.get(PipelineProvenance.HEADER_PIPELINE_ID));
        row.put("run_id", metadata.get(PipelineProvenance.HEADER_RUN_ID));
        row.put("source_type", metadata.get(PipelineProvenance.HEADER_SOURCE_TYPE));
        row.put("sink_type", metadata.get(PipelineProvenance.HEADER_SINK_TYPE));
        row.put("source_auth", metadata.get(PipelineProvenance.HEADER_SOURCE_AUTH));
        row.put("sink_auth", metadata.get(PipelineProvenance.HEADER_SINK_AUTH));
        row.put("security_type", metadata.get(PipelineProvenance.HEADER_SECURITY_TYPE));
        row.put("transform_chain", metadata.get(PipelineProvenance.HEADER_TRANSFORM_CHAIN));
        row.put("transform_version", metadata.get(PipelineProvenance.HEADER_TRANSFORM_VERSION));
        row.put("feature_version", metadata.get(PipelineProvenance.HEADER_FEATURE_VERSION));
        row.put("prompt_version", metadata.get(PipelineProvenance.HEADER_PROMPT_VERSION));
        row.put("model_name", metadata.get(PipelineProvenance.HEADER_MODEL_NAME));
        row.put("model_alias", metadata.get(PipelineProvenance.HEADER_MODEL_ALIAS));
        row.put("model_version", metadata.get(PipelineProvenance.HEADER_MODEL_VERSION));
        row.put("model_run_id", metadata.get(PipelineProvenance.HEADER_MODEL_RUN_ID));
        row.put("model_experiment_id", metadata.get(PipelineProvenance.HEADER_MODEL_EXPERIMENT_ID));
        row.put("model_stage", metadata.get(PipelineProvenance.HEADER_MODEL_STAGE));
        row.put("inference_timestamp", metadata.get(PipelineProvenance.HEADER_INFERENCE_TIMESTAMP));
        row.put("config_sha256", metadata.get(PipelineProvenance.HEADER_CONFIG_SHA256));
        row.put("policy_sha256", metadata.get(PipelineProvenance.HEADER_POLICY_SHA256));
        return row;
    }

    static Object normalizeEmbedding(List<Float> embedding) {
        if (embedding == null || embedding.isEmpty()) {
            return null;
        }
        for (Float value : embedding) {
            if (value == null) {
                return null;
            }
        }
        return List.copyOf(embedding);
    }

    static Object normalizeEmbedding(float[] embedding) {
        if (embedding == null || embedding.length == 0) {
            return null;
        }
        return Arrays.copyOf(embedding, embedding.length);
    }

    static String resolveSourceText(PipelinePayload<Object> payload, String fallback) {
        final String sourceText = payload.metadata().get(SOURCE_TEXT_METADATA_KEY);
        return firstNonBlank(sourceText, fallback);
    }

    private static String safeText(String value) {
        return (value == null || value.isBlank()) ? "" : value;
    }

    static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    static String requiredEventKey(String... values) {
        final String eventKey = firstNonBlank(values);
        if (eventKey == null) {
            throw new IllegalStateException("Cannot produce Snowflake row: event key candidates were blank");
        }
        return eventKey;
    }

    private static Object unwrapPayloadValue(Object value) {
        if (value instanceof Optional<?> optional) {
            return optional.orElse(null);
        }
        return value;
    }
}
