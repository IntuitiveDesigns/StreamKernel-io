/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.avro.EnrichedTicket;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnowflakeRowMapperTest {

    private final SnowflakeRowMapper mapper = new SnowflakeRowMapper();

    @Test
    void toRowMapsWireEventWithProvenanceAndEmbedding() {
        final PipelinePayload<Object> payload = new PipelinePayload<>(
                "evt-1",
                WireEvent.vector(
                        "customer said hello".getBytes(),
                        new float[]{0.25f, 0.75f},
                        Map.of("wire.header", "true"),
                        "ticket-17"
                ),
                Instant.parse("2026-04-16T12:00:00Z"),
                Map.ofEntries(
                        Map.entry("streamkernel.source.text", "customer said hello"),
                        Map.entry("streamkernel.provenance.pipeline.id", "sk-pipeline"),
                        Map.entry("streamkernel.provenance.run.id", "run-1"),
                        Map.entry("streamkernel.provenance.source.type", "SYNTHETIC"),
                        Map.entry("streamkernel.provenance.sink.type", "SNOWFLAKE_SNOWPIPE_STREAMING"),
                        Map.entry("streamkernel.provenance.model.run.id", "model-run-9"),
                        Map.entry("streamkernel.provenance.model.experiment.id", "exp-3"),
                        Map.entry("streamkernel.provenance.model.stage", "production"),
                        Map.entry("streamkernel.provenance.inference.timestamp", "2026-04-17T12:00:00Z"),
                        Map.entry("streamkernel.provenance.feature.version", "features-v3"),
                        Map.entry("streamkernel.provenance.prompt.version", "prompt-v4"),
                        Map.entry("streamkernel.provenance.transform.version", "transform-v7"),
                        Map.entry("streamkernel.provenance.config.sha256", "abc123")
                )
        );

        final Map<String, Object> row = mapper.toRow(payload, true);

        assertEquals("evt-1", row.get("event_id"));
        assertEquals("ticket-17", row.get("event_key"));
        assertEquals(Instant.parse("2026-04-16T12:00:00Z").toEpochMilli(), row.get("event_ts"));
        assertEquals("customer said hello", row.get("source_text"));
        assertEquals("customer said hello", row.get("payload_text"));
        assertEquals("sk-pipeline", row.get("pipeline_id"));
        assertEquals("run-1", row.get("run_id"));
        assertEquals("SYNTHETIC", row.get("source_type"));
        assertEquals("SNOWFLAKE_SNOWPIPE_STREAMING", row.get("sink_type"));
        assertEquals("model-run-9", row.get("model_run_id"));
        assertEquals("exp-3", row.get("model_experiment_id"));
        assertEquals("production", row.get("model_stage"));
        assertEquals("2026-04-17T12:00:00Z", row.get("inference_timestamp"));
        assertEquals("features-v3", row.get("feature_version"));
        assertEquals("prompt-v4", row.get("prompt_version"));
        assertEquals("transform-v7", row.get("transform_version"));
        assertEquals("abc123", row.get("config_sha256"));
        assertInstanceOf(float[].class, row.get("embedding"));
        assertArrayEquals(new float[]{0.25f, 0.75f}, (float[]) row.get("embedding"));
        assertNotNull(row.get("metadata"));
    }

    @Test
    void toRowMapsEnrichedTicketFields() {
        final EnrichedTicket ticket = new EnrichedTicket();
        ticket.setTicketId("ticket-42");
        ticket.setDescription("warehouse sync is lagging");
        ticket.setSentiment("NEUTRAL");
        ticket.setEmbedding(List.of(1.0f, 2.0f, 3.0f));

        final PipelinePayload<Object> payload = new PipelinePayload<>(
                "evt-2",
                ticket,
                Instant.parse("2026-04-16T12:05:00Z"),
                Map.of("streamkernel.provenance.pipeline.id", "sk-pipeline")
        );

        final Map<String, Object> row = mapper.toRow(payload, true);

        assertEquals("ticket-42", row.get("event_key"));
        assertEquals("warehouse sync is lagging", row.get("payload_text"));
        assertEquals("warehouse sync is lagging", row.get("source_text"));
        assertEquals("NEUTRAL", row.get("sentiment"));
        assertEquals(List.of(1.0f, 2.0f, 3.0f), row.get("embedding"));
        assertEquals("sk-pipeline", row.get("pipeline_id"));
    }

    @Test
    void toRowRejectsBlankEventKeys() {
        final EnrichedTicket ticket = new EnrichedTicket();
        ticket.setTicketId("  ");
        ticket.setDescription("warehouse sync is lagging");

        final PipelinePayload<Object> payload = new PipelinePayload<>(
                " ",
                ticket,
                Instant.parse("2026-04-16T12:05:00Z"),
                Map.of()
        );

        final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> mapper.toRow(payload, true)
        );

        assertEquals("Cannot produce Snowflake row: event key candidates were blank", error.getMessage());
    }

    @Test
    void toRowRejectsPartialListEmbeddings() {
        final EnrichedTicket ticket = new EnrichedTicket();
        ticket.setTicketId("ticket-42");
        ticket.setDescription("warehouse sync is lagging");
        ticket.setEmbedding(java.util.Arrays.asList(1.0f, null, 3.0f));

        final PipelinePayload<Object> payload = new PipelinePayload<>(
                "evt-2",
                ticket,
                Instant.parse("2026-04-16T12:05:00Z"),
                Map.of()
        );

        final Map<String, Object> row = mapper.toRow(payload, true);

        assertNull(row.get("embedding"));
    }

    @Test
    void toRowLeavesMissingSourceTextAsNull() {
        final PipelinePayload<Object> payload = new PipelinePayload<>(
                "evt-3",
                new float[]{0.5f, 0.25f},
                Instant.parse("2026-04-16T12:10:00Z"),
                Map.of()
        );

        final Map<String, Object> row = mapper.toRow(payload, true);

        assertNull(row.get("payload_text"));
        assertNull(row.get("source_text"));
    }

    @Test
    void toRowSkipsEmptyOptionalPayloads() {
        final PipelinePayload<Object> payload = new PipelinePayload<>(
                "evt-4",
                Optional.empty(),
                Instant.parse("2026-04-16T12:10:00Z"),
                Map.of()
        );

        assertNull(mapper.toRow(payload, false));
    }

    @Test
    void buildClientPropertiesNormalizesInlinePemAndPassesThroughOverrides() {
        final Properties props = new Properties();
        props.setProperty("snowflake.url", "https://acme.snowflakecomputing.com");
        props.setProperty("snowflake.role", "STREAMKERNEL_ROLE");
        props.setProperty("snowflake.private.key", "-----BEGIN PRIVATE KEY-----\nABC123\n-----END PRIVATE KEY-----");
        props.setProperty("snowflake.property.max_client_lag", "5 seconds");

        final Properties built = SnowflakeSnowpipeStreamingSink.buildClientProperties(
                PipelineConfig.from(props, "inline"),
                "acme",
                "streamkernel"
        );

        assertEquals("https://acme.snowflakecomputing.com", built.getProperty("url"));
        assertEquals("STREAMKERNEL_ROLE", built.getProperty("role"));
        assertEquals("ABC123", built.getProperty("private_key"));
        assertEquals("5 seconds", built.getProperty("max_client_lag"));
    }
}
