/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins.transform;

import com.intuitivedesigns.streamkernel.avro.EnrichedTicket;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddingToEnrichedTicketTransformerTest {

    private final EmbeddingToEnrichedTicketTransformer transformer =
            new EmbeddingToEnrichedTicketTransformer();

    @Test
    void floatArrayPayloadUsesEmptyDescriptionInsteadOfArrayIdentity() {
        final PipelinePayload<Object> input = new PipelinePayload<>("payload-1", new float[]{1.0f, 2.0f});

        final PipelinePayload<EnrichedTicket> output = transformer.transform(input);

        assertNotNull(output);
        assertEquals("payload-1", output.data().getTicketId());
        assertEquals("", output.data().getDescription());
        assertEquals("NEUTRAL", output.data().getSentiment());
        assertEquals(List.of(1.0f, 2.0f), output.data().getEmbedding());
    }

    @Test
    void wireEventWithoutEmbeddingFailsFast() {
        final WireEvent wireEvent = WireEvent.bytes(
                "hello".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                "ticket-1");
        final PipelinePayload<Object> input = new PipelinePayload<>("payload-1", wireEvent);

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> transformer.transform(input));

        assertTrue(thrown.getMessage().contains("missing an embedding vector"));
    }

    @Test
    void wireEventUsesPreservedSourceTextFromMetadata() {
        final WireEvent wireEvent = WireEvent.vector(
                "trimmed".getBytes(StandardCharsets.UTF_8),
                new float[]{1.0f, 2.0f},
                Map.of(),
                "ticket-1");
        final PipelinePayload<Object> input = new PipelinePayload<>(
                "payload-1",
                wireEvent,
                Map.of("streamkernel.source.text", "customer said hello")
        );

        final PipelinePayload<EnrichedTicket> output = transformer.transform(input);

        assertNotNull(output);
        assertEquals("customer said hello", output.data().getDescription());
        assertEquals(List.of(1.0f, 2.0f), output.data().getEmbedding());
    }

    @Test
    void unsupportedPayloadTypeFailsFast() {
        final PipelinePayload<Object> input = new PipelinePayload<>("payload-1", 42);

        final IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> transformer.transform(input));

        assertTrue(thrown.getMessage().contains(Integer.class.getName()));
    }

    @Test
    void blankIdsFallBackToGeneratedUuid() {
        final PipelinePayload<Object> input = new PipelinePayload<>("", new float[]{9.0f});

        final PipelinePayload<EnrichedTicket> output = transformer.transform(input);

        assertNotNull(output);
        assertNotNull(output.data().getTicketId());
        assertTrue(!output.data().getTicketId().isBlank());
        assertEquals(output.data().getTicketId(), UUID.fromString(output.data().getTicketId()).toString());
    }

    @Test
    void enrichedTicketPassThroughRepairsMissingIdAndDefaults() {
        final EnrichedTicket ticket = new EnrichedTicket();
        ticket.setDescription(null);
        ticket.setSentiment("");
        ticket.setEmbedding(List.of(5.0f));

        final PipelinePayload<Object> input = new PipelinePayload<>("payload-1", ticket);

        final PipelinePayload<EnrichedTicket> output = transformer.transform(input);

        assertEquals("payload-1", output.data().getTicketId());
        assertEquals("", output.data().getDescription());
        assertEquals("NEUTRAL", output.data().getSentiment());
        assertEquals(List.of(5.0f), output.data().getEmbedding());
    }
}
