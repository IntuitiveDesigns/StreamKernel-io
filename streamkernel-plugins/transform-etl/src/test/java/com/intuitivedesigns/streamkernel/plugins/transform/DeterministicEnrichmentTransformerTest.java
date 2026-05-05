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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DeterministicEnrichmentTransformerTest {

    private final DeterministicEnrichmentTransformer transformer = new DeterministicEnrichmentTransformer(8);

    @Test
    void stringPayloadProducesStableEnrichedTicket() {
        final PipelinePayload<Object> first = new PipelinePayload<>("ticket-1", "checkout timeout");
        final PipelinePayload<Object> second = new PipelinePayload<>("ticket-1", "checkout timeout");

        final PipelinePayload<EnrichedTicket> output1 = transformer.transform(first);
        final PipelinePayload<EnrichedTicket> output2 = transformer.transform(second);

        assertNotNull(output1);
        assertEquals("ticket-1", output1.data().getTicketId());
        assertEquals("checkout timeout", output1.data().getDescription());
        assertEquals("NEGATIVE", output1.data().getSentiment());
        assertEquals(8, output1.data().getEmbedding().size());
        assertEquals(output1.data().getEmbedding(), output2.data().getEmbedding());
    }

    @Test
    void differentTextProducesDifferentEmbedding() {
        final List<Float> first = transformer.transform(new PipelinePayload<>("a", "alpha")).data().getEmbedding();
        final List<Float> second = transformer.transform(new PipelinePayload<>("b", "beta")).data().getEmbedding();

        assertNotEquals(first, second);
    }

    @Test
    void wireEventUsesKeyAndPreservedText() {
        final WireEvent wireEvent = WireEvent.bytes(
                "trimmed".getBytes(StandardCharsets.UTF_8),
                Map.of(),
                "wire-key"
        );
        final PipelinePayload<Object> input = new PipelinePayload<>(
                "payload-1",
                wireEvent,
                Map.of("streamkernel.source.text", "customer success")
        );

        final PipelinePayload<EnrichedTicket> output = transformer.transform(input);

        assertEquals("wire-key", output.data().getTicketId());
        assertEquals("customer success", output.data().getDescription());
        assertEquals("POSITIVE", output.data().getSentiment());
    }

    @Test
    void existingEmbeddingIsPreserved() {
        final WireEvent wireEvent = WireEvent.vector(
                "healthy".getBytes(StandardCharsets.UTF_8),
                new float[]{0.25f, 0.5f},
                Map.of(),
                "wire-key"
        );

        final PipelinePayload<EnrichedTicket> output =
                transformer.transform(new PipelinePayload<>("payload-1", wireEvent));

        assertEquals(List.of(0.25f, 0.5f), output.data().getEmbedding());
    }
}
