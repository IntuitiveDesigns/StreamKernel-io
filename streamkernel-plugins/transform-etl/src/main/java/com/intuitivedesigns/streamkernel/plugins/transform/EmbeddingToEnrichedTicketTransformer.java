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
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.model.WireEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Shapes the in-process embedding output into the stable row contract that our
 * external sinks and Spark validation layer can consume.
 */
public final class EmbeddingToEnrichedTicketTransformer implements Transformer<Object, EnrichedTicket> {

    private static final String DEFAULT_SENTIMENT = "NEUTRAL";
    private static final String SOURCE_TEXT_METADATA_KEY = "streamkernel.source.text";

    @Override
    public PipelinePayload<EnrichedTicket> transform(PipelinePayload<Object> input) {
        if (input == null || input.data() == null) {
            return null;
        }

        final Object value = input.data();
        if (value instanceof EnrichedTicket ticket) {
            return input.withData(normalizeTicket(input, ticket));
        }

        if (value instanceof WireEvent wireEvent) {
            return input.withData(toTicket(input, wireEvent));
        }

        if (value instanceof float[] embedding) {
            return input.withData(toTicket(input, null, embedding, resolveDescription(input, null)));
        }

        throw new IllegalArgumentException(
                "EmbeddingToEnrichedTicketTransformer requires EnrichedTicket, WireEvent, or float[] payloads but received "
                        + value.getClass().getName());
    }

    private static EnrichedTicket toTicket(PipelinePayload<?> input, WireEvent wireEvent) {
        final float[] embedding = requireEmbedding(wireEvent.vector(), "WireEvent");
        final String description = resolveDescription(input, wireEvent);
        return toTicket(input, wireEvent, embedding, description);
    }

    private static EnrichedTicket toTicket(PipelinePayload<?> input,
                                           WireEvent wireEvent,
                                           float[] embedding,
                                           String description) {
        final float[] requiredEmbedding = requireEmbedding(embedding, "Embedding payload");
        final EnrichedTicket ticket = new EnrichedTicket();
        ticket.setTicketId(resolveTicketId(input, wireEvent));
        ticket.setDescription(safeText(description));
        ticket.setSentiment(DEFAULT_SENTIMENT);
        ticket.setEmbedding(toFloatList(requiredEmbedding));
        return ticket;
    }

    private static EnrichedTicket normalizeTicket(PipelinePayload<?> input, EnrichedTicket ticket) {
        final String ticketId = isBlank(ticket.getTicketId())
                ? resolveTicketId(input, null)
                : ticket.getTicketId();
        final String description = safeText(ticket.getDescription());
        final String sentiment = isBlank(ticket.getSentiment())
                ? DEFAULT_SENTIMENT
                : ticket.getSentiment();

        if (ticketId.equals(ticket.getTicketId())
                && description.equals(ticket.getDescription())
                && sentiment.equals(ticket.getSentiment())) {
            return ticket;
        }

        final EnrichedTicket normalized = new EnrichedTicket();
        normalized.setTicketId(ticketId);
        normalized.setDescription(description);
        normalized.setSentiment(sentiment);
        normalized.setEmbedding(ticket.getEmbedding());
        return normalized;
    }

    private static String resolveTicketId(PipelinePayload<?> input, WireEvent wireEvent) {
        if (wireEvent != null && !isBlank(wireEvent.key())) {
            return wireEvent.key();
        }
        if (input != null && !isBlank(input.id())) {
            return input.id();
        }
        return UUID.randomUUID().toString();
    }

    private static String resolveDescription(PipelinePayload<?> input, WireEvent wireEvent) {
        if (input != null) {
            final String metadataText = input.metadata().get(SOURCE_TEXT_METADATA_KEY);
            if (metadataText != null) {
                return safeText(metadataText);
            }
        }
        return wireEvent == null ? "" : safeText(wireEvent.text());
    }

    private static String safeText(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value;
    }

    private static List<Float> toFloatList(float[] embedding) {
        final ArrayList<Float> values = new ArrayList<>(embedding.length);
        for (final float v : embedding) {
            values.add(v);
        }
        return Collections.unmodifiableList(values);
    }

    private static float[] requireEmbedding(float[] embedding, String source) {
        if (embedding == null || embedding.length == 0) {
            throw new IllegalStateException(source + " is missing an embedding vector.");
        }
        return embedding;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
