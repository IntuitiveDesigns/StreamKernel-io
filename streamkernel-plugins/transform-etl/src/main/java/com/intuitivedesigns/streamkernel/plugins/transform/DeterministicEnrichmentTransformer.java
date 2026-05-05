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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Public-safe enrichment for connector demos that need the EnrichedTicket row
 * contract without loading model artifacts or in-process inference engines.
 */
public final class DeterministicEnrichmentTransformer implements Transformer<Object, EnrichedTicket> {

    private static final String SOURCE_TEXT_METADATA_KEY = "streamkernel.source.text";
    private static final String DEFAULT_SENTIMENT = "NEUTRAL";
    private static final int DEFAULT_DIMENSION = 16;

    private final int dimension;

    public DeterministicEnrichmentTransformer() {
        this(DEFAULT_DIMENSION);
    }

    public DeterministicEnrichmentTransformer(int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        this.dimension = dimension;
    }

    @Override
    public PipelinePayload<EnrichedTicket> transform(PipelinePayload<Object> input) {
        if (input == null || input.data() == null) {
            return null;
        }

        final Object value = input.data();
        if (value instanceof EnrichedTicket ticket) {
            return input.withData(normalizeTicket(input, ticket));
        }

        final String description = resolveDescription(input, value);
        final EnrichedTicket ticket = new EnrichedTicket();
        ticket.setTicketId(resolveTicketId(input, value));
        ticket.setDescription(description);
        ticket.setSentiment(resolveSentiment(description));
        ticket.setEmbedding(toFloatList(resolveEmbedding(value, description)));
        return input.withData(ticket);
    }

    private EnrichedTicket normalizeTicket(PipelinePayload<?> input, EnrichedTicket ticket) {
        final String description = safeText(ticket.getDescription());
        final List<Float> embedding = (ticket.getEmbedding() == null || ticket.getEmbedding().isEmpty())
                ? toFloatList(deterministicEmbedding(description))
                : List.copyOf(ticket.getEmbedding());

        final EnrichedTicket normalized = new EnrichedTicket();
        normalized.setTicketId(isBlank(ticket.getTicketId()) ? resolveTicketId(input, null) : ticket.getTicketId());
        normalized.setDescription(description);
        normalized.setSentiment(isBlank(ticket.getSentiment()) ? resolveSentiment(description) : ticket.getSentiment());
        normalized.setEmbedding(embedding);
        return normalized;
    }

    private float[] resolveEmbedding(Object value, String description) {
        if (value instanceof WireEvent wireEvent && wireEvent.vector() != null && wireEvent.vector().length > 0) {
            return wireEvent.vector();
        }
        if (value instanceof float[] embedding && embedding.length > 0) {
            return embedding;
        }
        return deterministicEmbedding(description);
    }

    private float[] deterministicEmbedding(String description) {
        final byte[] source = safeText(description).getBytes(StandardCharsets.UTF_8);
        final byte[] hash = sha256(source);
        final float[] embedding = new float[dimension];
        for (int i = 0; i < embedding.length; i++) {
            final int unsigned = hash[i % hash.length] & 0xff;
            embedding[i] = (unsigned / 127.5f) - 1.0f;
        }
        return embedding;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private String resolveDescription(PipelinePayload<?> input, Object value) {
        final String metadataText = input.metadata().get(SOURCE_TEXT_METADATA_KEY);
        if (!isBlank(metadataText)) {
            return metadataText;
        }
        if (value instanceof WireEvent wireEvent) {
            return safeText(wireEvent.text());
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (value instanceof CharSequence chars) {
            return safeText(chars.toString());
        }
        if (value instanceof float[]) {
            return "";
        }
        return safeText(String.valueOf(value));
    }

    private static String resolveTicketId(PipelinePayload<?> input, Object value) {
        if (value instanceof WireEvent wireEvent && !isBlank(wireEvent.key())) {
            return wireEvent.key();
        }
        if (input != null && !isBlank(input.id())) {
            return input.id();
        }
        return UUID.randomUUID().toString();
    }

    private static String resolveSentiment(String description) {
        final String value = safeText(description).toLowerCase(Locale.ROOT);
        if (containsAny(value, "error", "failed", "failure", "timeout", "broken", "risk", "delayed")) {
            return "NEGATIVE";
        }
        if (containsAny(value, "resolved", "healthy", "success", "working", "thanks", "fresh")) {
            return "POSITIVE";
        }
        return DEFAULT_SENTIMENT;
    }

    private static boolean containsAny(String value, String... needles) {
        for (String needle : needles) {
            if (value.contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private static List<Float> toFloatList(float[] embedding) {
        final ArrayList<Float> values = new ArrayList<>(embedding.length);
        for (float v : embedding) {
            values.add(v);
        }
        return Collections.unmodifiableList(values);
    }

    private static String safeText(String value) {
        return value == null ? "" : value;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
