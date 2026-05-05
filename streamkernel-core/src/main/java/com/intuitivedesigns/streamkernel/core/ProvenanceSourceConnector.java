/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Source wrapper that injects stable provenance headers into every payload.
 */
public final class ProvenanceSourceConnector<T> implements SourceConnector<T> {

    private final SourceConnector<T> delegate;
    private final Map<String, String> provenanceHeaders;

    private ProvenanceSourceConnector(SourceConnector<T> delegate, Map<String, String> provenanceHeaders) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.provenanceHeaders = Map.copyOf(provenanceHeaders);
    }

    public static <T> SourceConnector<T> wrap(SourceConnector<T> delegate, Map<String, String> provenanceHeaders) {
        Objects.requireNonNull(delegate, "delegate");
        if (provenanceHeaders == null || provenanceHeaders.isEmpty()) {
            return delegate;
        }
        return new ProvenanceSourceConnector<>(delegate, provenanceHeaders);
    }

    @Override
    public void connect() {
        delegate.connect();
    }

    @Override
    public void disconnect() {
        delegate.disconnect();
    }

    @Override
    public PipelinePayload<T> fetch() {
        return enrich(delegate.fetch());
    }

    @Override
    public List<PipelinePayload<T>> fetchBatch(int maxBatchSize) {
        final List<PipelinePayload<T>> batch = delegate.fetchBatch(maxBatchSize);
        if (batch == null || batch.isEmpty()) {
            return List.of();
        }

        final ArrayList<PipelinePayload<T>> enriched = new ArrayList<>(batch.size());
        for (PipelinePayload<T> payload : batch) {
            enriched.add(enrich(payload));
        }
        return enriched;
    }

    private PipelinePayload<T> enrich(PipelinePayload<T> payload) {
        if (payload == null) {
            return null;
        }

        final LinkedHashMap<String, String> metadata = new LinkedHashMap<>(payload.metadata());
        metadata.putAll(provenanceHeaders);
        return payload.withMetadata(metadata);
    }
}
