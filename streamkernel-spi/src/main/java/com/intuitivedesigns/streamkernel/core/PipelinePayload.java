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

package com.intuitivedesigns.streamkernel.core;

import java.time.Instant;
import java.util.*;

/**
 * The standard envelope for all data flowing through the pipeline.
 *
 * Design Principles:
 * - Immutability: Thread-safe by default.
 * - Provenance: Preserves ID and Timestamp across transformations.
 * - Extensibility: Arbitrary metadata for routing/tracing without changing the schema.
 *
 * @param id Unique ID (Correlation ID) for tracing.
 * @param data The payload content.
 * @param timestamp The original ingestion time (Event Time).
 * @param metadata Context (routing keys, auth tokens, tracing spans).
 */
public record PipelinePayload<T>(
        String id,
        T data,
        Instant timestamp,
        Map<String, String> metadata
) {

    // -----------------------------------------------------------------------
    // 1. COMPACT CONSTRUCTOR (Validation & Defensive Copy)
    // -----------------------------------------------------------------------
    public PipelinePayload {
        Objects.requireNonNull(id, "PipelinePayload id cannot be null");
        if (timestamp == null) timestamp = Instant.now();

        // Ensure metadata is immutable and never null
        metadata = (metadata == null) ? Map.of() : Map.copyOf(metadata);
    }

    // -----------------------------------------------------------------------
    // 2. ADDITIONAL CONSTRUCTORS (For Tests/Compatibility)
    // -----------------------------------------------------------------------

    public PipelinePayload(String id, T data, Map<String, String> metadata) {
        this(id, data, Instant.now(), metadata);
    }

    public PipelinePayload(String id, T data) {
        this(id, data, Instant.now(), Map.of());
    }

    // -----------------------------------------------------------------------
    // 3. FACTORY METHODS
    // -----------------------------------------------------------------------

    public static <T> PipelinePayload<T> of(T data) {
        return new PipelinePayload<>(UUID.randomUUID().toString(), data, Instant.now(), Map.of());
    }

    // -----------------------------------------------------------------------
    // 4. BRIDGE METHODS (Fixes Compilation Errors in Plugins)
    // -----------------------------------------------------------------------

    /**
     * Alias for metadata().
     * Allows plugins using the old .headers() API to keep working.
     */
    public Map<String, String> headers() {
        return metadata;
    }

    /**
     * Alias for withMetadata().
     * Allows plugins using the old .withHeaders() API to keep working.
     */
    public PipelinePayload<T> withHeaders(Map<String, String> newHeaders) {
        return withMetadata(newHeaders);
    }

    // -----------------------------------------------------------------------
    // 5. WITHER METHODS (Functional Modification)
    // -----------------------------------------------------------------------

    public <R> PipelinePayload<R> withData(R newData) {
        return new PipelinePayload<>(id, newData, timestamp, metadata);
    }

    public PipelinePayload<T> withHeader(String key, String value) {
        Map<String, String> newMeta = new HashMap<>(this.metadata);
        newMeta.put(key, value);
        return new PipelinePayload<>(id, data, timestamp, newMeta);
    }

    public PipelinePayload<T> withMetadata(Map<String, String> newMetadata) {
        return new PipelinePayload<>(id, data, timestamp, newMetadata);
    }
}