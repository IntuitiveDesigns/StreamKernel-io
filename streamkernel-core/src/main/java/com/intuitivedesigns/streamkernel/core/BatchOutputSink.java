/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import java.util.List;

/**
 * Optional sink capability interface for **batch-oriented output**.
 *
 * Why this exists:
 * - Many high-throughput output systems (Kafka producers, bulk DB writers, object storage, HTTP bulk endpoints)
 *   are significantly more efficient when invoked in batches rather than per record.
 * - StreamKernel’s orchestrator already operates on micro-batches; this interface allows sinks to consume
 *   the batch directly and avoid per-record overhead.
 *
 * Contract:
 * - Implementations MAY override {@link #writeBatch(List)} to provide a true bulk write implementation.
 * - Implementations MUST still implement {@link OutputSink#write(PipelinePayload)} for compatibility,
 *   because the runtime may fall back to per-record writes in some modes.
 *
 * Enterprise considerations:
 * - Ordering: Unless explicitly documented by a sink, batch order should be treated as "best effort".
 * - Partial failure: True batch sinks should define their behavior:
 *     - all-or-nothing (transactional)
 *     - partial success with per-record error reporting
 *     - best-effort fire-and-forget
 *   For acquisition-grade sinks, prefer explicit semantics and metrics for each outcome.
 *
 * Performance guidance:
 * - Avoid copying the batch list unless required.
 * - Avoid allocating per-record objects inside the batch loop.
 * - Prefer I/O calls that accept arrays/collections (bulk) when available.
 */
public interface BatchOutputSink<T> extends OutputSink<T> {

    /**
     * Writes an entire StreamKernel micro-batch in one call for maximum throughput.
     *
     * Default behavior:
     * - Provides a safe compatibility fallback by iterating the batch and delegating to
     *   {@link #write(PipelinePayload)} for each record.
     *
     * When to override:
     * - Override this method when the sink supports true bulk writes (e.g., Kafka send batch, JDBC batch,
     *   Mongo bulkWrite, Elasticsearch bulk API, S3 multipart, etc.).
     *
     * Failure semantics (default implementation):
     * - If {@link #write(PipelinePayload)} throws, the exception propagates immediately and subsequent
     *   records are not written.
     *
     * @param batch list of payloads in the batch (may be null/empty; treated as no-op)
     * @throws Exception if the sink fails to write (implementation-defined semantics)
     */
    default void writeBatch(List<PipelinePayload<T>> batch) throws Exception {
        if (batch == null || batch.isEmpty()) return;

        // Compatibility fallback: per-record writes.
        // Batch-capable sinks should override this to perform a single bulk write for throughput.
        for (PipelinePayload<T> p : batch) {
            write(p);
        }
    }
}
