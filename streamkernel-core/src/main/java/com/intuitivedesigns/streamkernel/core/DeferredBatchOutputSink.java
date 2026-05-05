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
 * Optional sink capability for batch sinks that acknowledge durability asynchronously.
 *
 * Why this exists:
 * - Some sinks can accept work quickly but complete durability on a background thread.
 * - The orchestrator still needs to route failures to DLQ using the original source payloads.
 *
 * Contract:
 * - {@link #writeBatchDeferred(List, List)} MUST enqueue or reject the batch immediately.
 * - Success/failure is reported later through {@link #drainCompletedWrites()}.
 * - Returned completions MUST be removed from the sink's internal queue exactly once.
 */
public interface DeferredBatchOutputSink<I, O> extends BatchOutputSink<O> {

    /**
     * Enqueue a batch for asynchronous durability.
     *
     * @param batch transformed outputs destined for the primary sink
     * @param sourceInputs original source payloads that produced the outputs; used for DLQ routing
     * @throws Exception if the sink cannot accept the batch
     */
    void writeBatchDeferred(List<PipelinePayload<O>> batch,
                            List<PipelinePayload<I>> sourceInputs) throws Exception;

    /**
     * Drain completed asynchronous write results.
     *
     * @return completed results since the last drain; never null
     */
    default List<DeferredWriteResult<I>> drainCompletedWrites() {
        return List.of();
    }
}
