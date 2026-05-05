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

/**
 * A transformation step in the pipeline.
 *
 * Capabilities:
 * - 1-to-1 Mapping: Transform data types (JSON -> Object).
 * - Filtering: Return {@code null} to drop the event from the stream.
 * - Enrichment: Call external APIs or Caches (using {@link #init()} and {@link #close()}).
 *
 * @param <I> Input data type
 * @param <O> Output data type
 */
public interface Transformer<I, O> extends AutoCloseable {

    /**
     * Initialize resources (optional).
     * Called once when the pipeline starts.
     * Use this to set up HTTP clients, database connections, or load ML models.
     */
    default void init() throws Exception {
        // no-op by default
    }

    /**
     * Transform the input payload.
     *
     * <p><b>Contract:</b></p>
     * <ul>
     * <li><b>Traceability:</b> Use {@code input.withData(newData)} to ensure IDs and Timestamps are preserved.</li>
     * <li><b>Filtering:</b> Return {@code null} to drop this event. The Orchestrator will skip downstream steps.</li>
     * <li><b>Failure:</b> Throwing an exception triggers the DLQ flow.</li>
     * </ul>
     *
     * @param input the incoming payload
     * @return the transformed payload, or {@code null} to filter it out.
     * @throws Exception if a non-recoverable error occurs.
     */
    PipelinePayload<O> transform(PipelinePayload<I> input) throws Exception;

    /**
     * Clean up resources (optional).
     * Called once when the pipeline stops.
     */
    @Override
    default void close() throws Exception {
        // no-op by default
    }
}