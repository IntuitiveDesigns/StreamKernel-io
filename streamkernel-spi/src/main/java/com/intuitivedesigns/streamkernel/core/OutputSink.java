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

import com.intuitivedesigns.streamkernel.core.PipelinePayload;

/**
 * OutputSink is the terminal component of a StreamKernel pipeline.
 *
 * Contract:
 * - Implementations MUST be thread-safe or be safely used by the Orchestrator's concurrency model.
 * - write(...) MUST throw on failure; the Orchestrator will handle DLQ routing.
 * - flush() SHOULD be implemented if the sink buffers data.
 *
 * @param <T> payload type
 */
public interface OutputSink<T> extends AutoCloseable {

    /**
     * Write a single payload to the external system.
     *
     * @param payload pipeline payload envelope (never null)
     * @throws Exception on any failure that should trigger DLQ behavior
     */
    void write(PipelinePayload<T> payload) throws Exception;

    /**
     * Flush any buffered writes. Default is a no-op.
     */
    default void flush() throws Exception {
        // no-op
    }

    /**
     * Optional stable identifier for observability/logging.
     */
    default String id() {
        return getClass().getSimpleName();
    }

    /**
     * Default close calls flush. Implementations may override to close resources.
     */
    @Override
    default void close() throws Exception {
        flush();
    }
}
