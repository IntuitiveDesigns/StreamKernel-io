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

import java.util.List;
import java.util.Objects;

/**
 * Optional transformer extension for implementations that can process a whole worker batch
 * more efficiently than repeated single-record calls.
 *
 * Implementations must preserve input order and return exactly one {@link Result} per input.
 * A result may contain:
 * - a non-null output payload
 * - a null output payload to indicate an intentional drop/filter
 * - an exception to indicate a per-record failure
 *
 * Throwing from {@link #transformBatch(List)} is reserved for catastrophic failures that
 * invalidate the whole submitted batch.
 */
public interface BatchTransformer<I, O> {

    default boolean isBatchTransformPreferred() {
        return true;
    }

    List<Result<O>> transformBatch(List<PipelinePayload<I>> inputs) throws Exception;

    record Result<T>(PipelinePayload<T> output, Exception error) {
        public Result {
            if (output != null && error != null) {
                throw new IllegalArgumentException("Batch transform result cannot contain both output and error");
            }
        }

        public static <T> Result<T> success(PipelinePayload<T> output) {
            return new Result<>(output, null);
        }

        public static <T> Result<T> failure(Exception error) {
            return new Result<>(null, Objects.requireNonNull(error, "error"));
        }

        public boolean hasError() {
            return error != null;
        }
    }
}
