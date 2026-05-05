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

// streamkernel-spi/src/main/java/com/intuitivedesigns/streamkernel/spi/SemanticCache.java
package com.intuitivedesigns.streamkernel.spi;

import com.intuitivedesigns.streamkernel.core.PipelinePayload;

/**
 * Optional extension of Cache for transform-aware caching.
 * Implement when cache can serve transformed results (ex: embeddings, HTTP responses, enrichments).
 */
public interface SemanticCache<I, O> extends Cache<Object, Object> {

    /** Return cached transformed result or null if miss. */
    PipelinePayload<O> lookup(PipelinePayload<I> input);

    /** Store transformed result after transformer completes. */
    void store(PipelinePayload<I> input, PipelinePayload<O> output);
}
