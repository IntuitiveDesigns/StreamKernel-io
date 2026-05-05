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
 * Completion record for a deferred primary-sink write.
 *
 * @param sourceInputs original source payloads associated with the write
 * @param failure null on success; non-null on failure
 */
public record DeferredWriteResult<I>(List<PipelinePayload<I>> sourceInputs, Exception failure) {

    public DeferredWriteResult {
        sourceInputs = (sourceInputs == null) ? List.of() : List.copyOf(sourceInputs);
    }

    public boolean succeeded() {
        return failure == null;
    }
}
