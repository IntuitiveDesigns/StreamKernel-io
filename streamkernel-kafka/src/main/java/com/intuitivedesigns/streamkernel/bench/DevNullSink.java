/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.bench;

import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;

/**
 * No-op sink used for SOURCE benchmark scenarios.
 * Lets you measure source + transform throughput without downstream I/O.
 */
public final class DevNullSink<T> implements OutputSink<T> {

    @Override
    public void write(PipelinePayload<T> payload) {
        // intentionally no-op
    }
}

