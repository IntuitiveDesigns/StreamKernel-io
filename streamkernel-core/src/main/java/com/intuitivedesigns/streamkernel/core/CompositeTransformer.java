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
 * Marker interface for composite transformers (chains, wrappers, etc.)
 * to support enterprise-grade introspection and telemetry without reflection.
 */
public interface CompositeTransformer {
    List<Transformer<?, ?>> children();
}
