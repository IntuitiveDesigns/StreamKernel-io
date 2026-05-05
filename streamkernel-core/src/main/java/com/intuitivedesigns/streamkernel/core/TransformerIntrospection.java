/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/**
 * Utility for locating a specific transformer instance inside nested transformer structures.
 *
 * Why this exists
 * ---------------
 * StreamKernel supports composition via:
 *   - ChainedTransformer
 *   - Other CompositeTransformer implementations
 *   - Potential future wrappers (metrics, tracing, feature flags, etc.)
 *
 * Runtime components sometimes need to detect whether a specific transformer exists
 * inside the configured pipeline. Examples:
 *
 *   • Speedometer detecting HTTP embedding transformers for additional metrics
 *   • Readiness checks verifying AI transformers initialized correctly
 *   • Diagnostics / telemetry tooling enumerating pipeline capabilities
 *
 * This class provides a **safe and reflection-free traversal** mechanism.
 *
 * Design goals
 * ------------
 * 1) No reflection:
 *      - Avoids brittle inspection of private fields.
 *      - Improves auditability and static analysis friendliness.
 *
 * 2) Iterative traversal:
 *      - Avoids recursion and stack growth for long transformer chains.
 *
 * 3) CompositeTransformer contract:
 *      - Only traverses children exposed via CompositeTransformer.children().
 *      - Ensures traversal respects public, intentional composition boundaries.
 *
 * 4) Fail-safe behavior:
 *      - Returns null when not found.
 *      - Never throws for missing transformer types.
 *
 * Complexity:
 * -----------
 * Time:  O(N) where N is number of transformers in the chain.
 * Space: O(depth) using an explicit stack.
 */
public final class TransformerIntrospection {

    private TransformerIntrospection() {}

    /**
     * Finds the first transformer instance of the requested type within a transformer tree.
     *
     * Traversal strategy:
     * - Depth-first search using an explicit stack.
     * - Starting from the root transformer.
     * - Descends into children only when the transformer implements {@link CompositeTransformer}.
     *
     * Matching semantics:
     * - Uses {@link Class#isInstance(Object)} so subclasses and proxies match.
     *
     * Typical usage:
     *   HttpEmbeddingTransformer http =
     *       TransformerIntrospection.findFirst(transformer, HttpEmbeddingTransformer.class);
     *
     * @param root the root transformer (may be null)
     * @param type the desired transformer class to locate
     * @param <T>  transformer type
     * @return first matching transformer instance, or null if not found
     */
    public static <T> T findFirst(Transformer<?, ?> root, Class<T> type) {
        Objects.requireNonNull(type, "type");

        if (root == null) return null;

        // Explicit stack avoids recursion and supports arbitrarily deep chains.
        final Deque<Transformer<?, ?>> stack = new ArrayDeque<>();
        stack.push(root);

        while (!stack.isEmpty()) {
            final Transformer<?, ?> cur = stack.pop();

            // Direct match check
            if (type.isInstance(cur)) {
                return type.cast(cur);
            }

            // Traverse children when transformer exposes composite structure.
            if (cur instanceof CompositeTransformer ct) {
                for (Transformer<?, ?> child : ct.children()) {
                    if (child != null) stack.push(child);
                }
            }
        }

        // Not found
        return null;
    }
}
