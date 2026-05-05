/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Allocation-free transformer chain implementation.
 *
 * Purpose
 * -------
 * Many production pipelines require multiple transformation steps:
 *     source → parse → enrich → AI → normalize → serialize → sink
 *
 * Rather than forcing users to build a monolithic transformer, StreamKernel supports
 * a chain of small, single-purpose transformers composed together at runtime.
 *
 * This class provides:
 *  - Deterministic execution order (exactly as configured)
 *  - Zero per-record allocations during chaining
 *  - Early-exit semantics for drop/filter behavior
 *  - Introspection support for observability tooling
 *
 * Enterprise / acquisition posture
 * --------------------------------
 *  • No reflection used at runtime.
 *  • Fail-fast behavior — exceptions propagate to the orchestrator and DLQ handling.
 *  • Deterministic ordering guarantees reproducibility across environments.
 *  • Compatible with performance-critical pipelines (hot path optimized).
 *
 * Type model
 * ----------
 * This class operates on raw Object payloads internally:
 *
 *   Transformer<Object,Object> → allows heterogeneous chains:
 *       String → JSON → Domain → float[] → WireEvent → bytes
 *
 * Type safety is enforced by configuration discipline and testing rather than generics,
 * which would otherwise make heterogeneous chaining impractical.
 */
public final class ChainedTransformer
        implements Transformer<Object, Object>, CompositeTransformer, BatchTransformer<Object, Object> {

    /**
     * Ordered transformer chain used during execution.
     *
     * Important:
     * - Stored as an array to avoid iterator allocations during transform().
     * - Array traversal is measurably faster and allocation-free.
     */
    private final Transformer<Object, Object>[] chain;
    private final boolean batchTransformPreferred;

    /**
     * Read-only view exposed for introspection/telemetry tools.
     */
    private final List<Transformer<?, ?>> view;

    /**
     * Constructs a new transformer chain.
     *
     * Validation:
     * - Chain must not be null.
     * - Chain must contain at least one transformer.
     * - All elements must be non-null.
     *
     * Type safety contract:
     * - The array is cast to Transformer<Object,Object>[] internally.
     * - This is safe by construction: PipelineOrchestrator calls transform(PipelinePayload<Object>)
     *   on the ChainedTransformer, and each step treats the payload as Object.
     * - Type correctness across step boundaries is enforced by configuration discipline
     *   and integration tests rather than Java generics, which cannot express heterogeneous
     *   chains (String → JSON → Domain → float[] → WireEvent) without this erasure.
     * - If a type mismatch occurs at runtime it will surface as a ClassCastException in the
     *   failing transformer, be caught by the orchestrator's per-record exception handler,
     *   and routed to the DLQ with the full stack trace — not silently dropped.
     *
     * @param chain ordered array of transformers configured for the pipeline
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ChainedTransformer(Transformer[] chain) {
        Objects.requireNonNull(chain, "chain");
        if (chain.length == 0) {
            throw new IllegalArgumentException("ChainedTransformer requires at least one transformer");
        }
        for (int i = 0; i < chain.length; i++) {
            if (chain[i] == null) {
                throw new NullPointerException("chain element at index " + i + " is null");
            }
        }

        // Safe by the type safety contract documented above.
        this.chain = (Transformer<Object, Object>[]) chain;

        boolean prefersBatch = false;
        for (final Transformer<?, ?> step : this.chain) {
            if (step instanceof BatchTransformer<?, ?> batchStep
                    && batchStep.isBatchTransformPreferred()) {
                prefersBatch = true;
                break;
            }
        }
        this.batchTransformPreferred = prefersBatch;

        // Arrays.asList returns a fixed-size list backed by the array (no copy).
        // Wrapping as unmodifiable prevents callers from using set(i, ...) to mutate the chain
        // through the introspection view — the array itself remains the authoritative source.
        this.view = Collections.unmodifiableList(Arrays.asList(this.chain));
    }

    /**
     * Executes the transformer chain.
     *
     * Execution semantics:
     * - Input payload flows sequentially through each transformer.
     * - Output of transformer[i] becomes input to transformer[i+1].
     *
     * Drop / filter semantics:
     * - If any transformer returns null → the record is dropped immediately.
     * - Remaining transformers are NOT invoked.
     *
     * Failure semantics:
     * - Exceptions propagate to the PipelineOrchestrator, which applies DLQ routing and metrics.
     * - A ClassCastException here indicates a type mismatch between adjacent chain steps.
     *   It will be caught by the orchestrator's per-record handler and routed to the DLQ
     *   with the full stack trace. Check your transform.chain configuration if you see these.
     *
     * Performance characteristics:
     * - Tight indexed loop over array → zero iterator allocations.
     * - No intermediate collections.
     * - Early exit when record is dropped.
     *
     * @param in pipeline payload entering the chain
     * @return transformed payload, or null if any step returns null (intentional drop)
     * @throws Exception propagated from any transformer step; includes step index in message
     */
    @Override
    public PipelinePayload<Object> transform(PipelinePayload<Object> in) throws Exception {
        PipelinePayload<Object> cur = in;

        for (int i = 0; i < chain.length; i++) {
            Object before = (cur == null) ? null : cur.data();
            // System.out.println("CHAIN before step=" + i + " [" + chain[i].getClass().getSimpleName() + "] payloadClass=" + (before == null ? "null" : before.getClass().getName()));

            try {
                cur = chain[i].transform(cur);
                Object after = (cur == null) ? null : cur.data();
                // System.out.println("CHAIN after  step=" + i + " [" + chain[i].getClass().getSimpleName() + "] payloadClass=" + (after == null ? "null" : after.getClass().getName()));
            } catch (Exception e) {
                // Re-throw with step context so DLQ entries and logs identify which transformer failed.
                // The orchestrator will catch this and record it with the full stack trace.
                throw new Exception("Transform chain failed at step " + i
                        + " [" + chain[i].getClass().getSimpleName() + "]", e);
            }
            if (cur == null) return null; // intentional drop/filter semantics
        }

        return cur;
    }

    @Override
    public List<BatchTransformer.Result<Object>> transformBatch(List<PipelinePayload<Object>> inputs)
            throws Exception {

        final int n = (inputs == null) ? 0 : inputs.size();
        final ArrayList<BatchTransformer.Result<Object>> state = new ArrayList<>(n);
        if (n == 0) {
            return state;
        }

        ArrayList<Integer> activeIndexes = new ArrayList<>(n);
        ArrayList<PipelinePayload<Object>> activePayloads = new ArrayList<>(n);

        for (int i = 0; i < n; i++) {
            final PipelinePayload<Object> input = inputs.get(i);
            state.add(BatchTransformer.Result.success(input));
            if (input != null) {
                activeIndexes.add(i);
                activePayloads.add(input);
            }
        }

        for (int stepIndex = 0; stepIndex < chain.length; stepIndex++) {
            if (activePayloads.isEmpty()) {
                break;
            }

            final Transformer<Object, Object> step = chain[stepIndex];
            final List<BatchTransformer.Result<Object>> stepResults;
            try {
                stepResults = transformStepBatch(step, activePayloads);
            } catch (Exception e) {
                final Exception wrapped = wrapStepFailure(stepIndex, step, e);
                for (int i = 0; i < activeIndexes.size(); i++) {
                    state.set(activeIndexes.get(i), BatchTransformer.Result.failure(wrapped));
                }
                return state;
            }

            if (stepResults == null || stepResults.size() != activePayloads.size()) {
                final Exception wrapped = wrapStepFailure(
                        stepIndex,
                        step,
                        new IllegalStateException("Batch transformer returned "
                                + ((stepResults == null) ? "null" : stepResults.size())
                                + " results for " + activePayloads.size() + " inputs"));
                for (int i = 0; i < activeIndexes.size(); i++) {
                    state.set(activeIndexes.get(i), BatchTransformer.Result.failure(wrapped));
                }
                return state;
            }

            final ArrayList<Integer> nextIndexes = new ArrayList<>(activeIndexes.size());
            final ArrayList<PipelinePayload<Object>> nextPayloads = new ArrayList<>(activePayloads.size());

            for (int i = 0; i < stepResults.size(); i++) {
                final int slot = activeIndexes.get(i);
                final BatchTransformer.Result<Object> stepResult = stepResults.get(i);

                if (stepResult == null) {
                    state.set(slot, BatchTransformer.Result.failure(
                            wrapStepFailure(stepIndex, step,
                                    new IllegalStateException("Batch transformer returned null result"))));
                    continue;
                }

                if (stepResult.hasError()) {
                    state.set(slot, BatchTransformer.Result.failure(
                            wrapStepFailure(stepIndex, step, stepResult.error())));
                    continue;
                }

                final PipelinePayload<Object> out = stepResult.output();
                state.set(slot, BatchTransformer.Result.success(out));
                if (out != null) {
                    nextIndexes.add(slot);
                    nextPayloads.add(out);
                }
            }

            activeIndexes = nextIndexes;
            activePayloads = nextPayloads;
        }

        return state;
    }

    @Override
    public boolean isBatchTransformPreferred() {
        return batchTransformPreferred;
    }

    /**
     * Returns an ordered, read-only view of child transformers.
     */
    @Override
    public List<Transformer<?, ?>> children() {
        return view;
    }

    @Override
    public String toString() {
        return "ChainedTransformer" + view;
    }

    @SuppressWarnings("unchecked")
    private static List<BatchTransformer.Result<Object>> transformStepBatch(
            Transformer<Object, Object> step,
            List<PipelinePayload<Object>> inputs) throws Exception {

        if (step instanceof BatchTransformer<?, ?> batchStep) {
            return ((BatchTransformer<Object, Object>) batchStep).transformBatch(inputs);
        }

        final ArrayList<BatchTransformer.Result<Object>> out = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            final PipelinePayload<Object> input = inputs.get(i);
            try {
                out.add(BatchTransformer.Result.success(step.transform(input)));
            } catch (Exception e) {
                out.add(BatchTransformer.Result.failure(e));
            }
        }
        return out;
    }

    private static Exception wrapStepFailure(int stepIndex, Transformer<Object, Object> step, Exception cause) {
        return new Exception("Transform chain failed at step " + stepIndex
                + " [" + step.getClass().getSimpleName() + "]", cause);
    }
}
