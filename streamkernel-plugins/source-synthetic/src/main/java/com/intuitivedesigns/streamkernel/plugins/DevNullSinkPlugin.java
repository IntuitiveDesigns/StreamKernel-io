/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.OutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * DevNullSinkPlugin (SINK)
 * ========================
 * Registers a "discard" sink under {@code sink.type=DEVNULL} and/or {@code dlq.type=DEVNULL}.
 *
 * Why this exists
 * ---------------
 * Enterprise streaming systems need a deterministic, zero-side-effect sink for:
 *  • Benchmark baselines (measure *pipeline* overhead without external I/O variability)
 *  • Safe defaults (DLQ not configured, dev/test environments)
 *  • Load-testing harnesses (validate backpressure, batching, concurrency, security gates)
 *
 * This sink intentionally performs *no I/O*, *no buffering*, and *no retries*. It is the
 * canonical "black hole" endpoint: once written, data is considered consumed and discarded.
 *
 * Acquisition-grade guarantees
 * ----------------------------
 *  • Deterministic behavior: constant-time operations, no allocations beyond call overhead.
 *  • Does not throw: sink must not disrupt pipeline hot path during benchmark/control runs.
 *  • Lifecycle-safe: close/flush are no-ops and idempotent.
 *
 * Operational caution
 * -------------------
 * Using DEVNULL in production can mask data loss. This plugin logs a WARN on creation
 * to make the behavior explicit and auditable in startup logs.
 */
public final class DevNullSinkPlugin implements SinkPlugin {

    /** Stable SPI identifier for plugin catalog lookup. */
    public static final String ID = "DEVNULL";

    private static final Logger log = LoggerFactory.getLogger(DevNullSinkPlugin.class);

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SINK;
    }

    /**
     * Creates a DEVNULL sink instance.
     *
     * Contract:
     *  • {@code config} and {@code metrics} are required by the plugin SPI, even though DEVNULL
     *    does not use them. Keeping the signature consistent enables drop-in substitution
     *    without conditional logic in the factory.
     *
     * Logging:
     *  • Emits a WARN to ensure operators are aware that a sink is discarding outputs.
     *    This is intentionally *not* INFO: it is a correctness-affecting configuration.
     */
    @Override
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        // Keep the message accurate without implying the PRIMARY sink is necessarily devnull.
        // This sink may be configured as the primary sink or as the DLQ sink.
        log.warn("DEVNULL sink created. Any writes routed to this sink will be discarded.");

        return new DevNullSink<>();
    }

    /**
     * A zero-side-effect sink implementation.
     *
     * Design notes:
     *  • Generic payload type {@code <T>} allows DEVNULL to be used for any pipeline output type.
     *  • {@link #write(PipelinePayload)} is intentionally empty to minimize overhead.
     *  • {@link #flush()} exists for interface completeness; does nothing.
     *  • {@link #close()} exists for lifecycle symmetry; does nothing.
     *
     * Thread-safety:
     *  • Stateless and therefore thread-safe by construction.
     */
    private static final class DevNullSink<T> implements OutputSink<T> {

        /**
         * Discards the payload.
         *
         * Important:
         *  • Does not validate payload and does not throw. DEVNULL is commonly used in benchmark
         *    "control" runs where the goal is to measure maximum achievable throughput.
         *  • If you ever need a "strict" devnull (e.g., assert non-null payloads), implement
         *    a separate sink (e.g., ASSERT_SINK) so benchmarks remain stable.
         */
        @Override
        public void write(PipelinePayload<T> payload) {
            // Intentionally discard.
        }

        /**
         * No-op flush.
         *
         * Rationale:
         *  • DEVNULL buffers nothing; flushing has no meaning.
         *  • Included to satisfy sink contract and allow uniform orchestration logic.
         */
        @Override
        public void flush() {
            // no-op
        }

        /**
         * No-op close.
         *
         * Rationale:
         *  • DEVNULL holds no resources.
         *  • Idempotent by design; multiple close calls are harmless.
         */
        @Override
        public void close() {
            // no-op
        }
    }
}
