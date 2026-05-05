/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

/**
 * High-accuracy, low-overhead latency recording using HdrHistogram Recorder.
 *
 * Recording is concurrency-friendly; snapshotting is done by a single thread on a fixed window.
 * Values are stored as microseconds to reduce range while preserving resolution.
 */
public final class LatencyRecorder {

    // Tracks up to 60s (in micros) with 3 significant digits.
    // Adjust max if you expect longer tail latencies.
    private static final long MAX_LATENCY_US = TimeUnit.SECONDS.toMicros(60);

    private final Recorder recorder;

    public LatencyRecorder() {
        // 3 sig digits is a strong default (good accuracy, small footprint).
        this.recorder = new Recorder(MAX_LATENCY_US, 3);
    }

    /** Record a duration in nanoseconds. */
    public void recordNanos(long nanos) {
        if (nanos <= 0) return;
        long us = TimeUnit.NANOSECONDS.toMicros(nanos);
        if (us <= 0) us = 1; // avoid 0 which can collapse small values
        if (us > MAX_LATENCY_US) us = MAX_LATENCY_US;
        recorder.recordValue(us);
    }

    /**
     * Takes an interval snapshot and resets the interval.
     * Must be called by ONE thread (your metrics pusher is perfect).
     */
    public LatencySnapshot snapshotAndReset() {
        Histogram h = recorder.getIntervalHistogram(); // swaps interval internally
        return LatencySnapshot.fromHistogramMicros(h);
    }

    /** Immutable view of interval stats. */
    public static final class LatencySnapshot {
        private final double p50Ms;
        private final double p95Ms;
        private final double p99Ms;
        private final double p999Ms;
        private final double maxMs;
        private final long count;

        private LatencySnapshot(double p50Ms, double p95Ms, double p99Ms, double p999Ms, double maxMs, long count) {
            this.p50Ms = p50Ms;
            this.p95Ms = p95Ms;
            this.p99Ms = p99Ms;
            this.p999Ms = p999Ms;
            this.maxMs = maxMs;
            this.count = count;
        }

        static LatencySnapshot fromHistogramMicros(Histogram h) {
            if (h == null || h.getTotalCount() == 0) {
                return new LatencySnapshot(0, 0, 0, 0, 0, 0);
            }
            // histogram values are in microseconds
            double p50 = h.getValueAtPercentile(50.0) / 1000.0;
            double p95 = h.getValueAtPercentile(95.0) / 1000.0;
            double p99 = h.getValueAtPercentile(99.0) / 1000.0;
            double p999 = h.getValueAtPercentile(99.9) / 1000.0;
            double max = h.getMaxValue() / 1000.0;
            return new LatencySnapshot(p50, p95, p99, p999, max, h.getTotalCount());
        }

        public double p50Millis()  { return p50Ms; }
        public double p95Millis()  { return p95Ms; }
        public double p99Millis()  { return p99Ms; }
        public double p999Millis() { return p999Ms; }
        public double maxMillis()  { return maxMs; }
        public long count()        { return count; }
    }
}