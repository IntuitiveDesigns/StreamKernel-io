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

package com.intuitivedesigns.streamkernel.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer-based MetricsRuntime optimized for high-throughput stream processing.
 *
 * Notes:
 * - Uses CompositeMeterRegistry for pluggable observability (Prometheus, JMX, etc.).
 * - Implements lock-free push gauges with double precision via AtomicDouble.
 * - Minimal allocation strategy: Caches counters, timers, and gauge state.
 */
public final class MicrometerMetricsRuntime implements MetricsRuntime {

    private static final Logger log = LoggerFactory.getLogger(MicrometerMetricsRuntime.class);

    private final MeterRegistry registry;

    // Hot-path metric caches to avoid builder overhead
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Timer> timers = new ConcurrentHashMap<>();
    private final Map<String, AtomicDouble> gaugeState = new ConcurrentHashMap<>();

    public MicrometerMetricsRuntime(MeterRegistry registry) {
        this.registry = registry != null ? registry : new CompositeMeterRegistry();
    }

    /**
     * Fix: Safely adds a specific registry if the root is composite.
     */
    public void addRegistry(MeterRegistry specificRegistry) {
        if (specificRegistry != null && this.registry instanceof CompositeMeterRegistry composite) {
            composite.add(specificRegistry);
        } else if (specificRegistry != null) {
            log.warn("Cannot add registry '{}' - root registry is not composite.", specificRegistry.getClass().getSimpleName());
        }
    }

    @Override
    public Object registry() {
        return registry;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public String type() {
        return "MICROMETER";
    }

    @Override
    public void counter(String name) {
        counter(name, 1.0);
    }

    @Override
    public void counter(String name, double increment) {
        if (name == null || name.isBlank() || increment <= 0) return;

        counters.computeIfAbsent(name, n -> Counter.builder(n).register(registry))
                .increment(increment);
    }

    @Override
    public void timer(String name, long durationMillis) {
        if (name == null || name.isBlank()) return;

        long val = Math.max(0L, durationMillis);
        timers.computeIfAbsent(name, n -> Timer.builder(n).register(registry))
                .record(val, TimeUnit.MILLISECONDS);
    }

    /**
     * Improved Gauge: Uses AtomicDouble for precision and strong references for registry persistence.
     */
    @Override
    public void gauge(String name, double value) {
        if (name == null || name.isBlank()) return;

        gaugeState.computeIfAbsent(name, key -> {
            AtomicDouble state = new AtomicDouble(value);
            Gauge.builder(key, state, AtomicDouble::get)
                    .strongReference(true) // Crucial for persistent tracking in push-style gauges
                    .register(registry);
            return state;
        }).set(value);
    }

    @Override
    public double counterValue(String name) {
        try {
            Counter c = registry.find(name).counter();
            return (c != null) ? c.count() : Double.NaN;
        } catch (Exception ignored) {
            return Double.NaN;
        }
    }

    @Override
    public double gaugeValue(String name) {
        try {
            Meter m = registry.find(name).meter();
            if (m == null) return Double.NaN;
            for (Measurement meas : m.measure()) {
                if (meas != null) return meas.getValue();
            }
            return Double.NaN;
        } catch (Exception ignored) {
            return Double.NaN;
        }
    }

    @Override
    public TimerSnapshot timerSnapshot(String name) {
        try {
            Timer t = registry.find(name).timer();
            if (t == null) return TimerSnapshot.NaN;

            HistogramSnapshot snap = t.takeSnapshot();
            double maxMs = snap.max(TimeUnit.MILLISECONDS);
            double p50 = Double.NaN;
            double p99 = Double.NaN;

            for (ValueAtPercentile v : snap.percentileValues()) {
                if (v == null) continue;
                if (Math.abs(v.percentile() - 0.50) < 0.001) p50 = v.value(TimeUnit.MILLISECONDS);
                if (Math.abs(v.percentile() - 0.99) < 0.001) p99 = v.value(TimeUnit.MILLISECONDS);
            }

            return new TimerSnapshot(p50, p99, maxMs);
        } catch (Exception ignored) {
            return TimerSnapshot.NaN;
        }
    }

    @Override
    public void close() {
        registry.close();
        log.info("Micrometer Metrics Runtime Closed.");
    }

    /**
     * Lock-free double state holder to prevent precision loss.
     */
    private static final class AtomicDouble extends Number {
        private final AtomicLong bits;

        AtomicDouble(double initialValue) {
            this.bits = new AtomicLong(Double.doubleToLongBits(initialValue));
        }

        void set(double newValue) {
            bits.set(Double.doubleToLongBits(newValue));
        }

        double get() {
            return Double.longBitsToDouble(bits.get());
        }

        @Override public int intValue() { return (int) get(); }
        @Override public long longValue() { return (long) get(); }
        @Override public float floatValue() { return (float) get(); }
        @Override public double doubleValue() { return get(); }
    }
}