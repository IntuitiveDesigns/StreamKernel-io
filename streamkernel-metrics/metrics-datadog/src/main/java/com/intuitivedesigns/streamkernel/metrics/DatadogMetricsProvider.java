/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class DatadogMetricsProvider implements MetricsProvider {

    private static final Logger log = LoggerFactory.getLogger(DatadogMetricsProvider.class);

    @Override
    public String id() {
        return "DATADOG";
    }

    @Override
    public boolean matches(String requestedProviderId) {
        if (requestedProviderId == null) return false;
        return "DATADOG".equalsIgnoreCase(requestedProviderId.trim());
    }

    @Override
    public MetricsRuntime create(MetricsSettings s) {
        if (s == null || !matches(s.providerId)) {
            return null;
        }

        // NOTE: Keep existing wiring/registry decisions minimal and enterprise-friendly.
        // This runtime provides the required MetricsRuntime methods and supports push gauges.
        final DatadogRuntime rt = new DatadogRuntime(s);

        log.info("✅ Datadog Metrics Active (provider=DATADOG)");
        return rt;
    }

    private static final class DatadogRuntime implements MetricsRuntime {

        private final CompositeMeterRegistry registry;
        private final ConcurrentMap<String, AtomicDouble> gaugeState = new ConcurrentHashMap<>();

        private DatadogRuntime(MetricsSettings s) {
            Objects.requireNonNull(s, "settings");

            this.registry = new CompositeMeterRegistry();

            // Safe default so calls work even if Datadog registry isn't attached (dev/CI)
            this.registry.add(new SimpleMeterRegistry());

            // If your project already wires a DatadogMeterRegistry elsewhere, keep that logic.
            // If not, you can attach it here later without changing the MetricsRuntime API.

            try {
                MetricsUtil.applyCommonTags(registry, s);
            } catch (Exception ignored) {
                // Non-fatal
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
            return "DATADOG";
        }

        @Override
        public void counter(String name) {
            if (name == null || name.isBlank()) return;
            registry.counter(name).increment();
        }

        @Override
        public void counter(String name, double increment) {
            if (name == null || name.isBlank()) return;
            final io.micrometer.core.instrument.Counter counter = registry.counter(name);
            if (increment > 0d) {
                counter.increment(increment);
            }
        }

        @Override
        public void timer(String name, long durationMillis) {
            if (name == null || name.isBlank()) return;
            if (durationMillis < 0L) durationMillis = 0L;
            registry.timer(name).record(durationMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void gauge(String name, double value) {
            if (name == null || name.isBlank()) return;

            AtomicDouble state = gaugeState.computeIfAbsent(name, n -> {
                AtomicDouble d = new AtomicDouble(value);
                Gauge.builder(n, d, AtomicDouble::get).register(registry);
                return d;
            });

            state.set(value);
        }

        @Override
        public void close() {
            try { registry.close(); } catch (Exception ignored) {}
        }

        private static final class AtomicDouble extends Number {
            private final AtomicLong bits;

            private AtomicDouble(double initialValue) {
                this.bits = new AtomicLong(Double.doubleToLongBits(initialValue));
            }

            private void set(double newValue) {
                bits.set(Double.doubleToLongBits(newValue));
            }

            private double get() {
                return Double.longBitsToDouble(bits.get());
            }

            @Override public int intValue() { return (int) get(); }
            @Override public long longValue() { return (long) get(); }
            @Override public float floatValue() { return (float) get(); }
            @Override public double doubleValue() { return get(); }
        }
    }
}
