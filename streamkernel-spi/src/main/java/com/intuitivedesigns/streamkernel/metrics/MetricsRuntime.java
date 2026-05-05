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

/**
 * Minimal, enterprise-friendly metrics facade.
 *
 * Goals:
 * - Ultra-low overhead call sites
 * - Stable API surface across providers
 * - Provider-agnostic (Micrometer/Prometheus/JMX/etc.)
 */
public interface MetricsRuntime extends AutoCloseable {

    /**
     * Provider-native registry object (Micrometer MeterRegistry, Prometheus Registry, etc.).
     * May be a sentinel object for NOOP.
     */
    Object registry();

    /**
     * True if metrics are actively recording.
     *
     * IMPORTANT:
     * Default is TRUE to avoid "silent NOOP" when a provider forgets to override.
     * A dedicated NoopMetricsRuntime should override to false.
     */
    default boolean enabled() {
        return true;
    }

    /**
     * Provider identifier (e.g., "PROMETHEUS", "DATADOG", "NOOP").
     */
    default String type() {
        return "UNKNOWN";
    }

    // ---- Counters ----

    /**
     * Increment counter by 1.
     */
    default void counter(String name) {
        counter(name, 1.0);
    }

    /**
     * Increment counter by an arbitrary value.
     */
    void counter(String name, double increment);

    // ---- Timers ----

    /**
     * Records a duration in milliseconds for the named timer.
     */
    void timer(String name, long durationMillis);

    // ---- Gauges ----

    /**
     * Sets a gauge to a value (push-style gauge).
     */
    void gauge(String name, double value);

    /**
     * Sets a gauge from a long value.
     *
     * <p>This is primarily for count/byte-size gauges. Implementations may store gauges as
     * doubles because most metrics backends expose gauge samples as floating-point values;
     * realistic byte-count gauges remain exactly represented far beyond scrape payload sizes.</p>
     */
    default void gauge(String name, long value) {
        gauge(name, (double) value);
    }

    // ---- Optional Readback (best-effort) ----

    default double counterValue(String name) {
        return Double.NaN;
    }

    default double gaugeValue(String name) {
        return Double.NaN;
    }

    default TimerSnapshot timerSnapshot(String name) {
        return TimerSnapshot.NaN;
    }

    record TimerSnapshot(double p50Millis, double p99Millis, double maxMillis) {
        public static final TimerSnapshot NaN =
                new TimerSnapshot(Double.NaN, Double.NaN, Double.NaN);
    }

    /**
     * Optional provider-specific binding hook (KafkaProducer, JVM metrics, etc.).
     * NOOP by default.
     */
    default void bind(Object binder) { }

    /**
     * Close resources (NOOP implementations should be empty).
     */
    @Override
    void close();
}
