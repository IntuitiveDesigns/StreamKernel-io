/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.metrics;

public final class NoopMetricsProvider implements MetricsProvider {

    @Override
    public String id() {
        return "NONE";
    }

    @Override
    public boolean matches(String requestedProviderId) {
        if (requestedProviderId == null) return false;
        final String s = requestedProviderId.trim();
        return s.equalsIgnoreCase("NONE") || s.equalsIgnoreCase("NOOP");
    }

    @Override
    public MetricsRuntime create(MetricsSettings settings) {
        if (settings == null || !matches(settings.providerId)) {
            return null;
        }
        return NoopMetricsRuntime.INSTANCE;
    }

    private static final class NoopMetricsRuntime implements MetricsRuntime {

        private static final NoopMetricsRuntime INSTANCE = new NoopMetricsRuntime();

        private static final Object REGISTRY = new Object();

        @Override
        public Object registry() {
            return REGISTRY;
        }

        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public String type() {
            return "NOOP";
        }

        @Override
        public void counter(String name) {
            // no-op
        }

        @Override
        public void counter(String name, double increment) {
            // no-op
        }

        @Override
        public void timer(String name, long durationMillis) {
            // no-op
        }

        @Override
        public void gauge(String name, double value) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
