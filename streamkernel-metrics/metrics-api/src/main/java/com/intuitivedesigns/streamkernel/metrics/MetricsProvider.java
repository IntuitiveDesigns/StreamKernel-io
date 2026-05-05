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
 * Service Provider Interface (SPI) for Metrics implementations.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}.
 * To add a new metrics backend (e.g., Prometheus, Datadog), implement this interface
 * and register it in {@code META-INF/services/com.intuitivedesigns.streamkernel.metrics.MetricsProvider}.
 */
public interface MetricsProvider {

    /**
     * The unique identifier for this provider (e.g., "PROMETHEUS", "DATADOG").
     * <p>This ID is used to match against the {@code metrics.type} configuration.
     */
    String id();

    /**
     * Creates a runtime instance for this provider if the settings match.
     *
     * @param settings The configuration settings.
     * @return A valid {@link MetricsRuntime} if this provider is selected,
     * or {@code null} if the provider should be skipped.
     */
    MetricsRuntime create(MetricsSettings settings);

    /**
     * Helper to check if this provider is the one requested in config.
     *
     * @param configuredId The value from {@code metrics.type}.
     * @return true if the IDs match (case-insensitive).
     */
    default boolean matches(String configuredId) {
        return configuredId != null && id().equalsIgnoreCase(configuredId.trim());
    }
}