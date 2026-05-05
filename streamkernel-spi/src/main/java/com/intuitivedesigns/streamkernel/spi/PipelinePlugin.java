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

package com.intuitivedesigns.streamkernel.spi;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

/**
 * Base SPI contract for StreamKernel plugins.
 *
 * <p>Enterprise expectations:
 * <ul>
 *   <li>{@link #id()} is stable and unique within its {@link #kind()} domain.</li>
 *   <li>{@link #create(PipelineConfig, MetricsRuntime)} is deterministic for a given config.</li>
 *   <li>Creation failures should throw with actionable context (plugin-local validation is encouraged).</li>
 * </ul>
 *
 * @param <T> The runtime component type produced by this plugin.
 */
public interface PipelinePlugin<T> {

    /**
     * Stable plugin identifier (e.g., "KAFKA", "SYNTHETIC", "NOOP", "OPA_SIDECAR").
     */
    String id();

    /**
     * Plugin category used for discovery and routing.
     */
    PluginKind kind();

    /**
     * Create a configured runtime instance.
     *
     * @param config  immutable configuration snapshot for the current run
     * @param metrics metrics runtime for counters/gauges/timers
     * @return a fully-initialized runtime component
     * @throws Exception if creation fails
     */
    T create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}
