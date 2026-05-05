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
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;

/**
 * SPI Definition for Pipeline Sources.
 *
 * <p><b>Generics Note:</b> {@code SourceConnector<?>} allows sources to produce
 * any type of data (String, AvroRecord, byte[]). The Orchestrator handles
 * type bridging at runtime.</p>
 */
public interface SourcePlugin extends PipelinePlugin<SourceConnector<?>> {

    String id(); // e.g. "SYNTHETIC", "KAFKA", "REST"

    @Override
    default PluginKind kind() {
        return PluginKind.SOURCE;
    }

    @Override
    SourceConnector<?> create(PipelineConfig config, MetricsRuntime metrics) throws Exception;
}