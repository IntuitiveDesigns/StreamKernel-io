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
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.output.SnowflakeSnowpipeStreamingSink;
import com.intuitivedesigns.streamkernel.spi.SinkPlugin;

import java.util.Objects;

public final class SnowflakeSnowpipeStreamingSinkPlugin implements SinkPlugin {

    public static final String ID = "SNOWFLAKE_SNOWPIPE_STREAMING";

    @Override
    public String id() {
        return ID;
    }

    @Override
    public OutputSink<?> create(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");
        return SnowflakeSnowpipeStreamingSink.fromConfig(config, metrics);
    }
}
