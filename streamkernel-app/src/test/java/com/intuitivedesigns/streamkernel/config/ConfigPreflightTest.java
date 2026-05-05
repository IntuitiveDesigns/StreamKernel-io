/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigPreflightTest {

    @AfterEach
    void tearDown() {
        System.clearProperty("sk.run.id");
    }

    @Test
    void validateAllowsMinimalProfileWithoutRunIdWhenBenchAndProvenanceAreOff() {
        assertDoesNotThrow(() -> ConfigPreflight.validateOrThrow(minimalConfig()));
    }

    @Test
    void validateRequiresRunIdWhenBenchmarkModeIsEnabled() {
        final PipelineConfig config = configWith("streamkernel.bench.enabled", "true");

        final IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> ConfigPreflight.validateOrThrow(config));

        assertTrue(error.getMessage().contains("PIPELINE[RUN_ID]"));
        assertTrue(error.getMessage().contains("-Dsk.run.id"));
    }

    @Test
    void validateRequiresRunIdWhenProvenanceIsEnabled() {
        final PipelineConfig config = configWith("streamkernel.provenance.enabled", "true");

        final IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, () -> ConfigPreflight.validateOrThrow(config));

        assertTrue(error.getMessage().contains("PIPELINE[RUN_ID]"));
        assertTrue(error.getMessage().contains("metrics.tag.run_id"));
    }

    @Test
    void validateAcceptsRunIdFromJvmProperty() {
        System.setProperty("sk.run.id", "run-123");
        final PipelineConfig config = configWith("streamkernel.bench.enabled", "true");

        assertDoesNotThrow(() -> ConfigPreflight.validateOrThrow(config));
    }

    @Test
    void validateAcceptsRunIdFromMetricsTag() {
        final PipelineConfig config = configWith(
                "streamkernel.provenance.enabled", "true",
                "metrics.tag.run_id", "manual-run-7"
        );

        assertDoesNotThrow(() -> ConfigPreflight.validateOrThrow(config));
    }

    private static PipelineConfig minimalConfig() {
        return configWith();
    }

    private static PipelineConfig configWith(String... extraPairs) {
        final Properties props = new Properties();
        props.setProperty("pipeline.id", "test-pipeline");
        props.setProperty("pipeline.parallelism", "1");
        props.setProperty("pipeline.batch.size", "16");
        props.setProperty("source.type", "SYNTHETIC");
        props.setProperty("source.synthetic.payload.size", "32");
        props.setProperty("sink.type", "DEVNULL");

        for (int i = 0; i < extraPairs.length; i += 2) {
            props.setProperty(extraPairs[i], extraPairs[i + 1]);
        }

        return PipelineConfig.from(props, "inline");
    }
}
