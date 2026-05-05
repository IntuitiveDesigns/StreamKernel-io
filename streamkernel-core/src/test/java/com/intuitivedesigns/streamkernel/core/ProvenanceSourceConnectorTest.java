/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.core;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProvenanceSourceConnectorTest {

    @Test
    void fromConfigReturnsNoHeadersUnlessProvenanceIsEnabled() {
        final Properties props = new Properties();
        props.setProperty("pipeline.id", "test-pipeline");
        props.setProperty("source.type", "SYNTHETIC");
        props.setProperty("sink.type", "DEVNULL");
        props.setProperty("security.type", "PERMIT_ALL");
        props.setProperty("metrics.tag.run_id", "run-7");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        assertTrue(PipelineProvenance.fromConfig(config).headers().isEmpty());
    }

    @Test
    void wrapAddsHeadersWithoutDroppingExistingMetadata() {
        final Properties props = new Properties();
        props.setProperty("pipeline.id", "test-pipeline");
        props.setProperty("source.type", "SYNTHETIC");
        props.setProperty("sink.type", "DEVNULL");
        props.setProperty("security.type", "PERMIT_ALL");
        props.setProperty("metrics.tag.run_id", "run-7");
        props.setProperty("streamkernel.provenance.enabled", "true");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final SourceConnector<String> delegate = new SourceConnector<>() {
            @Override
            public void connect() {
            }

            @Override
            public void disconnect() {
            }

            @Override
            public PipelinePayload<String> fetch() {
                return new PipelinePayload<>("payload-1", "hello", Map.of("existing", "present"));
            }
        };

        final SourceConnector<String> wrapped = ProvenanceSourceConnector.wrap(
                delegate,
                PipelineProvenance.fromConfig(config).headers()
        );

        final PipelinePayload<String> payload = wrapped.fetch();

        assertEquals("present", payload.metadata().get("existing"));
        assertEquals("test-pipeline", payload.metadata().get("streamkernel.provenance.pipeline.id"));
        assertEquals("run-7", payload.metadata().get("streamkernel.provenance.run.id"));
        assertEquals("SYNTHETIC", payload.metadata().get("streamkernel.provenance.source.type"));
        assertEquals("DEVNULL", payload.metadata().get("streamkernel.provenance.sink.type"));
        assertEquals("PERMIT_ALL", payload.metadata().get("streamkernel.provenance.security.type"));
        assertNotNull(payload.metadata().get("streamkernel.provenance.config.sha256"));
    }

    @Test
    void fromConfigIncludesExtendedModelLineageHeaders() {
        final Properties props = new Properties();
        props.setProperty("pipeline.id", "test-pipeline");
        props.setProperty("source.type", "SYNTHETIC");
        props.setProperty("sink.type", "KAFKA");
        props.setProperty("metrics.tag.run_id", "pipeline-run-7");
        props.setProperty("streamkernel.provenance.enabled", "true");
        props.setProperty("model.name", "risk-model");
        props.setProperty("model.version", "42");
        props.setProperty("model.run.id", "model-run-9");
        props.setProperty("model.experiment.id", "exp-11");
        props.setProperty("model.stage", "production");
        props.setProperty("ai.feature.version", "features-v3");
        props.setProperty("ai.prompt.version", "prompt-v5");
        props.setProperty("transform.version", "transform-v8");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        final Map<String, String> headers = PipelineProvenance.fromConfig(config).headers();

        assertEquals("risk-model", headers.get(PipelineProvenance.HEADER_MODEL_NAME));
        assertEquals("42", headers.get(PipelineProvenance.HEADER_MODEL_VERSION));
        assertEquals("model-run-9", headers.get(PipelineProvenance.HEADER_MODEL_RUN_ID));
        assertEquals("exp-11", headers.get(PipelineProvenance.HEADER_MODEL_EXPERIMENT_ID));
        assertEquals("production", headers.get(PipelineProvenance.HEADER_MODEL_STAGE));
        assertEquals("features-v3", headers.get(PipelineProvenance.HEADER_FEATURE_VERSION));
        assertEquals("prompt-v5", headers.get(PipelineProvenance.HEADER_PROMPT_VERSION));
        assertEquals("transform-v8", headers.get(PipelineProvenance.HEADER_TRANSFORM_VERSION));

        final Map<String, String> provenanceOnly = PipelineProvenance.extractProvenanceHeaders(Map.of(
                PipelineProvenance.HEADER_MODEL_NAME, "risk-model",
                "streamkernel.source.text", "do not copy"
        ));
        assertEquals("risk-model", provenanceOnly.get(PipelineProvenance.HEADER_MODEL_NAME));
        assertTrue(!provenanceOnly.containsKey("streamkernel.source.text"));
    }
}
