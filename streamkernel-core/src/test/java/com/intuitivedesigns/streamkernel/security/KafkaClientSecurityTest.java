/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.security;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaClientSecurityTest {

    @Test
    void applyPrefersComponentScopeAndBuildsOidcClientCredentialsConfig() {
        final Properties props = new Properties();
        props.setProperty("kafka.security.protocol", "SSL");
        props.setProperty("kafka.oidc.enabled", "true");
        props.setProperty("kafka.oidc.token.endpoint.url", "https://issuer.example/token");
        props.setProperty("kafka.oidc.client.id", "global-client");
        props.setProperty("kafka.oidc.client.secret", "global-secret");
        props.setProperty("source.kafka.security.protocol", "SASL_SSL");
        props.setProperty("source.kafka.oidc.client.id", "source-client");
        props.setProperty("source.kafka.oidc.client.secret", "source-secret");
        props.setProperty("source.kafka.oidc.scope", "kafka.read");
        props.setProperty("source.kafka.property.sasl.login.connect.timeout.ms", "12000");

        final Properties kafkaProps = new Properties();
        KafkaClientSecurity.apply(PipelineConfig.from(props, "inline"), kafkaProps, "source.kafka.");

        assertEquals("SASL_SSL", kafkaProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("OAUTHBEARER", kafkaProps.getProperty("sasl.mechanism"));
        assertEquals("https://issuer.example/token",
                kafkaProps.getProperty("sasl.oauthbearer.token.endpoint.url"));
        assertEquals("12000", kafkaProps.getProperty("sasl.login.connect.timeout.ms"));
        assertTrue(kafkaProps.getProperty("sasl.jaas.config").contains("clientId=\"source-client\""));
        assertTrue(kafkaProps.getProperty("sasl.jaas.config").contains("clientSecret=\"source-secret\""));
        assertTrue(kafkaProps.getProperty("sasl.jaas.config").contains("scope=\"kafka.read\""));
    }

    @Test
    void describeAuthModeDetectsTlsAndOidc() {
        final Properties tlsProps = new Properties();
        tlsProps.setProperty("sink.kafka.security.protocol", "SSL");
        tlsProps.setProperty("sink.kafka.ssl.keystore.location", "/tmp/client.keystore.p12");

        assertEquals("MTLS",
                KafkaClientSecurity.describeAuthMode(PipelineConfig.from(tlsProps, "inline"), "sink.kafka."));

        final Properties oidcProps = new Properties();
        oidcProps.setProperty("sink.kafka.oidc.enabled", "true");

        assertEquals("OIDC",
                KafkaClientSecurity.describeAuthMode(PipelineConfig.from(oidcProps, "inline"), "sink.kafka."));
    }
}
