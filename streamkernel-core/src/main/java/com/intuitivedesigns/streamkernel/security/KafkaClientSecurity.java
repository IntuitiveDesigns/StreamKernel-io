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

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Shared Kafka security property resolver for both sources and sinks.
 *
 * Supported config shapes:
 * - global aliases:   kafka.*
 * - component aliases: source.kafka.* / sink.kafka.*
 * - raw pass-through: kafka.property.<actual.kafka.prop>=...
 *                     source.kafka.property.<actual.kafka.prop>=...
 *                     sink.kafka.property.<actual.kafka.prop>=...
 * - OIDC aliases:     *.oidc.*
 */
public final class KafkaClientSecurity {

    private static final String GLOBAL_PREFIX = "kafka.";
    private static final String OIDC_ENABLED = "oidc.enabled";
    private static final String OIDC_TOKEN_ENDPOINT = "oidc.token.endpoint.url";
    private static final String OIDC_CLIENT_ID = "oidc.client.id";
    private static final String OIDC_CLIENT_SECRET = "oidc.client.secret";
    private static final String OIDC_SCOPE = "oidc.scope";

    private static final String K_SECURITY_PROTOCOL = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    private static final String K_SASL_MECHANISM = "sasl.mechanism";
    private static final String K_SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String K_SASL_LOGIN_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";
    private static final String K_OAUTH_TOKEN_ENDPOINT = "sasl.oauthbearer.token.endpoint.url";

    private static final String OAUTH_CALLBACK_HANDLER =
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler";
    private static final String OAUTH_LOGIN_MODULE =
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";

    private static final Map<String, String> DIRECT_MAPPINGS = Map.ofEntries(
            Map.entry("security.protocol", K_SECURITY_PROTOCOL),
            Map.entry("sasl.mechanism", K_SASL_MECHANISM),
            Map.entry("sasl.jaas.config", K_SASL_JAAS_CONFIG),
            Map.entry("sasl.login.callback.handler.class", K_SASL_LOGIN_CALLBACK_HANDLER_CLASS),
            Map.entry("sasl.oauthbearer.token.endpoint.url", K_OAUTH_TOKEN_ENDPOINT),
            Map.entry("ssl.keystore.location", "ssl.keystore.location"),
            Map.entry("ssl.keystore.password", "ssl.keystore.password"),
            Map.entry("ssl.keystore.type", "ssl.keystore.type"),
            Map.entry("ssl.key.password", "ssl.key.password"),
            Map.entry("ssl.truststore.location", "ssl.truststore.location"),
            Map.entry("ssl.truststore.password", "ssl.truststore.password"),
            Map.entry("ssl.truststore.type", "ssl.truststore.type"),
            Map.entry("ssl.endpoint.identification.algorithm", "ssl.endpoint.identification.algorithm")
    );

    private KafkaClientSecurity() {}

    public static void apply(PipelineConfig config, Properties props, String componentPrefix) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(props, "props");

        final String prefix = normalizeComponentPrefix(componentPrefix);

        applyDirectMappings(config, props, GLOBAL_PREFIX);
        applyPropertyPassthrough(config, props, GLOBAL_PREFIX + "property.");

        applyDirectMappings(config, props, prefix);
        applyPropertyPassthrough(config, props, prefix + "property.");

        applyOidc(config, props, prefix);
    }

    public static String describeAuthMode(PipelineConfig config, String componentPrefix) {
        Objects.requireNonNull(config, "config");

        final String prefix = normalizeComponentPrefix(componentPrefix);
        if (oidcEnabled(config, prefix)) {
            return "OIDC";
        }

        final String mechanism = firstNonBlank(
                config.getString(prefix + "sasl.mechanism", null),
                config.getString(GLOBAL_PREFIX + "sasl.mechanism", null)
        );
        if (!isBlank(mechanism)) {
            return "SASL_" + mechanism.trim().toUpperCase(Locale.ROOT);
        }

        final String protocol = firstNonBlank(
                config.getString(prefix + "security.protocol", null),
                config.getString(GLOBAL_PREFIX + "security.protocol", null)
        );
        if (!isBlank(protocol) && protocol.toUpperCase(Locale.ROOT).contains("SSL")) {
            final boolean hasClientMaterial = !isBlank(firstNonBlank(
                    config.getString(prefix + "ssl.keystore.location", null),
                    config.getString(GLOBAL_PREFIX + "ssl.keystore.location", null)
            ));
            return hasClientMaterial ? "MTLS" : "TLS";
        }

        return "PLAINTEXT";
    }

    private static void applyDirectMappings(PipelineConfig config, Properties props, String prefix) {
        for (Map.Entry<String, String> entry : DIRECT_MAPPINGS.entrySet()) {
            final String value = config.getString(prefix + entry.getKey(), null);
            if (!isBlank(value)) {
                props.put(entry.getValue(), value.trim());
            }
        }
    }

    private static void applyPropertyPassthrough(PipelineConfig config, Properties props, String prefix) {
        for (String key : config.keys()) {
            if (!key.startsWith(prefix) || key.length() <= prefix.length()) {
                continue;
            }

            final String kafkaProperty = key.substring(prefix.length()).trim();
            if (kafkaProperty.isEmpty()) {
                continue;
            }

            final String value = config.getString(key, null);
            if (!isBlank(value)) {
                props.put(kafkaProperty, value);
            }
        }
    }

    private static void applyOidc(PipelineConfig config, Properties props, String prefix) {
        if (!oidcEnabled(config, prefix)) {
            return;
        }

        props.putIfAbsent(K_SECURITY_PROTOCOL, "SASL_SSL");
        props.putIfAbsent(K_SASL_MECHANISM, "OAUTHBEARER");
        props.putIfAbsent(K_SASL_LOGIN_CALLBACK_HANDLER_CLASS, OAUTH_CALLBACK_HANDLER);

        final String tokenEndpoint = firstNonBlank(
                config.getString(prefix + OIDC_TOKEN_ENDPOINT, null),
                config.getString(GLOBAL_PREFIX + OIDC_TOKEN_ENDPOINT, null),
                props.getProperty(K_OAUTH_TOKEN_ENDPOINT)
        );
        if (isBlank(tokenEndpoint)) {
            throw new IllegalArgumentException(
                    "Kafka OIDC requires " + prefix + OIDC_TOKEN_ENDPOINT + " or kafka." + OIDC_TOKEN_ENDPOINT
            );
        }
        props.put(K_OAUTH_TOKEN_ENDPOINT, tokenEndpoint);

        if (props.containsKey(K_SASL_JAAS_CONFIG)) {
            return;
        }

        final String clientId = firstNonBlank(
                config.getString(prefix + OIDC_CLIENT_ID, null),
                config.getString(GLOBAL_PREFIX + OIDC_CLIENT_ID, null)
        );
        final String clientSecret = firstNonBlank(
                config.getString(prefix + OIDC_CLIENT_SECRET, null),
                config.getString(GLOBAL_PREFIX + OIDC_CLIENT_SECRET, null)
        );
        if (isBlank(clientId) || isBlank(clientSecret)) {
            throw new IllegalArgumentException(
                    "Kafka OIDC requires client credentials via "
                            + prefix + OIDC_CLIENT_ID + " / " + prefix + OIDC_CLIENT_SECRET
                            + " or kafka." + OIDC_CLIENT_ID + " / kafka." + OIDC_CLIENT_SECRET
            );
        }

        final String scope = firstNonBlank(
                config.getString(prefix + OIDC_SCOPE, null),
                config.getString(GLOBAL_PREFIX + OIDC_SCOPE, null)
        );
        props.put(K_SASL_JAAS_CONFIG, buildOauthJaasConfig(clientId, clientSecret, scope));
    }

    private static boolean oidcEnabled(PipelineConfig config, String prefix) {
        final String componentValue = config.getString(prefix + OIDC_ENABLED, null);
        if (!isBlank(componentValue)) {
            return Boolean.parseBoolean(componentValue.trim());
        }
        return config.getBoolean(GLOBAL_PREFIX + OIDC_ENABLED, false);
    }

    private static String buildOauthJaasConfig(String clientId, String clientSecret, String scope) {
        final StringBuilder jaas = new StringBuilder(160)
                .append(OAUTH_LOGIN_MODULE)
                .append(" required")
                .append(" clientId=\"").append(escapeJaasValue(clientId)).append('"')
                .append(" clientSecret=\"").append(escapeJaasValue(clientSecret)).append('"');

        if (!isBlank(scope)) {
            jaas.append(" scope=\"").append(escapeJaasValue(scope)).append('"');
        }

        return jaas.append(';').toString();
    }

    private static String escapeJaasValue(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String normalizeComponentPrefix(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("componentPrefix cannot be blank");
        }
        final String normalized = value.trim();
        return normalized.endsWith(".") ? normalized : (normalized + ".");
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value.trim();
            }
        }
        return null;
    }
}
