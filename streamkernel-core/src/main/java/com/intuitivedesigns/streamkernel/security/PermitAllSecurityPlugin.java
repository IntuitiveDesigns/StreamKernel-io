/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.security;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.PluginKind;
import com.intuitivedesigns.streamkernel.spi.SecurityPlugin;
import com.intuitivedesigns.streamkernel.spi.SecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PermitAllSecurityPlugin is a SECURITY-kind plugin that returns a {@link SecurityProvider}
 * which always authorizes every request.
 *
 * Enterprise / acquisition posture:
 * - This plugin exists to support:
 *     - local development,
 *     - benchmarks where security overhead is intentionally excluded,
 *     - early bring-up environments where a real policy engine is not yet provisioned.
 *
 * Security warning:
 * - PERMIT_ALL is NOT appropriate for production systems that handle sensitive data or enforce RBAC/ABAC.
 * - Treat this as "explicitly insecure by design" and require operators to opt-in via config.
 *
 * Operational characteristics:
 * - Zero network calls
 * - Near-zero CPU overhead
 * - No internal state
 *
 * Suggested enterprise hardening (optional, outside the scope of this implementation):
 * - Add config guardrails:
 *     - require `security.permit_all.allowed=true` or an explicit environment gate (e.g., env=dev)
 * - Emit a metric (e.g., security_permit_all_enabled=1) for dashboards/compliance checks
 * - Surface a startup banner that includes the resolved environment and config file path
 */
public final class PermitAllSecurityPlugin implements SecurityPlugin {

    /** Stable plugin id used in configuration: security.type=PERMIT_ALL */
    public static final String ID = "PERMIT_ALL";

    private static final Logger log = LoggerFactory.getLogger(PermitAllSecurityPlugin.class);

    @Override
    public String id() {
        return ID;
    }

    @Override
    public PluginKind kind() {
        return PluginKind.SECURITY;
    }

    /**
     * Create a permit-all security provider.
     *
     * The factory logs a WARNING because enabling this policy is a security-sensitive action.
     * Logging here (creation time) ensures the warning appears during startup, before traffic flows.
     *
     * @param config   pipeline config (unused; present to satisfy SPI contract)
     * @param metrics  metrics runtime (unused; present to satisfy SPI contract)
     * @return provider that always returns true for authorization checks
     */
    @Override
    public SecurityProvider create(PipelineConfig config, MetricsRuntime metrics) {
        log.warn("SECURITY WARNING: Using PERMIT_ALL policy. All actions are allowed.");
        return new PermitAllProvider();
    }

    // ---------------------------------------------------------------------
    // Implementation
    // ---------------------------------------------------------------------

    /**
     * PermitAllProvider always returns true.
     *
     * Fail semantics:
     * - There are no failure modes: this provider cannot deny and cannot error.
     *
     * Enterprise note:
     * - This provider is intentionally minimal and side-effect free.
     * - It is safe for benchmarks where you want to isolate performance characteristics
     *   of the pipeline execution path without policy enforcement overhead.
     */
    private static final class PermitAllProvider implements SecurityProvider {

        /**
         * Authorization decision.
         *
         * IMPORTANT:
         * - This method currently allows everything, regardless of principal/resource/action.
         *
         * Parameter order caution:
         * - Many systems use (principal, action, resource) order.
         * - This interface uses a specific order defined by {@link SecurityProvider}.
         *   Ensure callers pass arguments consistently to avoid confusion when switching providers.
         *
         * @param principal identity requesting access (user/service account)
         * @param resource  resource being accessed (topic, stream, dataset, etc.)
         * @param action    action being performed (read, write, admin, etc.)
         * @return always true
         */
        @Override
        public boolean isAllowed(String principal, String resource, String action) {
            // In future OPA integration, this decision would be derived from policy evaluation (Rego / OPA Data API).
            // Here, it's intentionally unconditional.
            return true;
        }

        /**
         * No-op close.
         *
         * This provider holds no resources (no threads, sockets, files, or buffers),
         * so there is nothing to release.
         */
        @Override
        public void close() {
            // No-op
        }
    }
}
