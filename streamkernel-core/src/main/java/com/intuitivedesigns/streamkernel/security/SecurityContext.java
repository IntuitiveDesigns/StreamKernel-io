/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.security;

import java.util.Objects;

/**
 * Immutable value object representing the **security execution context** for a running pipeline.
 *
 * This object is intentionally lightweight and allocation-friendly because it is passed through
 * the hot path of the orchestrator and security provider.
 *
 * Conceptually this record answers:
 *      "WHO is doing WHAT to WHICH RESOURCE?"
 *
 * plus additional resilience settings for the source connector.
 *
 * Why this exists:
 * - Decouples the pipeline from any specific security implementation (OPA, IAM, custom RBAC, etc.)
 * - Provides a stable contract between:
 *      PipelineOrchestrator  →  SecurityProvider
 * - Ensures consistent defaults and normalization across the entire system.
 *
 * Enterprise perspective:
 * - Treat this as the canonical *authorization envelope*.
 * - Future extensions may include:
 *      tenantId, environment, labels, requestId, attributes, etc.
 *
 * Thread-safety:
 * - Records are immutable → safe to share across threads.
 */
public record SecurityContext(

        /**
         * Principal performing the action.
         *
         * Examples:
         * - "streamkernel-service"
         * - "payments-pipeline"
         * - "svc-fraud-detection"
         * - "user:alice@example.com"
         *
         * This value is passed directly to the SecurityProvider and typically maps to:
         * - Kubernetes service account
         * - workload identity
         * - IAM role
         */
        String principal,

        /**
         * Action being performed.
         *
         * Typical values:
         * - read
         * - write
         * - admin
         * - transform
         *
         * Keep action vocabulary small and consistent across pipelines.
         */
        String action,

        /**
         * Resource being accessed.
         *
         * Typical values:
         * - Kafka topic
         * - database collection
         * - stream name
         * - dataset identifier
         *
         * This value should be stable and human-readable so it can appear in:
         * - policy logs
         * - audit trails
         * - dashboards
         */
        String resource,

        /**
         * Source error policy flag.
         *
         * true  → fail fast when the source connector throws errors.
         * false → retry with exponential backoff.
         *
         * Enterprise rationale:
         * - Some pipelines (fraud, compliance, trading) must fail immediately.
         * - Others (ETL, analytics) should retry indefinitely.
         */
        boolean failFastOnSourceError,

        /**
         * Initial backoff for source errors (milliseconds).
         *
         * Used when failFastOnSourceError = false.
         *
         * This is the starting delay in an exponential backoff sequence.
         */
        long sourceErrorBackoffInitialMs,

        /**
         * Maximum backoff for source errors (milliseconds).
         *
         * Backoff grows exponentially until it reaches this ceiling.
         *
         * Prevents runaway sleep durations.
         */
        long sourceErrorBackoffMaxMs
) {

    /**
     * Canonical constructor used by the Java record.
     *
     * Responsibilities:
     * - Normalize blank/null values.
     * - Enforce sane backoff boundaries.
     * - Guarantee non-null, non-empty values for core fields.
     *
     * These guardrails ensure downstream code can avoid defensive checks.
     */
    public SecurityContext {

        // Normalize primary identity fields.
        // This guarantees the security provider never receives null/blank inputs.
        principal = normalizeNonBlank(principal, "unknown-service");
        action = normalizeNonBlank(action, "write");
        resource = normalizeNonBlank(resource, "unknown-resource");

        // Ensure backoff is never negative.
        if (sourceErrorBackoffInitialMs < 0) {
            sourceErrorBackoffInitialMs = 0;
        }

        // Ensure max >= initial.
        if (sourceErrorBackoffMaxMs < sourceErrorBackoffInitialMs) {
            sourceErrorBackoffMaxMs = sourceErrorBackoffInitialMs;
        }
    }

    /**
     * Normalizes strings to ensure they are non-null and non-blank.
     *
     * This prevents:
     * - NullPointerExceptions in security providers
     * - Invalid policy queries
     * - Hard-to-debug configuration issues
     */
    private static String normalizeNonBlank(String s, String fallback) {
        if (s == null) return fallback;
        final String t = s.trim();
        return t.isEmpty() ? fallback : t;
    }

    /**
     * Convenience factory method for common cases.
     *
     * Uses sensible default retry settings:
     * - failFastOnSourceError = false
     * - initial backoff = 250ms
     * - max backoff = 5 seconds
     *
     * Intended for:
     * - tests
     * - simple pipelines
     * - programmatic pipeline creation
     */
    public static SecurityContext of(String principal, String action, String resource) {
        return new SecurityContext(principal, action, resource, false, 250L, 5000L);
    }
}
