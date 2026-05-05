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

/**
 * Runtime contract for authorization checks.
 *
 * <p><b>Performance Note:</b> This method is often called on the "Hot Path"
 * (per event). Implementations MUST use caching (e.g., Caffeine) to avoid
 * network latency on every call.</p>
 */
public interface SecurityProvider extends AutoCloseable {

    /**
     * Checks if the action is authorized.
     *
     * @param identity The actor (e.g. "service-account-a" or "user:123")
     * @param action   The operation (e.g. "write", "read", "delete")
     * @param resource The target (e.g. "topic:sales-events", "index:users")
     * @return true if allowed, false if denied.
     */
    boolean isAllowed(String identity, String action, String resource);

    /**
     * Lifecycle hook to clean up resources (HTTP clients, thread pools).
     * Default is no-op for stateless providers (like AllowAll).
     */
    @Override
    default void close() {
        // No-op by default
    }
}