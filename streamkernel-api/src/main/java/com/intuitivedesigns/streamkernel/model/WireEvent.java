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

package com.intuitivedesigns.streamkernel.model;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class WireEvent {

    private final byte[] bytes;
    private final Map<String, String> headers;
    private final String key;
    private final float[] vector;

    /**
     * Cache for lazy string materialization to avoid repeated UTF-8 decoding overhead.
     * Non-final and transient; not part of the serialized state.
     */
    private transient String cachedText;

    private WireEvent(byte[] bytes, Map<String, String> headers, String key, float[] vector) {
        this.bytes = Objects.requireNonNull(bytes, "bytes");
        this.headers = (headers == null || headers.isEmpty())
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(headers));
        this.key = key;
        this.vector = vector;
    }

    // --- Factory Methods ---

    public static WireEvent bytes(byte[] bytes, Map<String, String> headers) {
        return new WireEvent(bytes, headers, null, null);
    }

    public static WireEvent bytes(byte[] bytes, Map<String, String> headers, String key) {
        return new WireEvent(bytes, headers, key, null);
    }

    /** Restored 3-arg method to fix the compilation error in the Encoder */
    public static WireEvent vector(byte[] bytes, float[] vector, Map<String, String> headers) {
        return new WireEvent(bytes, headers, null, vector);
    }

    public static WireEvent vector(byte[] bytes, float[] vector, Map<String, String> headers, String key) {
        return new WireEvent(bytes, headers, key, vector);
    }

    // --- Performance Helpers ---

    /**
     * Returns the text representation of the bytes.
     * Uses lazy-loading and caching to eliminate repeated decoding on the hot path.
     */
    public String text() {
        String t = cachedText;
        if (t == null) {
            t = new String(bytes, StandardCharsets.UTF_8);
            cachedText = t;
        }
        return t;
    }

    // --- Getters ---

    public byte[] bytes() { return bytes; }
    public Map<String, String> headers() { return headers; }
    public String key() { return key; }
    public float[] vector() { return vector; }

    @Override
    public String toString() {
        return "WireEvent{bytes=" + bytes.length +
                ", headers=" + headers.size() +
                ", key=" + key +
                ", vector=" + (vector == null ? "null" : vector.length) + "}";
    }
}