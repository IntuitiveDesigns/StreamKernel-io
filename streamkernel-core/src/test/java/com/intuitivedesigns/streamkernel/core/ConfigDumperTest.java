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

package com.intuitivedesigns.streamkernel.core;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigDumperTest {

    @Test
    void sanitizeLeavesTokenizerSettingsVisible() throws Exception {
        assertEquals("128", sanitize("ai.embedding.tokenizer.max.length", "128"));
        assertEquals("/models/tokenizer.json", sanitize("ai.embedding.tokenizer.uri", "/models/tokenizer.json"));
    }

    @Test
    void sanitizeStillMasksSecretLikeKeys() throws Exception {
        assertEquals("***", sanitize("api.token", "abcd"));
        assertEquals("***", sanitize("source.kafka.oidc.client.secret", "secret-value"));
    }

    @Test
    void sanitizeMasksCredentialsEmbeddedInMongoUris() throws Exception {
        assertEquals("mongodb://***@localhost:27017/db",
                sanitize("mongodb.uri", "mongodb://user:pass@localhost:27017/db"));
    }

    private static String sanitize(String key, String value) throws Exception {
        final Method sanitize = ConfigDumper.class.getDeclaredMethod("sanitize", String.class, String.class);
        sanitize.setAccessible(true);
        return (String) sanitize.invoke(null, key, value);
    }
}
