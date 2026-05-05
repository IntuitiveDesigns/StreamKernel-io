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

package com.intuitivedesigns.streamkernel.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigTest {

    @AfterEach
    void tearDown() {
        System.clearProperty("sk.config.path");
        System.clearProperty("pipeline.config.test.value");
        PipelineConfig.resetForTests();
    }

    @Test
    void getLoadsConfiguredFileOnceAndCachesTheInstance(@TempDir Path tempDir) throws Exception {
        final Path configFile = tempDir.resolve("pipeline.properties");
        Files.writeString(configFile, "pipeline.id=test-pipeline\npipeline.parallelism=4\n");

        System.setProperty("sk.config.path", configFile.toString());

        final PipelineConfig first = PipelineConfig.get();
        final PipelineConfig second = PipelineConfig.get();

        assertSame(first, second);
        assertEquals("test-pipeline", first.getString("pipeline.id", null));
        assertEquals(4, first.getInt("pipeline.parallelism", 0));
        assertEquals(configFile.toAbsolutePath().toString(), first.loadedFromPath());
    }

    @Test
    void overlayAppliesOverridesWithoutMutatingTheBaseConfig() {
        final Properties props = new Properties();
        props.setProperty("alpha", "1");
        props.setProperty("remove.me", "present");
        props.setProperty("dynamic", "${sys:pipeline.config.test.value:fallback}");
        final PipelineConfig base = PipelineConfig.from(props, "base.properties");

        System.setProperty("pipeline.config.test.value", "resolved");

        final Map<String, String> overrides = new LinkedHashMap<>();
        overrides.put("alpha", "2");
        overrides.put("remove.me", null);
        overrides.put("added", "new-value");

        final PipelineConfig overlay = PipelineConfig.overlay(base, overrides);

        assertEquals("1", base.getString("alpha", null));
        assertTrue(base.hasPath("remove.me"));
        assertEquals("resolved", base.getString("dynamic", null));

        assertEquals("2", overlay.getString("alpha", null));
        assertFalse(overlay.hasPath("remove.me"));
        assertEquals("new-value", overlay.getString("added", null));
        assertEquals("resolved", overlay.getString("dynamic", null));
        assertEquals("base.properties (overlay)", overlay.loadedFromPath());
    }

    @Test
    void typedAccessorsAndViewsHonorFallbacksAndResolvedValues() {
        final Properties props = new Properties();
        props.setProperty("good.int", "17");
        props.setProperty("bad.int", "not-a-number");
        props.setProperty("good.long", "123456789");
        props.setProperty("flag", "true");
        props.setProperty("placeholder", "${sys:missing.value:default-value}");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        assertEquals(17, config.getInt("good.int", 0));
        assertEquals(9, config.getInt("bad.int", 9));
        assertEquals(123456789L, config.getLong("good.long", 0L));
        assertEquals(77L, config.getLong("missing.long", 77L));
        assertTrue(config.getBoolean("flag", false));
        assertTrue(config.getBoolean("missing.flag", true));
        assertEquals("default-value", config.getString("placeholder", null));
        assertEquals("default-value", config.get("placeholder"));
        assertEquals("default-value", config.asMap().get("placeholder"));
        assertTrue(config.keys().contains("good.int"));
        assertNull(config.get("missing"));
    }

    @Test
    void filePlaceholderLoadsSecretValueAndStripsTrailingNewline(@TempDir Path tempDir) throws Exception {
        final Path secretFile = tempDir.resolve("secret.txt");
        Files.writeString(secretFile, "top-secret\n");

        final Properties props = new Properties();
        props.setProperty("secret.value", "${file:" + secretFile.toAbsolutePath() + "}");
        final PipelineConfig config = PipelineConfig.from(props, "inline");

        assertEquals("top-secret", config.getString("secret.value", null));
        assertEquals("top-secret", config.asMap().get("secret.value"));
    }
}
