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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

public final class ServicePluginRegistry<T extends PipelinePlugin<?>> {
    private final Map<String, T> byId;

    public ServicePluginRegistry(Class<T> spiType) {
        // Use the thread's context classloader (standard for webapps/frameworks)
        this(spiType, Thread.currentThread().getContextClassLoader());
    }

    public ServicePluginRegistry(Class<T> spiType, ClassLoader cl) {
        Map<String, T> tmp = new LinkedHashMap<>();
        ServiceLoader<T> loader = ServiceLoader.load(spiType, cl);
        for (T plugin : loader) {
            // We can now call .id() directly because T extends PipelinePlugin
            String id = PluginIds.normalize(plugin.id());
            if (id.isEmpty()) {
                throw new IllegalStateException("Plugin id() must not be blank for " + plugin.getClass().getName());
            }
            if (tmp.containsKey(id)) {
                throw new IllegalStateException("Duplicate plugin ID '" + id + "' for SPI " + spiType.getSimpleName() + ". Conflict between: " + tmp.get(id).getClass().getName() + " and " + plugin.getClass().getName());
            }
            tmp.put(id, plugin);
        }
        this.byId = Collections.unmodifiableMap(tmp);
    }

    public T require(String id, String configKeyName) {
        String key = PluginIds.normalize(id);
        T plugin = byId.get(key);
        if (plugin == null) {
            throw new IllegalArgumentException("No plugin found for '" + configKeyName + "=" + id + "'. " + "Available options: " + byId.keySet());
        }
        return plugin;
    }

    public Set<String> availableIds() {
        return byId.keySet();
    }

    public Optional<T> get(String id) {
        return Optional.ofNullable(byId.get(PluginIds.normalize(id)));
    }
}