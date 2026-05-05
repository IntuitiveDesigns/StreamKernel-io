/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins.transformer;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public final class HttpEmbeddingStubServer {

    // Env vars to avoid OS-specific scripting
    // PORT=8088
    // EMBED_DIM=768
    // SLEEP_MS=3
    // JITTER_MS=2
    public static void main(String[] args) throws Exception {
        int port = intEnv("PORT", 8088);
        int dim = intEnv("EMBED_DIM", 768);
        int sleepMs = intEnv("SLEEP_MS", 0);
        int jitterMs = intEnv("JITTER_MS", 0);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/embed", ex -> {
            try {
                handleEmbed(ex, dim, sleepMs, jitterMs);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        server.setExecutor(Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors())));
        server.start();

        System.out.println("HTTP Embedding Stub Server listening on http://localhost:" + port + "/embed"
                + " dim=" + dim + " sleepMs=" + sleepMs + " jitterMs=" + jitterMs);
    }

    private static void handleEmbed(HttpExchange ex, int dim, int sleepMs, int jitterMs) throws Exception {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            ex.sendResponseHeaders(405, -1);
            ex.close();
            return;
        }

        // Simulate model latency
        int extra = (jitterMs <= 0) ? 0 : ThreadLocalRandom.current().nextInt(0, jitterMs + 1);
        int total = sleepMs + extra;
        if (total > 0) Thread.sleep(total);

        // Response (keep tiny + stable)
        String json = "{\"dim\":" + dim + ",\"ms\":" + total + "}";

        byte[] out = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(200, out.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(out);
        }
        ex.close();
    }

    private static int intEnv(String k, int def) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return def;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
    }
}
