/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * StreamKernelConsumer
 * ====================
 * Standalone, lightweight Kafka consumer utility used to validate StreamKernel sink correctness
 * and to measure end-to-end consumer throughput (EPS) on a topic.
 *
 * Primary use-cases
 * -----------------
 *  1) Sink validation: confirm KafkaSink / KafkaAvroSink are producing expected volume and format.
 *  2) Throughput measurement: compute stable EPS over a fixed time window without per-record iteration.
 *  3) Operational sanity checks: optionally compute total lag on a cadence without dominating runtime.
 *
 * Design principles (enterprise / benchmark-friendly)
 * --------------------------------------------------
 *  • Deterministic time-window reporting (stable EPS signal)
 *  • Safe shutdown using consumer.wakeup() (poll interruption without hacks)
 *  • Lag sampling is gated (endOffsets()/position() can be expensive)
 *  • Minimal allocations in the hot loop (records.count() is O(1))
 *
 * Configuration model
 * -------------------
 *  • Loads Kafka client properties from a classpath resource: consumer.properties
 *  • Supports system-property overrides for:
 *      - consumer.properties.file   (alternate properties resource name)
 *      - consumer.poll.ms          (poll timeout)
 *      - consumer.report.ms        (report interval)
 *      - consumer.topic.default    (fallback topic if no CLI arg provided)
 *
 * Notes
 * -----
 *  • This tool intentionally does not commit offsets explicitly; it inherits that behavior from
 *    the loaded consumer.properties configuration. For pure benchmarking, you typically disable
 *    auto-commit and run from latest.
 */
public final class StreamKernelConsumer {

    private static final Logger log = LoggerFactory.getLogger(StreamKernelConsumer.class);

    private static final String PROPERTIES_FILE = "consumer.properties";

    // System properties (optional overrides)
    private static final String P_PROPERTIES_FILE = "consumer.properties.file";
    private static final String P_POLL_MS = "consumer.poll.ms";
    private static final String P_REPORT_MS = "consumer.report.ms";
    private static final String P_TOPIC_DEFAULT = "consumer.topic.default";

    // Defaults
    private static final long DEFAULT_POLL_MS = 500L;
    private static final long DEFAULT_REPORT_MS = 1_000L;
    private static final String DEFAULT_TOPIC = "streamkernel-default";

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final long pollMs;
    private final long reportMs;

    public StreamKernelConsumer(String topic) {
        Objects.requireNonNull(topic, "topic");

        final Properties props = loadProperties(resolvePropertiesFile());

        // Ensure deserializers are present even if the properties file is minimal.
        props.putIfAbsent("key.deserializer", StringDeserializer.class.getName());
        props.putIfAbsent("value.deserializer", StringDeserializer.class.getName());

        this.pollMs = clampLong(getLong(P_POLL_MS, DEFAULT_POLL_MS), 10L, 60_000L);
        this.reportMs = clampLong(getLong(P_REPORT_MS, DEFAULT_REPORT_MS), 250L, 60_000L);

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));

        // Fast, safe shutdown that will break out of poll() promptly.
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "streamkernel-consumer-shutdown"));
    }

    private static String resolvePropertiesFile() {
        final String override = System.getProperty(P_PROPERTIES_FILE);
        if (override == null) return PROPERTIES_FILE;
        final String trimmed = override.trim();
        return trimmed.isEmpty() ? PROPERTIES_FILE : trimmed;
    }

    private Properties loadProperties(String resourceName) {
        final Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            if (input == null) {
                throw new IllegalStateException(
                        "Unable to find " + resourceName + " on the classpath. " +
                                "Create it under src/main/resources or set -D" + P_PROPERTIES_FILE + "=<name>."
                );
            }
            props.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("Error loading consumer config from " + resourceName, ex);
        }
        return props;
    }

    public void run() {
        log.info("StreamKernelConsumer started. pollMs={} reportMs={}", pollMs, reportMs);

        long totalCount = 0L;

        long windowStartNs = System.nanoTime();
        long windowCount = 0L;
        long lastReportNs = windowStartNs;

        long lastLag = -1L;
        long lastLagSampleNs = 0L;

        try {
            while (running.get()) {
                final ConsumerRecords<String, String> records;
                try {
                    records = consumer.poll(Duration.ofMillis(pollMs));
                } catch (org.apache.kafka.common.errors.WakeupException we) {
                    // Expected on shutdown.
                    break;
                }

                // Hot-path optimization: records.count() is O(1); no need to iterate the records.
                if (!records.isEmpty()) {
                    final int count = records.count();
                    windowCount += count;
                    totalCount += count;
                }

                final long nowNs = System.nanoTime();

                // Fixed report window (time-based)
                if (TimeUnit.NANOSECONDS.toMillis(nowNs - lastReportNs) >= reportMs) {

                    // Lag sampling: gate at most once per report interval (endOffsets() can be costly).
                    if (TimeUnit.NANOSECONDS.toMillis(nowNs - lastLagSampleNs) >= reportMs) {
                        if (consumer.assignment() != null && !consumer.assignment().isEmpty()) {
                            lastLag = safeComputeTotalLag();
                            lastLagSampleNs = nowNs;
                        }
                    }

                    logWindow(windowStartNs, windowCount, totalCount, lastLag);

                    // Reset window
                    windowStartNs = nowNs;
                    windowCount = 0L;
                    lastReportNs = nowNs;
                }
            }
        } finally {
            try {
                // Close triggers immediate group leave / rebalance (important for clean test runs).
                consumer.close();
            } catch (Exception ignored) {
            }
            log.info("StreamKernelConsumer stopped. Total records consumed={}", totalCount);
        }
    }

    private void logWindow(long windowStartNs, long windowCount, long totalCount, long totalLag) {
        final long nowNs = System.nanoTime();
        final long elapsedNs = nowNs - windowStartNs;
        if (elapsedNs <= 0) return;

        final double seconds = elapsedNs / 1_000_000_000.0;
        final double eps = windowCount / seconds;

        if (totalLag >= 0) {
            log.info("Consumed {} records in ~{}s → ~{} EPS (total={}, lag={})",
                    windowCount,
                    String.format("%.3f", seconds),
                    String.format("%,.0f", eps),
                    totalCount,
                    totalLag
            );
        } else {
            log.info("Consumed {} records in ~{}s → ~{} EPS (total={})",
                    windowCount,
                    String.format("%.3f", seconds),
                    String.format("%,.0f", eps),
                    totalCount
            );
        }
    }

    /**
     * Computes total lag across current assignment.
     *
     * Lag = endOffset(tp) - position(tp)
     *
     * Returns -1 if sampling fails (never breaks the consumer loop).
     */
    private long safeComputeTotalLag() {
        try {
            final Set<TopicPartition> assignment = consumer.assignment();
            if (assignment == null || assignment.isEmpty()) return -1L;

            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);

            long totalLag = 0L;
            for (TopicPartition tp : assignment) {
                final long end = endOffsets.getOrDefault(tp, 0L);
                final long pos = consumer.position(tp);
                if (end > pos) totalLag += (end - pos);
            }
            return totalLag;
        } catch (Exception e) {
            log.debug("Lag sampling failed: {}", e.getMessage());
            return -1L;
        }
    }

    private void shutdown() {
        if (!running.compareAndSet(true, false)) return;
        log.info("Shutdown requested for StreamKernelConsumer...");
        try {
            // Interrupts poll() promptly.
            consumer.wakeup();
        } catch (Exception ignored) {
        }
    }

    // ---------------------------------------------------------
    // MAIN ENTRYPOINT
    // ---------------------------------------------------------
    public static void main(String[] args) {
        final String topic =
                (args.length > 0 && args[0] != null && !args[0].isBlank())
                        ? args[0].trim()
                        : getString(P_TOPIC_DEFAULT, DEFAULT_TOPIC);

        new StreamKernelConsumer(topic).run();
    }

    // ---- system property helpers ----

    private static String getString(String key, String def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        return v.isEmpty() ? def : v;
    }

    private static long getLong(String key, long def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        if (v.isEmpty()) return def;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }
}
