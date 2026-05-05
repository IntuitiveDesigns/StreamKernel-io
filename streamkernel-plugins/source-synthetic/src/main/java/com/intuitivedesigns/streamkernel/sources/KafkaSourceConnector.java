/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.sources;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.security.KafkaClientSecurity;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * KafkaSourceConnector
 * ===================
 * Kafka-backed {@link SourceConnector} implementation that emits {@link PipelinePayload} records with
 * String values (Kafka record value deserialized as UTF-8 string by {@link StringDeserializer}).
 *
 * Enterprise goals & design constraints
 * ------------------------------------
 * This connector is written to be acquisition/enterprise friendly:
 *
 * 1) Canonical configuration namespace
 *    - Primary keys are under {@code source.kafka.*} so configuration is self-describing and
 *      avoids collisions with sink or global Kafka settings.
 *
 * 2) Backward-compatible aliases
 *    - A small set of legacy keys (e.g., {@code kafka.bootstrap.servers}) are supported to reduce
 *      migration friction for early adopters and internal benchmarks.
 *
 * 3) No config mutation
 *    - The connector reads from {@link PipelineConfig} but never writes back or mutates it.
 *      This keeps runtime behavior deterministic and avoids "spooky action at a distance" in logs.
 *
 * 4) Deterministic, safe lifecycle
 *    - connect(): idempotent; safe to call multiple times.
 *    - disconnect(): idempotent; safe to call multiple times.
 *    - fetchBatch(): defensive checks; never throws due to metrics.
 *
 * 5) Auditable behavior over cleverness
 *    - Uses KafkaConsumer.poll() with explicit limits and stable defaults.
 *    - Uses small metadata headers to support downstream debugging and DLQ correlation.
 *
 * Offset semantics & delivery model
 * --------------------------------
 * - This connector intentionally does NOT commit offsets in the hot path.
 * - If {@code enable.auto.commit=true}, Kafka handles commits automatically.
 * - If {@code enable.auto.commit=false}, you may optionally commit during disconnect via
 *   {@code source.kafka.commit.on.disconnect=true} (best-effort).
 *
 * This is appropriate for benchmarking and for pipelines where idempotent sinks or at-least-once
 * delivery is acceptable. If you require exactly-once semantics, you’ll need a coordinated commit
 * strategy aligned with sink acknowledgment (e.g., transactional sinks or explicit commit after sink success).
 *
 * Payload ID strategy
 * -------------------
 * - If Kafka record key is non-blank, it becomes PipelinePayload.id().
 * - Otherwise, ID is derived from topic/partition/offset for stable uniqueness and traceability.
 *
 * Threading model
 * --------------
 * - KafkaConsumer is NOT thread-safe; this connector assumes fetch/fetchBatch are called serially
 *   from a single dispatcher thread (which aligns with StreamKernel’s dispatcher design).
 */
public final class KafkaSourceConnector implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceConnector.class);

    // ---------------------------------------------------------------------
    // Canonical keys (preferred)
    // ---------------------------------------------------------------------

    /** Kafka bootstrap servers list. */
    private static final String K_BOOTSTRAP = "source.kafka.bootstrap.servers";
    /** Topic to subscribe to (required). */
    private static final String K_TOPIC = "source.kafka.topic";
    /** Consumer group id. */
    private static final String K_GROUP_ID = "source.kafka.group.id";
    /** Client id prefix (random suffix added per connect for uniqueness). */
    private static final String K_CLIENT_ID = "source.kafka.client.id";

    /** Max records returned by a single poll call (upper bound). */
    private static final String K_MAX_POLL_RECORDS = "source.kafka.max.poll.records";
    /** Poll duration in milliseconds. */
    private static final String K_POLL_MS = "source.kafka.poll.ms";
    /** auto.offset.reset: earliest|latest|none */
    private static final String K_AUTO_OFFSET_RESET = "source.kafka.auto.offset.reset";

    /** fetch.min.bytes: increase to favor throughput over latency. */
    private static final String K_FETCH_MIN_BYTES = "source.kafka.fetch.min.bytes";
    /** fetch.max.wait.ms: broker wait time to accumulate fetch.min.bytes. */
    private static final String K_FETCH_MAX_WAIT_MS = "source.kafka.fetch.max.wait.ms";

    /** enable.auto.commit: Kafka will auto-commit offsets periodically. */
    private static final String K_ENABLE_AUTO_COMMIT = "source.kafka.enable.auto.commit";

    /**
     * commit.on.disconnect: if enabled and auto-commit is disabled, best-effort commitSync is attempted on shutdown.
     * Useful for demos/benchmarks where you want to avoid re-reading the last batch on restart.
     */
    private static final String K_COMMIT_ON_DISCONNECT = "source.kafka.commit.on.disconnect";

    // ---------------------------------------------------------------------
    // Backward-compatible aliases (legacy keys)
    // ---------------------------------------------------------------------

    private static final String A_BOOTSTRAP_1 = "kafka.bootstrap.servers";
    private static final String A_TOPIC_1 = "kafka.input.topic";
    private static final String A_TOPIC_2 = "source.kafka.topic.name";
    private static final String A_GROUP_1 = "kafka.consumer.group.id";
    private static final String A_GROUP_2 = "kafka.consumer.group";
    private static final String A_OFFSET_RESET_1 = "kafka.consumer.auto.offset.reset";
    private static final String A_MAX_POLL_1 = "kafka.consumer.max.poll.records";

    // ---------------------------------------------------------------------
    // Defaults (safe, benchmark-friendly)
    // ---------------------------------------------------------------------

    private static final String D_BOOTSTRAP = "localhost:9092";
    private static final String D_GROUP = "streamkernel-consumer";
    private static final String D_CLIENT = "streamkernel-consumer";
    private static final int D_MAX_POLL = 5000;
    private static final int D_POLL_MS = 25;
    private static final String D_OFFSET_RESET = "latest";
    private static final int D_FETCH_MIN_BYTES = 1;
    private static final int D_FETCH_MAX_WAIT_MS = 500;
    private static final boolean D_ENABLE_AUTO_COMMIT = false;
    private static final boolean D_COMMIT_ON_DISCONNECT = false;

    // ---------------------------------------------------------------------
    // Immutable config (resolved once)
    // ---------------------------------------------------------------------

    private final String topic;
    private final String bootstrap;
    private final String groupId;
    private final String clientId;
    private final int maxPollRecords;
    private final int pollMs;
    private final String autoOffsetReset;
    private final int fetchMinBytes;
    private final int fetchMaxWaitMs;
    private final boolean enableAutoCommit;
    private final boolean commitOnDisconnect;
    private final PipelineConfig config;

    // ---------------------------------------------------------------------
    // Runtime state
    // ---------------------------------------------------------------------

    private final MetricsRuntime metrics;

    /** connect() idempotency gate. */
    private final AtomicBoolean connected = new AtomicBoolean(false);

    /** disconnect() idempotency gate. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Kafka consumer instance; created on connect().
     *
     * Note: KafkaConsumer is not thread-safe. This connector assumes a single-threaded fetch loop.
     */
    private KafkaConsumer<String, String> consumer;

    // Lightweight counters to support quick diagnostics even without metrics backend.
    private final AtomicLong readTotal = new AtomicLong(0);
    private final AtomicLong errorTotal = new AtomicLong(0);

    private KafkaSourceConnector(
            PipelineConfig config,
            String topic,
            String bootstrap,
            String groupId,
            String clientId,
            int maxPollRecords,
            int pollMs,
            String autoOffsetReset,
            int fetchMinBytes,
            int fetchMaxWaitMs,
            boolean enableAutoCommit,
            boolean commitOnDisconnect,
            MetricsRuntime metrics
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.bootstrap = Objects.requireNonNull(bootstrap, "bootstrap");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.maxPollRecords = maxPollRecords;
        this.pollMs = pollMs;
        this.autoOffsetReset = Objects.requireNonNull(autoOffsetReset, "autoOffsetReset");
        this.fetchMinBytes = fetchMinBytes;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
        this.enableAutoCommit = enableAutoCommit;
        this.commitOnDisconnect = commitOnDisconnect;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    /**
     * Factory that resolves configuration with canonical keys and legacy aliases.
     *
     * Enterprise rationale:
     * - Centralizes all config parsing so runtime execution is stable and does not re-read config.
     * - Provides backward compatibility without mutating config.
     * - Performs fail-fast validation for required inputs (topic).
     */
    public static KafkaSourceConnector fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        // Topic is the only hard requirement.
        final String topic = requireNonBlank(firstNonBlank(
                config.getString(K_TOPIC, null),
                config.getString(A_TOPIC_1, null),
                config.getString(A_TOPIC_2, null)
        ), "Kafka source requires topic (source.kafka.topic)");

        // Bootstrap servers: prefer canonical, then aliases, then default.
        final String bootstrap = firstNonBlank(
                config.getString(K_BOOTSTRAP, null),
                config.getString(A_BOOTSTRAP_1, null),
                config.getString("kafka.broker", null),
                D_BOOTSTRAP
        );

        // Group ID: prefer canonical, then legacy, then default.
        final String groupId = firstNonBlank(
                config.getString(K_GROUP_ID, null),
                config.getString(A_GROUP_1, null),
                config.getString(A_GROUP_2, null),
                config.getString("kafka.consumer.group", null),
                D_GROUP
        );

        // Client ID is treated as a prefix; a random UUID suffix is added per connect().
        final String clientId = firstNonBlank(
                config.getString(K_CLIENT_ID, null),
                config.getString("kafka.client.id", null),
                D_CLIENT
        );

        // max.poll.records: accept either canonical or legacy key; clamp to avoid accidental pathological configs.
        final int maxPoll = clampInt(firstInt(
                config,
                K_MAX_POLL_RECORDS,
                A_MAX_POLL_1,
                D_MAX_POLL
        ), 1, 1_000_000);

        // poll.ms: low latency default, but bounded to avoid misconfiguration.
        final int pollMs = clampInt(config.getInt(K_POLL_MS, D_POLL_MS), 1, 60_000);

        // auto.offset.reset: normalized to allowed Kafka values; default is "latest" to avoid replay surprises.
        final String offsetReset = normalizeOffsetReset(firstNonBlank(
                config.getString(K_AUTO_OFFSET_RESET, null),
                config.getString(A_OFFSET_RESET_1, null),
                D_OFFSET_RESET
        ));

        // fetch tuning: keep defaults safe; allow raising for throughput.
        final int fetchMinBytes = clampInt(config.getInt(K_FETCH_MIN_BYTES, D_FETCH_MIN_BYTES), 1, 1 << 28);
        final int fetchMaxWaitMs = clampInt(config.getInt(K_FETCH_MAX_WAIT_MS, D_FETCH_MAX_WAIT_MS), 1, 60_000);

        // commit semantics: conservative by default.
        final boolean enableAutoCommit = config.getBoolean(K_ENABLE_AUTO_COMMIT, D_ENABLE_AUTO_COMMIT);
        final boolean commitOnDisconnect = config.getBoolean(K_COMMIT_ON_DISCONNECT, D_COMMIT_ON_DISCONNECT);

        return new KafkaSourceConnector(
                config,
                topic,
                bootstrap,
                groupId,
                clientId,
                maxPoll,
                pollMs,
                offsetReset,
                fetchMinBytes,
                fetchMaxWaitMs,
                enableAutoCommit,
                commitOnDisconnect,
                metrics
        );
    }

    /**
     * Connects and subscribes the consumer to the configured topic.
     *
     * Idempotent:
     * - If already connected, does nothing.
     *
     * Enterprise notes:
     * - Adds a random UUID suffix to client.id to prevent collisions across multiple JVMs.
     * - Uses StringDeserializer for key/value (value becomes PipelinePayload.data()).
     * - Keeps Kafka internal metrics sampling low to reduce scheduling variance during benchmarks.
     */
    @Override
    public void connect() {
        if (!connected.compareAndSet(false, true)) return;

        final Properties props = new Properties();

        // Required Kafka settings
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Ensure client.id uniqueness per process instance (important in benchmarks and when scaling horizontally).
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId + "-" + UUID.randomUUID());

        // Deserialize into Strings; value is treated as record payload by this connector.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Offset commit strategy
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(enableAutoCommit));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        // Fetch/poll tuning
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxPollRecords));
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, Integer.toString(fetchMinBytes));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.toString(fetchMaxWaitMs));

        // Reduce internal metrics overhead/variance (still safe defaults).
        // This helps benchmark reproducibility without disabling Kafka metrics entirely.
        props.putIfAbsent(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, "2");
        props.putIfAbsent(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "30000");
        KafkaClientSecurity.apply(config, props, "source.kafka.");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));

        log.info("KafkaSourceConnector connected. topic='{}' groupId='{}' bootstrap='{}' maxPollRecords={} autoCommit={} offsetReset={}",
                topic, groupId, bootstrap, maxPollRecords, enableAutoCommit, autoOffsetReset);
    }

    /**
     * Fetches a single record (convenience wrapper around fetchBatch(1)).
     *
     * @return one payload, or null if no records are available
     */
    @Override
    public PipelinePayload<String> fetch() {
        final List<PipelinePayload<String>> batch = fetchBatch(1);
        return batch.isEmpty() ? null : batch.get(0);
    }

    /**
     * Fetches up to maxBatchSize records from Kafka.
     *
     * Enterprise behavior:
     * - Returns empty list on "no data" or any recoverable error.
     * - Never throws due to metrics emission.
     * - Enforces a hard cap of maxPollRecords (Kafka poll cap) to preserve determinism.
     *
     * Implementation details:
     * - Uses consumer.poll(Duration.ofMillis(pollMs)).
     * - Captures an Instant timestamp once per batch to reduce per-record allocations.
     * - Includes minimal Kafka metadata in PipelinePayload.metadata() for observability:
     *     kafka.topic, kafka.partition, kafka.offset
     *
     * @param maxBatchSize requested max size; clamped to [0, maxPollRecords]
     * @return list of PipelinePayload records (possibly empty)
     */
    @Override
    public List<PipelinePayload<String>> fetchBatch(int maxBatchSize) {
        if (closed.get()) return Collections.emptyList();
        if (consumer == null) throw new IllegalStateException("KafkaSourceConnector not connected");

        final int n = clampInt(maxBatchSize, 0, maxPollRecords);
        if (n <= 0) return Collections.emptyList();

        try {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollMs));
            if (records == null || records.isEmpty()) return Collections.emptyList();

            final int limit = Math.min(n, records.count());
            final ArrayList<PipelinePayload<String>> out = new ArrayList<>(limit);

            // Capture one timestamp for the entire batch (reduces allocations and keeps batch timing consistent).
            final Instant now = Instant.now();

            int i = 0;
            for (ConsumerRecord<String, String> r : records) {
                if (i++ >= limit) break;

                // ID selection:
                // - Prefer Kafka key if present (supports idempotency semantics when upstream provides keys).
                // - Otherwise derive a stable ID from topic-partition-offset (unique and traceable).
                final String id = (r.key() != null && !r.key().isBlank())
                        ? r.key()
                        : (topic + "-" + r.partition() + "-" + r.offset());

                // Minimal metadata for debugging / DLQ correlation.
                // Keep these keys stable across versions; treat them as part of the integration contract.
                final Map<String, String> meta = Map.of(
                        "kafka.topic", r.topic(),
                        "kafka.partition", Integer.toString(r.partition()),
                        "kafka.offset", Long.toString(r.offset())
                );

                out.add(new PipelinePayload<>(id, r.value(), now, meta));
            }

            final int count = out.size();
            if (count > 0) {
                readTotal.addAndGet(count);
                incCounterSafe(metrics, "streamkernel.source.kafka.read.total", count);
            }

            return out;

        } catch (Throwable t) {
            // Errors should not crash the pipeline; orchestration layer may choose fail-fast behavior.
            errorTotal.incrementAndGet();
            incCounterSafe(metrics, "streamkernel.source.kafka.error.total", 1);
            log.warn("KafkaSourceConnector poll/fetch error", t);
            return Collections.emptyList();
        }
    }

    /**
     * Disconnects and closes the consumer.
     *
     * Idempotent:
     * - If already closed, does nothing.
     *
     * Shutdown semantics:
     * - Optionally commits offsets on shutdown if:
     *     commitOnDisconnect=true AND enableAutoCommit=false
     * - Always attempts to wakeup and close with a bounded timeout.
     *
     * Enterprise note:
     * - Offset commits on shutdown are best-effort and should not be relied upon as a correctness mechanism
     *   for exactly-once semantics. They are mainly helpful for demos and local benchmarks.
     */
    @Override
    public void disconnect() {
        if (!closed.compareAndSet(false, true)) return;

        final KafkaConsumer<String, String> c = this.consumer;
        this.consumer = null;

        if (c != null) {
            try {
                if (commitOnDisconnect && !enableAutoCommit) {
                    // Best-effort commit: do not allow shutdown to hang indefinitely.
                    try { c.commitSync(Duration.ofSeconds(5)); } catch (Throwable ignored) {}
                }
            } finally {
                // Ensure the poll loop can exit promptly if blocked.
                try { c.wakeup(); } catch (Throwable ignored) {}

                // Close with timeout to avoid shutdown deadlocks.
                try { c.close(Duration.ofSeconds(5)); } catch (Throwable ignored) {}
            }
        }

        log.info("KafkaSourceConnector disconnected. readTotal={} errorTotal={}", readTotal.get(), errorTotal.get());
    }

    // ---------------------------------------------------------------------
    // Helpers (kept private and dependency-free)
    // ---------------------------------------------------------------------

    /**
     * Emits a metrics counter increment if a metrics runtime is available.
     *
     * Enterprise posture:
     * - Metrics must never break the hot path.
     * - Uses reflection to remain compatible with multiple metrics runtime shapes without compile-time coupling.
     * - If no compatible method is found, does nothing.
     */
    private static void incCounterSafe(MetricsRuntime metrics, String name, long delta) {
        if (metrics == null) return;
        try {
            // 1) counter(String, long)
            try {
                var m = metrics.getClass().getMethod("counter", String.class, long.class);
                m.invoke(metrics, name, delta);
                return;
            } catch (NoSuchMethodException ignored) {}

            // 2) counter(String, double)
            try {
                var m = metrics.getClass().getMethod("counter", String.class, double.class);
                m.invoke(metrics, name, (double) delta);
                return;
            } catch (NoSuchMethodException ignored) {}

            // 3) counter(String) + counterInc(String, long)
            try {
                var m = metrics.getClass().getMethod("counter", String.class);
                m.invoke(metrics, name);
            } catch (NoSuchMethodException ignored) {}

            try {
                var m = metrics.getClass().getMethod("counterInc", String.class, long.class);
                m.invoke(metrics, name, delta);
            } catch (NoSuchMethodException ignored) {}

        } catch (Throwable ignored) {
            // Metrics must never impact ingestion.
        }
    }

    /**
     * Normalizes Kafka auto.offset.reset.
     * Kafka accepts earliest/latest/none; anything else falls back to default.
     */
    private static String normalizeOffsetReset(String v) {
        if (v == null) return D_OFFSET_RESET;
        final String x = v.trim().toLowerCase(Locale.ROOT);
        return switch (x) {
            case "earliest", "latest", "none" -> x;
            default -> D_OFFSET_RESET;
        };
    }

    /**
     * Attempts to parse an int from either canonical or alias key, falling back to def.
     * Uses getString() to tolerate configs that store numbers as strings.
     */
    private static int firstInt(PipelineConfig config, String k1, String k2, int def) {
        try {
            final String v1 = config.getString(k1, null);
            if (v1 != null && !v1.isBlank()) return Integer.parseInt(v1.trim());
        } catch (Throwable ignored) {}
        try {
            final String v2 = config.getString(k2, null);
            if (v2 != null && !v2.isBlank()) return Integer.parseInt(v2.trim());
        } catch (Throwable ignored) {}
        return def;
    }

    /** Returns the first non-blank string (trimmed), or null if none exist. */
    private static String firstNonBlank(String... vals) {
        for (String v : vals) {
            if (v != null && !v.trim().isEmpty()) return v.trim();
        }
        return null;
    }

    /** Fail-fast validation for required config values. */
    private static String requireNonBlank(String v, String msg) {
        if (v == null || v.trim().isEmpty()) throw new IllegalArgumentException(msg);
        return v.trim();
    }

    /** Clamps an integer to [min, max]. */
    private static int clampInt(int v, int min, int max) {
        if (v < min) return min;
        if (v > max) return max;
        return v;
    }
}
