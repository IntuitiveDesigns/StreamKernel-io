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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pulsar-backed {@link SourceConnector} for benchmark and proof-point use cases.
 *
 * Delivery semantics:
 * - Default behavior acknowledges records on fetch so the benchmark can drain a
 *   topic backlog deterministically without extra sink/source coordination.
 * - This is intentionally benchmark-friendly rather than exactly-once.
 */
public final class PulsarSourceConnector implements SourceConnector<String> {

    private static final Logger log = LoggerFactory.getLogger(PulsarSourceConnector.class);

    private static final String K_SERVICE_URL = "source.pulsar.service.url";
    private static final String K_TOPIC = "source.pulsar.topic";
    private static final String K_SUBSCRIPTION = "source.pulsar.subscription";
    private static final String K_BATCH_SIZE = "source.pulsar.batch.size";
    private static final String K_POLL_MS = "source.pulsar.poll.ms";
    private static final String K_SUBSCRIPTION_TYPE = "source.pulsar.subscription.type";
    private static final String K_INITIAL_POSITION = "source.pulsar.subscription.initial.position";
    private static final String K_ACK_ON_FETCH = "source.pulsar.acknowledge.on.fetch";
    private static final String K_ACK_TIMEOUT_MS = "source.pulsar.ack.timeout.ms";
    private static final String K_RECEIVER_QUEUE_SIZE = "source.pulsar.receiver.queue.size";
    private static final String K_CHARSET = "source.pulsar.charset";

    private static final String A_SERVICE_URL = "pulsar.service.url";
    private static final String A_TOPIC = "pulsar.topic";
    private static final String A_SUBSCRIPTION = "pulsar.subscription";
    private static final String A_BATCH_SIZE = "pulsar.batch.size";
    private static final String A_SUBSCRIPTION_TYPE = "pulsar.subscription.type";
    private static final String A_INITIAL_POSITION = "pulsar.subscription.initial.position";
    private static final String A_ACK_ON_FETCH = "pulsar.acknowledge.on.fetch";
    private static final String A_ACK_TIMEOUT_MS = "pulsar.ack.timeout.ms";
    private static final String A_RECEIVER_QUEUE_SIZE = "pulsar.receiver.queue.size";
    private static final String A_CHARSET = "pulsar.charset";

    private static final String D_SERVICE_URL = "pulsar://localhost:6650";
    private static final String D_SUBSCRIPTION = "streamkernel-pulsar";
    private static final int D_BATCH_SIZE = 1000;
    private static final int D_POLL_MS = 25;
    private static final SubscriptionType D_SUBSCRIPTION_TYPE = SubscriptionType.Exclusive;
    private static final SubscriptionInitialPosition D_INITIAL_POSITION = SubscriptionInitialPosition.Earliest;
    private static final boolean D_ACK_ON_FETCH = true;
    private static final int D_ACK_TIMEOUT_MS = 30_000;
    private static final int D_RECEIVER_QUEUE_SIZE = 1000;
    private static final Charset D_CHARSET = StandardCharsets.UTF_8;

    private final String serviceUrl;
    private final String topic;
    private final String subscription;
    private final int batchSize;
    private final int pollMs;
    private final SubscriptionType subscriptionType;
    private final SubscriptionInitialPosition initialPosition;
    private final boolean acknowledgeOnFetch;
    private final int ackTimeoutMs;
    private final int receiverQueueSize;
    private final Charset charset;
    private final int drainWaitMs;
    private final MetricsRuntime metrics;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong readTotal = new AtomicLong(0);
    private final AtomicLong errorTotal = new AtomicLong(0);
    private final Object lifecycleLock = new Object();

    private volatile PulsarClient client;
    private volatile Consumer<byte[]> consumer;

    private PulsarSourceConnector(
            String serviceUrl,
            String topic,
            String subscription,
            int batchSize,
            int pollMs,
            SubscriptionType subscriptionType,
            SubscriptionInitialPosition initialPosition,
            boolean acknowledgeOnFetch,
            int ackTimeoutMs,
            int receiverQueueSize,
            Charset charset,
            MetricsRuntime metrics
    ) {
        this.serviceUrl = Objects.requireNonNull(serviceUrl, "serviceUrl");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.subscription = Objects.requireNonNull(subscription, "subscription");
        this.batchSize = batchSize;
        this.pollMs = pollMs;
        this.subscriptionType = Objects.requireNonNull(subscriptionType, "subscriptionType");
        this.initialPosition = Objects.requireNonNull(initialPosition, "initialPosition");
        this.acknowledgeOnFetch = acknowledgeOnFetch;
        this.ackTimeoutMs = ackTimeoutMs;
        this.receiverQueueSize = receiverQueueSize;
        this.charset = Objects.requireNonNull(charset, "charset");
        this.drainWaitMs = Math.max(1, Math.min(5, pollMs));
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    public static PulsarSourceConnector fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String topic = requireNonBlank(firstNonBlank(
                config.getString(K_TOPIC, null),
                config.getString(A_TOPIC, null)
        ), "Pulsar source requires topic (source.pulsar.topic)");

        final String serviceUrl = firstNonBlank(
                config.getString(K_SERVICE_URL, null),
                config.getString(A_SERVICE_URL, null),
                D_SERVICE_URL
        );
        final String subscription = firstNonBlank(
                config.getString(K_SUBSCRIPTION, null),
                config.getString(A_SUBSCRIPTION, null),
                D_SUBSCRIPTION
        );

        final int batchSize = clampInt(
                firstInt(config, K_BATCH_SIZE, A_BATCH_SIZE, D_BATCH_SIZE),
                1,
                1_000_000
        );
        final int pollMs = clampInt(config.getInt(K_POLL_MS, D_POLL_MS), 1, 60_000);
        final SubscriptionType subscriptionType = resolveSubscriptionType(firstNonBlank(
                config.getString(K_SUBSCRIPTION_TYPE, null),
                config.getString(A_SUBSCRIPTION_TYPE, null),
                D_SUBSCRIPTION_TYPE.name()
        ));
        final SubscriptionInitialPosition initialPosition = resolveInitialPosition(firstNonBlank(
                config.getString(K_INITIAL_POSITION, null),
                config.getString(A_INITIAL_POSITION, null),
                D_INITIAL_POSITION.name()
        ));
        final boolean acknowledgeOnFetch = resolveBoolean(
                config,
                K_ACK_ON_FETCH,
                A_ACK_ON_FETCH,
                D_ACK_ON_FETCH
        );
        final int ackTimeoutMs = clampInt(
                firstInt(
                        config,
                        K_ACK_TIMEOUT_MS,
                        A_ACK_TIMEOUT_MS,
                        acknowledgeOnFetch ? 0 : D_ACK_TIMEOUT_MS
                ),
                0,
                3_600_000
        );
        final int receiverQueueSize = clampInt(
                firstInt(config, K_RECEIVER_QUEUE_SIZE, A_RECEIVER_QUEUE_SIZE, D_RECEIVER_QUEUE_SIZE),
                1,
                1_000_000
        );
        final Charset charset = parseCharset(firstNonBlank(
                config.getString(K_CHARSET, null),
                config.getString(A_CHARSET, null),
                D_CHARSET.name()
        ));

        return new PulsarSourceConnector(
                serviceUrl,
                topic,
                subscription,
                batchSize,
                pollMs,
                subscriptionType,
                initialPosition,
                acknowledgeOnFetch,
                ackTimeoutMs,
                receiverQueueSize,
                charset,
                metrics
        );
    }

    @Override
    public void connect() {
        synchronized (lifecycleLock) {
            if (consumer != null) {
                return;
            }
            if (!connected.compareAndSet(false, true)) {
                return;
            }
            closed.set(false);

            PulsarClient newClient = null;
            Consumer<byte[]> newConsumer = null;
            try {
                newClient = PulsarClient.builder()
                        .serviceUrl(serviceUrl)
                        .build();

                ConsumerBuilder<byte[]> consumerBuilder = newClient.newConsumer(Schema.BYTES)
                        .topic(topic)
                        .subscriptionName(subscription)
                        .subscriptionType(subscriptionType)
                        .subscriptionInitialPosition(initialPosition)
                        .receiverQueueSize(receiverQueueSize);

                if (!acknowledgeOnFetch && ackTimeoutMs > 0) {
                    consumerBuilder = consumerBuilder.ackTimeout(ackTimeoutMs, TimeUnit.MILLISECONDS);
                }

                newConsumer = consumerBuilder.subscribe();
                this.client = newClient;
                this.consumer = newConsumer;

                log.info(
                        "PulsarSourceConnector connected. topic='{}' subscription='{}' serviceUrl='{}' batchSize={} pollMs={} subscriptionType={} initialPosition={} acknowledgeOnFetch={} ackTimeoutMs={}",
                        topic,
                        subscription,
                        serviceUrl,
                        batchSize,
                        pollMs,
                        subscriptionType,
                        initialPosition,
                        acknowledgeOnFetch,
                        ackTimeoutMs
                );
            } catch (Exception e) {
                connected.set(false);
                closed.set(false);
                closeConsumerQuietly(newConsumer, "Failed closing Pulsar consumer after connect failure");
                closeClientQuietly(newClient, "Failed closing Pulsar client after connect failure");
                throw new RuntimeException("Failed connecting Pulsar source", e);
            }
        }
    }

    @Override
    public PipelinePayload<String> fetch() {
        final List<PipelinePayload<String>> batch = fetchBatch(1);
        return batch.isEmpty() ? null : batch.get(0);
    }

    @Override
    public List<PipelinePayload<String>> fetchBatch(int maxBatchSize) {
        if (closed.get()) {
            return Collections.emptyList();
        }

        final Consumer<byte[]> currentConsumer = this.consumer;
        if (currentConsumer == null) {
            throw new IllegalStateException("PulsarSourceConnector not connected");
        }

        final int limit = clampInt(maxBatchSize, 0, batchSize);
        if (limit <= 0) {
            return Collections.emptyList();
        }

        try {
            final Message<byte[]> first = currentConsumer.receive(pollMs, TimeUnit.MILLISECONDS);
            if (first == null) {
                return Collections.emptyList();
            }

            final ArrayList<PipelinePayload<String>> out = new ArrayList<>(limit);
            final ArrayList<MessageId> acknowledgements = acknowledgeOnFetch ? new ArrayList<>(limit) : null;

            out.add(toPayload(first));
            if (acknowledgements != null) {
                acknowledgements.add(first.getMessageId());
            }

            while (out.size() < limit) {
                final Message<byte[]> next = currentConsumer.receive(drainWaitMs, TimeUnit.MILLISECONDS);
                if (next == null) {
                    break;
                }
                out.add(toPayload(next));
                if (acknowledgements != null) {
                    acknowledgements.add(next.getMessageId());
                }
            }

            if (acknowledgements != null && !acknowledgements.isEmpty()) {
                acknowledgeBatch(currentConsumer, acknowledgements);
            }

            final int count = out.size();
            if (count > 0) {
                readTotal.addAndGet(count);
                incCounterSafe("streamkernel.source.pulsar.read.total", count);
            }

            return out;
        } catch (Exception e) {
            errorTotal.incrementAndGet();
            incCounterSafe("streamkernel.source.pulsar.error.total", 1);
            log.warn("PulsarSourceConnector receive/fetch error", e);
            return Collections.emptyList();
        }
    }

    @Override
    public void disconnect() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        final Consumer<byte[]> currentConsumer;
        final PulsarClient currentClient;
        synchronized (lifecycleLock) {
            currentConsumer = this.consumer;
            currentClient = this.client;
            this.consumer = null;
            this.client = null;
            connected.set(false);
        }

        closeConsumerQuietly(currentConsumer, "Failed closing Pulsar consumer");
        closeClientQuietly(currentClient, "Failed closing Pulsar client");

        log.info("PulsarSourceConnector disconnected. readTotal={} errorTotal={}", readTotal.get(), errorTotal.get());
    }

    private PipelinePayload<String> toPayload(Message<byte[]> message) {
        final Instant timestamp = (message.getPublishTime() > 0L)
                ? Instant.ofEpochMilli(message.getPublishTime())
                : Instant.now();

        final String messageId = String.valueOf(message.getMessageId());
        final String messageKey = message.hasKey() ? message.getKey() : null;
        final Map<String, String> metadata = buildMetadata(message, messageId, messageKey);

        final String id = (messageKey != null && !messageKey.isBlank())
                ? messageKey
                : (topic + "-" + messageId);

        final byte[] payload = message.getValue();
        final String value = (payload == null || payload.length == 0)
                ? ""
                : new String(payload, charset);

        return new PipelinePayload<>(id, value, timestamp, metadata);
    }

    private void acknowledgeBatch(Consumer<byte[]> consumer, List<MessageId> messageIds) throws Exception {
        if (messageIds.isEmpty()) {
            return;
        }

        consumer.acknowledge(messageIds);
    }

    private static boolean resolveBoolean(PipelineConfig config, String canonicalKey, String aliasKey, boolean def) {
        final String canonical = config.getString(canonicalKey, null);
        if (canonical != null && !canonical.isBlank()) {
            return Boolean.parseBoolean(canonical.trim());
        }
        final String alias = config.getString(aliasKey, null);
        if (alias != null && !alias.isBlank()) {
            return Boolean.parseBoolean(alias.trim());
        }
        return def;
    }

    private static SubscriptionType resolveSubscriptionType(String value) {
        if (value == null || value.isBlank()) {
            return D_SUBSCRIPTION_TYPE;
        }

        final String normalized = value.trim().replace('-', '_');
        for (SubscriptionType type : SubscriptionType.values()) {
            if (type.name().equalsIgnoreCase(normalized)) {
                return type;
            }
        }
        log.warn("Unknown Pulsar subscription type '{}'; defaulting to {}", value, D_SUBSCRIPTION_TYPE);
        return D_SUBSCRIPTION_TYPE;
    }

    private static SubscriptionInitialPosition resolveInitialPosition(String value) {
        if (value == null || value.isBlank()) {
            return D_INITIAL_POSITION;
        }

        final String normalized = value.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "LATEST" -> SubscriptionInitialPosition.Latest;
            case "EARLIEST" -> SubscriptionInitialPosition.Earliest;
            default -> {
                log.warn("Unknown Pulsar subscription initial position '{}'; defaulting to {}", value, D_INITIAL_POSITION);
                yield D_INITIAL_POSITION;
            }
        };
    }

    private static int firstInt(PipelineConfig config, String key1, String key2, int def) {
        try {
            final String value = config.getString(key1, null);
            if (value != null && !value.isBlank()) {
                return Integer.parseInt(value.trim());
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid integer for Pulsar config '{}'; defaulting to {}", key1, def);
        }
        try {
            final String value = config.getString(key2, null);
            if (value != null && !value.isBlank()) {
                return Integer.parseInt(value.trim());
            }
        } catch (NumberFormatException e) {
            log.warn("Invalid integer for Pulsar config '{}'; defaulting to {}", key2, def);
        }
        return def;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    private static String requireNonBlank(String value, String message) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return value.trim();
    }

    private static Charset parseCharset(String value) {
        if (value == null || value.isBlank()) {
            return D_CHARSET;
        }
        try {
            return Charset.forName(value.trim());
        } catch (Exception e) {
            log.warn("Unknown Pulsar charset '{}'; defaulting to {}", value, D_CHARSET.name());
            return D_CHARSET;
        }
    }

    private Map<String, String> buildMetadata(Message<byte[]> message, String messageId, String messageKey) {
        final Map<String, String> properties = message.getProperties();
        final int propertyCount = (properties == null) ? 0 : properties.size();
        final String publishTime = Long.toString(message.getPublishTime());
        final String topicName = message.getTopicName();

        if (propertyCount == 0) {
            if (messageKey != null) {
                return Map.of(
                        "pulsar.topic", topicName,
                        "pulsar.message.id", messageId,
                        "pulsar.publish.time", publishTime,
                        "pulsar.key", messageKey
                );
            }
            return Map.of(
                    "pulsar.topic", topicName,
                    "pulsar.message.id", messageId,
                    "pulsar.publish.time", publishTime
            );
        }

        final LinkedHashMap<String, String> metadata = new LinkedHashMap<>(mapCapacity(3 + propertyCount + (messageKey != null ? 1 : 0)));
        metadata.put("pulsar.topic", topicName);
        metadata.put("pulsar.message.id", messageId);
        metadata.put("pulsar.publish.time", publishTime);
        if (messageKey != null) {
            metadata.put("pulsar.key", messageKey);
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            metadata.put("pulsar.property." + entry.getKey(), entry.getValue());
        }
        return metadata;
    }

    private static int mapCapacity(int expectedEntries) {
        return Math.max(8, (int) Math.ceil(expectedEntries / 0.75d));
    }

    private void incCounterSafe(String name, long delta) {
        try {
            metrics.counter(name, delta);
        } catch (RuntimeException e) {
            log.debug("Ignoring metrics counter failure for '{}'", name, e);
        }
    }

    private void closeConsumerQuietly(Consumer<byte[]> targetConsumer, String message) {
        if (targetConsumer == null) {
            return;
        }
        try {
            targetConsumer.close();
        } catch (Exception e) {
            log.warn(message, e);
        }
    }

    private void closeClientQuietly(PulsarClient targetClient, String message) {
        if (targetClient == null) {
            return;
        }
        try {
            targetClient.close();
        } catch (Exception e) {
            log.warn(message, e);
        }
    }

    private static int clampInt(int value, int min, int max) {
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }
}
