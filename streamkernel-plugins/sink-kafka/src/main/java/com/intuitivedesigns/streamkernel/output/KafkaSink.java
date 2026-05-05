/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.BatchOutputSink;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.PipelineProvenance;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import com.intuitivedesigns.streamkernel.security.KafkaClientSecurity;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class KafkaSink implements BatchOutputSink<Object> {

    // Stable metric names
    private static final String M_OK = "streamkernel.kafka.sink.sent.ok.total";
    private static final String M_FAIL = "streamkernel.kafka.sink.sent.fail.total";
    private static final String M_SEND_MS = "streamkernel.kafka.sink.send.ms";
    private static final String M_INFLIGHT = "streamkernel.kafka.sink.inflight";
    private static final String M_INFLIGHT_TIMEOUT = "streamkernel.kafka.sink.inflight.acquire.timeout.total";
    // Kafka producer gauges (sampled)
    private static final String M_BUFFER_AVAILABLE_BYTES = "streamkernel.kafka.sink.buffer.available.bytes";
    private static final String M_BUFFER_TOTAL_BYTES = "streamkernel.kafka.sink.buffer.total.bytes";
    private static final String M_RECORD_QUEUE_TIME_AVG_MS = "streamkernel.kafka.sink.record.queue.time.avg.ms";
    private static final String M_REQUEST_LATENCY_AVG_MS = "streamkernel.kafka.sink.request.latency.avg.ms";
    private static final String M_THROTTLE_TIME_AVG_MS = "streamkernel.kafka.sink.throttle.time.avg.ms";
    private static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(30);
    private static final long CLOSE_DRAIN_TIMEOUT_MS = 30_000L;
    private static final long CLOSE_DRAIN_SLEEP_MS = 10L;
    // ---- Counters REQUIRED by KafkaApp ----
    private final AtomicLong sentOkTotal = new AtomicLong();
    private final AtomicLong sentFailTotal = new AtomicLong();
    private final AtomicLong inFlightTotal = new AtomicLong();
    private final Producer<String, byte[]> producer;
    private final String topic;
    private final boolean sync;
    private final boolean transactional;
    // Inflight throttling (enterprise-safe; avoids hot spin)
    private final Semaphore inflightPermits;
    private final long inflightLimit;
    private final long inflightAcquireTimeoutMs;
    // Metrics
    private final MetricsRuntime metrics;
    private final boolean metricsEnabled;
    // Bind guard (per sink instance, not JVM-wide)
    private final AtomicBoolean bound = new AtomicBoolean(false);
    private final Object transactionLock = new Object();

    // Optional: reduce gauge spam (set 0 to disable sampling)
    private final long inflightGaugeEveryN;
    private final AtomicLong inflightGaugeCounter = new AtomicLong();

    private KafkaSink(Producer<String, byte[]> producer,
                      String topic,
                      boolean sync,
                      boolean transactional,
                      long inflightLimit,
                      long inflightAcquireTimeoutMs,
                      long inflightGaugeEveryN,
                      MetricsRuntime metrics) {

        this.producer = Objects.requireNonNull(producer, "producer");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.sync = sync;
        this.transactional = transactional;

        this.inflightLimit = inflightLimit;
        this.inflightAcquireTimeoutMs = inflightAcquireTimeoutMs;
        this.inflightPermits = new Semaphore((int) inflightLimit, false);

        this.inflightGaugeEveryN = inflightGaugeEveryN;

        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.metricsEnabled = metrics.enabled();
    }

    // ------------------------------------------------------------------
    // Factory
    // ------------------------------------------------------------------

    public static KafkaSink fromConfig(PipelineConfig cfg, MetricsRuntime metrics) {
        Objects.requireNonNull(cfg, "cfg");
        Objects.requireNonNull(metrics, "metrics"); // keep strict to avoid silent no-metrics runs

        final String topic = kafkaTopic(cfg);
        final Properties props = buildProducerProperties(cfg, topic);

        final boolean sync = cfg.getBoolean("sink.kafka.sync", false);

        final long inflightLimit = clampLong(
                cfg.getLong("streamkernel.sink.inflight.max", 200_000L),
                1L,
                10_000_000L
        );

        final long inflightAcquireTimeoutMs = clampLong(
                cfg.getLong("streamkernel.sink.inflight.acquire.timeout.ms", 30_000L),
                1L,
                300_000L
        );

        // Optional: only update inflight gauge every N events (0 disables gauge updates)
        final long inflightGaugeEveryN = clampLong(
                cfg.getLong("streamkernel.sink.inflight.gauge.everyN", 10_000L),
                0L,
                10_000_000L
        );

        final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        final boolean transactional = hasTransactionalId(props);
        if (transactional) {
            producer.initTransactions();
        }

        return new KafkaSink(
                producer,
                topic,
                sync,
                transactional,
                inflightLimit,
                inflightAcquireTimeoutMs,
                inflightGaugeEveryN,
                metrics
        );
    }

    static Properties buildProducerProperties(PipelineConfig cfg) {
        Objects.requireNonNull(cfg, "cfg");
        return buildProducerProperties(cfg, kafkaTopic(cfg));
    }

    private static Properties buildProducerProperties(PipelineConfig cfg, String topic) {
        final String bootstrap = firstNonBlank(
                cfg.getString("sink.kafka.bootstrap.servers", null),
                cfg.getString("kafka.bootstrap.servers", null)
        );

        if (isBlank(topic)) throw new IllegalArgumentException("Kafka topic missing (sink.kafka.topic)");
        if (isBlank(bootstrap))
            throw new IllegalArgumentException("Kafka bootstrap.servers missing (sink.kafka.bootstrap.servers)");

        final Properties props = new Properties();

        // Required
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Good practice: stable client.id for readable producer metrics
        final String clientId = firstNonBlank(
                cfg.getString("sink.kafka.client.id", null),
                cfg.getString("kafka.producer.client.id", null),
                "streamkernel-sink-" + UUID.randomUUID()
        );
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        // ---- Producer tuning (legacy first, sink.kafka.* preferred)
        map(cfg, props, "kafka.producer.acks", ProducerConfig.ACKS_CONFIG);
        map(cfg, props, "kafka.producer.batch.size", ProducerConfig.BATCH_SIZE_CONFIG);
        map(cfg, props, "kafka.producer.linger.ms", ProducerConfig.LINGER_MS_CONFIG);
        map(cfg, props, "kafka.producer.buffer.memory", ProducerConfig.BUFFER_MEMORY_CONFIG);
        map(cfg, props, "kafka.producer.retries", ProducerConfig.RETRIES_CONFIG);
        map(cfg, props, "kafka.producer.compression", ProducerConfig.COMPRESSION_TYPE_CONFIG);
        map(cfg, props, "kafka.producer.idempotence", ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);

        map(cfg, props, "sink.kafka.acks", ProducerConfig.ACKS_CONFIG);
        map(cfg, props, "sink.kafka.batch.size", ProducerConfig.BATCH_SIZE_CONFIG);
        map(cfg, props, "sink.kafka.linger.ms", ProducerConfig.LINGER_MS_CONFIG);
        map(cfg, props, "sink.kafka.buffer.memory", ProducerConfig.BUFFER_MEMORY_CONFIG);
        map(cfg, props, "sink.kafka.retries", ProducerConfig.RETRIES_CONFIG);
        map(cfg, props, "sink.kafka.compression.type", ProducerConfig.COMPRESSION_TYPE_CONFIG);
        map(cfg, props, "sink.kafka.enable.idempotence", ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        map(cfg, props, "kafka.producer.transactional.id", ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        map(cfg, props, "sink.kafka.transactional.id", ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        map(cfg, props, "sink.kafka.max.in.flight.requests.per.connection", ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        map(cfg, props, "sink.kafka.delivery.timeout.ms", ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        map(cfg, props, "sink.kafka.request.timeout.ms", ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        map(cfg, props, "sink.kafka.max.request.size", ProducerConfig.MAX_REQUEST_SIZE_CONFIG);

        KafkaClientSecurity.apply(cfg, props, "sink.kafka.");

        return props;
    }

    // ------------------------------------------------------------------
    // OutputSink
    // ------------------------------------------------------------------

    private static byte[] encode(Object o) {
        if (o instanceof byte[] b) return b;
        if (o instanceof WireEvent w) return w.bytes();
        if (o instanceof String s) return s.getBytes(StandardCharsets.UTF_8);
        if (o instanceof CharSequence cs) return cs.toString().getBytes(StandardCharsets.UTF_8);
        return String.valueOf(o).getBytes(StandardCharsets.UTF_8);
    }

    private static String kafkaTopic(PipelineConfig cfg) {
        return firstNonBlank(
                cfg.getString("sink.kafka.topic", null),
                cfg.getString("kafka.output.topic", null),
                cfg.getString("sink.topic", null)
        );
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String firstNonBlank(String... v) {
        if (v == null) return null;
        for (String s : v) if (!isBlank(s)) return s.trim();
        return null;
    }

    private static void map(PipelineConfig cfg, Properties p, String from, String to) {
        final String v = cfg.getString(from, null);
        if (v != null) p.put(to, v);
    }

    private static long clampLong(long v, long min, long max) {
        return Math.max(min, Math.min(max, v));
    }

    @Override
    public void write(PipelinePayload<Object> payload) throws Exception {
        if (writableData(payload) == null) return;
        if (transactional) {
            // A single transactional write is a one-record transaction; callers should prefer writeBatch.
            writeBatch(List.of(payload));
            return;
        }

        writeSingle(payload);
    }

    @Override
    public void writeBatch(List<PipelinePayload<Object>> batch) throws Exception {
        if (batch == null || batch.isEmpty()) return;
        if (!transactional) {
            BatchOutputSink.super.writeBatch(batch);
            return;
        }

        final int recordCount = writablePayloadCount(batch);
        if (recordCount == 0) return;

        if (!acquireInflightPermits(recordCount)) {
            sentFailTotal.addAndGet(recordCount);
            if (metricsEnabled) {
                metrics.counter(M_INFLIGHT_TIMEOUT, recordCount);
            }
            throw new IllegalStateException(
                    "KafkaSink inflight limit reached (" + inflightLimit + ") for " +
                            inflightAcquireTimeoutMs + "ms while reserving " + recordCount +
                            " record(s); failing fast to avoid deadlock."
            );
        }

        final long startNs = sendTimerStartNs();
        int sentRecords = 0;
        try {
            bindProducerMetricsIfNeeded();
            synchronized (transactionLock) {
                boolean transactionStarted = false;
                try {
                    producer.beginTransaction();
                    transactionStarted = true;
                    for (int i = 0, m = batch.size(); i < m; i++) {
                        final PipelinePayload<Object> payload = batch.get(i);
                        final Object data = writableData(payload);
                        if (data == null) continue;

                        final ProducerRecord<String, byte[]> record = buildRecord(payload, data);
                        producer.send(record);
                        sentRecords++;
                    }
                    producer.commitTransaction();
                    transactionStarted = false;
                } catch (Exception e) {
                    if (transactionStarted) {
                        abortTransactionQuietly(e);
                    }
                    throw e;
                }
            }

            recordSendSuccess(sentRecords);
        } catch (Exception e) {
            recordSendFailure(recordCount);
            throw e;
        } finally {
            onBatchComplete(startNs, recordCount);
        }
    }

    private void writeSingle(PipelinePayload<Object> payload) throws Exception {
        final Object data = writableData(payload);
        if (data == null) return;

        if (!acquireInflightPermits(1)) {
            sentFailTotal.incrementAndGet();
            if (metricsEnabled) metrics.counter(M_INFLIGHT_TIMEOUT, 1.0);
            throw new IllegalStateException(
                    "KafkaSink inflight limit reached (" + inflightLimit + ") for " +
                            inflightAcquireTimeoutMs + "ms; failing fast to avoid deadlock."
            );
        }

        final long startNs = sendTimerStartNs();
        boolean callbackRegistered = false;
        boolean completionHandled = false;
        boolean failureRecorded = false;
        try {
            bindProducerMetricsIfNeeded();

            final ProducerRecord<String, byte[]> record = buildRecord(payload, data);

            if (sync) {
                try {
                    producer.send(record).get();
                    recordSendSuccess(1);
                } catch (Exception e) {
                    recordSendFailure(1);
                    failureRecorded = true;
                    throw e;
                } finally {
                    onSendComplete(startNs);
                    completionHandled = true;
                }
                return;
            }

            try {
                producer.send(record, (md, ex) -> {
                    try {
                        if (ex == null) {
                            recordSendSuccess(1);
                        } else {
                            recordSendFailure(1);
                        }
                    } finally {
                        onSendComplete(startNs);
                    }
                });
                callbackRegistered = true;
            } catch (Exception e) {
                if (!failureRecorded) {
                    recordSendFailure(1);
                    failureRecorded = true;
                }
                throw e;
            }
        } catch (Exception e) {
            if (!callbackRegistered && !completionHandled) {
                if (!failureRecorded) {
                    recordSendFailure(1);
                }
                onSendComplete(startNs);
            }
            throw e;
        }
    }

    private void bindProducerMetricsIfNeeded() {
        if (metricsEnabled && bound.compareAndSet(false, true)) {
            try {
                metrics.bind(producer);
            } catch (RuntimeException | Error e) {
                bound.set(false);
                throw e;
            }
        }
    }

    private boolean acquireInflightPermits(int permits) throws InterruptedException {
        if (!inflightPermits.tryAcquire(permits, inflightAcquireTimeoutMs, TimeUnit.MILLISECONDS)) {
            return false;
        }
        inFlightTotal.addAndGet(permits);
        maybeGaugeInflight();
        return true;
    }

    private void onSendComplete(long startNs) {
        try {
            recordSendTimer(startNs);
        } finally {
            inFlightTotal.decrementAndGet();
            inflightPermits.release();
            maybeGaugeInflight();
        }
    }

    private void onBatchComplete(long startNs, int permits) {
        try {
            recordSendTimer(startNs);
        } finally {
            inFlightTotal.addAndGet(-permits);
            inflightPermits.release(permits);
            maybeGaugeInflight();
        }
    }

    private static Object writableData(PipelinePayload<Object> payload) {
        return payload == null ? null : payload.data();
    }

    private static int writablePayloadCount(List<PipelinePayload<Object>> batch) {
        int count = 0;
        for (int i = 0, m = batch.size(); i < m; i++) {
            if (writableData(batch.get(i)) != null) count++;
        }
        return count;
    }

    private ProducerRecord<String, byte[]> buildRecord(PipelinePayload<Object> payload, Object data) {
        final ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, encode(data));
        applyHeaders(record, payload, data);
        return record;
    }

    private static void applyHeaders(ProducerRecord<String, byte[]> record,
                                     PipelinePayload<Object> payload,
                                     Object data) {
        final LinkedHashMap<String, String> headers = new LinkedHashMap<>();
        if (data instanceof WireEvent wireEvent && wireEvent.headers() != null) {
            headers.putAll(wireEvent.headers());
        }
        headers.putAll(PipelineProvenance.extractProvenanceHeaders(
                payload == null ? null : payload.metadata()
        ));

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            record.headers().remove(entry.getKey());
            record.headers().add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
    }

    private long sendTimerStartNs() {
        return metricsEnabled ? System.nanoTime() : -1L;
    }

    private void recordSendTimer(long startNs) {
        if (metricsEnabled && startNs >= 0L) {
            final long durMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            try {
                metrics.timer(M_SEND_MS, durMs);
            } catch (Throwable ignored) {
                // Metrics are best-effort and must not affect permit accounting.
            }
        }
    }

    private void recordSendSuccess(long records) {
        if (records <= 0) return;
        sentOkTotal.addAndGet(records);
        if (metricsEnabled) {
            try {
                metrics.counter(M_OK, records);
            } catch (Throwable ignored) {
                // Metrics are best-effort and must not affect send accounting.
            }
        }
    }

    private void recordSendFailure(long records) {
        if (records <= 0) return;
        sentFailTotal.addAndGet(records);
        if (metricsEnabled) {
            try {
                metrics.counter(M_FAIL, records);
            } catch (Throwable ignored) {
                // Metrics are best-effort and must not affect send accounting.
            }
        }
    }

    // ------------------------------------------------------------------
    // REQUIRED by KafkaApp (do NOT remove)
    // ------------------------------------------------------------------

    private void maybeGaugeInflight() {
        if (!metricsEnabled) return;
        if (inflightGaugeEveryN <= 0) return;

        long c = inflightGaugeCounter.incrementAndGet();
        if ((c % inflightGaugeEveryN) == 0) {
            try {
                metrics.gauge(M_INFLIGHT, inFlightTotal.get());
                sampleProducerMetrics();
            } catch (Throwable ignored) {
                // Metrics are best-effort and must not affect sink correctness.
            }
        }
    }

    private void sampleProducerMetrics() {
        if (!metricsEnabled) return;
        try {
            // Producer metrics are exposed by Kafka client; we publish a tiny, stable subset as gauges.
            // Names correspond to KafkaMetric.metricName().name() values.
            final var ms = producer.metrics();
            if (ms == null || ms.isEmpty()) return;

            for (var e : ms.entrySet()) {
                final var mn = e.getKey();
                if (mn == null) continue;
                final var m = e.getValue();
                final Object v = (m == null) ? null : m.metricValue();
                if (!(v instanceof Number num)) continue;

                switch (mn.name()) {
                    case "buffer-available-bytes" -> metrics.gauge(M_BUFFER_AVAILABLE_BYTES, num.doubleValue());
                    case "buffer-total-bytes" -> metrics.gauge(M_BUFFER_TOTAL_BYTES, num.doubleValue());
                    case "record-queue-time-avg" -> metrics.gauge(M_RECORD_QUEUE_TIME_AVG_MS, num.doubleValue());
                    case "request-latency-avg" -> metrics.gauge(M_REQUEST_LATENCY_AVG_MS, num.doubleValue());
                    case "throttle-time-avg" -> metrics.gauge(M_THROTTLE_TIME_AVG_MS, num.doubleValue());
                    default -> {
                    }
                }
            }
        } catch (Throwable ignored) {
            // best-effort only
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    @Override
    public void flush() {
        synchronized (transactionLock) {
            producer.flush();
        }
    }

    @Override
    public void close() {
        try {
            waitForInflightToDrain();
            flush();
        } finally {
            producer.close(PRODUCER_CLOSE_TIMEOUT);
        }
    }

    private void abortTransactionQuietly(Exception original) {
        try {
            producer.abortTransaction();
        } catch (Exception abortFailure) {
            original.addSuppressed(abortFailure);
        }
    }

    private boolean waitForInflightToDrain() {
        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CLOSE_DRAIN_TIMEOUT_MS);
        while (inFlightTotal.get() > 0) {
            if (System.nanoTime() >= deadlineNs) {
                maybeGaugeInflight();
                return false;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(CLOSE_DRAIN_SLEEP_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    private static boolean hasTransactionalId(Properties props) {
        final String value = props.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        return value != null && !value.trim().isEmpty();
    }

    public long sentOkTotal() {
        return sentOkTotal.get();
    }

    public long sentFailTotal() {
        return sentFailTotal.get();
    }

    public long inFlightTotal() {
        return inFlightTotal.get();
    }
}
