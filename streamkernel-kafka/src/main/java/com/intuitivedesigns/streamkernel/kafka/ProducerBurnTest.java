/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.kafka;

import com.intuitivedesigns.streamkernel.bench.SyntheticSource;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

/**
 * NETWORK burn test: pushes synthetic payloads to Kafka as fast as possible.
 * This measures broker + network + producer stack, not StreamKernel orchestration.
 */
public final class ProducerBurnTest {

    private ProducerBurnTest() {}

    public static void main(String[] args) {
        final String bootstrap = System.getProperty("bench.bootstrap", "localhost:9092").trim();
        final String topic = System.getProperty("bench.topic", "bench-topic").trim();
        final int payloadBytes = getInt("bench.payload.bytes", 1024);
        final boolean highEntropy = Boolean.parseBoolean(System.getProperty("bench.high.entropy", "false").trim());
        final int batchSize = getInt("bench.batch", 2000);
        final long reportMs = getLong("bench.report.ms", 3000);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Throughput tuning defaults (override with -D if desired)
        props.put(ProducerConfig.ACKS_CONFIG, System.getProperty("bench.acks", "1").trim());
        props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(getInt("bench.linger.ms", 5)));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(getInt("bench.kafka.batch.bytes", 131072)));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(getLong("bench.kafka.buffer.memory", 268435456L)));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, System.getProperty("bench.compression", "lz4").trim());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(getInt("bench.max.in.flight", 5)));

        System.out.println("Starting ProducerBurnTest");
        System.out.printf("CONFIG: bootstrap=%s topic=%s payloadBytes=%d entropy=%s batch=%d reportMs=%d%n",
                bootstrap, topic, payloadBytes, highEntropy ? "HIGH" : "LOW", batchSize, reportMs);

        final SyntheticSource source = new SyntheticSource(payloadBytes, highEntropy);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            final LongAdder counter = new LongAdder();
            long windowStartMs = System.currentTimeMillis();

            while (true) {
                final List<PipelinePayload<String>> batch = source.fetchBatch(batchSize);
                for (PipelinePayload<String> p : batch) {
                    producer.send(new ProducerRecord<>(topic, p.data()));
                }
                counter.add(batch.size());

                final long nowMs = System.currentTimeMillis();
                if (nowMs - windowStartMs >= reportMs) {
                    final double seconds = (nowMs - windowStartMs) / 1000.0;
                    final long total = counter.sum();
                    final double eps = seconds <= 0 ? 0.0 : total / seconds;

                    System.out.printf("NETWORK SPEED: %,.0f events/sec (window=%dms)%n", eps, (nowMs - windowStartMs));

                    counter.reset();
                    windowStartMs = nowMs;
                }
            }
        }
    }

    private static int getInt(String key, int def) {
        final String v = System.getProperty(key);
        if (v == null) return def;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
    }

    private static long getLong(String key, long def) {
        final String v = System.getProperty(key);
        if (v == null) return def;
        try { return Long.parseLong(v.trim()); } catch (Exception e) { return def; }
    }
}
