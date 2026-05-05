/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.bench;

import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class SyntheticSource implements SourceConnector<String> {

    public enum TextProfile {
        ENTROPY,
        SUPPORT,
        DATABRICKS
    }

    private static final Logger log = LoggerFactory.getLogger(SyntheticSource.class);
    private static final int DEFAULT_BUFFER_COUNT = 262_144;
    private static final int MAX_BUFFER_CAPACITY = 1 << 30;
    private static final int DEFAULT_RECYCLED_LIST_CAP = 8_192;

    private static final String[] SUPPORT_PRODUCTS = {
            "billing dashboard", "search service", "checkout page", "warehouse sync", "mobile login flow", "claims API"
    };
    private static final String[] SUPPORT_ISSUES = {
            "slow responses during peak traffic", "duplicate records after retry", "timeouts on save",
            "missing updates in the nightly export", "intermittent authentication failures", "stale results after cache refresh"
    };
    private static final String[] SUPPORT_IMPACTS = {
            "orders are delayed", "agents cannot close cases", "customers are retrying the same request",
            "reporting is behind schedule", "inventory is out of sync", "service level targets are at risk"
    };
    private static final String[] SUPPORT_ACTIONS = {
            "collect recent traces", "replay the failed batch", "restart the worker pool",
            "verify the downstream queue depth", "compare the latest deployment diff", "capture tokenizer and model timings"
    };
    private static final String[] DATABRICKS_JOBS = {
            "bronze_ingest", "silver_enrichment", "gold_metrics_rollup", "feature_store_refresh", "customer360_backfill", "fraud_signal_join"
    };
    private static final String[] DATABRICKS_TABLES = {
            "delta.customer_events", "delta.support_tickets", "delta.payments_bronze",
            "delta.orders_silver", "delta.device_health", "delta.ml_features"
    };
    private static final String[] DATABRICKS_SYMPTOMS = {
            "late-arriving partitions", "skew on a shuffle stage", "checkpoint lag after autoscaling",
            "unexpected schema drift", "long-running merge commits", "dropped rows in a malformed batch"
    };
    private static final String[] DATABRICKS_CLUSTERS = {
            "etl-west-2", "lakehouse-prod-a", "sql-warehouse-07", "feature-lab-3", "streaming-core-1", "gold-pipeline-2"
    };
    private static final String[] DATABRICKS_ACTIONS = {
            "a fresh checkpoint review", "a tighter partition strategy", "a cluster resize before the next window",
            "a policy audit", "a Delta optimize pass", "a schema contract check"
    };

    private final PipelinePayload<String>[] ringBuffer;
    private final int mask;
    private final AtomicLong sequence = new AtomicLong(0);
    private final boolean highEntropy;
    private final boolean unsafeReuseBatch;
    private final TextProfile textProfile;
    private final ThreadLocal<ArrayList<PipelinePayload<String>>> recycledBatch;

    public SyntheticSource(int payloadSize, boolean highEntropy) {
        this(payloadSize, DEFAULT_BUFFER_COUNT, highEntropy, false, TextProfile.ENTROPY);
    }

    @SuppressWarnings("unchecked")
    public SyntheticSource(int payloadSize, int bufferCount, boolean highEntropy, boolean unsafeReuseBatch) {
        this(payloadSize, bufferCount, highEntropy, unsafeReuseBatch, TextProfile.ENTROPY);
    }

    @SuppressWarnings("unchecked")
    public SyntheticSource(int payloadSize, int bufferCount, boolean highEntropy, boolean unsafeReuseBatch, TextProfile textProfile) {
        if (payloadSize <= 0) throw new IllegalArgumentException("payloadSize must be positive");
        if (bufferCount <= 0) throw new IllegalArgumentException("bufferCount must be positive");
        if (bufferCount > MAX_BUFFER_CAPACITY) throw new IllegalArgumentException("bufferCount exceeds limit (1<<30)");

        int capacity = nextPowerOfTwoOrThrow(bufferCount);
        this.mask = capacity - 1;
        this.highEntropy = highEntropy;
        this.unsafeReuseBatch = unsafeReuseBatch;
        this.textProfile = (textProfile == null) ? TextProfile.ENTROPY : textProfile;
        this.ringBuffer = (PipelinePayload<String>[]) new PipelinePayload[capacity];
        this.recycledBatch = unsafeReuseBatch ? ThreadLocal.withInitial(() -> new ArrayList<>(DEFAULT_RECYCLED_LIST_CAP)) : null;

        log.info("Initializing SyntheticSource: capacity={} (requested={}) payloadChars={} textProfile={} entropyMode={} unsafeReuseBatch={}",
                capacity, bufferCount, payloadSize, this.textProfile, highEntropy ? "HIGH" : "LOW", unsafeReuseBatch);

        for (int i = 0; i < capacity; i++) {
            ringBuffer[i] = PipelinePayload.of(buildPayload(payloadSize, i));
        }

        log.info("SyntheticSource initialization complete.");
    }

    @Override
    public void connect() {}

    @Override
    public void disconnect() {}

    @Override
    public PipelinePayload<String> fetch() {
        long seq = sequence.getAndIncrement();
        return ringBuffer[(int) (seq & mask)];
    }

    @Override
    public List<PipelinePayload<String>> fetchBatch(int maxBatchSize) {
        if (maxBatchSize <= 0) return Collections.emptyList();
        final List<PipelinePayload<String>> batch;
        if (unsafeReuseBatch) {
            ArrayList<PipelinePayload<String>> reusable = recycledBatch.get();
            reusable.clear();
            reusable.ensureCapacity(maxBatchSize);
            batch = reusable;
        } else {
            batch = new ArrayList<>(maxBatchSize);
        }
        long start = sequence.getAndAdd(maxBatchSize);
        for (int i = 0; i < maxBatchSize; i++) {
            int index = (int) ((start + i) & mask);
            batch.add(ringBuffer[index]);
        }
        return batch;
    }

    private String buildPayload(int payloadSize, int slot) {
        return switch (textProfile) {
            case SUPPORT -> generateSupportSentence(payloadSize, slot);
            case DATABRICKS -> generateDatabricksSentence(payloadSize, slot);
            case ENTROPY -> highEntropy ? generateSplitMix64String(payloadSize, slot) : generateLowEntropyString(payloadSize, slot);
        };
    }

    private static int nextPowerOfTwoOrThrow(int value) {
        int highestOneBit = Integer.highestOneBit(value);
        if (value == highestOneBit) return value;
        int next = highestOneBit << 1;
        if (next <= 0 || next > MAX_BUFFER_CAPACITY) {
            throw new IllegalArgumentException("bufferCount too large to round to next power-of-two safely: " + value);
        }
        return next;
    }

    private static String generateLowEntropyString(int len, int slot) {
        char[] chars = new char[len];
        int pos = 0;
        if (len > 0) chars[pos++] = 'E';
        pos = writeHexIntUpper(slot, chars, pos);
        if (pos < len) chars[pos++] = ':';
        while (pos < len) chars[pos++] = 'X';
        return new String(chars);
    }

    private static String generateSupportSentence(int len, int slot) {
        final String text = "Case " + ticketId(slot) + ": customer reports " + pick(SUPPORT_ISSUES, slot, 0)
                + " in " + pick(SUPPORT_PRODUCTS, slot, 1) + ". Impact is " + pick(SUPPORT_IMPACTS, slot, 2)
                + ". Next action is " + pick(SUPPORT_ACTIONS, slot, 3) + ".";
        return fitToLength(text, len);
    }

    private static String generateDatabricksSentence(int len, int slot) {
        final String text = "Databricks job " + pick(DATABRICKS_JOBS, slot, 0) + " on " + pick(DATABRICKS_TABLES, slot, 1)
                + " reports " + pick(DATABRICKS_SYMPTOMS, slot, 2) + ". Cluster " + pick(DATABRICKS_CLUSTERS, slot, 3)
                + " needs " + pick(DATABRICKS_ACTIONS, slot, 4) + " before the next Delta load.";
        return fitToLength(text, len);
    }

    private static String ticketId(int slot) { return "SK-" + (10_000 + Math.floorMod(slot, 90_000)); }
    private static String pick(String[] values, int slot, int salt) { return values[Math.floorMod(slot + (salt * 17), values.length)]; }

    private static String fitToLength(String text, int len) {
        if (text.length() == len) return text;
        if (text.length() > len) return trimToLength(text, len);
        final StringBuilder builder = new StringBuilder(len);
        while (builder.length() < len) {
            if (builder.length() > 0) builder.append(' ');
            builder.append(text);
        }
        return trimToLength(builder.toString(), len);
    }

    private static String trimToLength(String text, int len) {
        if (text.length() <= len) return text;
        int cut = len;
        final int minWordBoundary = Math.max(24, len / 2);
        final int space = text.lastIndexOf(' ', len - 1);
        if (space >= minWordBoundary) cut = space;
        String trimmed = text.substring(0, cut).trim();
        if (trimmed.isEmpty()) trimmed = text.substring(0, len);
        if (trimmed.length() > len) trimmed = trimmed.substring(0, len);
        return trimmed;
    }

    private static int writeHexIntUpper(int value, char[] out, int pos) {
        int remaining = out.length - pos;
        if (remaining <= 0) return pos;
        boolean started = false;
        for (int shift = 28; shift >= 0 && remaining > 0; shift -= 4) {
            int nibble = (value >>> shift) & 0xF;
            if (!started) {
                if (nibble == 0 && shift != 0) continue;
                started = true;
            }
            out[pos++] = (char) (nibble < 10 ? ('0' + nibble) : ('A' + (nibble - 10)));
            remaining--;
        }
        if (!started && remaining > 0) out[pos++] = '0';
        return pos;
    }

    private static String generateSplitMix64String(int len, int slot) {
        char[] chars = new char[len];
        long state = 0x9E3779B97F4A7C15L ^ (long) slot;
        for (int i = 0; i < len; i++) {
            state += 0x9E3779B97F4A7C15L;
            long z = state;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            z = z ^ (z >>> 31);
            chars[i] = mapToBase64ish((int) (z & 63));
        }
        return new String(chars);
    }

    private static char mapToBase64ish(int v) {
        if (v < 26) return (char) ('A' + v);
        if (v < 52) return (char) ('a' + (v - 26));
        if (v < 62) return (char) ('0' + (v - 52));
        return (v == 62) ? '+' : '/';
    }
}
