/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.bench;

import com.intuitivedesigns.streamkernel.avro.CustomerEvent;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Synthetic AVRO CustomerEvent source backed by immutable ring-baked batches.
 */
public final class SyntheticAvroSource implements SourceConnector<CustomerEvent> {

    private static final Logger log = LoggerFactory.getLogger(SyntheticAvroSource.class);

    private static final String KEY_RING_BATCHES = "source.synthetic.ring.batches";
    private static final String KEY_RING_BATCH_SIZE = "source.synthetic.ring.batch.size";
    private static final String KEY_ID_PREFIX = "source.synthetic.id.prefix";
    private static final String KEY_SEQ_START = "source.synthetic.seq.start";
    private static final String KEY_FIXED_BOOT_TIME = "source.synthetic.fixed.boot.time";
    private static final String KEY_DICT_SIZE = "source.synthetic.dictionary.size";

    private static final int DEFAULT_RING_BATCHES = 100;
    private static final int DEFAULT_RING_BATCH_SIZE = 4000;
    private static final String DEFAULT_ID_PREFIX = "c-";
    private static final long DEFAULT_SEQ_START = 0L;

    private static final String[] DESCRIPTIONS = {
            "My screen is flickering red and failing",
            "I love this product, it works perfectly",
            "System crash when loading data",
            "Best purchase ever, highly recommended"
    };

    private static final String TIER_PLATINUM = "PLATINUM";
    private static final String TIER_STANDARD = "STANDARD";

    private final int ringBatches;
    private final int preBakedBatchSize;
    private final String idPrefix;
    private final Instant fixedBootTime; // nullable
    private final Map<String, String> emptyMeta;

    private final String[] dict;

    private final List<List<PipelinePayload<CustomerEvent>>> ring;
    private final AtomicInteger ringIdx = new AtomicInteger(0);

    public SyntheticAvroSource(PipelineConfig config, MetricsRuntime metrics) {
        this.ringBatches = clampInt(parseInt(config.getString(KEY_RING_BATCHES, Integer.toString(DEFAULT_RING_BATCHES))), 1, Integer.MAX_VALUE);
        this.preBakedBatchSize = clampInt(parseInt(config.getString(KEY_RING_BATCH_SIZE, Integer.toString(DEFAULT_RING_BATCH_SIZE))), 1, Integer.MAX_VALUE);

        this.idPrefix = config.getString(KEY_ID_PREFIX, DEFAULT_ID_PREFIX);
        final long seqStart = parseLong(config.getString(KEY_SEQ_START, Long.toString(DEFAULT_SEQ_START)));

        final boolean fixedBoot = Boolean.parseBoolean(config.getString(KEY_FIXED_BOOT_TIME, "true"));
        this.fixedBootTime = fixedBoot ? Instant.now() : null;

        this.emptyMeta = Map.of();

        final int dictSize = clampInt(parseInt(config.getString(KEY_DICT_SIZE, Integer.toString(DESCRIPTIONS.length))), 1, DESCRIPTIONS.length);
        this.dict = new String[dictSize];
        System.arraycopy(DESCRIPTIONS, 0, dict, 0, dictSize);

        this.ring = preBakeRing(seqStart);

        log.info("SyntheticAvroSource ready: ringBatches={} preBakedBatchSize={} dictSize={}", ringBatches, preBakedBatchSize, dictSize);
    }

    @Override
    public void connect() { }

    @Override
    public void disconnect() { }

    @Override
    public PipelinePayload<CustomerEvent> fetch() {
        return ring.get(nextRingIndex()).get(0);
    }

    @Override
    public List<PipelinePayload<CustomerEvent>> fetchBatch(int maxBatchSize) {
        if (maxBatchSize <= 0) return Collections.emptyList();

        final List<PipelinePayload<CustomerEvent>> preBaked = ring.get(nextRingIndex());
        if (maxBatchSize >= preBakedBatchSize) return preBaked;
        return preBaked.subList(0, maxBatchSize);
    }

    private List<List<PipelinePayload<CustomerEvent>>> preBakeRing(long seqStart) {
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();
        final Instant boot = (fixedBootTime != null) ? fixedBootTime : Instant.now();

        final ArrayList<List<PipelinePayload<CustomerEvent>>> tmpRing = new ArrayList<>(ringBatches);

        long seq = seqStart;
        for (int i = 0; i < ringBatches; i++) {
            final ArrayList<PipelinePayload<CustomerEvent>> batch = new ArrayList<>(preBakedBatchSize);

            for (int j = 0; j < preBakedBatchSize; j++) {
                seq++;
                final String id = idPrefix + seq;

                final String desc = dict[rnd.nextInt(dict.length)];
                final String tier = rnd.nextBoolean() ? TIER_PLATINUM : TIER_STANDARD;

                final CustomerEvent event = CustomerEvent.newBuilder()
                        .setCustomerId(id)
                        .setName(desc)
                        .setTier(tier)
                        .build();

                final Instant ts = (fixedBootTime != null) ? boot : Instant.now();
                batch.add(new PipelinePayload<>(id, event, ts, emptyMeta));
            }

            tmpRing.add(Collections.unmodifiableList(batch));
        }

        return Collections.unmodifiableList(tmpRing);
    }

    private int nextRingIndex() {
        return (ringIdx.getAndIncrement() & 0x7FFFFFFF) % ringBatches;
    }

    private static int parseInt(String s) {
        try { return Integer.parseInt((s == null ? "" : s).trim()); }
        catch (Exception e) { return 0; }
    }

    private static long parseLong(String s) {
        try { return Long.parseLong((s == null ? "" : s).trim()); }
        catch (Exception e) { return 0L; }
    }

    private static int clampInt(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }
}
