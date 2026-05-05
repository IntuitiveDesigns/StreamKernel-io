/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

/*
 * FILE: streamkernel-bench/src/main/java/com/intuitivedesigns/streamkernel/bench/SyntheticSource.java
 *
 * Phase 3 (Preserved + High-Performance):
 * - Protobuf support (Generic + Generated) ✅
 * - JSON / AVRO / PROTOBUF supported via config ✅
 * - INLINE (byte payload) + SCHEMA modes ✅
 * - Power-of-two ring sizing ✅
 * - No functionality removed ✅
 *
 * Performance upgrades:
 * - Ring stores prebuilt PipelinePayload<String> (no per-event wrapper allocation)
 * - fetchBatch(int) returns a lightweight immutable List view over the ring
 *   (no per-record allocations, no per-record atomic)
 * - cursor is plain long (dispatcher is single-threaded)
 */
package com.intuitivedesigns.streamkernel.bench;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.SourceConnector;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.concurrent.ThreadLocalRandom;

public final class SyntheticSource implements SourceConnector<String> {

    // ---------------------------------------------------------------------
    // Config keys (preserved)
    // ---------------------------------------------------------------------
    private static final String KEY_MODE         = "source.synthetic.mode";           // INLINE | SCHEMA
    private static final String KEY_FORMAT       = "source.synthetic.format";         // JSON | AVRO | PROTOBUF
    private static final String KEY_AVRO_TYPE    = "source.synthetic.avro.type";      // GENERIC | SPECIFIC
    private static final String KEY_AVRO_CLASS   = "source.synthetic.avro.class";
    private static final String KEY_PROTO_CLASS  = "source.synthetic.proto.class";
    private static final String KEY_FIXTURE_PATH = "source.synthetic.fixture.path";
    private static final String KEY_SCHEMA_PATH  = "source.synthetic.schema.path";
    private static final String KEY_POOL_SIZE    = "source.synthetic.fixture.pool.size";
    private static final String KEY_ID_PREFIX    = "source.synthetic.id.prefix";
    private static final String KEY_SEQ_START    = "source.synthetic.seq.start";
    private static final String KEY_FIXED_TS     = "source.synthetic.fixed.timestamp";
    private static final String KEY_PAYLOAD_BYTES= "source.synthetic.payload.size";
    private static final String KEY_BUFFER_SIZE  = "source.synthetic.buffer.size";
    private static final String KEY_ENTROPY      = "source.synthetic.entropy";

    // ---------------------------------------------------------------------
    // High-performance ring: prebuilt payloads (no per-event wrapper alloc)
    // ---------------------------------------------------------------------
    private final PipelinePayload<String>[] ring;
    private final int ringMask;
    private long cursor; // plain long: dispatcher is single-threaded in your orchestrator

    // ---------------------------------------------------------------------
    // Immutable list view over the ring (no backing array/list alloc per batch)
    // ---------------------------------------------------------------------
    private static final class RingBatchList extends AbstractList<PipelinePayload<String>> implements RandomAccess {
        private final PipelinePayload<String>[] ring;
        private final int mask;
        private final long base;
        private final int size;

        RingBatchList(PipelinePayload<String>[] ring, int mask, long base, int size) {
            this.ring = ring;
            this.mask = mask;
            this.base = base;
            this.size = size;
        }

        @Override
        public PipelinePayload<String> get(int index) {
            if (index < 0 || index >= size) throw new IndexOutOfBoundsException("index=" + index + " size=" + size);
            int idx = (int) ((base + index) & mask);
            return ring[idx];
        }

        @Override
        public int size() {
            return size;
        }
    }

    // ---------------------------------------------------------------------
    // Constructors
    // ---------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public SyntheticSource(PipelineConfig cfg, MetricsRuntime metrics) {
        Objects.requireNonNull(cfg, "cfg");

        int ringSize = powerOfTwo(Math.max(1, parseInt(cfg.getString(KEY_BUFFER_SIZE, "524288"))));
        this.ring = (PipelinePayload<String>[]) new PipelinePayload[ringSize];
        this.ringMask = ringSize - 1;
        this.cursor = 0L;

        int poolSize = powerOfTwo(Math.max(1, parseInt(cfg.getString(KEY_POOL_SIZE, "2048"))));

        String mode = cfg.getString(KEY_MODE, "INLINE");
        String format = cfg.getString(KEY_FORMAT, "JSON");

        String avroType = cfg.getString(KEY_AVRO_TYPE, "GENERIC");
        String avroClass = cfg.getString(KEY_AVRO_CLASS, "");
        String protoClass = cfg.getString(KEY_PROTO_CLASS, "");

        String fixturePath = cfg.getString(KEY_FIXTURE_PATH, "");
        String schemaPath = cfg.getString(KEY_SCHEMA_PATH, "");

        String idPrefix = cfg.getString(KEY_ID_PREFIX, "sk-");
        long seqStart = parseLong(cfg.getString(KEY_SEQ_START, "0"));
        boolean fixedTs = Boolean.parseBoolean(cfg.getString(KEY_FIXED_TS, "true"));

        int payloadBytes = parseInt(cfg.getString(KEY_PAYLOAD_BYTES, "512"));
        boolean highEntropy = "HIGH".equalsIgnoreCase(cfg.getString(KEY_ENTROPY, "LOW"));

        if ("SCHEMA".equalsIgnoreCase(mode)) {
            switch (format.toUpperCase()) {
                case "AVRO" -> {
                    if ("SPECIFIC".equalsIgnoreCase(avroType)) {
                        prefillAvroSpecific(schemaPath, avroClass, idPrefix, seqStart, fixedTs, poolSize);
                    } else {
                        prefillAvroGeneric(schemaPath, idPrefix, seqStart, fixedTs, poolSize);
                    }
                }
                case "PROTOBUF" -> prefillProtobuf(protoClass, idPrefix, seqStart, fixedTs, poolSize);
                default -> prefillJson(fixturePath, idPrefix, seqStart, fixedTs, poolSize, payloadBytes, highEntropy);
            }
        } else {
            prefillInline(payloadBytes, highEntropy);
        }
    }

    @SuppressWarnings("unchecked")
    public SyntheticSource(int payloadBytes, boolean highEntropy) {
        int ringSize = 524288;
        this.ring = (PipelinePayload<String>[]) new PipelinePayload[ringSize];
        this.ringMask = ringSize - 1;
        this.cursor = 0L;
        prefillInline(payloadBytes, highEntropy);
    }

    // ---------------------------------------------------------------------
    // SourceConnector
    // ---------------------------------------------------------------------

    @Override
    public void connect() {}

    /**
     * Compatibility single-record fetch.
     * High-perf: returns a prebuilt payload (no allocation).
     */
    @Override
    public PipelinePayload<String> fetch() {
        int idx = (int) (cursor++ & ringMask);
        return ring[idx];
    }

    /**
     * High-performance batch fetch used by PipelineOrchestrator.
     *
     * Key point:
     * - Returns an immutable List VIEW over the ring.
     * - No per-record allocations.
     * - Only 1 small object allocated per batch (the view).
     * - Safe under concurrency because each batch gets its own view object.
     */
    @Override
    public List<PipelinePayload<String>> fetchBatch(int batchSize) {
        int n = Math.max(0, batchSize);
        long base = cursor;
        cursor = base + n;
        return new RingBatchList(ring, ringMask, base, n);
    }

    @Override
    public void disconnect() {}

    // ---------------------------------------------------------------------
    // Prefill implementations (preserved behavior)
    // ---------------------------------------------------------------------

    private void prefillInline(int payloadBytes, boolean highEntropy) {
        byte[] buf = new byte[Math.max(0, payloadBytes)];
        for (int i = 0; i <= ringMask; i++) {
            fillBytes(buf, highEntropy);
            String s = new String(buf, StandardCharsets.ISO_8859_1);
            ring[i] = PipelinePayload.of(s);
        }
    }

    private void prefillJson(
            String fixturePath,
            String idPrefix,
            long seqStart,
            boolean fixedTs,
            int poolSize,
            int payloadBytes,
            boolean highEntropy
    ) {
        String template = loadText(fixturePath);
        Instant boot = Instant.now();

        int ringSize = ringMask + 1;
        int effectivePool = Math.min(poolSize, ringSize);
        effectivePool = powerOfTwo(Math.max(1, effectivePool)); // keep power-of-two for masking

        List<PipelinePayload<String>> pool = new ArrayList<>(effectivePool);

        for (int i = 0; i < effectivePool; i++) {
            long seq = seqStart + i;
            String ts = fixedTs ? boot.toString() : Instant.now().toString();
            String json = template
                    .replace("${ID}", idPrefix + seq)
                    .replace("${SEQ}", Long.toString(seq))
                    .replace("${TS}", ts);

            if (json.contains("${TEXT}")) {
                byte[] buf = new byte[Math.max(0, payloadBytes)];
                fillBytes(buf, highEntropy);
                json = json.replace("${TEXT}", new String(buf, StandardCharsets.ISO_8859_1));
            }
            pool.add(PipelinePayload.of(json));
        }

        int poolMask = effectivePool - 1;
        for (int i = 0; i <= ringMask; i++) {
            ring[i] = pool.get(i & poolMask);
        }
    }

    private void prefillAvroGeneric(
            String schemaPath,
            String idPrefix,
            long seqStart,
            boolean fixedTs,
            int poolSize
    ) {
        Schema schema = new Schema.Parser().parse(loadText(schemaPath));
        Instant boot = Instant.now();

        int ringSize = ringMask + 1;
        int effectivePool = Math.min(poolSize, ringSize);
        effectivePool = powerOfTwo(Math.max(1, effectivePool));

        List<PipelinePayload<String>> pool = new ArrayList<>(effectivePool);

        for (int i = 0; i < effectivePool; i++) {
            GenericRecord rec = new GenericData.Record(schema);
            long seq = seqStart + i;

            for (Schema.Field f : schema.getFields()) {
                String n = f.name().toLowerCase();
                Object v = switch (f.schema().getType()) {
                    case STRING -> n.contains("id") ? idPrefix + seq :
                            n.contains("time") ? (fixedTs ? boot.toString() : Instant.now().toString()) : "";
                    case LONG -> seq;
                    case INT -> (int) seq;
                    case BOOLEAN -> true;
                    default -> null;
                };
                rec.put(f.name(), v);
            }
            pool.add(PipelinePayload.of(avroToJson(schema, rec)));
        }

        int poolMask = effectivePool - 1;
        for (int i = 0; i <= ringMask; i++) {
            ring[i] = pool.get(i & poolMask);
        }
    }

    private void prefillAvroSpecific(
            String schemaPath, // preserved arg (not required for Specific; kept for parity)
            String className,
            String idPrefix,
            long seqStart,
            boolean fixedTs,
            int poolSize
    ) {
        try {
            Class<?> cls = Class.forName(className);
            Schema schema = (Schema) cls.getMethod("getClassSchema").invoke(null);
            Instant boot = Instant.now();

            int ringSize = ringMask + 1;
            int effectivePool = Math.min(poolSize, ringSize);
            effectivePool = powerOfTwo(Math.max(1, effectivePool));

            List<PipelinePayload<String>> pool = new ArrayList<>(effectivePool);

            for (int i = 0; i < effectivePool; i++) {
                SpecificRecord rec = (SpecificRecord) SpecificData.get().newInstance(cls, schema);
                long seq = seqStart + i;

                for (Schema.Field f : schema.getFields()) {
                    String n = f.name().toLowerCase();
                    Object v = switch (f.schema().getType()) {
                        case STRING -> n.contains("id") ? idPrefix + seq :
                                n.contains("time") ? (fixedTs ? boot.toString() : Instant.now().toString()) : "";
                        case LONG -> seq;
                        case INT -> (int) seq;
                        case BOOLEAN -> true;
                        default -> null;
                    };
                    rec.put(f.pos(), v);
                }
                pool.add(PipelinePayload.of(avroSpecificToJson(schema, rec)));
            }

            int poolMask = effectivePool - 1;
            for (int i = 0; i <= ringMask; i++) {
                ring[i] = pool.get(i & poolMask);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void prefillProtobuf(
            String className,
            String idPrefix,
            long seqStart,
            boolean fixedTs,
            int poolSize
    ) {
        try {
            Class<?> cls = Class.forName(className);
            Message prototype = (Message) cls.getMethod("getDefaultInstance").invoke(null);
            Descriptors.Descriptor desc = prototype.getDescriptorForType();
            Instant boot = Instant.now();

            int ringSize = ringMask + 1;
            int effectivePool = Math.min(poolSize, ringSize);
            effectivePool = powerOfTwo(Math.max(1, effectivePool));

            List<PipelinePayload<String>> pool = new ArrayList<>(effectivePool);

            for (int i = 0; i < effectivePool; i++) {
                long seq = seqStart + i;
                DynamicMessage.Builder b = DynamicMessage.newBuilder(desc);

                for (Descriptors.FieldDescriptor f : desc.getFields()) {
                    String n = f.getName().toLowerCase();
                    Object v = switch (f.getType()) {
                        case STRING -> n.contains("id") ? idPrefix + seq :
                                n.contains("time") ? (fixedTs ? boot.toString() : Instant.now().toString()) : "";
                        case INT32 -> (int) seq;
                        case INT64 -> seq;
                        case BOOL -> true;
                        default -> null;
                    };
                    if (v != null) b.setField(f, v);
                }

                String json = JsonFormat.printer().print(b.build());
                pool.add(PipelinePayload.of(json));
            }

            int poolMask = effectivePool - 1;
            for (int i = 0; i <= ringMask; i++) {
                ring[i] = pool.get(i & poolMask);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ---------------------------------------------------------------------
    // Avro helpers (preserved)
    // ---------------------------------------------------------------------

    private static String avroToJson(Schema s, GenericRecord r) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder enc = EncoderFactory.get().jsonEncoder(s, out);
            new GenericDatumWriter<>(s).write(r, enc);
            enc.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String avroSpecificToJson(Schema s, SpecificRecord r) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder enc = EncoderFactory.get().jsonEncoder(s, out);
            new SpecificDatumWriter<SpecificRecord>(s).write(r, enc);
            enc.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ---------------------------------------------------------------------
    // Misc helpers (preserved)
    // ---------------------------------------------------------------------

    private static void fillBytes(byte[] b, boolean entropy) {
        if (entropy) ThreadLocalRandom.current().nextBytes(b);
        else for (int i = 0; i < b.length; i++) b[i] = 'A';
    }

    private static String loadText(String path) {
        try {
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("SyntheticSource: required path is blank");
            }
            if (path.startsWith("classpath:")) {
                String p = path.substring("classpath:".length());
                try (InputStream in = SyntheticSource.class.getResourceAsStream(p)) {
                    if (in == null) throw new IllegalArgumentException("Classpath resource not found: " + path);
                    return new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
            return Files.readString(Path.of(path), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int parseInt(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return 0; }
    }

    private static long parseLong(String s) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return 0L; }
    }

    private static int powerOfTwo(int v) {
        int n = 1;
        while (n < v) n <<= 1;
        return n;
    }
}
