/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.plugins.wireevent;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.model.WireEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class StringToWireEventTransformer implements Transformer<Object, WireEvent> {

    public static final String SOURCE_TEXT_METADATA_KEY = "streamkernel.source.text";

    public static final String KEY_USE_PAYLOAD_ID_AS_KEY = "transform.string_to_wireevent.use.payload.id.as.key";
    public static final String KEY_DEFAULT_KEY = "transform.string_to_wireevent.default.key";
    public static final String KEY_CHARSET = "transform.string_to_wireevent.charset";
    public static final String KEY_METADATA_HEADERS_KEY = "transform.string_to_wireevent.metadata.headers.key";
    public static final String KEY_METADATA_RECORD_KEY = "transform.string_to_wireevent.metadata.record.key";
    public static final String KEY_DROP_NULL = "transform.string_to_wireevent.drop.null";
    public static final String KEY_TRIM = "transform.string_to_wireevent.trim";

    private static final Logger LOG = LoggerFactory.getLogger(StringToWireEventTransformer.class);

    private static final String DEFAULT_HEADERS_KEY = "wire.headers";
    private static final String DEFAULT_RECORD_KEY = "wire.key";
    private static final String C_DROPPED_NULL = "sk_string_to_wireevent_dropped_null_total";
    private static final String M_STRING_ENCODE_MS = "sk_wireevent_string_encode_ms";
    private static final String G_STRING_ENCODE_BYTES = "sk_wireevent_string_encode_bytes";

    private static final byte[] EMPTY_BYTES = new byte[0];

    @SuppressWarnings("unchecked")
    private static final Map<String, String> NO_HEADERS =
            (Map<String, String>) (Map<?, ?>) Collections.emptyMap();

    private final ThreadLocal<CharsetEncoder> encoderLocal;
    private final MetricsRuntime metrics;
    private final Charset charset;
    private final String headersMetaKey;
    private final String recordMetaKey;
    private final boolean dropNull;
    private final boolean trim;
    private final boolean usePayloadIdAsKey;
    private final String defaultKey;

    public StringToWireEventTransformer(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        this.usePayloadIdAsKey = config.getBoolean(KEY_USE_PAYLOAD_ID_AS_KEY, false);
        this.defaultKey = config.getString(KEY_DEFAULT_KEY, null);
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.charset = parseCharset(config.getString(KEY_CHARSET, StandardCharsets.UTF_8.name()));
        this.headersMetaKey = nonBlank(config.getString(KEY_METADATA_HEADERS_KEY, null), DEFAULT_HEADERS_KEY);
        this.recordMetaKey = nonBlank(config.getString(KEY_METADATA_RECORD_KEY, null), DEFAULT_RECORD_KEY);
        this.dropNull = config.getBoolean(KEY_DROP_NULL, true);
        this.trim = config.getBoolean(KEY_TRIM, false);
        final Charset cs = this.charset;
        this.encoderLocal = ThreadLocal.withInitial(cs::newEncoder);
    }

    private static byte[] encodeString(String s, Charset charset, boolean trim,
                                       ThreadLocal<CharsetEncoder> encoderLocal) {
        if (s == null || s.isEmpty()) return EMPTY_BYTES;
        if (!trim) {
            return s.getBytes(charset);
        }
        final int len = s.length();
        int start = 0;
        int end = len;
        while (start < end && Character.isWhitespace(s.charAt(start))) start++;
        while (end > start && Character.isWhitespace(s.charAt(end - 1))) end--;
        if (start == end) return EMPTY_BYTES;
        if (start == 0 && end == len) return s.getBytes(charset);
        final CharBuffer cb = CharBuffer.wrap(s, start, end);
        final CharsetEncoder encoder = encoderLocal.get();
        encoder.reset();
        final int maxBytes = (int) Math.ceil(encoder.maxBytesPerChar() * (end - start));
        final ByteBuffer out = ByteBuffer.allocate(maxBytes);
        final CoderResult cr = encoder.encode(cb, out, true);
        if (cr.isError()) {
            return s.substring(start, end).getBytes(charset);
        }
        encoder.flush(out);
        out.flip();
        final byte[] result = new byte[out.limit()];
        out.get(result);
        return result;
    }

    private static String textForMetadata(String s, boolean trim) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        if (!trim) {
            return s;
        }
        final int len = s.length();
        int start = 0;
        int end = len;
        while (start < end && Character.isWhitespace(s.charAt(start))) start++;
        while (end > start && Character.isWhitespace(s.charAt(end - 1))) end--;
        if (start == end) {
            return "";
        }
        if (start == 0 && end == len) {
            return s;
        }
        return s.substring(start, end);
    }

    private void recordEncodeMetrics(long durationNs, int payloadBytes) {
        if (!metrics.enabled()) {
            return;
        }
        final double millis = durationNs / 1_000_000.0;
        metrics.timer(M_STRING_ENCODE_MS, Math.max(1L, Math.round(millis)));
        metrics.gauge(G_STRING_ENCODE_BYTES, payloadBytes);
    }

    private static String nonBlank(String v, String def) {
        return (v == null || v.isBlank()) ? def : v;
    }

    private static Charset parseCharset(String name) {
        if (name == null || name.isBlank()) return StandardCharsets.UTF_8;
        try {
            return Charset.forName(name.trim());
        } catch (Exception e) {
            LOG.warn("Invalid charset '{}' for StringToWireEventTransformer; falling back to UTF-8.", name);
            return StandardCharsets.UTF_8;
        }
    }

    @Override
    public void init() { }

    @Override
    public void close() { }

    @Override
    public PipelinePayload<WireEvent> transform(PipelinePayload<Object> input) {
        if (input == null) return null;
        final Object data = input.data();
        if (data instanceof WireEvent) {
            @SuppressWarnings("unchecked") final PipelinePayload<WireEvent> cast =
                    (PipelinePayload<WireEvent>) (PipelinePayload<?>) input;
            return cast;
        }

        final Map<String, String> meta = input.metadata();
        String key = (meta != null && recordMetaKey != null) ? meta.get(recordMetaKey) : null;
        if ((key == null || key.isBlank()) && usePayloadIdAsKey) {
            key = input.id();
        }
        if ((key == null || key.isBlank()) && defaultKey != null && !defaultKey.isBlank()) {
            key = defaultKey;
        }

        if (data == null) {
            if (dropNull) {
                metrics.counter(C_DROPPED_NULL);
                return null;
            }
            return input.withData(WireEvent.bytes(EMPTY_BYTES, NO_HEADERS, key));
        }

        if (data instanceof byte[] bytes) {
            return input.withData(WireEvent.bytes(bytes, NO_HEADERS, key));
        }

        final String s = (data instanceof String str) ? str : String.valueOf(data);
        final long encodeStartNs = System.nanoTime();
        final byte[] bytes = encodeString(s, charset, trim, encoderLocal);
        final WireEvent event = WireEvent.bytes(bytes, NO_HEADERS, key);
        recordEncodeMetrics(System.nanoTime() - encodeStartNs, bytes.length);
        return input.withHeader(SOURCE_TEXT_METADATA_KEY, textForMetadata(s, trim)).withData(event);
    }
}
