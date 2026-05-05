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
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.Readyable;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class SnowflakeSnowpipeStreamingSink implements BatchOutputSink<Object>, Readyable {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeSnowpipeStreamingSink.class);

    private static final String KEY_ACCOUNT = "snowflake.account";
    private static final String KEY_URL = "snowflake.url";
    private static final String KEY_USER = "snowflake.user";
    private static final String KEY_ROLE = "snowflake.role";
    private static final String KEY_WAREHOUSE = "snowflake.warehouse";
    private static final String KEY_DATABASE = "snowflake.database";
    private static final String KEY_SCHEMA = "snowflake.schema";
    private static final String KEY_PIPE = "snowflake.pipe";
    private static final String KEY_CLIENT_NAME = "snowflake.client.name";
    private static final String KEY_CHANNEL_NAME = "snowflake.channel.name";
    private static final String KEY_OFFSET_TOKEN = "snowflake.channel.offset.token";
    private static final String KEY_PRIVATE_KEY = "snowflake.private.key";
    private static final String KEY_PRIVATE_KEY_FILE = "snowflake.private.key.file";
    private static final String KEY_AUTHENTICATOR = "snowflake.authenticator";
    private static final String KEY_AUTHORIZATION_TYPE = "snowflake.authorization.type";
    private static final String KEY_FLUSH_ON_WRITE = "snowflake.flush.on.write";
    private static final String KEY_COMMIT_TIMEOUT_MS = "snowflake.commit.timeout.ms";
    private static final String KEY_STRICT_PAYLOAD_TYPE = "snowflake.strict.payload.type";
    private static final String PROPERTY_PREFIX = "snowflake.property.";
    private static final String PARAMETER_PREFIX = "snowflake.parameter.";

    private static final String M_BATCHES_TOTAL = "streamkernel.snowflake.sink.batches.total";
    private static final String M_BATCHES_EMPTY = "streamkernel.snowflake.sink.batches.empty.total";
    private static final String M_ROWS_TOTAL = "streamkernel.snowflake.sink.rows.total";
    private static final String M_COMMIT_MS = "streamkernel.snowflake.sink.commit.ms";
    private static final String M_ERROR_TOTAL = "streamkernel.snowflake.sink.error.total";

    private final MetricsRuntime metrics;
    private final String clientName;
    private final String database;
    private final String schema;
    private final String pipe;
    private final String channelName;
    private final String initialOffsetToken;
    private final Properties clientProperties;
    private final Map<String, Object> parameterOverrides;
    private final boolean flushOnWrite;
    private final boolean strictPayloadType;
    private final Duration commitTimeout;
    private final SnowflakeRowMapper rowMapper = new SnowflakeRowMapper();
    private final Object lifecycleLock = new Object();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong nextOffset = new AtomicLong(0L);

    private ExecutorService executor;
    private SnowflakeStreamingIngestClient client;
    private SnowflakeStreamingIngestChannel channel;

    private SnowflakeSnowpipeStreamingSink(MetricsRuntime metrics,
                                           String clientName,
                                           String database,
                                           String schema,
                                           String pipe,
                                           String channelName,
                                           String initialOffsetToken,
                                           Properties clientProperties,
                                           Map<String, Object> parameterOverrides,
                                           boolean flushOnWrite,
                                           boolean strictPayloadType,
                                           Duration commitTimeout) {
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.clientName = Objects.requireNonNull(clientName, "clientName");
        this.database = Objects.requireNonNull(database, "database");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.pipe = Objects.requireNonNull(pipe, "pipe");
        this.channelName = Objects.requireNonNull(channelName, "channelName");
        this.initialOffsetToken = initialOffsetToken;
        this.clientProperties = copyProperties(clientProperties);
        this.parameterOverrides = Map.copyOf(parameterOverrides);
        this.flushOnWrite = flushOnWrite;
        this.strictPayloadType = strictPayloadType;
        this.commitTimeout = Objects.requireNonNull(commitTimeout, "commitTimeout");
    }

    public static SnowflakeSnowpipeStreamingSink fromConfig(PipelineConfig config, MetricsRuntime metrics) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(metrics, "metrics");

        final String account = requireNonBlank(config.getString(KEY_ACCOUNT, null), KEY_ACCOUNT);
        final String user = requireNonBlank(config.getString(KEY_USER, null), KEY_USER);
        final String database = requireNonBlank(config.getString(KEY_DATABASE, null), KEY_DATABASE);
        final String schema = requireNonBlank(config.getString(KEY_SCHEMA, null), KEY_SCHEMA);
        final String pipe = requireNonBlank(config.getString(KEY_PIPE, null), KEY_PIPE);
        final String pipelineId = firstNonBlank(config.getString("pipeline.id", null), "streamkernel");
        final String clientName = firstNonBlank(config.getString(KEY_CLIENT_NAME, null), pipelineId + "-snowflake");
        final String channelName = firstNonBlank(config.getString(KEY_CHANNEL_NAME, null), pipelineId + "-channel");
        final String initialOffsetToken = blankToNull(config.getString(KEY_OFFSET_TOKEN, null));
        final boolean flushOnWrite = config.getBoolean(KEY_FLUSH_ON_WRITE, true);
        final boolean strictPayloadType = config.getBoolean(KEY_STRICT_PAYLOAD_TYPE, false);
        final Duration commitTimeout = Duration.ofMillis(Math.max(1L, config.getLong(KEY_COMMIT_TIMEOUT_MS, 120_000L)));

        final Properties clientProperties = buildClientProperties(config, account, user);
        final Map<String, Object> parameterOverrides = buildParameterOverrides(config);

        return new SnowflakeSnowpipeStreamingSink(
                metrics,
                clientName,
                database,
                schema,
                pipe,
                channelName,
                initialOffsetToken,
                clientProperties,
                parameterOverrides,
                flushOnWrite,
                strictPayloadType,
                commitTimeout
        );
    }

    @Override
    public void verifyReady() throws Exception {
        ensureInitialized();
        synchronized (lifecycleLock) {
            channel.getChannelStatus();
        }
    }

    @Override
    public void write(PipelinePayload<Object> payload) throws Exception {
        if (payload == null || payload.data() == null) {
            return;
        }
        writeBatch(List.of(payload));
    }

    @Override
    public void writeBatch(List<PipelinePayload<Object>> batch) throws Exception {
        if (batch == null || batch.isEmpty()) {
            metrics.counter(M_BATCHES_EMPTY);
            return;
        }

        ensureInitialized();
        final List<Map<String, Object>> rows = rowMapper.toRows(batch, strictPayloadType);
        if (rows.isEmpty()) {
            metrics.counter(M_BATCHES_EMPTY);
            return;
        }

        final long startNs = System.nanoTime();
        final long startOffset = nextOffset.getAndAdd(rows.size());
        final long endOffset = startOffset + rows.size() - 1L;

        try {
            synchronized (lifecycleLock) {
                ensureOpen();
                channel.appendRows(rows, Long.toString(startOffset), Long.toString(endOffset));
                if (flushOnWrite) {
                    channel.initiateFlush();
                }
                awaitFuture(
                        channel.waitForCommit(token -> offsetAtLeast(token, endOffset), commitTimeout),
                        "Snowflake waitForCommit",
                        commitTimeout
                );
            }
            metrics.counter(M_BATCHES_TOTAL);
            metrics.counter(M_ROWS_TOTAL, rows.size());
            metrics.timer(M_COMMIT_MS, elapsedMillis(startNs));
        } catch (Exception e) {
            metrics.counter(M_ERROR_TOTAL);
            throw e;
        }
    }

    @Override
    public void flush() throws Exception {
        if (!initialized.get()) {
            return;
        }
        synchronized (lifecycleLock) {
            ensureOpen();
            channel.initiateFlush();
            awaitFuture(channel.waitForFlush(commitTimeout), "Snowflake waitForFlush", commitTimeout);
        }
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        Exception failure = null;
        synchronized (lifecycleLock) {
            if (channel != null && !channel.isClosed()) {
                try {
                    channel.close(true, commitTimeout);
                } catch (Exception e) {
                    failure = e;
                }
            }

            if (client != null && !client.isClosed()) {
                try {
                    awaitFuture(client.close(true, commitTimeout), "Snowflake client close", commitTimeout);
                } catch (Exception e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
        }

        if (executor != null) {
            executor.shutdownNow();
        }

        if (failure != null) {
            throw failure;
        }
    }

    private void ensureInitialized() throws Exception {
        if (initialized.get()) {
            return;
        }

        synchronized (lifecycleLock) {
            if (initialized.get()) {
                return;
            }
            ensureNotClosed();

            final ExecutorService newExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("sk-snowflake-commit"));
            try {
                SnowflakeStreamingIngestClientFactory.Builder builder =
                        SnowflakeStreamingIngestClientFactory.builder(clientName, database, schema, pipe)
                                .setProperties(copyProperties(clientProperties))
                                .setExecutorService(newExecutor);
                if (!parameterOverrides.isEmpty()) {
                    builder = builder.setParameterOverrides(parameterOverrides);
                }

                final SnowflakeStreamingIngestClient newClient = builder.build();
                final OpenChannelResult openResult = isBlank(initialOffsetToken)
                        ? newClient.openChannel(channelName)
                        : newClient.openChannel(channelName, initialOffsetToken);
                final SnowflakeStreamingIngestChannel newChannel = Objects.requireNonNull(
                        openResult.getChannel(),
                        "Snowflake openChannel returned a null channel"
                );

                this.executor = newExecutor;
                this.client = newClient;
                this.channel = newChannel;
                this.nextOffset.set(resolveNextOffset(newChannel.getLatestCommittedOffsetToken(), initialOffsetToken));
                this.initialized.set(true);

                log.info("SnowflakeSnowpipeStreamingSink initialized. db={} schema={} pipe={} channel={}",
                        database, schema, pipe, channelName);
            } catch (Exception e) {
                newExecutor.shutdownNow();
                throw e;
            }
        }
    }

    private void ensureOpen() {
        ensureNotClosed();
        if (channel == null || channel.isClosed()) {
            throw new IllegalStateException("Snowflake channel is not open");
        }
        if (client == null || client.isClosed()) {
            throw new IllegalStateException("Snowflake client is not open");
        }
    }

    private void ensureNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("SnowflakeSnowpipeStreamingSink is already closed");
        }
    }

    static Properties buildClientProperties(PipelineConfig config, String account, String user) {
        final Properties props = new Properties();
        props.put("account", account);
        props.put("user", user);
        props.put("url", firstNonBlank(
                config.getString(KEY_URL, null),
                "https://" + account + ".snowflakecomputing.com"
        ));

        mapIfPresent(config, props, KEY_ROLE, "role");
        mapIfPresent(config, props, KEY_WAREHOUSE, "warehouse");
        mapIfPresent(config, props, KEY_AUTHENTICATOR, "authenticator");
        mapIfPresent(config, props, KEY_AUTHORIZATION_TYPE, "authorization_type");

        final String privateKey = blankToNull(config.getString(KEY_PRIVATE_KEY, null));
        final String privateKeyFile = blankToNull(config.getString(KEY_PRIVATE_KEY_FILE, null));
        if (looksLikePem(privateKeyFile)) {
            props.put("private_key", normalizePrivateKey(privateKeyFile));
        } else if (!isBlank(privateKeyFile)) {
            props.put("private_key_file", privateKeyFile.trim());
        } else if (!isBlank(privateKey)) {
            props.put("private_key", normalizePrivateKey(privateKey));
        }

        for (String key : config.keys()) {
            if (!key.startsWith(PROPERTY_PREFIX) || key.length() <= PROPERTY_PREFIX.length()) {
                continue;
            }
            final String propertyName = key.substring(PROPERTY_PREFIX.length()).trim();
            if (propertyName.isEmpty()) {
                continue;
            }
            final String value = config.getString(key, null);
            if (!isBlank(value)) {
                props.put(propertyName, value);
            }
        }

        if (isBlank(props.getProperty("private_key")) && isBlank(props.getProperty("private_key_file"))) {
            throw new IllegalStateException(
                    "Snowflake sink requires snowflake.private.key, snowflake.private.key.file, or a mapped snowflake.property.private_key"
            );
        }

        return props;
    }

    static Map<String, Object> buildParameterOverrides(PipelineConfig config) {
        final LinkedHashMap<String, Object> overrides = new LinkedHashMap<>();
        for (String key : config.keys()) {
            if (!key.startsWith(PARAMETER_PREFIX) || key.length() <= PARAMETER_PREFIX.length()) {
                continue;
            }
            final String parameterName = key.substring(PARAMETER_PREFIX.length()).trim();
            if (parameterName.isEmpty()) {
                continue;
            }
            final String value = config.getString(key, null);
            if (!isBlank(value)) {
                overrides.put(parameterName, value);
            }
        }
        return overrides;
    }

    private static long resolveNextOffset(String committedOffsetToken, String configuredOffsetToken) {
        final String candidate = firstNonBlank(committedOffsetToken, configuredOffsetToken);
        if (isBlank(candidate)) {
            return 0L;
        }

        try {
            return Long.parseLong(candidate.trim()) + 1L;
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Snowflake channel offset tokens must remain numeric for StreamKernel-managed replay. Received: "
                            + candidate,
                    e
            );
        }
    }

    private static boolean offsetAtLeast(String token, long expectedOffset) {
        if (isBlank(token)) {
            return false;
        }
        try {
            return Long.parseLong(token.trim()) >= expectedOffset;
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Snowflake returned a non-numeric committed offset token: " + token, e);
        }
    }

    private static void awaitFuture(CompletableFuture<?> future, String operation, Duration timeout) throws Exception {
        try {
            final Object result = future.get();
            if (result instanceof Boolean completed && !completed) {
                throw new TimeoutException(operation + " timed out after " + timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new IllegalStateException(operation + " failed", cause);
        }
    }

    private static long elapsedMillis(long startNs) {
        return Math.max(1L, (System.nanoTime() - startNs) / 1_000_000L);
    }

    private static Properties copyProperties(Properties source) {
        final Properties copy = new Properties();
        copy.putAll(source);
        return copy;
    }

    private static void mapIfPresent(PipelineConfig config, Properties props, String configKey, String propertyKey) {
        final String value = config.getString(configKey, null);
        if (!isBlank(value)) {
            props.put(propertyKey, value.trim());
        }
    }

    private static String normalizePrivateKey(String value) {
        return value
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replace("\r", "")
                .replace("\n", "")
                .trim();
    }

    private static boolean looksLikePem(String value) {
        return !isBlank(value) && value.contains("BEGIN PRIVATE KEY");
    }

    private static String requireNonBlank(String value, String key) {
        if (isBlank(value)) {
            throw new IllegalStateException("Missing required Snowflake sink config key: " + key);
        }
        return value.trim();
    }

    private static String blankToNull(String value) {
        return isBlank(value) ? null : value.trim();
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String threadName;

        private NamedThreadFactory(String threadName) {
            this.threadName = threadName;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            final Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        }
    }
}
