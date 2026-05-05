/*
 * Copyright (c) 2026 Steven Lopez
 * SPDX-License-Identifier: LicenseRef-SSAL-1.0
 *
 * Licensed under the StreamKernel Source Available License (SSAL) v1.0.
 * See the LICENSE file in the project root for the full license text.
 */

package com.intuitivedesigns.streamkernel.output;

import com.intuitivedesigns.streamkernel.core.PipelinePayload;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Strategy for selecting a Kafka partition for a given payload.
 *
 * Implementations must be thread-safe.
 */
@FunctionalInterface
public interface PartitionStrategy {

    /**
     * @param payload        payload envelope
     * @param partitionCount number of partitions in the topic (must be > 0)
     * @return partition in range [0, partitionCount-1]
     */
    int partition(PipelinePayload<?> payload, int partitionCount);

    /**
     * Round-robin partitioning (0,1,2,0,1,2,...).
     */
    static PartitionStrategy roundRobin() {
        final AtomicInteger ctr = new AtomicInteger(0);
        return (payload, count) -> {
            if (count <= 0) return 0;
            return (ctr.getAndIncrement() & 0x7fffffff) % count;
        };
    }

    /**
     * Hashes payload.id() to keep related events ordered to the same partition.
     */
    static PartitionStrategy keyHash() {
        return (payload, count) -> {
            if (count <= 0) return 0;
            Objects.requireNonNull(payload, "payload");
            final String key = payload.id();
            if (key == null) return 0;
            return (key.hashCode() & 0x7fffffff) % count;
        };
    }
}
