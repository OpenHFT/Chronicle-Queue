/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.metrics.CounterMetric;
import net.openhft.chronicle.wire.metrics.Metrics;
import org.junit.After;
import org.junit.Test;

import java.lang.management.ManagementFactory;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * The allocation gate for the appender touch points (review #28): appending with metrics
 * enabled allocates zero bytes steady-state on the appending thread. Measured with
 * {@code com.sun.management.ThreadMXBean#getThreadAllocatedBytes} after a warmup that lets
 * lazy initialisation (store/index/instrument/JIT paths) settle; the threshold tolerates
 * the occasional cold mapping of a new block, not a per-append allocation.
 */
public class AppendMetricsAllocationTest extends QueueTestCommon {

    private static final int WARMUP = 50_000;
    private static final int MEASURED = 100_000;
    // Catches even a small per-append allocation (an Object[8] per append would be ~3 MB)
    // while tolerating occasional block remaps and measurement noise.
    private static final long MAX_ALLOCATED_BYTES = 64 << 10;

    @After
    public void resetMetrics() {
        Metrics.resetForTesting();
    }

    @SuppressWarnings("deprecation") // Thread.getId(): Java 8 baseline, threadId() unavailable
    @Test
    public void appendWithMetricsEnabledAllocatesZeroSteadyState() {
        java.lang.management.ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        assumeTrue("requires com.sun.management.ThreadMXBean",
                threadMXBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocations = (com.sun.management.ThreadMXBean) threadMXBean;
        assumeTrue(allocations.isThreadAllocatedMemorySupported());
        if (!allocations.isThreadAllocatedMemoryEnabled())
            allocations.setThreadAllocatedMemoryEnabled(true);

        // enabled BEFORE construction, per the resolve-once policy
        CapturingMetricsOut capture = new CapturingMetricsOut();
        Metrics.install(source -> capture);

        final long threadId = Thread.currentThread().getId();
        try (ChronicleQueue queue = single(getTmpDir()).build();
             ExcerptAppender appender = queue.createAppender()) {

            for (int i = 0; i < WARMUP; i++)
                append(appender, i);

            // prime the measurement path so its own lazy initialisation is not counted
            allocations.getThreadAllocatedBytes(threadId);
            long before = allocations.getThreadAllocatedBytes(threadId);
            for (int i = 0; i < MEASURED; i++)
                append(appender, i);
            long allocated = allocations.getThreadAllocatedBytes(threadId) - before;

            assertTrue("appending " + MEASURED + " excerpts with metrics enabled allocated "
                    + allocated + " bytes; expected <= " + MAX_ALLOCATED_BYTES, allocated <= MAX_ALLOCATED_BYTES);

            // prove the metrics really were live while we measured
            Metrics.registry(QueueMetrics.APPENDER_SOURCE).flush(capture, 1, 1);
        }

        CounterMetric appends = capture.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL);
        assertNotNull(appends);
        assertEquals(WARMUP + MEASURED, appends.count());
    }

    private void append(ExcerptAppender appender, int i) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().bytes().writeLong(i);
        }
    }
}
