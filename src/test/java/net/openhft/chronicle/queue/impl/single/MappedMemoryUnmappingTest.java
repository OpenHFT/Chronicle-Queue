/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.testframework.GcControls;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.fail;

public final class MappedMemoryUnmappingTest extends QueueTestCommon {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void shouldUnmapMemoryAsCycleRolls() throws IOException {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        long initialQueueMappedMemory = 0L;

        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.
                binary(tmp.newFolder()).testBlockSize().rollCycle(TEST_SECONDLY).
                timeProvider(clock::get).build();
             final ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < 100; i++) {
                appender.writeDocument(System.nanoTime(), (d, t) -> d.int64(t));
                clock.addAndGet(TimeUnit.SECONDS.toMillis(1L));
                if (initialQueueMappedMemory == 0L) {
                    initialQueueMappedMemory = OS.memoryMapped();
                }
            }
        }

        GcControls.waitForGcCycle();

        final long timeoutAt = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
        while (System.currentTimeMillis() < timeoutAt) {
            if (OS.memoryMapped() < 2 * initialQueueMappedMemory) {
                return;
            }
        }

        fail(String.format("Mapped memory (%dB) did not fall below threshold (%dB)",
                OS.memoryMapped(), 2 * initialQueueMappedMemory));
    }
}
