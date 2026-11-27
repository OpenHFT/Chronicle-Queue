/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class AppenderInspectionLifecycleTest extends QueueTestCommon {
    @Test
    public void publishedCycleProbePairsStoreFileEvents() {
        File path = getTmpDir();
        CountingListener listener = new CountingListener();
        try (SingleChronicleQueue queue = builder(path, 0).storeFileListener(listener).build();
             ExcerptAppender stalled = queue.createAppender()) {
            stalled.writeText("old");
            try (SingleChronicleQueue other = builder(path, TestRollCycles.TEST_DAILY.lengthInMillis()).build();
                 ExcerptAppender publisher = other.createAppender()) {
                publisher.writeText("published");
            }
            stalled.writeText("followed");
            assertEquals(1, stalled.cycle());
        }
        BackgroundResourceReleaser.releasePendingResources();
        assertTrue(listener.acquired.get() >= 3);
        assertEquals(listener.acquired.get(), listener.released.get());
    }

    static SingleChronicleQueueBuilder builder(File path, long time) {
        return SingleChronicleQueueBuilder.binary(path).testBlockSize()
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(() -> time);
    }

    static final class CountingListener implements StoreFileListener {
        final AtomicInteger acquired = new AtomicInteger();
        final AtomicInteger released = new AtomicInteger();

        @Override
        public void onAcquired(int cycle, File file) {
            acquired.incrementAndGet();
        }

        @Override
        public void onReleased(int cycle, File file) {
            released.incrementAndGet();
        }
    }
}
