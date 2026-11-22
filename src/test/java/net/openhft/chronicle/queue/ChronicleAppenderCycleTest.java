/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * Reproduces the assertion in StoreAppender.checkWritePositionHeaderNumber().
 * See https://github.com/OpenHFT/Chronicle-Queue/issues/611.
 */
public class ChronicleAppenderCycleTest extends QueueTestCommon {
    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void testAppenderCycle() {
        for (int i = 0; i < 20; ++i)
            runTest("testAppenderCycle-" + i);
    }

    private void runTest(String id) {
        try (AppenderCycleTestResources resources = new AppenderCycleTestResources(id)) {
            CountDownLatch steady = new CountDownLatch(2);
            int n = 468;
            resources.start("cycle-writer", appender -> {
                appender.cycle();
                for (int i = 0; i < n; ++i)
                    appender.writeBytes(resources.message);
                steady.countDown();
                resources.await(resources.go, "go");
                for (int i = 0; i < n; ++i)
                    appender.writeBytes(resources.message);
            });
            resources.start("cycle-observer", appender -> {
                steady.countDown();
                resources.await(resources.go, "go");
                for (int i = 0; i < 2 * n; ++i)
                    appender.cycle();
            });
            resources.await(steady, "steady");
            resources.go.countDown();
            resources.awaitDone();
            try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(resources.directory).testBlockSize().build()) {
                assertEquals("Both write phases must complete", 2L * n, queue.entryCount());
            }
        }
    }
}
