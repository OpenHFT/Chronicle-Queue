/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class TailerSequenceRaceConditionTest extends QueueTestCommon {
    private final AtomicBoolean failedToMoveToEnd = new AtomicBoolean(false);
    private final ExecutorService threadPool = Executors.newFixedThreadPool(8,
            new NamedThreadFactory("test"));

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void shouldAlwaysBeAbleToTail() throws InterruptedException {
        ChronicleQueue[] queues = new ChronicleQueue[10];
        for (int i = 0; i < 10; i++) {
            final ChronicleQueue queue = createNewQueue();
            queues[i] = queue;
            for (int j = 0; j < 4; j++) {
                threadPool.submit(() -> attemptToMoveToTail(queue));
            }

            threadPool.submit(() -> appendToQueue(queue));

            for (int j = 0; j < 4; j++) {
                threadPool.submit(() -> attemptToMoveToTail(queue));
            }
        }

        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(5L, TimeUnit.SECONDS));
        assertFalse(failedToMoveToEnd.get());
        Closeable.closeQuietly(queues);
    }

    @Override
    public void tearDown() {
        super.tearDown();
        threadPool.shutdownNow();
    }

    private void appendToQueue(final ChronicleQueue queue) {
        try (final ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < 31; i++) {
                if (queue.isClosed())
                    return;
                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("foo");
                }
            }
        }
    }

    private void attemptToMoveToTail(final ChronicleQueue queue) {
        final StoreTailer tailer =
                (StoreTailer) queue.createTailer();
        try {
            tailer.toEnd();
        } catch (IllegalStateException e) {
            e.printStackTrace();
            failedToMoveToEnd.set(true);
        }
    }

    private ChronicleQueue createNewQueue() {
        return SingleChronicleQueueBuilder.
                binary(getTmpDir())
                .rollCycle(HOURLY)
                .build();
    }
}
