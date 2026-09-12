/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class TailerSequenceRaceConditionTest extends QueueTestCommon {
    private final ExecutorService threadPool = Executors.newFixedThreadPool(8,
            new NamedThreadFactory("test"));

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void shouldAlwaysBeAbleToTail() throws Exception {
        runRace(new ArrayList<>(), this::attemptToMoveToTail);
    }

    @Test
    public void workerFailureIsReportedAndQueuesAreClosed() {
        List<ChronicleQueue> queues = new ArrayList<>();
        IllegalStateException failure = new IllegalStateException("injected tailer failure");
        ExecutionException reported = assertThrows(ExecutionException.class,
                () -> runRace(queues, queue -> { throw failure; }));
        assertSame(failure, reported.getCause());
        assertTrue(threadPool.isTerminated());
        assertTrue(queues.stream().allMatch(ChronicleQueue::isClosed));
    }

    private void runRace(List<ChronicleQueue> queues, Consumer<ChronicleQueue> tailerAction) throws Exception {
        List<Future<?>> workers = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                final ChronicleQueue queue = createNewQueue();
                queues.add(queue);
                for (int j = 0; j < 4; j++)
                    workers.add(threadPool.submit(() -> tailerAction.accept(queue)));

                workers.add(threadPool.submit(() -> appendToQueue(queue)));

                for (int j = 0; j < 4; j++)
                    workers.add(threadPool.submit(() -> tailerAction.accept(queue)));
            }

            threadPool.shutdown();
            assertTrue("Race workers timed out", threadPool.awaitTermination(5L, TimeUnit.SECONDS));
            // Future.get surfaces acquisition failures as well as failures inside toEnd().
            for (Future<?> worker : workers)
                worker.get();
        } finally {
            threadPool.shutdownNow();
            try {
                assertTrue("Race workers did not stop", threadPool.awaitTermination(5L, TimeUnit.SECONDS));
            } finally {
                Closeable.closeQuietly(queues);
            }
        }
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
        try (ExcerptTailer tailer = queue.createTailer()) {
            tailer.toEnd();
        }
    }

    private ChronicleQueue createNewQueue() {
        return SingleChronicleQueueBuilder.
                binary(getTmpDir())
                .rollCycle(HOURLY)
                .build();
    }
}
