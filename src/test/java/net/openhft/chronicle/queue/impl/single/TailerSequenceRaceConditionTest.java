package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class TailerSequenceRaceConditionTest extends ChronicleQueueTestBase {
    private final AtomicBoolean failedToMoveToEnd = new AtomicBoolean(false);
    private final ExecutorService threadPool = Executors.newFixedThreadPool(8,
            new NamedThreadFactory("test"));

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

    @After
    public void tearDown() {
        threadPool.shutdownNow();
    }

    private void appendToQueue(final ChronicleQueue queue) {
        for (int i = 0; i < 31; i++) {
            final ExcerptAppender appender = queue.acquireAppender();
            if (queue.isClosed())
                return;
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("foo");
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
                .rollCycle(RollCycles.HOURLY)
                .build();
    }
}
