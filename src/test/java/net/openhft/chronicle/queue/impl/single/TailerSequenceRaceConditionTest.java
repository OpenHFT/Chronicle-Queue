package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class TailerSequenceRaceConditionTest {
    private final AtomicBoolean failedToMoveToEnd = new AtomicBoolean(false);
    private final ExecutorService threadPool = Executors.newFixedThreadPool(8);

    @Test
    public void shouldAlwaysBeAbleToTail() throws Exception {
        for (int i = 0; i < 10; i++) {
            final SingleChronicleQueue queue = createNewQueue();
            for (int j = 0; j < 4; j++) {
                threadPool.submit(() -> attemptToMoveToTail(queue));
            }

            threadPool.submit(() -> appendToQueue(queue));

            for (int j = 0; j < 4; j++) {
                threadPool.submit(() -> attemptToMoveToTail(queue));
            }
        }

        threadPool.shutdown();
        assertThat(threadPool.awaitTermination(5L, TimeUnit.SECONDS), is(true));
        assertThat(failedToMoveToEnd.get(), is(false));
    }

    @After
    public void tearDown() throws Exception {
        threadPool.shutdownNow();
    }

    private void appendToQueue(final SingleChronicleQueue queue) {
        for (int i = 0; i < 31; i++) {
            final ExcerptAppender appender = queue.acquireAppender();
            if (queue.isClosed())
                return;
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("foo");
            }
        }
    }

    private void attemptToMoveToTail(final SingleChronicleQueue queue) {
        final SingleChronicleQueueExcerpts.StoreTailer tailer =
                (SingleChronicleQueueExcerpts.StoreTailer) queue.createTailer();
        try {
            tailer.toEnd();
        } catch (IllegalStateException e) {
            e.printStackTrace();
            failedToMoveToEnd.set(true);
        }
    }

    private SingleChronicleQueue createNewQueue() {
        return SingleChronicleQueueBuilder.
                binary(DirectoryUtils.tempDir(TailerSequenceRaceConditionTest.class.getSimpleName())).build();
    }
}
