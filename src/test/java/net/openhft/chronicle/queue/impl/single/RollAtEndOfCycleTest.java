package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public final class RollAtEndOfCycleTest {
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

    @Test
    public void shouldRollAndAppendToNewFile() throws Exception {
        try (final SingleChronicleQueue queue = createQueue()) {
            final ExcerptAppender appender = queue.acquireAppender();

            appender.writeDocument(1, (w, i) -> {
                w.int32(i);
            });

            final ExcerptTailer tailer = queue.createTailer();
            try (final DocumentContext context = tailer.readingDocument()) {
                assertTrue(context.isPresent());
            }

            assertQueueFileCount(queue.path.toPath(), 1);
            clock.addAndGet(TimeUnit.SECONDS.toMillis(2));

            assertFalse(tailer.readingDocument().isPresent());

            appender.writeDocument(2, (w, i) -> {
                w.int32(i);
            });

            assertQueueFileCount(queue.path.toPath(), 2);
            try (final DocumentContext context = tailer.readingDocument()) {
                assertTrue(context.isPresent());
            }
        }
    }

    @Test
    public void shouldAppendToExistingQueueFile() throws Exception {
        try (final SingleChronicleQueue queue = createQueue()) {
            final ExcerptAppender appender = queue.acquireAppender();

            appender.writeDocument(1, (w, i) -> {
                w.int32(i);
            });

            final ExcerptTailer tailer = queue.createTailer();
            try (final DocumentContext context = tailer.readingDocument()) {
                assertTrue(context.isPresent());
            }

            assertQueueFileCount(queue.path.toPath(), 1);

            assertFalse(tailer.readingDocument().isPresent());

            appender.writeDocument(2, (w, i) -> {
                w.int32(i);
            });

            assertQueueFileCount(queue.path.toPath(), 1);
            try (final DocumentContext context = tailer.readingDocument()) {
                assertTrue(context.isPresent());
            }
        }
    }

    private static void assertQueueFileCount(final Path path, final long expectedCount) throws IOException {
        final long count = Files.list(path).filter(p -> p.toString().
                endsWith(SingleChronicleQueue.SUFFIX)).count();

        assertThat(count, is(expectedCount));
    }

    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.
                binary(DirectoryUtils.tempDir(RollAtEndOfCycleTest.class.getName())).
                rollCycle(RollCycles.TEST_SECONDLY).testBlockSize().
                timeProvider(clock::get).
                build();
    }
}