package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public final class RollAtEndOfCycleTest extends ChronicleQueueTestBase {
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

    private static void assertQueueFileCount(final Path path, final long expectedCount) throws IOException {
        final long count = Files.list(path).filter(p -> p.toString().
                endsWith(SingleChronicleQueue.SUFFIX)).count();

        assertEquals(expectedCount, count);
    }

    @Test
    public void shouldRollAndAppendToNewFile() throws IOException {
        assumeFalse(Jvm.isArm());

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

            final ExcerptTailer newTailer = queue.createTailer();
            int totalCount = 0;
            while (true) {
                final DocumentContext context = newTailer.readingDocument();
                if (context.isPresent() && context.isData()) {
                    assertTrue(context.wire().read().int32() != 0);
                    totalCount++;
                } else if (!context.isPresent()) {
                    break;
                }
            }

            assertEquals(2, totalCount);
        }
    }

    @Test
    public void shouldAppendToExistingQueueFile() throws IOException {
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

    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.
                binary(getTmpDir()).
                rollCycle(RollCycles.TEST_SECONDLY).testBlockSize().
                timeProvider(clock::get).
                build();
    }
}