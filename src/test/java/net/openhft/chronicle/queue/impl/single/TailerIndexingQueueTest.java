package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public final class TailerIndexingQueueTest extends ChronicleQueueTestBase {
    private final File path = getTmpDir();
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

    private static void deleteFile(final Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new AssertionError("Could not delete", e);
        }
    }

    private static ChronicleQueue createQueue(final File path, final TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.
                binary(path).
                timeProvider(timeProvider).
                rollCycle(RollCycles.TEST_SECONDLY).
                testBlockSize().
                wireType(WireType.BINARY).
                build();
    }

    @Test
    public void tailerShouldBeAbleToMoveBackwardFromEndOfCycle() throws IOException {
        assumeFalse(OS.isWindows());
        try (final ChronicleQueue queue = createQueue(path, clock::get)) {
            final ExcerptAppender appender = queue.acquireAppender();
            // generate some cycle files
            range(0, 5).forEach(i -> {
                try (final DocumentContext ctx = appender.writingDocument()) {
                    ctx.wire().write().int32(i);
                    clock.addAndGet(TimeUnit.SECONDS.toMillis(10L));
                }
            });
        }

        // remove all but the first file
        try (Stream<Path> list = Files.list(this.path.toPath());
             Stream<Path> list2 = Files.list(this.path.toPath())) {
            final Path firstFile =
                    list.sorted(Comparator.comparing(Path::toString))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            list2.filter(p -> !p.equals(firstFile))
                    .forEach(TailerIndexingQueueTest::deleteFile);

            try (final ChronicleQueue queue = createQueue(path, SystemTimeProvider.INSTANCE)) {
                final ExcerptTailer tailer = queue.createTailer().toEnd();
                // move to END_OF_CYCLE
                try (final DocumentContext readCtx = tailer.readingDocument()) {
                    assertFalse(readCtx.isPresent());
                }
                assertEquals(TailerState.END_OF_CYCLE, tailer.state());

                tailer.direction(TailerDirection.BACKWARD);

                tailer.toEnd();
                assertTrue(tailer.readingDocument().isPresent());
            }
        }
    }
}