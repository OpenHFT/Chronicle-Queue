package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;

public final class TailerIndexingQueueTest {
    private final File path = DirectoryUtils.tempDir(AppenderFileHandleLeakTest.class.getSimpleName() + "-" + System.nanoTime());
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
        final Path firstFile =
                Files.list(this.path.toPath())
                        .sorted(Comparator.comparing(Path::toString))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        Files.list(this.path.toPath())
                .filter(p -> !p.equals(firstFile))
                .forEach(TailerIndexingQueueTest::deleteFile);

        try (final ChronicleQueue queue = createQueue(path, SystemTimeProvider.INSTANCE)) {
            final ExcerptTailer tailer = queue.createTailer().toEnd();
            // move to END_OF_CYCLE
            try (final DocumentContext readCtx = tailer.readingDocument()) {
                assertThat(readCtx.isPresent(), is(false));
            }
            assertThat(tailer.state(), is(TailerState.END_OF_CYCLE));

            tailer.direction(TailerDirection.BACKWARD);

            tailer.toEnd();
            assertThat(tailer.readingDocument().isPresent(), is(true));
        }
    }

    @After
    public void deleteDir() {
        DirectoryUtils.deleteDir(path);
    }
}