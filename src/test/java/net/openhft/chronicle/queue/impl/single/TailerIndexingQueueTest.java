/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public final class TailerIndexingQueueTest extends QueueTestCommon {
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
                rollCycle(TEST_SECONDLY).
                testBlockSize().
                wireType(WireType.BINARY).
                build();
    }

    @Test
    @DisplayName("Tailer moves backward from end of cycle")
    public void tailerShouldBeAbleToMoveBackwardFromEndOfCycle() throws IOException {
        assumeFalse(OS.isWindows(), "Windows does not support this test");
        try (final ChronicleQueue queue = createQueue(path, clock::get);
             final ExcerptAppender appender = queue.createAppender()) {
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
                    list.min(Comparator.comparing(Path::toString))
                            .orElseThrow(AssertionError::new);
            list2.filter(p -> !p.equals(firstFile))
                    .forEach(TailerIndexingQueueTest::deleteFile);

            try (final ChronicleQueue queue = createQueue(path, SystemTimeProvider.INSTANCE)) {
                final ExcerptTailer tailer = queue.createTailer().toEnd();
                // move to END_OF_CYCLE
                try (final DocumentContext readCtx = tailer.readingDocument()) {
                    assertFalse(readCtx.isPresent(), "tailer: no document at end of cycle");
                }
                assertEquals(TailerState.END_OF_CYCLE, tailer.state(), "tailer: state after reaching end of cycle");

                tailer.direction(TailerDirection.BACKWARD);

                tailer.toEnd();
                assertTrue(tailer.readingDocument().isPresent(), "tailer: can read when moving backward from end of cycle");
            }
        }
    }
}
