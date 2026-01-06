/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@SuppressWarnings({"deprecation", "removal"})
public final class RollAtEndOfCycleTest extends QueueTestCommon {
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

    private static void checkQueueFileCount(final Path path, final long expectedCount) throws IOException {
        try (Stream<Path> list = Files.list(path)) {
            final long count = list.filter(p -> p.toString().
                    endsWith(SingleChronicleQueue.SUFFIX)).count();

            assertEquals(expectedCount, count, "queue directory should contain exactly " + expectedCount + " .cq4 files");
        }
    }

    @Test
    @DisplayName("Queue rolls to a new file at end of cycle")
    public void shouldRollAndAppendToNewFile() throws IOException {
        assumeFalse(Jvm.isArm(), "Test relies on non-ARM timing behaviour");

        try (final SingleChronicleQueue queue = createQueue();
             final ExcerptAppender appender = queue.createAppender()) {

            appender.writeDocument(1, ValueOut::int32);

            final ExcerptTailer tailer = queue.createTailer();
            try (final DocumentContext context = tailer.readingDocument()) {
                assertTrue(context.isPresent(), "tailer should read first document before rolling");
            }

            checkQueueFileCount(queue.path.toPath(), 1);
            clock.addAndGet(TimeUnit.SECONDS.toMillis(2));

            assertFalse(tailer.readingDocument().isPresent(), "tailer should not find any more documents in current cycle after reading all entries");

            appender.writeDocument(2, ValueOut::int32);

            checkQueueFileCount(queue.path.toPath(), 2);
            try (final DocumentContext context = tailer.readingDocument()) {
                assertTrue(context.isPresent(), "tailer should read document from new roll cycle file after time boundary");
            }

            final ExcerptTailer newTailer = queue.createTailer();
            int totalCount = 0;
            while (true) {
                final DocumentContext context = newTailer.readingDocument();
                if (context.isPresent() && context.isData()) {
                    assertNotEquals(0, context.wire().read().int32(),
                            "new tailer should read non-zero document values");
                    totalCount++;
                } else if (!context.isPresent()) {
                    break;
                }
            }

            assertEquals(2, totalCount, "new tailer should read exactly 2 documents across both roll cycle files");
        }
    }

    @Test
    @DisplayName("Queue appends to existing file within same cycle")
    public void shouldAppendToExistingQueueFile() throws IOException {
        try (final SingleChronicleQueue queue = createQueue();
             final ExcerptAppender appender = queue.createAppender()) {

            appender.writeDocument(1, ValueOut::int32);

            final ExcerptTailer tailer = queue.createTailer();
            try (final DocumentContext context = tailer.readingDocument()) {
                assertTrue(context.isPresent(), "tailer should read first document in existing cycle");
            }

            checkQueueFileCount(queue.path.toPath(), 1);

            assertFalse(tailer.readingDocument().isPresent(), "tailer should not find any more documents after reading all entries in current cycle");

            appender.writeDocument(2, ValueOut::int32);

            checkQueueFileCount(queue.path.toPath(), 1);
            try (final DocumentContext context = tailer.readingDocument()) {
                assertTrue(context.isPresent(), "tailer should read second document appended to same roll cycle file");
            }
        }
    }

    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.
                binary(getTmpDir()).
                rollCycle(TEST_SECONDLY).testBlockSize().
                timeProvider(clock::get).
                build();
    }
}
