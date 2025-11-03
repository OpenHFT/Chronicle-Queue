/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.stress;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.*;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;

public class RollCycleMultiThreadTest extends QueueTestCommon {

    private static final RollCycle ROLL_CYCLE = TEST_DAILY;

    @Test
    public void testRead1() throws ExecutionException, InterruptedException {
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider();

        final ExecutorService scheduledExecutorService = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("testRead1"));

        try (ChronicleQueue queue0 = SingleChronicleQueueBuilder
                .binary(path)
                .testBlockSize()
                .rollCycle(ROLL_CYCLE)
                .timeProvider(timeProvider).build()) {

            ParallelQueueObserver observer = new ParallelQueueObserver(queue0);

            try (ChronicleQueue queue = SingleChronicleQueueBuilder
                    .binary(path)
                    .testBlockSize()
                    .rollCycle(ROLL_CYCLE)
                    .timeProvider(timeProvider)
                    .build();
                 ExcerptAppender appender = queue.createAppender()) {

                Assert.assertEquals(-2, (int) scheduledExecutorService.submit(observer::call).get());
                // two days pass
                timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(2));

                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("say").text("Day 3 data");
                }
                Assert.assertEquals(1, (int) scheduledExecutorService.submit(observer::call).get());
                assertEquals(1, observer.documentsRead);

            }
        } finally {
            scheduledExecutorService.shutdown();
            scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS);
        }
        finishedNormally = true;
    }

    @Test
    public void testRead2() throws ExecutionException, InterruptedException {
        finishedNormally = false;
        File path = getTmpDir();
        Assume.assumeFalse("Ignored on hugetlbfs as byte offsets will be different due to page size", PageUtil.isHugePage(path.getAbsolutePath()));
        SetTimeProvider timeProvider = new SetTimeProvider();

        final ExecutorService es = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("testRead2"));
        try (ChronicleQueue queue0 = SingleChronicleQueueBuilder
                .binary(path)
                .testBlockSize()
                .rollCycle(ROLL_CYCLE)
                .timeProvider(timeProvider)
                .build()) {

            final ParallelQueueObserver observer = new ParallelQueueObserver(queue0);

            try (ChronicleQueue queue = SingleChronicleQueueBuilder
                    .binary(path)
                    .blockSize(OS.SAFE_PAGE_SIZE)
                    .rollCycle(ROLL_CYCLE)
                    .timeProvider(timeProvider)
                    .build();
                 ExcerptAppender appender = queue.createAppender()) {

                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("say").text("Day 1 data");
                }

                Assert.assertEquals(1, (int) es.submit(observer).get());

                assertEquals("" +
                                "--- !!meta-data #binary\n" +
                                "header: !STStore {\n" +
                                "  wireType: !WireType BINARY_LIGHT,\n" +
                                "  metadata: !SCQMeta {\n" +
                                "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                                "    sourceId: 0\n" +
                                "  }\n" +
                                "}\n" +
                                "# position: 152, header: 0\n" +
                                "--- !!data #binary\n" +
                                "listing.highestCycle: 0\n" +
                                "# position: 192, header: 1\n" +
                                "--- !!data #binary\n" +
                                "listing.lowestCycle: 0\n" +
                                "# position: 232, header: 2\n" +
                                "--- !!data #binary\n" +
                                "listing.modCount: 5\n" +
                                "# position: 264, header: 3\n" +
                                "--- !!data #binary\n" +
                                "chronicle.write.lock: -9223372036854775808\n" +
                                "# position: 304, header: 4\n" +
                                "--- !!data #binary\n" +
                                "chronicle.append.lock: -9223372036854775808\n" +
                                "# position: 344, header: 5\n" +
                                "--- !!data #binary\n" +
                                "chronicle.lastIndexReplicated: -1\n" +
                                "# position: 392, header: 6\n" +
                                "--- !!data #binary\n" +
                                "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                                "# position: 448, header: 7\n" +
                                "--- !!data #binary\n" +
                                "chronicle.lastIndexMSynced: -1\n" +
                                "...\n" +
                                "# 130572 bytes remaining\n" +
                                "--- !!meta-data #binary\n" +
                                "header: !SCQStore {\n" +
                                "  writePosition: [\n" +
                                "    400,\n" +
                                "    1717986918400\n" +
                                "  ],\n" +
                                "  indexing: !SCQSIndexing {\n" +
                                "    indexCount: 8,\n" +
                                "    indexSpacing: 1,\n" +
                                "    index2Index: 200,\n" +
                                "    lastIndex: 1\n" +
                                "  },\n" +
                                "  dataFormat: 1\n" +
                                "}\n" +
                                "# position: 200, header: -1\n" +
                                "--- !!meta-data #binary\n" +
                                "index2index: [\n" +
                                "  # length: 8, used: 1\n" +
                                "  304,\n" +
                                "  0, 0, 0, 0, 0, 0, 0\n" +
                                "]\n" +
                                "# position: 304, header: -1\n" +
                                "--- !!meta-data #binary\n" +
                                "index: [\n" +
                                "  # length: 8, used: 1\n" +
                                "  400,\n" +
                                "  0, 0, 0, 0, 0, 0, 0\n" +
                                "]\n" +
                                "# position: 400, header: 0\n" +
                                "--- !!data #binary\n" +
                                "say: Day 1 data\n" +
                                "...\n" +
                                "# 130648 bytes remaining\n",
                        queue.dump());

                // two days pass
                timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(2));

                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("say").text("Day 3 data");
                }

                assertEquals("" +
                                "--- !!meta-data #binary\n" +
                                "header: !STStore {\n" +
                                "  wireType: !WireType BINARY_LIGHT,\n" +
                                "  metadata: !SCQMeta {\n" +
                                "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                                "    sourceId: 0\n" +
                                "  }\n" +
                                "}\n" +
                                "# position: 152, header: 0\n" +
                                "--- !!data #binary\n" +
                                "listing.highestCycle: 2\n" +
                                "# position: 192, header: 1\n" +
                                "--- !!data #binary\n" +
                                "listing.lowestCycle: 0\n" +
                                "# position: 232, header: 2\n" +
                                "--- !!data #binary\n" +
                                "listing.modCount: 8\n" +
                                "# position: 264, header: 3\n" +
                                "--- !!data #binary\n" +
                                "chronicle.write.lock: -9223372036854775808\n" +
                                "# position: 304, header: 4\n" +
                                "--- !!data #binary\n" +
                                "chronicle.append.lock: -9223372036854775808\n" +
                                "# position: 344, header: 5\n" +
                                "--- !!data #binary\n" +
                                "chronicle.lastIndexReplicated: -1\n" +
                                "# position: 392, header: 6\n" +
                                "--- !!data #binary\n" +
                                "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                                "# position: 448, header: 7\n" +
                                "--- !!data #binary\n" +
                                "chronicle.lastIndexMSynced: -1\n" +
                                "...\n" +
                                "# 130572 bytes remaining\n" +
                                "--- !!meta-data #binary\n" +
                                "header: !SCQStore {\n" +
                                "  writePosition: [\n" +
                                "    400,\n" +
                                "    1717986918400\n" +
                                "  ],\n" +
                                "  indexing: !SCQSIndexing {\n" +
                                "    indexCount: 8,\n" +
                                "    indexSpacing: 1,\n" +
                                "    index2Index: 200,\n" +
                                "    lastIndex: 1\n" +
                                "  },\n" +
                                "  dataFormat: 1\n" +
                                "}\n" +
                                "# position: 200, header: -1\n" +
                                "--- !!meta-data #binary\n" +
                                "index2index: [\n" +
                                "  # length: 8, used: 1\n" +
                                "  304,\n" +
                                "  0, 0, 0, 0, 0, 0, 0\n" +
                                "]\n" +
                                "# position: 304, header: -1\n" +
                                "--- !!meta-data #binary\n" +
                                "index: [\n" +
                                "  # length: 8, used: 1\n" +
                                "  400,\n" +
                                "  0, 0, 0, 0, 0, 0, 0\n" +
                                "]\n" +
                                "# position: 400, header: 0\n" +
                                "--- !!data #binary\n" +
                                "say: Day 1 data\n" +
                                "# position: 420, header: 0 EOF\n" +
                                "--- !!not-ready-meta-data #binary\n" +
                                "...\n" +
                                "# 130648 bytes remaining\n" +
                                "--- !!meta-data #binary\n" +
                                "header: !SCQStore {\n" +
                                "  writePosition: [\n" +
                                "    400,\n" +
                                "    1717986918400\n" +
                                "  ],\n" +
                                "  indexing: !SCQSIndexing {\n" +
                                "    indexCount: 8,\n" +
                                "    indexSpacing: 1,\n" +
                                "    index2Index: 200,\n" +
                                "    lastIndex: 1\n" +
                                "  },\n" +
                                "  dataFormat: 1\n" +
                                "}\n" +
                                "# position: 200, header: -1\n" +
                                "--- !!meta-data #binary\n" +
                                "index2index: [\n" +
                                "  # length: 8, used: 1\n" +
                                "  304,\n" +
                                "  0, 0, 0, 0, 0, 0, 0\n" +
                                "]\n" +
                                "# position: 304, header: -1\n" +
                                "--- !!meta-data #binary\n" +
                                "index: [\n" +
                                "  # length: 8, used: 1\n" +
                                "  400,\n" +
                                "  0, 0, 0, 0, 0, 0, 0\n" +
                                "]\n" +
                                "# position: 400, header: 0\n" +
                                "--- !!data #binary\n" +
                                "say: Day 3 data\n" +
                                "...\n" +
                                "# 130648 bytes remaining\n",
                        queue.dump());
                Assert.assertEquals(2, (int) es.submit(observer).get());

                // System.out.println(queue.dump());
                assertEquals(2, observer.documentsRead);
            }
        } finally {
            es.shutdown();
            es.awaitTermination(1, TimeUnit.SECONDS);
        }
        finishedNormally = true;
    }

    private static class ParallelQueueObserver implements Callable<Integer> {

        @NotNull
        private final ExcerptTailer tailer;
        private final StringBuilder sb = new StringBuilder();
        volatile int documentsRead;

        ParallelQueueObserver(@NotNull ChronicleQueue queue) {
            documentsRead = 0;
            tailer = queue.createTailer();
        }

        @Override
        public synchronized Integer call() {
            System.out.println("index=" + Long.toHexString(tailer.index()));

            try (final DocumentContext dc = tailer.readingDocument()) {
                System.out.println("... index=" + Long.toHexString(tailer.index()));

                if (!dc.isPresent())
                    return -2;

                sb.setLength(0);
                dc.wire().read("say").text(sb);

                if (sb.length() == 0) {
                    return -1;
                }
            }
            System.out.println("+++ index=" + Long.toHexString(tailer.index()));
            return ++documentsRead;
        }
    }
}
