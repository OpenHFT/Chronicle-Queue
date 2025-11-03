/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RollCycleTest extends QueueTestCommon {

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void newRollCycleIgnored() throws InterruptedException {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider();
        ParallelQueueObserver observer = new ParallelQueueObserver(timeProvider, path.toPath());

        try (ChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(path)
                .testBlockSize()
                .rollCycle(TEST_DAILY)
                .timeProvider(timeProvider)
                .build();
             ExcerptAppender appender = queue.createAppender()) {

            Thread thread = new Thread(observer);
            thread.start();

            observer.await();

            // two days pass
            timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(2));

            appender.writeText("0");

            // allow parallel tailer to finish iteration
            for (int i = 0; i < 5_000 && observer.documentsRead != 1; i++) {
                timeProvider.advanceMicros(100);
                Thread.sleep(1);
            }

            thread.interrupt();
        }

        assertEquals(1, observer.documentsRead);
        observer.queue.close();
    }

    @Test
    public void newRollCycleIgnored2() throws InterruptedException {
        finishedNormally = false;
        File path = getTmpDir();
        Assume.assumeFalse("Ignored on hugetlbfs as byte offsets will be different due to page size", PageUtil.isHugePage(path.getAbsolutePath()));

        SetTimeProvider timeProvider = new SetTimeProvider();
        ParallelQueueObserver observer = new ParallelQueueObserver(timeProvider, path.toPath());
        try (ChronicleQueue queue0 = observer.queue) {
            assertNotNull(queue0);
            int cyclesToWrite = 3;
            Thread thread = new Thread(observer);
            try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                    .blockSize(OS.SAFE_PAGE_SIZE) // for the same result on Windows and Linux
                    .rollCycle(TEST_DAILY)
                    .timeProvider(timeProvider)
                    .build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("0");

                thread.start();

                observer.await();

                for (int i = 1; i <= cyclesToWrite; i++) {
                    // two days pass
                    timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(2));
                    appender.writeText(Integer.toString(i));
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
                                "listing.highestCycle: 6\n" +
                                "# position: 192, header: 1\n" +
                                "--- !!data #binary\n" +
                                "listing.lowestCycle: 0\n" +
                                "# position: 232, header: 2\n" +
                                "--- !!data #binary\n" +
                                "listing.modCount: 9\n" +
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
                                "\"0\"\n" +
                                "# position: 408, header: 0 EOF\n" +
                                "--- !!not-ready-meta-data #binary\n" +
                                "...\n" +
                                "# 130660 bytes remaining\n" +
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
                                "\"1\"\n" +
                                "# position: 408, header: 0 EOF\n" +
                                "--- !!not-ready-meta-data #binary\n" +
                                "...\n" +
                                "# 130660 bytes remaining\n" +
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
                                "\"2\"\n" +
                                "# position: 408, header: 0 EOF\n" +
                                "--- !!not-ready-meta-data #binary\n" +
                                "...\n" +
                                "# 130660 bytes remaining\n" +
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
                                "\"3\"\n" +
                                "...\n" +
                                "# 130660 bytes remaining\n",
                        queue.dump().replaceAll("listing.modCount: \\d+", "listing.modCount: 9"));

                // allow parallel tailer to finish iteration
                for (int i = 0; i < 5_000 && observer.documentsRead != 1 + cyclesToWrite; i++) {
                    Thread.sleep(1);
                }
            } finally {
                thread.interrupt();
            }

            assertEquals(1 + cyclesToWrite, observer.documentsRead);
        }
        finishedNormally = true;
    }

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }

    static class ParallelQueueObserver implements Runnable, StoreFileListener {
        ChronicleQueue queue;
        CountDownLatch progressLatch;
        volatile int documentsRead;

        ParallelQueueObserver(TimeProvider timeProvider, @NotNull Path path) {
            queue = SingleChronicleQueueBuilder.binary(path.toFile())
                    .testBlockSize()
                    .rollCycle(TEST_DAILY)
                    .timeProvider(timeProvider)
                    .storeFileListener(this)
                    .build();

            documentsRead = 0;
            progressLatch = new CountDownLatch(1);
        }

        @Override
        public void run() {

            ExcerptTailer tailer = queue.createTailer();

            progressLatch.countDown();

            int lastDocId = -1;
            while (!Thread.currentThread().isInterrupted()) {

                String readText = tailer.readText();
                if (readText != null) {
                    // System.out.println("Read a document " + readText);
                    documentsRead++;
                    int docId = Integer.parseInt(readText);
                    assertEquals(docId, lastDocId + 1);
                    lastDocId = docId;
                }
            }
        }

        void await() throws InterruptedException {
            progressLatch.await();
        }

        public int documentsRead() {
            return documentsRead;
        }

        @Override
        public void onAcquired(int cycle, File file) {
            // System.out.println("Acquiring " + file);
        }

        @Override
        public void onReleased(int cycle, File file) {
            // System.out.println("Releasing " + file);
        }
    }
}
