/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.testframework.process.JavaProcessBuilder;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SCQIndexingTest {

    @Test
    void linearScanCountersShouldBeThreadSafe() throws NoSuchFieldException {
        assertEquals(AtomicInteger.class, SCQIndexing.class.getDeclaredField("linearScanCount").getType());
        assertEquals(AtomicInteger.class, SCQIndexing.class.getDeclaredField("linearScanByPositionCount").getType());
        assertEquals(AtomicLong.class, SCQIndexing.class.getDeclaredField("lastScannedIndex").getType());
    }

    @Test
    void sequenceForPositionMustNotCreateSecondaryIndexBlocks() throws Exception {
        Path tmpDir = Files.createTempDirectory("cq-iter1-t3");
        try {
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tmpDir.toFile()).testBlockSize().build();
                 ExcerptAppender appender = queue.createAppender();
                 ExcerptTailer tailer = queue.createTailer()) {

                appender.writeText("hello");
                Path storePath;
                try (Stream<Path> pathStream = Files.list(tmpDir)) {
                    storePath = pathStream
                            .filter(path -> path.toString().endsWith(".cq4"))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("No cq4 store file found"));
                }
                long sizeBefore = Files.size(storePath);
                long mtimeBefore = Files.getLastModifiedTime(storePath).toMillis();

                assertTrue(tailer.moveToIndex(queue.firstIndex()));

                long sizeAfter = Files.size(storePath);
                long mtimeAfter = Files.getLastModifiedTime(storePath).toMillis();
                assertEquals(sizeBefore, sizeAfter, "Read path must not grow the store file");
                assertEquals(mtimeBefore, mtimeAfter, "Read path must not modify store mtime");
            }
        } finally {
            IOTools.deleteDirWithFiles(tmpDir.toFile());
        }
    }

    @Test
    void sequenceForPositionMatchesLinearAndBinarySearch() throws IOException {
        Path tmpDir = Files.createTempDirectory("cq-iter1-t4");
        try {
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tmpDir.toFile()).testBlockSize().build()) {
                long[] indices = new long[200];
                try (ExcerptAppender appender = queue.createAppender()) {
                    for (int i = 0; i < indices.length; i++) {
                        appender.writeText("msg-" + i);
                        indices[i] = appender.lastIndexAppended();
                    }
                }

                try (ExcerptTailer tailer = queue.createTailer()) {
                    for (int i = 0; i < indices.length; i++) {
                        assertTrue(tailer.moveToIndex(indices[i]), "Unable to move to index " + indices[i]);
                        try (DocumentContext dc = tailer.readingDocument()) {
                            assertTrue(dc.isPresent(), "Expected document at index " + indices[i]);
                            assertEquals("msg-" + i, dc.wire().read().text());
                        }
                    }
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(tmpDir.toFile());
        }
    }

    @Test
    void tailerCanReadAllEntriesWhenIndexHasHoles() throws IOException {
        Path tmpDir = Files.createTempDirectory("cq-iter1-t5");
        try {
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tmpDir.toFile())
                    .indexSpacing(1)
                    .testBlockSize()
                    .build()) {
                int messageCount = 150;
                long[] writtenIndices = new long[messageCount];

                try (ExcerptAppender appender = queue.createAppender()) {
                    for (int i = 0; i < messageCount; i++) {
                        appender.writeText("entry-" + i);
                        writtenIndices[i] = appender.lastIndexAppended();
                    }

                    // Force an actual index hole in an already-indexed sequence slot.
                    StoreAppender storeAppender = (StoreAppender) appender;
                    SingleChronicleQueueStore store = storeAppender.store;
                    assertTrue(store != null, "Store must be initialized after writes");
                    SingleChronicleQueue singleQueue = (SingleChronicleQueue) queue;
                    long holeSequence = -1;
                    for (long writtenIndex : writtenIndices) {
                        long sequence = singleQueue.rollCycle().toSequenceNumber(writtenIndex);
                        if (sequence > 0 && store.indexing.indexable(sequence)) {
                            holeSequence = sequence;
                            break;
                        }
                    }
                    assertTrue(holeSequence >= 0, "Expected at least one indexable sequence");
                    store.indexing.setPositionForSequenceNumber(storeAppender, holeSequence, 0);
                }

                try (ExcerptTailer tailer = queue.createTailer()) {
                    for (int i = 0; i < messageCount; i++) {
                        assertTrue(tailer.moveToIndex(writtenIndices[i]), "Failed to move to index " + writtenIndices[i]);
                        try (DocumentContext dc = tailer.readingDocument()) {
                            assertTrue(dc.isPresent(), "Missing entry at position " + i);
                            assertEquals("entry-" + i, dc.wire().read().text());
                        }
                    }
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(tmpDir.toFile());
        }
    }

    @Test
    void sequenceForPositionWithInteriorHoleStillFindsCorrectAnchor() throws Exception {
        Path tmpDir = Files.createTempDirectory("cq-iter2-t4");
        try {
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tmpDir.toFile())
                    .indexSpacing(1)
                    .testBlockSize()
                    .build()) {

                int messageCount = 200;
                long[] indices = new long[messageCount];
                try (ExcerptAppender appender = queue.createAppender()) {
                    for (int i = 0; i < messageCount; i++) {
                        appender.writeText("hole-" + i);
                        indices[i] = appender.lastIndexAppended();
                    }

                    StoreAppender storeAppender = (StoreAppender) appender;
                    SingleChronicleQueueStore store = storeAppender.store;
                    assertTrue(store != null, "Store must be initialized after writes");
                    SingleChronicleQueue singleQueue = (SingleChronicleQueue) queue;
                    long holeSequence = singleQueue.rollCycle().toSequenceNumber(indices[80]);
                    store.indexing.setPositionForSequenceNumber(storeAppender, holeSequence, 0);
                }

                try (ExcerptTailer tailer = queue.createTailer()) {
                    for (int i : new int[]{60, 79, 80, 81, 120, 160, 199}) {
                        assertTrue(tailer.moveToIndex(indices[i]), "Unable to move to index " + indices[i]);
                        try (DocumentContext dc = tailer.readingDocument()) {
                            assertTrue(dc.isPresent(), "Expected document at index " + indices[i]);
                            assertEquals("hole-" + i, dc.wire().read().text());
                        }
                    }
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(tmpDir.toFile());
        }
    }

    @Test
    void sequenceForPositionMustNotCreateSecondaryIndexBlocksWithLargeQueue() throws Exception {
        Path tmpDir = Files.createTempDirectory("cq-iter2-t3");
        try {
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tmpDir.toFile())
                    .rollCycle(TestRollCycles.TEST4_DAILY)
                    .build()) {

                long[] indices = new long[100];
                try (ExcerptAppender appender = queue.createAppender()) {
                    for (int i = 0; i < indices.length; i++) {
                        appender.writeText("readonly-" + i);
                        indices[i] = appender.lastIndexAppended();
                    }
                }

                Map<Path, Long> sizesBefore = new HashMap<>();
                try (Stream<Path> listing = Files.list(tmpDir)) {
                    listing.filter(path -> path.toString().endsWith(".cq4"))
                            .forEach(path -> {
                                try {
                                    sizesBefore.put(path, Files.size(path));
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                }

                try (ExcerptTailer tailer = queue.createTailer()) {
                    for (int i = 0; i < indices.length; i++) {
                        assertTrue(tailer.moveToIndex(indices[i]), "Unable to move to index " + indices[i]);
                        try (DocumentContext dc = tailer.readingDocument()) {
                            assertTrue(dc.isPresent(), "Expected document at index " + indices[i]);
                            assertEquals("readonly-" + i, dc.wire().read().text());
                        }
                    }
                }

                try (Stream<Path> listing = Files.list(tmpDir)) {
                    listing.filter(path -> path.toString().endsWith(".cq4"))
                            .forEach(path -> {
                                try {
                                    long before = sizesBefore.getOrDefault(path, -1L);
                                    long after = Files.size(path);
                                    assertEquals(before, after,
                                            "store file " + path.getFileName() + " must not grow on read path");
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(tmpDir.toFile());
        }
    }

    @Test
    void concurrentTailersDoNotCorruptOrThrow() throws Exception {
        Path tmpDir = Files.createTempDirectory("cq-iter2-t5");
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tmpDir.toFile())
                    .rollCycle(TestRollCycles.TEST2_DAILY)
                    .build()) {

                long[] indices = new long[500];
                try (ExcerptAppender appender = queue.createAppender()) {
                    for (int i = 0; i < indices.length; i++) {
                        appender.writeText("concurrent-" + i);
                        indices[i] = appender.lastIndexAppended();
                    }
                }

                int threadCount = 4;
                int readsPerThread = indices.length / threadCount;
                CyclicBarrier barrier = new CyclicBarrier(threadCount);
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threadCount; t++) {
                    final int offset = t;
                    futures.add(executor.submit(() -> {
                        try {
                            barrier.await();
                            try (ExcerptTailer tailer = queue.createTailer()) {
                                for (int r = 0; r < readsPerThread; r++) {
                                    int entry = offset + (r * threadCount);
                                    long target = indices[entry];
                                    assertTrue(tailer.moveToIndex(target), "Unable to move to index " + target);
                                    try (DocumentContext dc = tailer.readingDocument()) {
                                        assertTrue(dc.isPresent(), "Expected document at index " + target);
                                        assertEquals("concurrent-" + entry, dc.wire().read().text());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }

                for (Future<?> future : futures) {
                    future.get(15, TimeUnit.SECONDS);
                }
            }
        } finally {
            executor.shutdownNow();
            IOTools.deleteDirWithFiles(tmpDir.toFile());
        }
    }

    @Test
    void tableStoreWriteLockSubprocessStartsWithin15Seconds() throws Exception {
        Path tempDir = IOTools.createTempDirectory("cq-iter2-t7");
        Process process = null;
        try {
            Path storePath = tempDir.resolve("test_store.cq4t");
            try (TableStore<Metadata.NoMeta> tableStore =
                         SingleTableBuilder.binary(storePath, Metadata.NoMeta.INSTANCE).build();
                 TableStoreWriteLock lock = new TableStoreWriteLock(tableStore, Pauser::balanced, 15_000L, "testLock")) {

                long startNanos = System.nanoTime();
                process = JavaProcessBuilder.create(TableStoreWriteLockTest.LockAndHoldUntilInterrupted.class)
                        .withProgramArguments(tableStore.file().getAbsolutePath(), Boolean.TRUE.toString())
                        .start();

                while (!lock.locked()) {
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    if (elapsedMs >= 15_000) {
                        fail("Locking subprocess did not acquire the lock within 15000 ms (elapsed=" + elapsedMs + " ms)");
                    }
                    Jvm.pause(25);
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Interrupted while waiting for locking subprocess");
                    }
                }

                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                assertTrue(elapsedMs < 10_000,
                        "Locking subprocess startup took " + elapsedMs
                                + " ms; TableStoreWriteLockTest timeouts may be marginal on this environment");
            }
        } finally {
            if (process != null) {
                process.destroy();
                if (!process.waitFor(3, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(3, TimeUnit.SECONDS);
                }
            }
            Jvm.pause(50);
            IOTools.deleteDirWithFiles(tempDir.toFile());
        }
    }
}
