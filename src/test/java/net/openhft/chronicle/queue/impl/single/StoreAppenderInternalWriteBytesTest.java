/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_SECONDLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class StoreAppenderInternalWriteBytesTest extends QueueTestCommon {

    private static final int MESSAGES_TO_WRITE = 200;
    private static final int REOPEN_INTERVAL = 17;
    private final SetTimeProvider timeProvider = new SetTimeProvider(Instant.ofEpochMilli(1_600_000_000_000L));

    @Before
    public void check64bit() {
        assumeTrue(Jvm.is64bit());
    }

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void internalWriteBytesShouldBeIdempotentUnderConcurrentUpdates() throws InterruptedException {
        testInternalWriteBytes(5, true);
    }

    @Test
    public void internalWriteBytesShouldBeIdempotent() throws InterruptedException {
        testInternalWriteBytes(5, false);
    }

    private void testInternalWriteBytes(int numCopiers, boolean concurrent) throws InterruptedException {
        final Path sourceDir = getTmpDir().toPath();
        final Path destinationDir = getTmpDir().toPath();

        populateSourceQueue(sourceDir);

        copySourceToDestination(numCopiers, concurrent, sourceDir, destinationDir);

        assertQueueContentsAreTheSame(sourceDir, destinationDir);

    }

    private void copySourceToDestination(int numCopiers, boolean concurrent, Path sourceDir, Path destinationDir) throws InterruptedException {
        ExecutorService es = newFixedThreadPool(concurrent ? numCopiers : 1);
        try {
            List<Future<?>> copierFutures = new ArrayList<>();
            for (int i = 0; i < numCopiers; i++) {
                copierFutures.add(es.submit(new QueueCopier(sourceDir, destinationDir, i)));
            }
            copierFutures.forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            es.shutdownNow();
            if (!es.awaitTermination(30, TimeUnit.SECONDS))
                fail("Copier threads did not stop before fixture cleanup");
        }
    }

    private void assertQueueContentsAreTheSame(Path sourceDir, Path destinationDir) {
        try (final ChronicleQueue sourceQueue = createQueue(sourceDir);
             final ChronicleQueue destinationQueue = createQueue(destinationDir)) {
//            System.out.println(destinationQueue.dump());
            /*
             * Normalise destination EOFs first
             *
             * part of the contract with {@link net.openhft.chronicle.queue.impl.single.StoreAppender.writeBytes(long, net.openhft.chronicle.bytes.BytesStore)}
             */
            try (final ExcerptAppender appender = destinationQueue.createAppender()) {
                appender.normaliseEOFs();
            }

            try (final ExcerptTailer sourceTailer = sourceQueue.createTailer();
                 final ExcerptTailer destinationTailer = destinationQueue.createTailer()) {
                Bytes<?> sourceBuffer = Bytes.allocateElasticOnHeap(1024);
                Bytes<?> destinationBuffer = Bytes.allocateElasticOnHeap(1024);
                Set<Integer> cycles = new HashSet<>();
                for (int i = 0; i < MESSAGES_TO_WRITE; i++) {
                    sourceBuffer.clear();
                    destinationBuffer.clear();
                    long sourceIndex = sourceTailer.index();
                    long destinationIndex = destinationTailer.index();
                    assert sourceTailer.readBytes(sourceBuffer) : "Source queue is shorter than expected";
                    assert destinationTailer.readBytes(destinationBuffer) : "Destination queue is shorter than expected";
                    assertEquals(sourceTailer.lastReadIndex(), destinationTailer.lastReadIndex());
                    cycles.add(TEST4_SECONDLY.toCycle(sourceTailer.lastReadIndex()));
                    final String s = destinationBuffer.toString();
                    assertEquals(format("Mismatch at index %s/%s was %s", Long.toHexString(sourceIndex), Long.toHexString(destinationIndex), s),
                            sourceBuffer.toString(), s.replaceAll(" - .*", ""));
                }
                assertEquals("Exercise three distinct roll files", 3, cycles.size());
                assertFalse("No extra source record", sourceTailer.readBytes(sourceBuffer.clear()));
                assertFalse("Repeated copies must not append extra records", destinationTailer.readBytes(destinationBuffer.clear()));
            }
        }
    }

    private class QueueCopier implements Runnable {

        private final Path sourceDir;
        private final Path destinationDir;
        private final int copyId;

        QueueCopier(Path sourceDir, Path destinationDir, int copyId) {
            this.sourceDir = sourceDir;
            this.destinationDir = destinationDir;
            this.copyId = copyId;
        }

        @Override
        public void run() {
//            LOGGER.info("Starting copier...");
            try (final ChronicleQueue sourceQueue = createQueue(sourceDir);
                 final ChronicleQueue destinationQueue = createQueue(destinationDir)) {
                try (final ExcerptTailer sourceTailer = sourceQueue.createTailer();
                     final ExcerptTailer destinationTailer = destinationQueue.createTailer();
                     final ExcerptAppender destinationAppender = destinationQueue.createAppender()) {
                    Bytes<?> buffer = Bytes.allocateElasticOnHeap(1024);
                    Bytes<?> prev = Bytes.allocateElasticOnHeap(1024);
                    long index;
                    int lastReopenedCycle = Integer.MIN_VALUE;
                    int copied = 0;
                    while (!Thread.currentThread().isInterrupted()) {
                        buffer.clear();
                        index = sourceTailer.index();
                        if (!sourceTailer.readBytes(buffer)) {
                            break;
                        }
                        index = sourceTailer.lastReadIndex();

                        if (prev.contentEquals(buffer))
                            fail("duplicate " + buffer);
                        buffer.append(" - ").append(copyId);
                        ((InternalAppender) destinationAppender).writeBytes(index, buffer);
                        try (@NotNull DocumentContext dc = destinationTailer.readingDocument()) {
                            if (!dc.isPresent()) {
                                fail("no write " + buffer);
                            }
                            final long dtIndex = destinationTailer.index();
                            if (dtIndex != index)
                                assertEquals(Long.toHexString(index), Long.toHexString(dtIndex));
                        }
                        prev.clear().append(buffer);
                        int cycle = TEST4_SECONDLY.toCycle(index);
                        copied++;
                        // Cover reopen-at-roll and periodic reopen without 1,000 mapping lifecycles.
                        if (cycle != lastReopenedCycle || copied % REOPEN_INTERVAL == 0) {
                            try (final ChronicleQueue dq = createQueue(destinationDir);
                                 final ExcerptAppender da = dq.createAppender()) {
                                assumeNotNull(dq);
                                assumeNotNull(da);
                            }
                            lastReopenedCycle = cycle;
                        }
                    }
                }
            }
//            LOGGER.info("Copier finished");
        }
    }

    private void populateSourceQueue(Path queueDir) {
        Jvm.debug().on(getClass(), "Populating source queue...");
        try (final ChronicleQueue queue = createQueue(queueDir);
             final ExcerptAppender appender = queue.createAppender()) {
            Bytes<?> buffer = Bytes.allocateElasticOnHeap(1024);
            for (int i = 0; i < MESSAGES_TO_WRITE; i++) {
                if (i == MESSAGES_TO_WRITE / 3 || i == 2 * MESSAGES_TO_WRITE / 3) {
                    timeProvider.advanceMillis(TEST4_SECONDLY.lengthInMillis());
                }
                buffer.clear();
                buffer.write(messageForIndex(i));
                appender.writeBytes(buffer);
            }
        }
        Jvm.debug().on(getClass(), "Populated source queue");
    }

    private byte[] messageForIndex(long index) {
        return format("Message %d", index).getBytes(StandardCharsets.UTF_8);
    }

    @NotNull
    private SingleChronicleQueue createQueue(Path queueDir) {
        return SingleChronicleQueueBuilder
                .binary(queueDir)
                .rollCycle(TEST4_SECONDLY)
                .timeProvider(timeProvider)
                .testBlockSize()
                .build();
    }
}
