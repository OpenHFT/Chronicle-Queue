/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_SECONDLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class StoreAppenderInternalWriteBytesTest extends QueueTestCommon {

    private static final int MESSAGES_TO_WRITE = 200;

    @BeforeEach
    public void check64bit() {
        assumeTrue(Jvm.is64bit(), "requires 64-bit JVM");
    }

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void internalWriteBytesShouldBeIdempotentUnderConcurrentUpdates() throws InterruptedException {
        int compared = testInternalWriteBytes(5, true);
        assertEquals(MESSAGES_TO_WRITE, compared, "internalWriteBytes: compared messages (concurrent)");
    }

    @Test
    public void internalWriteBytesShouldBeIdempotent() throws InterruptedException {
        int compared = testInternalWriteBytes(5, false);
        assertEquals(MESSAGES_TO_WRITE, compared, "internalWriteBytes: compared messages (single thread)");
    }

    private int testInternalWriteBytes(int numCopiers, boolean concurrent) throws InterruptedException {
        final Path sourceDir = IOTools.createTempDirectory("sourceQueue");
        final Path destinationDir = IOTools.createTempDirectory("destinationQueue");

        populateSourceQueue(sourceDir);

        copySourceToDestination(numCopiers, concurrent, sourceDir, destinationDir);

        int compared = assertQueueContentsAreTheSame(sourceDir, destinationDir);

        IOTools.deleteDirWithFiles(sourceDir.toFile());
        IOTools.deleteDirWithFiles(destinationDir.toFile());
        return compared;
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
            es.shutdown();
            assertTrue(es.awaitTermination(30, TimeUnit.SECONDS), "copier threads stopped");
        }
    }

    private int assertQueueContentsAreTheSame(Path sourceDir, Path destinationDir) {
        try (final ChronicleQueue sourceQueue = createQueue(sourceDir, null);
             final ChronicleQueue destinationQueue = createQueue(destinationDir)) {
            // System.out.println(destinationQueue.dump());
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
                for (int i = 0; i < MESSAGES_TO_WRITE; i++) {
                    sourceBuffer.clear();
                    destinationBuffer.clear();
                    long sourceIndex = sourceTailer.index();
                    long destinationIndex = destinationTailer.index();
                    assert sourceTailer.readBytes(sourceBuffer) : "Source queue is shorter than expected";
                    assert destinationTailer.readBytes(destinationBuffer) : "Destination queue is shorter than expected";
                    final String s = destinationBuffer.toString();
                    assertEquals(sourceBuffer.toString(), s.replaceAll(" - .*", ""), format("Mismatch at index %s/%s was %s", Long.toHexString(sourceIndex), Long.toHexString(destinationIndex), s));
                }
            }
        }
        return MESSAGES_TO_WRITE;
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
            // LOGGER.info("Starting copier...");
            try (final ChronicleQueue sourceQueue = createQueue(sourceDir, null);
                 final ChronicleQueue destinationQueue = createQueue(destinationDir, null)) {
                try (final ExcerptTailer sourceTailer = sourceQueue.createTailer();
                     final ExcerptTailer destinationTailer = destinationQueue.createTailer();
                     final ExcerptAppender destinationAppender = destinationQueue.createAppender()) {
                    Bytes<?> buffer = Bytes.allocateElasticOnHeap(1024);
                    Bytes<?> prev = Bytes.allocateElasticOnHeap(1024);
                    long index;
                    while (true) {
                        buffer.clear();
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
                                assertEquals(Long.toHexString(index), Long.toHexString(dtIndex), "tailer index matches written index");
                        }
                        prev.clear().append(buffer);
                        try (final ChronicleQueue dq = createQueue(destinationDir, null);
                             final ExcerptAppender da = dq.createAppender()) {
                            assertNotNull(dq, "destination queue reopened");
                            assertNotNull(da, "destination appender reopened");
                        }
                    }
                }
            }
            // LOGGER.info("Copier finished");
        }
    }

    private void populateSourceQueue(Path queueDir) {
        Jvm.debug().on(getClass(), "Populating source queue...");
        try (final ChronicleQueue queue = createQueue(queueDir);
             final ExcerptAppender appender = queue.createAppender()) {
            Bytes<?> buffer = Bytes.allocateElasticOnHeap(1024);
            for (int i = 0; i < MESSAGES_TO_WRITE; i++) {
                if (i == MESSAGES_TO_WRITE / 3 || i == 2 * MESSAGES_TO_WRITE / 3) {
                    Jvm.pause(1000);
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

    private SingleChronicleQueue createQueue(Path queueDir) {
        return createQueue(queueDir, null);
    }

    @NotNull
    private SingleChronicleQueue createQueue(Path queueDir, TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder
                .binary(queueDir)
                .rollCycle(TEST4_SECONDLY)
                .timeProvider(timeProvider)
                .testBlockSize()
                .build();
    }
}
