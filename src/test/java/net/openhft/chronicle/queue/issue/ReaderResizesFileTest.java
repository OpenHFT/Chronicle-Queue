/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.issue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ReaderResizesFileTest {
    private static final File QUEUE_DIR = new File(OS.getTarget(), "ReaderResizesFileTest-" + System.nanoTime());

    @After
    public void cleanup() {
        IOTools.deleteDirWithFiles(QUEUE_DIR);
    }

    @Test
    public void testReaderResizesFile() throws IOException {
        // go for the smallest possible block size to ensure we can test resizing
        int blockSize = 1 << 10;
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(QUEUE_DIR).rollCycle(TestRollCycles.TEST4_DAILY).blockSize(blockSize).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("Hello World");
            // retrieve the actual block size used by the queue
            long actualChunkSize = appender.wire().bytes().bytesStore().capacity();

            File[] files = QUEUE_DIR.listFiles((d, n) -> n.endsWith(".cq4"));
            assertNotNull(files, "Queue directory must exist");
            assertEquals(1, files.length, "Expected exactly one cycle file");
            File firstFile = files[0];

            assertEquals(actualChunkSize, firstFile.length());

            // Trigger a potential resize by writing more data
            try (DocumentContext dc = appender.writingDocument()) {
                Bytes<?> bytes = dc.wire().bytes();
                bytes.append("More data to increase file size");
                for (int i = 0; i < blockSize; i += 8)
                    bytes.writeLong(i);
            }
            assertEquals(actualChunkSize, firstFile.length());

            try (RandomAccessFile raf = new RandomAccessFile(firstFile, "rw");
                 FileLock lockFile = raf.getChannel().lock()) {
                assertNotNull(lockFile);
                for (int i = 1; i <= 2; i++) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        assertTrue(dc.isPresent(), "Document should be present");
                    }
                    assertEquals(actualChunkSize, firstFile.length());
                }

                try (DocumentContext dc = tailer.readingDocument()) {
                    assertFalse(dc.isPresent(), "Document should not be present");
                }
            }
        }
    }

    @Test
    public void testTailerRefCountStableDuringResize() throws IOException {
        int blockSize = 1 << 12;
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(QUEUE_DIR)
                .rollCycle(TestRollCycles.TEST4_DAILY)
                .blockSize(blockSize)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {

            final int entries = blockSize / 16;
            appender.writeBytes(bytes -> {
                for (int i = 0; i < entries; i++) {
                    bytes.writeLong(i);
                }
            });

            BytesStore<?, ?> heldStore = null;
            long openRefCount = 0;
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent(), "Initial document should be present");
                Bytes<?> tailerBytes = dc.wire().bytes();
                heldStore = tailerBytes.bytesStore();
                openRefCount = heldStore.refCount();

                try (DocumentContext writing = appender.writingDocument()) {
                    Bytes<?> bytes = writing.wire().bytes();
                    for (int i = 0; i < entries; i++) {
                        bytes.writeLong(i);
                    }
                }

                assertEquals(openRefCount, heldStore.refCount(),
                        "RefCount should remain unchanged while the tailer document is held open");
            }

            assertNotNull(heldStore);
            assertTrue(heldStore.refCount() <= openRefCount,
                    "RefCount should not increase after closing the tailer document");
        }
    }

    @Test
    public void testTailerHoldingDocumentAcrossRollsDoesNotResizeOldCycle() throws IOException {
        File queuePath = new File(QUEUE_DIR, "tailer-hold");
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(0L);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder(queuePath, net.openhft.chronicle.wire.WireType.BINARY)
                .testBlockSize()
                .timeProvider(timeProvider)
                .rollCycle(TestRollCycles.TEST4_SECONDLY)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {

            final long chunkSize = queue.blockSize();
            writeSequence(appender, 32);

            try (DocumentContext held = tailer.readingDocument()) {
                assertTrue(held.isPresent(), "First document should be present");

                File initialFile = soleCycleFile(queuePath);
                long initialLength = initialFile.length();
                assertTrue(initialLength >= chunkSize, "Initial cycle should allocate at least one chunk");

                timeProvider.advanceMillis(TestRollCycles.TEST4_SECONDLY.lengthInMillis());
                writeSequence(appender, 64);

                File[] cycleFiles = cq4Files(queuePath);
                assertEquals(2, cycleFiles.length, "Expected two cycle files after rolling");
                Arrays.sort(cycleFiles);
                assertEquals(initialLength, cycleFiles[0].length(), "Old cycle should not be resized while a tailer holds a document");
                assertEquals(initialLength, cycleFiles[1].length(), "New cycle should allocate the same chunk size");
            }

            try (DocumentContext next = tailer.readingDocument()) {
                assertTrue(next.isPresent(), "Tailer should advance once the held document is closed");
                assertEquals(64, next.wire().bytes().readRemaining() / Long.BYTES, "Second roll payload must be readable");
            }
        }
    }

    @Test
    public void testZeroLengthDocumentDoesNotBlockTailer() {
        File queuePath = new File(QUEUE_DIR, "zero-length");
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder(queuePath, net.openhft.chronicle.wire.WireType.BINARY)
                .testBlockSize()
                .rollCycle(TestRollCycles.TEST4_DAILY)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer primaryTailer = queue.createTailer("primary");
             ExcerptTailer verifierTailer = queue.createTailer("verifier")) {

            appender.writeBytes(b -> {
                // intentionally empty to create a zero-length payload
            });
            appender.writeText("expected");

            try (DocumentContext dc = primaryTailer.readingDocument()) {
                assertTrue(dc.isPresent(), "Zero-length document should still produce a frame");
                assertTrue(dc.wire().bytes().readRemaining() <= 16, "Zero-length document must not expose a meaningful payload");
            }

            // use a fresh tailer to prove subsequent documents remain readable
            assertEquals("expected", verifierTailer.readText(), "Verifier should advance to the next document");
        }
    }

    private static void writeSequence(ExcerptAppender appender, int count) {
        appender.writeBytes(bytes -> {
            for (int i = 0; i < count; i++) {
                bytes.writeLong(i);
            }
        });
    }

    private static File soleCycleFile(File dir) {
        File[] files = cq4Files(dir);
        assertEquals(1, files.length, "Expected exactly one cycle file");
        return files[0];
    }

    private static File[] cq4Files(File dir) {
        File[] files = dir.listFiles((d, name) -> name.endsWith(".cq4"));
        assertNotNull(files, "Queue directory must exist");
        return files;
    }
}
