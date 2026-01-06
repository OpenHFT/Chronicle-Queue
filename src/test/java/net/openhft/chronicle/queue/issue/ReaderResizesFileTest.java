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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"deprecation", "removal"})
public class ReaderResizesFileTest {
    private static final File QUEUE_DIR = new File(OS.getTarget(), "ReaderResizesFileTest-" + System.nanoTime());

    @AfterEach
    public void cleanup() {
        IOTools.deleteDirWithFiles(QUEUE_DIR);
    }

    @Test
    @DisplayName("Reader does not resize cycle file during reads")
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
            assertNotNull(files, "Queue directory must exist after first write");
            assertEquals(1, files.length, "Queue directory should contain exactly one cycle file after first write");
            File firstFile = files[0];

            assertEquals(actualChunkSize, firstFile.length(), "Queue cycle file should have initial size equal to one chunk after first write");

            // Trigger a potential resize by writing more data
            try (DocumentContext dc = appender.writingDocument()) {
                Bytes<?> bytes = dc.wire().bytes();
                bytes.append("More data to increase file size");
                for (int i = 0; i < blockSize; i += 8)
                    bytes.writeLong(i);
            }
            assertEquals(actualChunkSize, firstFile.length(), "Queue cycle file should remain at one chunk size after writing additional data");

            try (RandomAccessFile raf = new RandomAccessFile(firstFile, "rw");
                 FileLock lockFile = raf.getChannel().lock()) {
                assertNotNull(lockFile, "File lock should be successfully acquired on queue cycle file");
                for (int i = 1; i <= 2; i++) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        assertTrue(dc.isPresent(), "Tailer should read document under lock at iteration " + i);
                    }
                    assertEquals(actualChunkSize, firstFile.length(),
                            "Queue cycle file should maintain consistent size while reading with external file lock at iteration " + i);
                }

                try (DocumentContext dc = tailer.readingDocument()) {
                    assertFalse(dc.isPresent(), "Tailer should not read an extra document after two reads");
                }
            }
        }
    }

    @Test
    @DisplayName("Tailer refCount stays stable during resize")
    public void testTailerRefCountStableDuringResize() {
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

            BytesStore<?, ?> heldStore;
            long openRefCount;
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

                assertEquals(openRefCount, heldStore.refCount(), "RefCount should remain unchanged while the tailer document is held open");
            }

            assertNotNull(heldStore, "BytesStore reference should be held for verification");
            assertTrue(heldStore.refCount() <= openRefCount, "RefCount should not increase after closing the tailer document");
        }
    }

    @Test
    @DisplayName("Held tailer document does not resize old cycle")
    public void testTailerHoldingDocumentAcrossRollsDoesNotResizeOldCycle() {
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
                assertTrue(held.isPresent(), "Tailer should read first document before rolling");

                File initialFile = soleCycleFile(queuePath);
                long initialLength = initialFile.length();
                assertTrue(initialLength >= chunkSize,
                        "Initial cycle file length " + initialLength + " should be >= chunk size " + chunkSize);

                timeProvider.advanceMillis(TestRollCycles.TEST4_SECONDLY.lengthInMillis());
                writeSequence(appender, 64);

                File[] cycleFiles = cq4Files(queuePath);
                assertEquals(2, cycleFiles.length, "Queue directory should contain two cycle files after rolling");
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
    @DisplayName("Zero-length document does not block tailer")
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
        assertEquals(1, files.length, "Queue directory should contain exactly one cycle file for tailer hold test");
        return files[0];
    }

    private static File[] cq4Files(File dir) {
        File[] files = dir.listFiles((d, name) -> name.endsWith(".cq4"));
        assertNotNull(files, "Queue directory must exist for cycle file enumeration");
        return files;
    }
}
